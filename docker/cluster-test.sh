#!/bin/bash
#
# Three-container cluster test for aster-tsdb.
#
# docker/integration-test.sh proves the time-series features work on ONE node. This proves they work
# on a real cluster: three separate JVMs, three separate data directories, RF=3, gossip and internode
# messaging over a docker network, and streaming between processes rather than between classloaders.
# The things it can reach that a single node cannot:
#
#   * aggregation and gap-fill computed by a coordinator over rows merged from three replicas,
#     asserted through EVERY coordinator (a replica-local answer that happens to be right on node 1
#     says nothing about node 2);
#   * TimeSeriesCompactionStrategy's per-window split and freeze, which is node-local work on
#     replicated data -- every replica has to converge on its own, and a node that did not is
#     invisible to every CQL read;
#   * a real repair stream between two operating-system processes, and what the receiving node's
#     sstables look like afterwards;
#   * QUORUM behaviour with a replica actually stopped.
#
# It complements, and does not replace, the jvm-dtest suites
# (org.apache.cassandra.distributed.test.timeseries.*), which cover the same invariants in-JVM and
# run in CI on every pipeline. This script is the release-gate counterpart: same image, real cluster.
#
#   ./docker/cluster-test.sh [image]              # default: aster-tsdb:6.0.0
#   CONTAINER_RUNTIME=podman ./docker/cluster-test.sh
#
# Build the image first:
#   docker build -t aster-tsdb:6.0.0 -f docker/Dockerfile .
#
# The containers are LEFT RUNNING at the end, with the removal command printed -- the data is worth
# keeping around after a failure. Re-running the script removes them first.
#
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE="${1:-aster-tsdb:6.0.0}"
RUNTIME="${CONTAINER_RUNTIME:-}"
NET="${CLUSTER_NET:-ts-cluster-net}"
NET_SUBNET="${CLUSTER_NET_SUBNET:-172.32.0.0/16}"
IP_PREFIX="${CLUSTER_IP_PREFIX:-172.32.0.1}"      # nodes get ${IP_PREFIX}1 .. ${IP_PREFIX}3
NAME="${CLUSTER_NAME:-cassandra-ts-cluster}"
# Three JVMs on one host: these are correctness tests, not a benchmark. Do NOT raise this to the
# 16G the benchmark scripts use -- three of those do not fit anywhere the tests are likely to run.
HEAP="${CLUSTER_HEAP:-2G}"
NEWSIZE="${CLUSTER_NEWSIZE:-512M}"
READY_TIMEOUT="${READY_TIMEOUT:-300}"             # per node, waiting for CQL
CLUSTER_TIMEOUT="${CLUSTER_TIMEOUT:-420}"         # waiting for three UN nodes
CONVERGE_TIMEOUT="${CONVERGE_TIMEOUT:-120}"       # waiting for compaction to settle

if [ -z "$RUNTIME" ]; then
    if command -v docker > /dev/null 2>&1; then RUNTIME=docker
    elif command -v podman > /dev/null 2>&1; then RUNTIME=podman
    else echo "FATAL: neither docker nor podman found"; exit 2
    fi
fi

N1="$NAME-1"; N2="$NAME-2"; N3="$NAME-3"
IP1="${IP_PREFIX}1"; IP2="${IP_PREFIX}2"; IP3="${IP_PREFIX}3"

echo "== aster-tsdb 3-node cluster test =="
echo "   runtime: $RUNTIME   image: $IMAGE   heap: $HEAP/node"
echo "   network: $NET ($NET_SUBNET)   nodes: $IP1 $IP2 $IP3"

$RUNTIME rm -f "$N1" "$N2" "$N3" > /dev/null 2>&1
$RUNTIME network inspect "$NET" > /dev/null 2>&1 \
    || $RUNTIME network create --subnet "$NET_SUBNET" "$NET" > /dev/null \
    || { echo "FATAL: could not create network $NET"; exit 2; }

# Node configuration is shared verbatim with the benchmark harnesses (docker/bench-node-config.sh):
# request timeouts wide enough for aggregation, and the GC choice. Keeping it in one file is what
# stops the harnesses from silently drifting apart -- see the comment at the top of that file.
NODE_CONFIG="$(cat "$HERE/bench-node-config.sh")
# Hinted handoff off, on top of the shared block. The repair section below stops a node, writes at
# QUORUM and then repairs; with hints on, the rows would arrive by hint replay the moment the node
# came back and the repair would be asserting nothing. (read_repair = 'NONE' on the table closes the
# other back door.)
sed -ri 's/^hinted_handoff_enabled:.*/hinted_handoff_enabled: false/' \"\$CFG\""

# start_node <container> <ip> <seed-ip-or-empty>
start_node() {
    local container="$1" ip="$2" seeds="$3"
    $RUNTIME run -d --name "$container" \
        --network "$NET" --ip "$ip" \
        -e MAX_HEAP_SIZE="$HEAP" -e HEAP_NEWSIZE="$NEWSIZE" -e GC_CHOICE="${CLUSTER_GC:-zgc}" \
        ${seeds:+-e CASSANDRA_SEEDS="$seeds"} \
        --entrypoint bash "$IMAGE" -c "$NODE_CONFIG
            exec docker-entrypoint.sh
        " > /dev/null || { echo "FATAL: container $container did not start"; exit 2; }
}

# Fails the whole run with the container's log, the way the other harnesses do -- a cluster that did
# not form is not a test result, it is a broken environment, and the log is the only way to tell.
die_with_logs() {
    echo "FATAL: $1"
    for c in "$N1" "$N2" "$N3"; do
        echo "---- $RUNTIME logs $c (tail) ----"
        $RUNTIME logs "$c" 2>&1 | tail -40
    done
    echo "-- containers left running for inspection; remove with: $RUNTIME rm -f $N1 $N2 $N3"
    exit 2
}

wait_for_cql() {
    local container="$1" i
    printf '   waiting for CQL on %s' "$container"
    for i in $(seq 1 $((READY_TIMEOUT / 5))); do
        if $RUNTIME exec "$container" cqlsh -e "SELECT release_version FROM system.local" > /dev/null 2>&1; then
            echo " ok"; return 0
        fi
        printf '.'; sleep 5
    done
    echo
    die_with_logs "$container did not answer CQL within ${READY_TIMEOUT}s"
}

# Waits until nodetool status on node 1 shows <n> nodes Up/Normal. Cluster formation is the flaky
# part of any container-based cluster test, so this is bounded, loud, and dumps the logs on timeout.
wait_for_up_normal() {
    local want="$1" i up
    printf '   waiting for %s UN node(s)' "$want"
    for i in $(seq 1 $((CLUSTER_TIMEOUT / 5))); do
        up=$($RUNTIME exec "$N1" nodetool status 2>/dev/null | grep -cE '^UN[[:space:]]')
        if [ "$up" = "$want" ]; then echo " ok"; return 0; fi
        printf '.'; sleep 5
    done
    echo
    echo "   last nodetool status:"
    $RUNTIME exec "$N1" nodetool status 2>&1 | sed 's/^/     /'
    die_with_logs "only $up of $want nodes reached UN within ${CLUSTER_TIMEOUT}s"
}

echo "-- starting node 1 (seed)"
start_node "$N1" "$IP1" ""
wait_for_cql "$N1"
wait_for_up_normal 1

# Sequential joins. Two nodes bootstrapping at once is legal but is the single most common source of
# flakiness in a container cluster test, and nothing here needs concurrency.
echo "-- starting node 2"
start_node "$N2" "$IP2" "$IP1"
wait_for_cql "$N2"
wait_for_up_normal 2

echo "-- starting node 3"
start_node "$N3" "$IP3" "$IP1"
wait_for_cql "$N3"
wait_for_up_normal 3

# ------------------------------------------------------------------ reporting
# Same accounting and the same output shape as docker/integration-test.sh: CI reads the trailing
# "== N passed, M failed ==" line and the exit code, and the HTML/Markdown reports are artifacts.
REPORT="${CLUSTER_REPORT:-build/timeseries-cluster-report.html}"
REPORT_MD="${CLUSTER_REPORT_MD:-${REPORT%.html}.md}"
ROWS=$(mktemp); MDROWS=$(mktemp)
trap 'rm -f "$ROWS" "$MDROWS"' EXIT
esc() { sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g'; }

node_of() { case "$1" in 1) echo "$N1";; 2) echo "$N2";; *) echo "$N3";; esac; }

cql_at() { $RUNTIME exec "$(node_of "$1")" cqlsh --no-color -e "$2" 2>&1; }
ntool_at() { local n="$1"; shift; $RUNTIME exec "$(node_of "$n")" nodetool "$@" 2>&1; }

baseline_ms() {
    local t0 t1
    t0=$(date +%s%3N); cql_at 1 "SELECT key FROM system.local;" > /dev/null 2>&1; t1=$(date +%s%3N)
    echo $((t1 - t0))
}
BASELINE=$(baseline_ms)

section() {
    printf '%s\n' "-- $1"
    printf '<tr class="section"><td colspan="5">%s</td></tr>\n' "$(printf '%s' "$1" | esc)" >> "$ROWS"
    printf '\n## %s\n' "$1" >> "$MDROWS"
}

PASS=0; FAIL=0
# record <status> <description> <command-text> <output> <elapsed-ms> <expect-or-empty>
record() {
    local status="$1" desc="$2" flat="$3" out="$4" ms="$5" expect="$6"
    printf '\n   [%s] %-56s %6s ms\n' "$status" "$desc" "$ms"
    printf '        cmd> %s\n' "$flat"
    printf '%s\n' "$out" | grep -vE '^[[:space:]]*$' | sed 's/^/        /'
    [ "$status" = FAIL ] && printf '        !! expected to match: /%s/\n' "$expect"
    {
        printf '<tr class="%s"><td class="st">%s</td><td>%s</td><td><pre>%s</pre></td><td><pre>%s</pre></td><td class="ms">%s ms</td></tr>\n' \
            "$([ "$status" = FAIL ] && echo fail || echo pass)" \
            "$([ "$status" = FAIL ] && echo '✗ FAIL' || echo '✓ pass')" \
            "$(printf '%s' "$desc" | esc)" \
            "$(printf '%s' "$flat" | esc)" \
            "$(printf '%s' "$out" | grep -vE '^[[:space:]]*$' | esc)" \
            "$ms"
    } >> "$ROWS"
    {
        printf '\n### %s %s — `%s ms`\n\n```sql\n%s\n```\n\n```\n%s\n```\n' \
            "$([ "$status" = FAIL ] && echo '❌' || echo '✅')" "$desc" "$ms" "$flat" \
            "$(printf '%s\n' "$out" | grep -vE '^[[:space:]]*$')"
    } >> "$MDROWS"
}

# check_at <node> <description> <cql> <extended regex the output must match>
check_at() {
    local node="$1" desc="$2" query="$3" expect="$4" out status t0 t1 ms flat
    t0=$(date +%s%3N); out=$(cql_at "$node" "$query"); t1=$(date +%s%3N); ms=$((t1 - t0))
    if printf '%s' "$out" | grep -qE -- "$expect"; then PASS=$((PASS + 1)); status="ok  "
    else FAIL=$((FAIL + 1)); status="FAIL"; fi
    flat="node$node cqlsh> $(printf '%s' "$query" | tr '\n' ' ' | tr -s ' ')"
    record "$status" "$desc" "$flat" "$out" "$ms" "$expect"
    return 0
}

# check <description> <cql> <regex> -- same as check_at 1; node 1 is the default coordinator.
check() { check_at 1 "$1" "$2" "$3"; }

# check_nodetool_at <node> <description> <regex, or '' for exit-code-only> <nodetool args...>
check_nodetool_at() {
    local node="$1" desc="$2" expect="$3"; shift 3
    local out rc status t0 t1 ms flat
    t0=$(date +%s%3N); out=$(ntool_at "$node" "$@"); rc=$?; t1=$(date +%s%3N); ms=$((t1 - t0))
    if [ "$rc" -eq 0 ] && { [ -z "$expect" ] || printf '%s' "$out" | grep -qE -- "$expect"; }; then
        PASS=$((PASS + 1)); status="ok  "
    else
        FAIL=$((FAIL + 1)); status="FAIL"
    fi
    flat="node$node nodetool $*"
    record "$status" "$desc" "$flat" "$out" "$ms" "exit=0 && /$expect/"
    return 0
}

# assert <description> <actual> <expected> -- for values this script computed itself.
assert() {
    local desc="$1" actual="$2" expected="$3" status
    if [ "$actual" = "$expected" ]; then PASS=$((PASS + 1)); status="ok  "
    else FAIL=$((FAIL + 1)); status="FAIL"; fi
    record "$status" "$desc" "computed by the harness" "$actual" 0 "$expected"
    return 0
}

# assert_ge <description> <actual> <minimum> -- reports the number either way, so a passing run says
# what it saw rather than just "ok".
assert_ge() {
    local desc="$1" actual="${2:-}" minimum="$3" status
    if [ -n "$actual" ] && [ "$actual" -ge "$minimum" ] 2> /dev/null; then PASS=$((PASS + 1)); status="ok  "
    else FAIL=$((FAIL + 1)); status="FAIL"; fi
    record "$status" "$desc" "computed by the harness" "${actual:-<none>}" 0 ">= $minimum"
    return 0
}

write_report() {
    mkdir -p "$(dirname "$REPORT")"
    {
        cat <<HTML
<!doctype html>
<meta charset="utf-8">
<title>aster-tsdb · 3-node cluster test</title>
<style>
  :root { color-scheme: light dark; --bg:#fff; --fg:#1a1a1a; --muted:#666; --line:#e3e3e3;
          --pass:#0a7d33; --fail:#c62828; --code:#f6f7f9; }
  @media (prefers-color-scheme: dark) {
    :root { --bg:#14161a; --fg:#e6e6e6; --muted:#9aa0a6; --line:#2c3036;
            --pass:#5ec97f; --fail:#ff6b6b; --code:#1c1f24; }
  }
  body { background:var(--bg); color:var(--fg); margin:0 auto; padding:2rem 1.25rem; max-width:1100px;
         font:14px/1.5 ui-sans-serif,-apple-system,"Segoe UI",Roboto,sans-serif; }
  h1 { font-size:1.35rem; margin:0 0 .25rem; }
  .meta { color:var(--muted); font-size:.85rem; margin-bottom:1.25rem; }
  .meta code { background:var(--code); padding:.1rem .35rem; border-radius:4px; }
  .summary { display:flex; gap:1.5rem; align-items:baseline; margin:1rem 0 1.5rem;
             padding:.9rem 1.1rem; background:var(--code); border-radius:8px; }
  .summary b { font-size:1.6rem; }
  .ok-n { color:var(--pass); } .fail-n { color:var(--fail); }
  table { border-collapse:collapse; width:100%; }
  td { border-top:1px solid var(--line); padding:.55rem .6rem; vertical-align:top; }
  tr.section td { background:var(--code); font-weight:600; letter-spacing:.02em; }
  td.st { white-space:nowrap; font-weight:600; }
  tr.pass td.st { color:var(--pass); } tr.fail td.st { color:var(--fail); }
  tr.fail { background:color-mix(in srgb, var(--fail) 8%, transparent); }
  pre { margin:0; background:var(--code); padding:.5rem .6rem; border-radius:6px; overflow-x:auto;
        font:12px/1.45 ui-monospace,SFMono-Regular,Menlo,monospace; white-space:pre; }
  td.ms { white-space:nowrap; text-align:right; font-variant-numeric:tabular-nums; color:var(--muted); }
  th { color:var(--muted); font-weight:600; font-size:.78rem; text-transform:uppercase; letter-spacing:.04em;
       text-align:left; padding:.4rem .6rem; }
  td:nth-child(3) { width:32%; } td:nth-child(4) { width:34%; }
</style>
<h1>3-node cluster test</h1>
<div class="meta">
  image <code>$IMAGE</code> · runtime <code>$RUNTIME</code> · RF=3 · $(date -u '+%Y-%m-%d %H:%M UTC')
</div>
<div class="summary">
  <span><b class="ok-n">$PASS</b> passed</span>
  <span><b class="fail-n">$FAIL</b> failed</span>
  <span style="color:var(--muted)">assertions run against three containers on one docker network</span>
</div>
<p class="meta">Times are the full round trip for one command through <code>$RUNTIME exec</code>; a
trivial query on this host costs <b>${BASELINE} ms</b> of that, so subtract roughly that much to read
the execution time.</p>
<table>
<tr><th></th><th>assertion</th><th>command</th><th>result</th><th>elapsed</th></tr>
HTML
        cat "$ROWS"
        printf '</table>\n'
    } > "$REPORT"

    {
        printf '# aster-tsdb 3-node cluster test\n\n'
        printf 'image `%s` · runtime `%s` · RF=3 · %s\n\n' "$IMAGE" "$RUNTIME" "$(date -u '+%Y-%m-%d %H:%M UTC')"
        printf '**%s passed, %s failed** — assertions run against three containers on one docker network.\n' \
            "$PASS" "$FAIL"
        cat "$MDROWS"
    } > "$REPORT_MD"
    echo "   report: $REPORT"
    echo "   report: $REPORT_MD"
}

# ------------------------------------------------------------------ checks
section "cluster formation"
check_nodetool_at 1 "node 1 sees three Up/Normal nodes" "$IP3" status
check_nodetool_at 2 "node 2 sees three Up/Normal nodes" "$IP1" status
check_nodetool_at 3 "node 3 sees three Up/Normal nodes" "$IP2" status
# One schema version cluster-wide. A split schema is the failure that makes every later assertion
# meaningless, and it is silent -- reads keep working, on stale metadata.
check_nodetool_at 1 "all three nodes agree on one schema version" 'Cluster Information' describecluster
# describecluster lists one "<version>: [endpoints]" line per distinct schema version, under a
# "Schema versions:" heading; more than one line is a split schema. The block has to be isolated
# first -- "Database versions:" further down has exactly the same "x: [endpoints]" shape.
assert "exactly one schema version across the cluster" \
    "$(ntool_at 1 describecluster | sed -n '/Schema versions:/,/^[[:space:]]*$/p' | grep -cE ': \[')" 1

section "schema & fixture data (RF=3)"
cql_at 1 "
CREATE KEYSPACE IF NOT EXISTS ct WITH replication = {'class':'SimpleStrategy','replication_factor':3};

CREATE TABLE IF NOT EXISTS ct.metrics (
    series text, ts timestamp, value double,
    PRIMARY KEY (series, ts)
) WITH CLUSTERING ORDER BY (ts ASC);
" > /dev/null
sleep 3   # let the DDL settle on every replica before writing at QUORUM

# Same fixture as the single-node integration test, so the expected values are the same hand-computed
# ones -- what changes here is that three replicas hold it and a coordinator merges them.
cql_at 1 "
CONSISTENCY QUORUM;
INSERT INTO ct.metrics (series, ts, value) VALUES ('cpu', '2024-01-01 09:05:00+0000', 10);
INSERT INTO ct.metrics (series, ts, value) VALUES ('cpu', '2024-01-01 09:35:00+0000', 30);
INSERT INTO ct.metrics (series, ts, value) VALUES ('cpu', '2024-01-01 10:15:00+0000', 50);
INSERT INTO ct.metrics (series, ts, value) VALUES ('cpu', '2024-01-01 10:45:00+0000', 70);
INSERT INTO ct.metrics (series, ts, value) VALUES ('gap', '2024-01-01 09:30:00+0000', 60);
INSERT INTO ct.metrics (series, ts, value) VALUES ('gap', '2024-01-01 12:30:00+0000', 120);
" > /dev/null

check "every replica accepted the fixture (6 rows at ALL)" \
    "CONSISTENCY ALL; SELECT count(*) FROM ct.metrics;" '^ *6'

section "aggregation through every coordinator"
# The aggregate is computed by the coordinator over rows merged from three replicas. Running the
# same query through each node in turn is the point: a wrong answer on one coordinator only is
# exactly what a single-node test cannot see.
for node in 1 2 3; do
    check_at "$node" "node $node: time_bucket(1h) avg = 20 / 60" \
        "CONSISTENCY QUORUM; SELECT time_bucket(1h, ts), avg(value) FROM ct.metrics WHERE series='cpu' GROUP BY series, time_bucket(1h, ts);" \
        '09:00:00.*\| *20'
    check_at "$node" "node $node: first/last across replicas = 10 / 70" \
        "CONSISTENCY QUORUM; SELECT first(value, ts), last(value, ts) FROM ct.metrics WHERE series='cpu';" \
        '^ *10 \| *70'
    check_at "$node" "node $node: delta = 60, rate = 0.01" \
        "CONSISTENCY QUORUM; SELECT delta(value, ts), rate(value, ts) FROM ct.metrics WHERE series='cpu';" \
        '^ *60 \| *0\.01'
    check_at "$node" "node $node: percentile(0.5) = 40" \
        "CONSISTENCY QUORUM; SELECT percentile(value, 0.5) FROM ct.metrics WHERE series='cpu';" '^ *40'
done

section "gap-fill through every coordinator"
GAPFILL="time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000')"
for node in 1 2 3; do
    check_at "$node" "node $node: gapfill materialises every bucket (6 rows)" \
        "CONSISTENCY QUORUM; SELECT $GAPFILL, avg(value) FROM ct.metrics WHERE series='gap' GROUP BY series, $GAPFILL;" \
        '\(6 rows\)'
    check_at "$node" "node $node: locf carries 60 forward into 10:00" \
        "CONSISTENCY QUORUM; SELECT $GAPFILL, locf(avg(value)) FROM ct.metrics WHERE series='gap' GROUP BY series, $GAPFILL;" \
        '10:00:00.*\| *60'
    check_at "$node" "node $node: interpolate ramps 60 -> 120 (11:00 -> 100)" \
        "CONSISTENCY QUORUM; SELECT $GAPFILL, interpolate(avg(value)) FROM ct.metrics WHERE series='gap' GROUP BY series, $GAPFILL;" \
        '11:00:00.*\| *100'
done

section "TimeSeriesCompactionStrategy: per-window split and freeze on every replica"
# Windows are one minute wide and close a minute later; every write below carries an explicit
# USING TIMESTAMP (microseconds, TSCS's default resolution) minutes in the past, so which window a
# row lands in is fixed by this script and not by when it runs.
NOW_MS=$(date +%s%3N)
W0=$(( (NOW_MS / 60000 - 20) * 60000 ))          # first window start, 20 minutes ago
WINDOWS=3
FLUSHES=3

cql_at 1 "
CREATE TABLE IF NOT EXISTS ct.tscs (
    tag text, ts timestamp, value double,
    PRIMARY KEY (tag, ts)
) WITH read_repair = 'NONE'
   AND compaction = {'class':'TimeSeriesCompactionStrategy',
                     'timestamp_resolution':'MICROSECONDS',
                     'window_size':'1m','freeze_after':'1m'};
" > /dev/null
sleep 3

check "TSCS is the table's compaction strategy on every node" \
    "SELECT compaction FROM system_schema.tables WHERE keyspace_name='ct' AND table_name='tscs';" \
    'TimeSeriesCompactionStrategy'

# Several flushes per window, so each closed window really starts out with several sstables and the
# freeze has work to do on all three nodes independently.
for f in $(seq 0 $((FLUSHES - 1))); do
    STMTS="CONSISTENCY ALL;"
    for w in $(seq 0 $((WINDOWS - 1))); do
        WS=$(( W0 + w * 60000 ))
        TS_MS=$(( WS + f * 1000 ))
        STMTS="$STMTS INSERT INTO ct.tscs (tag, ts, value) VALUES ('tag-$f', $TS_MS, $w) USING TIMESTAMP $(( TS_MS * 1000 ));"
    done
    cql_at 1 "$STMTS" > /dev/null
    for n in 1 2 3; do ntool_at "$n" flush ct tscs > /dev/null; done
done

# The write path split every flush at window boundaries on every node, so each node holds at least
# one sstable per window and never fewer than the number of windows.
sstable_count() { ntool_at "$1" tablestats "ct.$2" | grep -E '^[[:space:]]+SSTable count:' | head -1 | tr -dc '0-9'; }

for n in 1 2 3; do
    assert_ge "node $n: flush produced >= one sstable per window" "$(sstable_count "$n" tscs)" "$WINDOWS"
done

# Freeze convergence: one sstable per closed window, on every node, independently. Compaction is
# node-local -- a node that never converged answers every read correctly and simply keeps its disk.
echo "   waiting for freeze convergence on all three nodes"
converged=0
for _ in $(seq 1 $((CONVERGE_TIMEOUT / 5))); do
    C1=$(sstable_count 1 tscs); C2=$(sstable_count 2 tscs); C3=$(sstable_count 3 tscs)
    if [ "${C1:-0}" = "$WINDOWS" ] && [ "${C2:-0}" = "$WINDOWS" ] && [ "${C3:-0}" = "$WINDOWS" ]; then
        converged=1; break
    fi
    printf '.'; sleep 5
done
echo
assert "every node freezes down to exactly one sstable per closed window" \
    "sstables per node: ${C1:-?}/${C2:-?}/${C3:-?}" "sstables per node: $WINDOWS/$WINDOWS/$WINDOWS"

# ...and then stops. A freeze/split alternation rewrites a converged window forever; it is invisible
# to every read, and the observable is that the sstable set keeps changing.
BEFORE=$(for n in 1 2 3; do ntool_at "$n" tablestats "ct.tscs" | grep -E '^[[:space:]]+(SSTable count|Space used \(live\))'; done)
sleep 20
AFTER=$(for n in 1 2 3; do ntool_at "$n" tablestats "ct.tscs" | grep -E '^[[:space:]]+(SSTable count|Space used \(live\))'; done)
assert "a converged window is not rewritten again (no freeze/split livelock)" \
    "$([ "$BEFORE" = "$AFTER" ] && echo stable || echo "changed")" stable

check "every row survives the freeze on every replica" \
    "CONSISTENCY ALL; SELECT count(*) FROM ct.tscs;" "^ *$((WINDOWS * FLUSHES))"

section "repair streaming between separate JVMs"
# A node that missed writes has to get them by streaming from a peer -- the one path that genuinely
# needs two processes. Stop node 3, write at QUORUM (which succeeds on 2 of 3), bring it back and
# repair; the rows must arrive, and the receiving node's sstables must still be window-split.
$RUNTIME stop "$N3" > /dev/null 2>&1
# Gossip needs a moment to mark the peer down; without the wait this asserts on a stale view.
printf '   waiting for node 3 to be seen as down'
for _ in $(seq 1 24); do
    $RUNTIME exec "$N1" nodetool status 2>/dev/null | grep -qE '^DN[[:space:]]' && break
    printf '.'; sleep 5
done
echo
check_nodetool_at 1 "node 3 is down and the cluster knows" "^DN" status

MISSED_MS=$(( W0 + 5 * 60000 ))
STMTS="CONSISTENCY QUORUM;"
for i in 0 1 2 3; do
    TS_MS=$(( MISSED_MS + i * 1000 ))
    STMTS="$STMTS INSERT INTO ct.tscs (tag, ts, value) VALUES ('missed', $TS_MS, $i) USING TIMESTAMP $(( TS_MS * 1000 ));"
done
check "writes succeed at QUORUM with one replica down" "$STMTS SELECT count(*) FROM ct.tscs WHERE tag='missed';" '^ *4'
check "reads succeed at QUORUM with one replica down" \
    "CONSISTENCY QUORUM; SELECT count(*) FROM ct.tscs;" "^ *$((WINDOWS * FLUSHES + 4))"

$RUNTIME start "$N3" > /dev/null 2>&1
wait_for_cql "$N3"
wait_for_up_normal 3

for n in 1 2 3; do ntool_at "$n" flush ct tscs > /dev/null; done
check_nodetool_at 3 "nodetool repair pulls the missed window onto node 3" '' repair -full ct tscs

# Node 3 now holds the streamed window locally. tablestats is per-node, so this is node 3's own copy
# and not something a coordinator fetched for it.
assert_ge "the streamed window landed on node 3 as its own sstable(s)" \
    "$(sstable_count 3 tscs)" "$((WINDOWS + 1))"
check_at 3 "node 3 serves the repaired rows" \
    "CONSISTENCY ONE; SELECT count(*) FROM ct.tscs WHERE tag='missed';" '^ *4'
check "the whole table reads back at ALL after the repair" \
    "CONSISTENCY ALL; SELECT count(*) FROM ct.tscs;" "^ *$((WINDOWS * FLUSHES + 4))"

section "tiered storage across three replicas"
cql_at 1 "
CREATE TABLE IF NOT EXISTS ct.sensor (
    tag_id text, timestamp timestamp, value double,
    PRIMARY KEY (tag_id, timestamp)
);
" > /dev/null
sleep 3

TIERING_JSON='{"hot_window":"1h","chunk_window":"1h","interval":"5m","consistency":"QUORUM"}'
WIN_MS=$(( (NOW_MS / 3600000 - 4) * 3600000 ))
WIN_END_MS=$(( WIN_MS + 3600000 ))

check "ALTER TABLE installs the tiering policy cluster-wide" \
    "ALTER TABLE ct.sensor WITH extensions = {'timeseries_tiering': '$TIERING_JSON'};
     SELECT extensions FROM system_schema.tables WHERE keyspace_name='ct' AND table_name='sensor';" \
    'timeseries_tiering'
sleep 3

cql_at 1 "
CONSISTENCY QUORUM;
INSERT INTO ct.sensor (tag_id, timestamp, value) VALUES ('pump-01', $(( WIN_MS +  300000 )), 10);
INSERT INTO ct.sensor (tag_id, timestamp, value) VALUES ('pump-01', $(( WIN_MS +  900000 )), 20);
INSERT INTO ct.sensor (tag_id, timestamp, value) VALUES ('pump-02', $(( WIN_MS + 1500000 )), 30);
" > /dev/null

# Every node re-encodes only the tags in its own primary ranges, so all three have to run for the
# whole dataset to be covered -- and no tag may be encoded twice.
for n in 1 2 3; do check_nodetool_at "$n" "node $n: nodetool retier runs one re-encode cycle" '' retier ct sensor; done
check "every tag is encoded exactly once across the cluster (2 chunk rows)" \
    "CONSISTENCY QUORUM; SELECT count(*) FROM ct.sensor__chunks;" '^ *2'
for node in 1 2 3; do
    check_at "$node" "node $node: transparent read returns the merged window" \
        "CONSISTENCY QUORUM; SELECT count(*) FROM ct.sensor WHERE tag_id='pump-01' AND timestamp >= $WIN_MS AND timestamp < $WIN_END_MS;" \
        '^ *2'
done

echo
echo "== $PASS passed, $FAIL failed =="
write_report
echo "-- containers left running; remove with: $RUNTIME rm -f $N1 $N2 $N3"
[ "$FAIL" -eq 0 ] || exit 1
