#!/usr/bin/env bash
# Tiering benchmark (design spec section 7): measures storage, re-encode throughput and
# transparent-read query times on the production-shaped tm_tag_point dataset AFTER enabling
# timeseries_tiering. Pair it with docker/scale-test.sh, which produces the untiered baseline
# from the same image, the same data and the same node configuration.
#
# Works on a COPY of the scale dataset (the original at $SRC_DATA is preserved for re-measuring;
# re-encoding deletes base rows, so it must never run against the original). The copy pins the
# node IP recorded in cluster metadata, so this reuses the scale network/IP and cannot run at the
# same time as scale-test.sh.
#
# The node configuration comes from docker/bench-node-config.sh, shared verbatim with
# scale-test.sh: the two halves are only comparable if their configuration is identical, and it
# has silently drifted before (see that file).
#
#   ./docker/tiering-bench.sh [image]
#
# Outputs: build/tiering-bench-tiered.html (query report) and a summary block on stdout that
# doc/timeseries/tiering-benchmark.md is written from.
set -o errexit -o nounset -o pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE="${1:-aster-tsdb:6.0.0}"
ROWS="${SCALE_ROWS:-20000000}"
SERIES="${SCALE_SERIES:-500}"
TABLE="${SCALE_TABLE:-tm_tag_point}"
SRC_DATA="${SCALE_DATA:-/home/common/cassandra-ts-tmtag-data}"
DATA="${TIERING_DATA:-/home/common/cassandra-ts-tmtag-tiering-data}"
HEAP="${SCALE_HEAP:-16G}"
REPORT="${TIERING_REPORT:-build/tiering-bench-tiered.html}"
PASSES="${SCALE_PASSES:-2}"
CONTAINER="cassandra-ts-scale"
RUNTIME="${CONTAINER_RUNTIME:-docker}"
NET="${SCALE_NET:-ts-scale-net}"
NET_SUBNET="${SCALE_NET_SUBNET:-172.30.0.0/16}"
NODE_IP="${SCALE_NODE_IP:-172.30.0.10}"

echo "== aster-tsdb tiering benchmark =="
echo "   image=$IMAGE rows=$ROWS series=$SERIES data(copy)=$DATA report=$REPORT"

if [ ! -d "$DATA/data" ] && [ ! -d "$DATA/scale" ]; then
    echo "-- copying scale dataset ($(du -sh "$SRC_DATA" | cut -f1)) to $DATA"
    rm -rf "$DATA"; mkdir -p "$DATA"
    /usr/bin/cp -a "$SRC_DATA/." "$DATA/"
fi
chmod -R 777 "$DATA" 2> /dev/null || true

$RUNTIME rm -f "$CONTAINER" > /dev/null 2>&1 || true
$RUNTIME network inspect "$NET" > /dev/null 2>&1 \
    || $RUNTIME network create --subnet "$NET_SUBNET" "$NET" > /dev/null

# Identical to the baseline harness by construction: same file, same expansion.
NODE_CONFIG="$(cat "$HERE/bench-node-config.sh")"

$RUNTIME run -d --name "$CONTAINER" \
    --network "$NET" --ip "$NODE_IP" \
    -e MAX_HEAP_SIZE="$HEAP" -e HEAP_NEWSIZE=4G -e GC_CHOICE="${SCALE_GC:-zgc}" \
    -v "$DATA:/opt/cassandra/data" \
    --entrypoint bash "$IMAGE" -c "$NODE_CONFIG
        exec docker-entrypoint.sh
    " > /dev/null

echo "-- waiting for CQL"
for i in $(seq 1 120); do
    if $RUNTIME exec "$CONTAINER" cqlsh -e 'SELECT release_version FROM system.local;' > /dev/null 2>&1; then
        break
    fi
    [ "$i" = 120 ] && { echo "FATAL: node did not come up"; $RUNTIME logs --tail 50 "$CONTAINER"; exit 1; }
    sleep 5
done

cql() { $RUNTIME exec "$CONTAINER" cqlsh --request-timeout=3600 -e "$1"; }

json='{"hot_window":"1h","chunk_window":"1h","interval":"7d"}'
hex=$(printf '%s' "$json" | od -A n -t x1 | tr -d ' \n')
echo "-- installing tiering policy: $json"
# gc_grace 0 lets the post-retier major compaction actually purge the re-encoder's range
# tombstones, so the storage numbers reflect real reclamation (single-node bench setting).
cql "ALTER TABLE scale.$TABLE WITH gc_grace_seconds = 0;" > /dev/null
cql "ALTER TABLE scale.$TABLE WITH extensions = {'timeseries_tiering': 0x$hex};" > /dev/null

echo "-- storage BEFORE re-encode"
$RUNTIME exec "$CONTAINER" nodetool tablestats "scale.$TABLE" | grep -E "Space used \(live\)|SSTable count" | sed 's/^/   base  /'
du -sh "$DATA" | sed 's/^/   dir   /'

echo "-- re-encoding all closed windows (nodetool retier, timed)"
T0=$(date +%s)
$RUNTIME exec "$CONTAINER" nodetool retier scale "$TABLE"
T1=$(date +%s)
RETIER_SECS=$((T1 - T0))
echo "   retier wall time: ${RETIER_SECS}s"

echo "-- tiering run stats"
cql "SELECT * FROM system_views.timeseries_tiering;" | sed 's/^/   /'

echo "-- flush + major compaction (apply range tombstones)"
$RUNTIME exec "$CONTAINER" nodetool flush scale
T0=$(date +%s)
$RUNTIME exec "$CONTAINER" nodetool compact scale "$TABLE"
T1=$(date +%s)
echo "   compact wall time: $((T1 - T0))s"

echo "-- storage AFTER re-encode + compaction"
$RUNTIME exec "$CONTAINER" nodetool tablestats "scale.$TABLE" | grep -E "Space used \(live\)|SSTable count" | sed 's/^/   base  /'
$RUNTIME exec "$CONTAINER" nodetool tablestats "scale.${TABLE}__chunks" | grep -E "Space used \(live\)|SSTable count" | sed 's/^/   chunk /'
$RUNTIME exec "$CONTAINER" nodetool flush scale "${TABLE}__chunks" > /dev/null 2>&1 || true
du -sh "$DATA" | sed 's/^/   dir   /'
cql "SELECT count(*) AS chunks FROM scale.${TABLE}__chunks;" | sed 's/^/   /'

echo "-- query pass over TIERED data (transparent reads), $PASSES pass(es)"
$RUNTIME cp docker/scale-workload.py "$CONTAINER":/tmp/scale-workload.py
mkdir -p "$(dirname "$REPORT")"
# Flag names and output plumbing must match scale-test.sh, the caller that is actually exercised:
# `query` writes its HTML report to STDOUT and takes --md-out/--json-out for the other two formats.
# There is no --report flag. Passing one made argparse exit 2, which errexit turned into a silent
# stop right after the storage numbers had printed -- so the run looked like it had produced a
# result and was simply missing its query section.
LAST=""
for p in $(seq 1 "$PASSES"); do
    echo "   pass $p/$PASSES"
    $RUNTIME exec "$CONTAINER" python3 /tmp/scale-workload.py query \
        --rows "$ROWS" --series "$SERIES" --load-secs 0 --image "$IMAGE (tiered)" \
        --table "$TABLE" \
        --md-out /tmp/tiering-report.md --json-out /tmp/tiering-results.json > "$REPORT"
    LAST=$p
done
$RUNTIME cp "$CONTAINER":/tmp/tiering-report.md "${REPORT%.html}.md"
$RUNTIME cp "$CONTAINER":/tmp/tiering-results.json "${REPORT%.html}.json"
echo "== tiering benchmark done: report=$REPORT retier=${RETIER_SECS}s =="
