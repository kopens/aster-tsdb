#!/bin/bash
#
# Scale test (UNTIERED BASELINE): load the production-shaped tm_tag_point table into a
# container-hosted node and measure how long the time-series CQL functions take on it. Produces
# an HTML report with the measured client-observed execution time of every query.
#
#   ./docker/scale-test.sh [image]
#     SCALE_ROWS=20000000       total rows to load
#     SCALE_SERIES=500          tags/partitions (rows per tag = SCALE_ROWS / SCALE_SERIES)
#     SCALE_LOADERS=12          loader processes inside the container
#     SCALE_DATA=/home/common/cassandra-ts-tmtag-data   host dir for the node's data files
#     SCALE_HEAP=16G
#     SCALE_REPORT=build/timeseries-scale-report.html
#
# The node configuration lives in docker/bench-node-config.sh and is shared verbatim with
# docker/tiering-bench.sh -- the two halves of the benchmark are only comparable if their
# configuration is identical, and it has silently drifted before.
#
# The data directory is bind-mounted from the host because the dataset does not fit in the
# container layer on a small /var partition.
#
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE="${1:-aster-tsdb:6.0.0}"
ROWS="${SCALE_ROWS:-20000000}"
SERIES="${SCALE_SERIES:-500}"
LOADERS="${SCALE_LOADERS:-12}"
DATA="${SCALE_DATA:-/home/common/cassandra-ts-tmtag-data}"
TABLE="${SCALE_TABLE:-tm_tag_point}"
HEAP="${SCALE_HEAP:-16G}"
REPORT="${SCALE_REPORT:-build/timeseries-scale-report.html}"
GC="${SCALE_GC:-zgc}"                  # zgc (generational, as shipped) | g1
PASSES="${SCALE_PASSES:-1}"            # query passes; the last one is the reported (warm) run
WBENCH_ROWS="${SCALE_WBENCH_ROWS:-0}"  # if >0, also time a write of this many rows
CONTAINER="cassandra-ts-scale"
RUNTIME="${CONTAINER_RUNTIME:-docker}"
# A reused data directory pins the node's address in cluster metadata, so the container needs a
# stable IP: on the default bridge docker hands out whatever is free and the node then tries to
# gossip with its former self and never reaches NORMAL. Hence a dedicated network + static IP.
NET="${SCALE_NET:-ts-scale-net}"
NET_SUBNET="${SCALE_NET_SUBNET:-172.30.0.0/16}"
NODE_IP="${SCALE_NODE_IP:-172.30.0.10}"

echo "== cassandra-timeseries scale test =="
echo "   image=$IMAGE rows=$ROWS series=$SERIES loaders=$LOADERS heap=$HEAP gc=$GC passes=$PASSES"
echo "   data=$DATA report=$REPORT"

SKIP_LOAD="${SCALE_SKIP_LOAD:-0}"      # reuse an already-loaded data directory
$RUNTIME rm -f "$CONTAINER" > /dev/null 2>&1
[ "$SKIP_LOAD" = 1 ] || rm -rf "$DATA"
mkdir -p "$DATA"; chmod 777 "$DATA"
mkdir -p "$(dirname "$REPORT")"

$RUNTIME network inspect "$NET" > /dev/null 2>&1 \
    || $RUNTIME network create --subnet "$NET_SUBNET" "$NET" > /dev/null

# The node config is shared verbatim with tiering-bench.sh; see docker/bench-node-config.sh.
NODE_CONFIG="$(cat "$HERE/bench-node-config.sh")"

$RUNTIME run -d --name "$CONTAINER" \
    --network "$NET" --ip "$NODE_IP" \
    -e MAX_HEAP_SIZE="$HEAP" -e HEAP_NEWSIZE=4G -e GC_CHOICE="$GC" \
    -v "$DATA:/opt/cassandra/data" \
    --entrypoint bash "$IMAGE" -c "$NODE_CONFIG
        exec docker-entrypoint.sh
    " > /dev/null || { echo "FATAL: container did not start"; exit 2; }

printf '   waiting for CQL'
ready=0
for _ in $(seq 1 120); do
    if $RUNTIME exec "$CONTAINER" cqlsh -e "SELECT release_version FROM system.local" > /dev/null 2>&1; then
        ready=1; break
    fi
    printf '.'; sleep 5
done
echo
[ "$ready" = 1 ] || { echo "FATAL: node not ready"; $RUNTIME logs "$CONTAINER" 2>&1 | tail -30; exit 2; }

# ---------------------------------------------------------------- load
# Loader and query client both run inside the container against 127.0.0.1 with the python
# driver that ships with cqlsh, so the measured times contain no docker-exec/cqlsh startup.
$RUNTIME cp docker/scale-workload.py "$CONTAINER:/tmp/scale-workload.py"

# The schema is defined once, in scale-workload.py, so the loader and the DDL cannot disagree.
$RUNTIME exec "$CONTAINER" sh -c \
    "python3 /tmp/scale-workload.py ddl --table '$TABLE' > /tmp/schema.cql && cqlsh -f /tmp/schema.cql" \
    || { echo "FATAL: schema creation failed"; exit 2; }

if [ "$SKIP_LOAD" = 1 ]; then
    LOAD_SECS="${SCALE_LOAD_SECS:-0}"
    echo "-- reusing the data already in $DATA (skipping load)"
else
    echo "-- loading $ROWS rows ($SERIES tags, $LOADERS processes)"
    LOAD_START=$(date +%s)
    $RUNTIME exec "$CONTAINER" python3 /tmp/scale-workload.py load \
        --rows "$ROWS" --series "$SERIES" --loaders "$LOADERS" --table "$TABLE" \
        || { echo "FATAL: load failed"; exit 2; }
    LOAD_SECS=$(( $(date +%s) - LOAD_START ))
    echo "-- load finished in ${LOAD_SECS}s"
fi

$RUNTIME exec "$CONTAINER" nodetool flush scale "$TABLE"
$RUNTIME exec "$CONTAINER" nodetool tablestats "scale.$TABLE" 2>/dev/null | grep -E "Space used \(live\)|Number of partitions" | head -2

# ---------------------------------------------------------------- query + report
echo "-- timing time-series queries ($PASSES pass(es), gc=$GC)"
pass=1
while [ "$pass" -le "$PASSES" ]; do
    [ "$pass" -lt "$PASSES" ] && echo "   warm-up pass $pass/$PASSES"
    $RUNTIME exec "$CONTAINER" python3 /tmp/scale-workload.py query \
        --rows "$ROWS" --series "$SERIES" --load-secs "$LOAD_SECS" --image "$IMAGE" \
        --table "$TABLE" \
        --md-out /tmp/scale-report.md --json-out /tmp/scale-results.json > "$REPORT"
    rc=$?
    [ $rc -eq 0 ] || break
    pass=$((pass + 1))
done

if [ $rc -ne 0 ]; then
    echo "FATAL: query phase failed"; exit 1
fi
# The Markdown twin is the copy that gets committed (GitLab renders .md, not .html blobs).
$RUNTIME cp "$CONTAINER:/tmp/scale-report.md" "${REPORT%.html}.md"
$RUNTIME cp "$CONTAINER:/tmp/scale-results.json" "${REPORT%.html}.json"

if [ "$WBENCH_ROWS" -gt 0 ]; then
    echo "-- write benchmark: $WBENCH_ROWS rows into scale.wbench (gc=$GC)"
    $RUNTIME exec "$CONTAINER" cqlsh -e "DROP TABLE IF EXISTS scale.wbench;"
    $RUNTIME exec "$CONTAINER" sh -c \
        "python3 /tmp/scale-workload.py ddl --table wbench > /tmp/wbench.cql && cqlsh -f /tmp/wbench.cql"
    $RUNTIME exec "$CONTAINER" python3 /tmp/scale-workload.py load \
        --rows "$WBENCH_ROWS" --series 200 --loaders "$LOADERS" --table wbench \
        --json-out /tmp/wbench.json
    $RUNTIME cp "$CONTAINER:/tmp/wbench.json" "${REPORT%.html}-write.json"
fi

# GC pause summary straight from the JVM log
$RUNTIME exec "$CONTAINER" sh -c 'cat $CASSANDRA_HOME/logs/gc.log' > "${REPORT%.html}-gc.log" 2>/dev/null
echo "   gc log: ${REPORT%.html}-gc.log"
echo "   report: $REPORT"
echo "   report: ${REPORT%.html}.md"
echo "-- container '$CONTAINER' left running; remove with: $RUNTIME rm -f $CONTAINER"
