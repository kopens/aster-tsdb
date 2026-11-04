#!/usr/bin/env bash
# One hour of a table's whole life on a candidate image: freeze, refreeze, TTL, retention and a restart, on two tables at
# production's write rate; see lifecycle-workload.py. Prints PASS or FAIL with the reasons and exits non-zero on FAIL, plus
# the per-window size and freeze time needed to project disk growth and freeze cost for real window sizes.
# usage: docker/lifecycle-test.sh <image> [minutes] [rows-per-second]
set -uo pipefail
IMAGE=${1:?image}; MINUTES=${2:-60}; RATE=${3:-10000}
HERE=$(cd "$(dirname "$0")" && pwd); W=${LIFECYCLE_DIR:-$HERE/../build/lifecycle}; C=cassandra-ts-life; D=cassandra-ts-life-driver; NET=ts-scale-net; IP=172.30.0.20
docker network inspect $NET >/dev/null 2>&1 || docker network create --subnet 172.30.0.0/16 $NET >/dev/null
docker rm -f $C $D >/dev/null 2>&1
\rm -rf "$W"; mkdir -p "$W/data" "$W/ctl"; chmod -R 777 "$W"
docker run -d --name $C --network $NET --ip $IP -e MAX_HEAP_SIZE=8G -e HEAP_NEWSIZE=2G -v "$W/data:/opt/cassandra/data" "$IMAGE" >/dev/null
ready() { for _ in $(seq 1 90); do docker exec $C cqlsh -e "SELECT now() FROM system.local" >/dev/null 2>&1 && return 0; sleep 5; done; echo "FAIL: node did not come up"; docker logs --tail 30 $C; exit 1; }
ready
TSCS="{'class':'TimeSeriesCompactionStrategy','window_size':'5m','freeze_after':'5m','retention':'30m','frozen_format':'columnar'}"
docker exec $C cqlsh -e "CREATE KEYSPACE life WITH replication = {'class':'SimpleStrategy','replication_factor':1};
  CREATE TABLE life.ttl_t (tag_id text, timestamp timestamp, value_numeric double, quality int, PRIMARY KEY (tag_id, timestamp))
    WITH CLUSTERING ORDER BY (timestamp DESC) AND gc_grace_seconds = 60 AND default_time_to_live = 1200 AND compaction = $TSCS;
  CREATE TABLE life.ret_t (tag_id text, timestamp timestamp, value_numeric double, quality int, PRIMARY KEY (tag_id, timestamp))
    WITH CLUSTERING ORDER BY (timestamp DESC) AND gc_grace_seconds = 60 AND compaction = $TSCS;"
docker run -d --name $D --network $NET -v "$W/ctl:/ctl" -v "$HERE/lifecycle-workload.py:/tmp/l.py:ro" --entrypoint python3 "$IMAGE" /tmp/l.py --host $IP --minutes "$MINUTES" --rate "$RATE" >/dev/null
START=$(date +%s); RESTARTED=0
: > "$W/samples.log"
while docker ps --format '{{.Names}}' | grep -qx $D; do
    sleep 60
    M=$(( ($(date +%s) - START) / 60 ))
    # At this rate a memtable outlives a 5-minute window; 41's flushes by size at about ten times the rows.
    [ $(( M % 2 )) = 0 ] && docker exec $C nodetool flush life >/dev/null 2>&1
    {
        echo "## minute $M load $(cut -d' ' -f1 /proc/loadavg)"
        docker exec $C nodetool info 2>/dev/null | grep -E "^Heap Memory|^Load"
        for t in ttl_t ret_t; do docker exec $C cqlsh -e "SELECT window_start, state, sstables, bytes_on_disk, rows FROM system_views.timeseries_windows WHERE keyspace_name='life' AND table_name='$t';" 2>/dev/null | sed -n '4,$p' | grep -v "rows)" | sed "s/^/$t /"; done
        echo "files tscol $(find "$W/data/data/life" -name '*-tscol-Data.db' | wc -l) other $(find "$W/data/data/life" -name '*-Data.db' ! -name '*-tscol-*' | wc -l)"
    } >> "$W/samples.log"
    if [ $RESTARTED = 0 ] && [ "$M" -ge $((MINUTES / 2)) ]; then
        RESTARTED=1
        touch "$W/ctl/pause"; sleep 5
        docker exec $C nodetool drain; docker restart $C >/dev/null; ready
        \rm -f "$W/ctl/pause"; echo "## restarted at minute $M" >> "$W/samples.log"
    fi
done
docker logs $D > "$W/driver.log" 2>&1
docker cp $C:/opt/cassandra/logs/system.log "$W/system.log" 2>/dev/null
docker exec $C sh -c 'cat /opt/cassandra/logs/system.log*' > "$W/all-system.log" 2>/dev/null
ERRS=$(grep -c "^ERROR" "$W/all-system.log" || true)
SUMMARY=$(grep '^SUMMARY' "$W/driver.log" | sed 's/^SUMMARY //')
echo "$SUMMARY"
python3 - "$W" "$SUMMARY" "$ERRS" <<'PY'
import json, re, sys
w, summary, errs = sys.argv[1], sys.argv[2], int(sys.argv[3])
fail = []
if not summary: fail.append('driver produced no summary')
else:
    s = json.loads(summary)
    for key in ('mismatches', 'resurrected', 'overwrite_lost'):
        if s[key]: fail.append('%s %d' % (key, s[key]))
    if s['checked'] == 0 or s['expired_checked'] == 0: fail.append('checks did not run')
    if s['late'] == 0: fail.append('no late writes')
if errs: fail.append('%d ERROR lines' % errs)
last = open(w + '/samples.log').read().split('## minute')[-1]
tscol = re.search(r'files tscol (\d+) other (\d+)', last)
if not tscol or int(tscol.group(1)) == 0: fail.append('no tscol files at the end')
windows = [l for l in last.splitlines() if l.startswith(('ttl_t', 'ret_t'))]
print('windows on the last sample: %d' % len(windows))
if len(windows) > 2 * 12: fail.append('windows are not being dropped: %d' % len(windows))
print('LIFECYCLE ' + ('PASS' if not fail else 'FAIL: ' + '; '.join(fail)))
sys.exit(1 if fail else 0)
PY
RESULT=$?
echo "-- freezes"; grep -h "Compacted .*tscol" "$W/all-system.log" | sed 's/.*Compacted/Compacted/; s/\/opt[^,]*\///' | cut -c1-230 | tail -30
docker rm -f $D >/dev/null 2>&1
exit $RESULT
