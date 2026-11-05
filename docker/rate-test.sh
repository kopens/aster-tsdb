#!/usr/bin/env bash
# A sustained write rate on a candidate image: tm_tag_point's shape at a fixed rows per second with reads alongside, 5-minute
# windows so freezes happen under load, then every tag's count and sum against what the server acknowledged; see
# rate-workload.py. Configured like 41's template but it says nothing about 41's hardware or 1-day windows. Prints PASS or
# FAIL with the reasons and exits non-zero on FAIL. The node is left running for inspection.
# usage: docker/rate-test.sh <image> [minutes] [rows-per-second]
set -uo pipefail
IMAGE=${1:?image}; MINUTES=${2:-30}; RATE=${3:-10000}; SECS=$((MINUTES * 60))
HERE=$(cd "$(dirname "$0")" && pwd); W=${RATE_DIR:-$HERE/../build/rate}; C=cassandra-ts-rate; D=cassandra-ts-rate-driver; NET=ts-scale-net; IP=172.30.0.30
MOUNTS="-v $HERE/rate-workload.py:/tmp/w.py:ro -v $HERE/scale-workload.py:/tmp/scale-workload.py:ro"
docker network inspect $NET >/dev/null 2>&1 || docker network create --subnet 172.30.0.0/16 $NET >/dev/null
docker rm -f $C $D >/dev/null 2>&1; \rm -rf "$W"; mkdir -p "$W/data" "$W/out"; chmod -R 777 "$W"
# 41's template values; timeouts stay shipped so a slow write counts as a failure.
CFG='F=$CASSANDRA_CONF/cassandra.yaml
for kv in concurrent_reads:256 native_transport_max_threads:256 memtable_heap_space:2GiB read_request_timeout:10000ms; do k=${kv%%:*}; v=${kv#*:}
  if grep -qE "^#? *$k:" $F; then sed -ri "s/^#? *$k:.*/$k: $v/" $F; else echo "$k: $v" >> $F; fi; done
exec docker-entrypoint.sh'
docker run -d --name $C --network $NET --ip $IP --cpus 16 -e MAX_HEAP_SIZE=16G -e HEAP_NEWSIZE=4G -v "$W/data:/opt/cassandra/data" --entrypoint bash "$IMAGE" -c "$CFG" >/dev/null
for _ in $(seq 1 90); do docker exec $C cqlsh -e "SELECT now() FROM system.local" >/dev/null 2>&1 && break; sleep 5; done
docker exec $C sh -c 'grep -E "^(concurrent_reads|native_transport_max_threads|memtable_heap_space|read_request_timeout):" $CASSANDRA_CONF/cassandra.yaml'
docker exec $C sh -c 'tr "\0" " " </proc/$(pgrep -f CassandraDaemon | head -1)/cmdline' | grep -oE -- '-Xmx[^ ]+|-XX:\+UseZGC' | sort -u
python3 - "$HERE/scale-workload.py" > "$W/out/ddl.cql" <<'PY'
import re,sys
s=open(sys.argv[1]).read(); d=re.search(r'DDL = """(.*?)"""',s,re.S).group(1).format(ks='bench',tbl='tm_tag_point')
d=re.sub(r"AND compaction = \{.*?\};", "AND compaction = {'class':'TimeSeriesCompactionStrategy','window_size':'5m','freeze_after':'5m','frozen_format':'columnar'};", d, flags=re.S)
print(d)
PY
docker cp "$W/out/ddl.cql" $C:/tmp/ddl.cql; docker exec $C cqlsh -f /tmp/ddl.cql || { echo FATAL ddl; exit 1; }
docker exec $C cqlsh -e "SELECT compaction FROM system_schema.tables WHERE keyspace_name='bench' AND table_name='tm_tag_point';" | sed -n 4p
docker run -d --name $D --network $NET --cpus 12 -v "$W/out:/out" $MOUNTS --entrypoint python3 "$IMAGE" /tmp/w.py run --host $IP --seconds "$SECS" --tags "$RATE" >/dev/null
sample() { echo "## $(date +%T) load $(cut -d' ' -f1 /proc/loadavg) $(docker exec $C nodetool info 2>/dev/null | grep -E '^Heap Memory' | tr -s ' ') tscol $(find "$W/data/data/bench" -name '*-tscol-Data.db' | wc -l) row $(find "$W/data/data/bench" -name '*-Data.db' ! -name '*-tscol-*' | wc -l) disk $(du -sm "$W/data/data/bench" | cut -f1)M commitlog $(du -sm "$W/data/commitlog" 2>/dev/null | cut -f1)M pending $(docker exec $C nodetool compactionstats 2>/dev/null | grep -i 'pending tasks' | tr -dc 0-9)"; }
: > "$W/out/samples.log"
cg() { for n in $C $D; do id=$(docker inspect -f '{{.Id}}' $n 2>/dev/null); f=$(ls -d /sys/fs/cgroup/system.slice/docker-$id.scope /sys/fs/cgroup/docker/$id 2>/dev/null | head -1); [ -n "$f" ] && echo "$(date +%T) $n $(grep -E 'nr_throttled|throttled_usec' $f/cpu.stat | tr '\n' ' ')"; done; }
( while docker ps --format '{{.Names}}' | grep -qx $D; do cg >> "$W/out/cpu.log"; echo "$(date +%T) $(cut -d' ' -f1-3 /proc/loadavg)" >> "$W/out/load.log"; sleep 5; done ) &
while docker ps --format '{{.Names}}' | grep -qx $D; do sample >> "$W/out/samples.log"; tail -1 "$W/out/samples.log"; sleep 60; done
docker logs $D 2>&1 | tail -30 > "$W/out/driver.log"; cat "$W/out/driver.log"
[ -s "$W/out/run.json" ] || { echo "FATAL no run.json"; exit 1; }
echo "-- flush and wait for closed windows to freeze"
docker exec $C nodetool flush bench; T0=$(date +%s)
for _ in $(seq 1 90); do R=$(find "$W/data/data/bench" -name '*-Data.db' ! -name '*-tscol-*' | wc -l); [ "$R" = 0 ] && break; sleep 10; done
echo "row-format sstables left $R after $(( $(date +%s) - T0 ))s"; sample | tee -a "$W/out/samples.log"
docker run --rm --network $NET -v "$W/out:/out" $MOUNTS --entrypoint python3 "$IMAGE" /tmp/w.py verify --host $IP
docker exec $C nodetool tablestats bench.tm_tag_point | grep -E "Space used \(live\)|SSTable count|Local write latency" 
docker exec $C nodetool tpstats > "$W/out/tpstats.txt"
docker exec $C sh -c 'cat /opt/cassandra/logs/system.log*' > "$W/out/system.log"
docker exec $C cat /opt/cassandra/logs/gc.log > "$W/out/gc.log" 2>/dev/null
TSCOL=$(find "$W/data/data/bench" -name '*-tscol-Data.db' | wc -l)
python3 - "$W/out" "$RATE" "$TSCOL" "$R" <<'PY'
import json,re,sys
o=sys.argv[1]; rate=int(sys.argv[2]); r=json.load(open(o+'/run.json')); v=json.load(open(o+'/verify.json')); log=open(o+'/system.log',errors='replace').read(); tp=open(o+'/tpstats.txt').read()
fail=[]
if r['written']==0 or v['rows']==0: fail.append('empty')
if v['rows']!=v['expected'] or v['mismatches']: fail.append('verify %s'%v)
if r['write_errors']: fail.append('write errors %d %s'%(r['write_errors'],r['write_first']))
if r['read_errors']: fail.append('read errors %d %s'%(r['read_errors'],r['read_first']))
if r['min_rate']<rate*0.99: fail.append('min rate %d'%r['min_rate'])
errs=len(re.findall(r'^ERROR',log,re.M)); oom=log.count('java.lang.OutOfMemoryError:')
if errs or oom: fail.append('ERROR %d OOM %d'%(errs,oom))
sec=tp.split('Message type')[-1]; drop=sum(int(l.split()[1]) for l in sec.splitlines()[1:] if len(l.split())>=2 and l.split()[1].isdigit())
if drop: fail.append('dropped %d'%drop)
tscol,rowfmt=int(sys.argv[3]),int(sys.argv[4])
if tscol==0 or rowfmt: fail.append('tscol sstables %d, row-format left %d'%(tscol,rowfmt))
print('errors',errs,'oom',oom,'dropped',drop)
print('RATE TEST '+('PASS' if not fail else 'FAIL: '+'; '.join(fail)))
sys.exit(1 if fail else 0)
PY
RESULT=$?
echo "-- freezes"; grep -h "Compacted .*tscol" "$W/out/system.log" | sed 's/.*Compacted/Compacted/; s/\/opt[^,]*\///' | cut -c1-220 | tail -12
docker rm -f $D >/dev/null 2>&1
exit $RESULT
