#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Read-throughput benchmark for the scale.tm_tag_point dataset loaded by scale-workload.py.

Runs INSIDE the container (same driver-import trick as scale-workload.py) so measured
numbers are real client-observed ops with no docker-exec overhead.

  python3 rwbench-read.py --series 500 --rows-per-series 40000 \
      --workers 12 --inflight 64 --duration 60 --pattern latest|point|range100

Patterns (per op):
  latest   SELECT ... WHERE tag_id=? LIMIT 1                      (dashboard "current value")
  point    SELECT ... WHERE tag_id=? AND timestamp=?              (single exact row)
  range100 SELECT ... WHERE tag_id=? AND timestamp>=? AND <?      (100-sample window scan)

Each worker process keeps --inflight async requests outstanding until --duration elapses.
Reports aggregate ops/s and rows/s as JSON on stdout.
"""
import argparse
import datetime
import glob
import json
import multiprocessing
import os
import random
import sys
import time

for _zip in glob.glob('/opt/cassandra/lib/cassandra-driver-internal-only-*.zip'):
    _ver = os.path.basename(_zip)[len('cassandra-driver-internal-only-'):-len('.zip')]
    sys.path.insert(0, os.path.join(_zip, 'cassandra-driver-' + _ver))
for _extra in ('geomet-0.1.0.zip', 'futures-2.1.6-py2.py3-none-any.zip'):
    _p = os.path.join('/opt/cassandra/lib', _extra)
    if os.path.exists(_p):
        sys.path.insert(0, _p)

KEYSPACE = 'scale'
EPOCH = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)

QUERIES = {
    'latest':   "SELECT tag_id, timestamp, value, value_numeric, value_boolean, quality "
                "FROM %s.%s WHERE tag_id = ? LIMIT 1",
    'point':    "SELECT tag_id, timestamp, value, value_numeric, value_boolean, quality "
                "FROM %s.%s WHERE tag_id = ? AND timestamp = ?",
    'range100': "SELECT tag_id, timestamp, value, value_numeric, value_boolean, quality "
                "FROM %s.%s WHERE tag_id = ? AND timestamp >= ? AND timestamp < ?",
}


def connect(attempts=3):
    """A node that just finished a bulk operation (a major compaction) can stall the burst
    of concurrent control connections a worker pool opens; one refused handshake must not kill a
    60-second benchmark, so connection setup retries with a short backoff."""
    from cassandra.cluster import Cluster, NoHostAvailable
    for attempt in range(attempts):
        try:
            # control_connection_timeout: the driver default of 2s dies on a cold node whose
            # first system/schema reads come from disk — the failure mode is NoHostAvailable
            # at connect while cqlsh (5s/10s timeouts) works fine against the same node.
            cluster = Cluster(['127.0.0.1'], connect_timeout=60, control_connection_timeout=30)
            session = cluster.connect()
            session.default_timeout = 120
            return cluster, session
        except NoHostAvailable:
            if attempt == attempts - 1:
                raise
            time.sleep(5 * (attempt + 1))


def bind_args(pattern, rnd, series, rows_per_series):
    tag = 'tag-%06d' % rnd.randrange(series)
    if pattern == 'latest':
        return (tag,)
    n = rnd.randrange(rows_per_series)
    ts = EPOCH + datetime.timedelta(seconds=n)
    if pattern == 'point':
        return (tag, ts)
    n = rnd.randrange(max(1, rows_per_series - 100))
    start = EPOCH + datetime.timedelta(seconds=n)
    return (tag, start, start + datetime.timedelta(seconds=100))


def worker(args):
    wid, a = args
    # Staggered connect: N simultaneous control connections against a cold or just-busy node
    # time out as a group; spreading them ~300ms apart keeps the burst under what it can absorb.
    time.sleep(wid * 0.3)
    cluster, session = connect()
    stmt = session.prepare(QUERIES[a.pattern] % (KEYSPACE, a.table))
    rnd = random.Random(0xBEEF ^ (wid * 2654435761))
    ops = rows = errors = 0
    inflight = []
    deadline = time.time() + a.duration
    while time.time() < deadline:
        while len(inflight) < a.inflight:
            inflight.append(session.execute_async(stmt, bind_args(a.pattern, rnd, a.series,
                                                                  a.rows_per_series)))
        f = inflight.pop(0)
        try:
            rs = f.result()
            rows += len(list(rs))
            ops += 1
        except Exception:
            errors += 1
    for f in inflight:
        try:
            rs = f.result()
            rows += len(list(rs))
            ops += 1
        except Exception:
            errors += 1
    cluster.shutdown()
    return ops, rows, errors


def latency_probe(a, n=2000):
    """Sequential (depth-1) op latencies in ms: what one client sees per request."""
    cluster, session = connect()
    stmt = session.prepare(QUERIES[a.pattern] % (KEYSPACE, a.table))
    rnd = random.Random(0xFACE)
    lat = []
    for _ in range(n):
        args = bind_args(a.pattern, rnd, a.series, a.rows_per_series)
        t0 = time.time()
        list(session.execute(stmt, args))
        lat.append((time.time() - t0) * 1000.0)
    cluster.shutdown()
    lat.sort()
    return {'p50_ms': lat[len(lat) // 2], 'p99_ms': lat[int(len(lat) * 0.99)],
            'avg_ms': sum(lat) / len(lat)}


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--table', default='tm_tag_point')
    p.add_argument('--series', type=int, required=True)
    p.add_argument('--rows-per-series', type=int, required=True)
    p.add_argument('--workers', type=int, default=12)
    p.add_argument('--inflight', type=int, default=64)
    p.add_argument('--duration', type=int, default=60)
    p.add_argument('--pattern', choices=sorted(QUERIES), required=True)
    p.add_argument('--probe', type=int, default=2000, help='sequential latency probe ops (0=skip)')
    a = p.parse_args()

    started = time.time()
    with multiprocessing.Pool(a.workers) as pool:
        results = pool.map(worker, [(w, a) for w in range(a.workers)])
    elapsed = time.time() - started

    ops = sum(r[0] for r in results)
    rows = sum(r[1] for r in results)
    errors = sum(r[2] for r in results)
    out = {'pattern': a.pattern, 'workers': a.workers, 'inflight': a.inflight,
           'duration_s': round(elapsed, 1), 'ops': ops, 'errors': errors,
           'ops_per_sec': round(ops / elapsed), 'rows_per_sec': round(rows / elapsed)}
    if a.probe:
        out['latency_seq'] = {k: round(v, 2) for k, v in latency_probe(a, a.probe).items()}
    json.dump(out, sys.stdout)
    print()


if __name__ == '__main__':
    main()
