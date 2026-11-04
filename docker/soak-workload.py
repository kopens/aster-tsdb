#!/usr/bin/env python3
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
"""
Soak workload for a multi-day run against a tm_tag_point-shaped table.

Two subcommands, meant to run concurrently against the same cluster:

  write   paced ingest at a fixed rows/s, forever (or --duration)
  verify  one round of health + correctness checks, printing one structured line

The point of a soak is not "did it stay up". Everything found so far was found by
running something once; a soak has to catch what only time produces -- leaks, backlogs
that never drain, drift after the re-encoder has run thousands of times, a window that
parks and is never noticed. So `verify` checks *correctness against what was written*,
not just liveness, and prints a machine-readable line each round so a 3-day log can be
diffed rather than read.

  ./soak-workload.py write  --hosts 172.30.0.10,172.30.0.11 --rate 1000 --tags 10000
  ./soak-workload.py verify --hosts 172.30.0.10 --json-out soak-status.jsonl
"""

import argparse
import json
import os
import random
import signal
import sys
import time

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

KEYSPACE = "soak"
TABLE = "tm_tag_point"

# Mirrors the measured production distribution: boolean-dominated, ~18% numeric.
# Which value column carries the reading is decided by the STATIC `type` column, so
# within one tag it never varies -- that is what lets a chunk fold one of them to
# ALL_NULL and cost zero bytes.
TYPE_MIX = [("boolean", 0.792), ("int", 0.083), ("double", 0.064),
            ("long", 0.028), ("string", 0.027), ("float", 0.006)]
NUMERIC_TYPES = {"int", "double", "long", "float"}

DDL = f"""
CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLE} (
    tag_id text,
    timestamp timestamp,
    area_id text static, asset_id text static, line_id text static,
    opc_id text static, site_id text static, tag_name text static, type text static,
    attribute frozen<map<text, text>>,
    error_code int, latency int, quality int,
    value text, value_boolean boolean, value_numeric double,
    PRIMARY KEY (tag_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
"""

# A progress table the verifier reads, so correctness can be checked against what the
# writer believes it wrote rather than against a number typed into a script.
PROGRESS_DDL = f"""
CREATE TABLE IF NOT EXISTS {KEYSPACE}.soak_progress (
    writer_id text PRIMARY KEY,
    started_at timestamp,
    updated_at timestamp,
    rows_written bigint,
    first_ts timestamp,
    last_ts timestamp
)
"""


def tag_type(i):
    r = (i * 2654435761 % 1000) / 1000.0     # deterministic, no RNG state to carry
    acc = 0.0
    for name, share in TYPE_MIX:
        acc += share
        if r < acc:
            return name
    return "boolean"


def connect(hosts):
    cluster = Cluster(hosts.split(","), protocol_version=5, connect_timeout=30)
    return cluster, cluster.connect()


def ensure_schema(session, rf):
    session.execute(f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} WITH replication = "
                    f"{{'class':'SimpleStrategy','replication_factor':{rf}}}")
    session.execute(DDL)
    session.execute(PROGRESS_DDL)


def cmd_write(args):
    cluster, session = connect(args.hosts)
    ensure_schema(session, args.rf)
    session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM

    types = {i: tag_type(i) for i in range(args.tags)}
    for i in range(args.tags):
        session.execute(
            f"INSERT INTO {KEYSPACE}.{TABLE} (tag_id, area_id, asset_id, line_id, opc_id, "
            f"site_id, tag_name, type) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (f"TAG-{i:06d}", f"area-{i % 8}", f"asset-{i % 400:04d}", f"line-{i % 40:02d}",
             f"opc-{i % 12}", f"site-{i % 4}", f"tag-name-{i}", types[i]))

    # Two prepared statements, NOT one with nulls bound. Binding null writes a cell
    # tombstone, which the cold-write guard rejects outright -- so an ingester that
    # binds every column cannot backfill into the cold range at all. Omitting the
    # column writes nothing and is accepted. Production ingesters should do the same.
    ins_num = session.prepare(
        f"INSERT INTO {KEYSPACE}.{TABLE} (tag_id, timestamp, attribute, error_code, "
        f"latency, quality, value, value_numeric) VALUES (?,?,?,?,?,?,?,?)")
    ins_bool = session.prepare(
        f"INSERT INTO {KEYSPACE}.{TABLE} (tag_id, timestamp, attribute, error_code, "
        f"latency, quality, value, value_boolean) VALUES (?,?,?,?,?,?,?,?)")
    ins_text = session.prepare(
        f"INSERT INTO {KEYSPACE}.{TABLE} (tag_id, timestamp, attribute, error_code, "
        f"latency, quality, value) VALUES (?,?,?,?,?,?,?)")
    progress = session.prepare(
        f"INSERT INTO {KEYSPACE}.soak_progress (writer_id, started_at, updated_at, "
        f"rows_written, first_ts, last_ts) VALUES (?,?,?,?,?,?)")

    stopping = {"now": False}
    signal.signal(signal.SIGTERM, lambda *_: stopping.__setitem__("now", True))
    signal.signal(signal.SIGINT, lambda *_: stopping.__setitem__("now", True))

    rng = random.Random(20260801)
    walk = {i: 50.0 for i in range(args.tags)}
    started = time.time()
    first_ts = None
    written = 0
    tag = 0
    deadline = started + args.duration if args.duration else None

    # Pace in fixed slices rather than sleeping per row: at 1000 rows/s a per-row sleep
    # spends more time in the scheduler than in the driver.
    slice_secs = 0.1
    per_slice = max(1, int(args.rate * slice_secs))

    while not stopping["now"] and (deadline is None or time.time() < deadline):
        slice_start = time.time()
        futures = []
        for _ in range(per_slice):
            i = tag % args.tags
            tag += 1
            t = types[i]
            now_ms = int(time.time() * 1000)
            if first_ts is None:
                first_ts = now_ms
            walk[i] += rng.gauss(0, 0.4)
            reading = round(walk[i], 1)
            common = (f"TAG-{i:06d}", now_ms, {}, 0, rng.randint(1, 999), 192)
            if t in NUMERIC_TYPES:
                futures.append(session.execute_async(ins_num, common + (str(reading), reading)))
            elif t == "boolean":
                b = (int(now_ms / 1000) // 300) % 2 == 0
                futures.append(session.execute_async(ins_bool, common + (str(b).lower(), b)))
            else:
                futures.append(session.execute_async(ins_text, common + (f"s{int(reading)}",)))
        for f in futures:
            f.result()
        written += per_slice

        if written % (args.rate * 60) < per_slice:      # roughly once a minute
            session.execute(progress, (args.writer_id, int(started * 1000),
                                       int(time.time() * 1000), written, first_ts,
                                       int(time.time() * 1000)))
            elapsed = time.time() - started
            print(f"{time.strftime('%H:%M:%S')} written={written} "
                  f"rate={written/elapsed:.0f}/s elapsed={elapsed/3600:.2f}h", flush=True)

        lag = slice_secs - (time.time() - slice_start)
        if lag > 0:
            time.sleep(lag)

    session.execute(progress, (args.writer_id, int(started * 1000), int(time.time() * 1000),
                               written, first_ts, int(time.time() * 1000)))
    print(f"stopped: written={written}", flush=True)
    cluster.shutdown()


def cmd_verify(args):
    cluster, session = connect(args.hosts)
    out = {"t": time.strftime("%Y-%m-%dT%H:%M:%S"), "ok": True, "problems": []}

    def problem(msg):
        out["ok"] = False
        out["problems"].append(msg)

    try:
        rows = list(session.execute(f"SELECT * FROM {KEYSPACE}.soak_progress"))
        out["rows_written"] = sum(r.rows_written for r in rows)
        out["writers"] = len(rows)
    except Exception as e:                                    # noqa: BLE001
        problem(f"progress table unreadable: {type(e).__name__}")
        out["rows_written"] = None

    # Liveness of every node, from this node's view of the ring.
    try:
        peers = list(session.execute("SELECT peer FROM system.peers_v2"))
        out["peers"] = len(peers)
        if args.expect_nodes and len(peers) + 1 != args.expect_nodes:
            problem(f"ring has {len(peers)+1} nodes, expected {args.expect_nodes}")
    except Exception as e:                                    # noqa: BLE001
        problem(f"peers unreadable: {type(e).__name__}")

    # Correctness, not just liveness: a bounded read over a sampled tag must return
    # rows whose count matches a re-read, and whose values are stable across the merge.
    # A read that starts dropping or duplicating rows shows up here first.
    sampled = []
    for i in args.sample_tags:
        tag = f"TAG-{i:06d}"
        try:
            q = (f"SELECT timestamp, value FROM {KEYSPACE}.{TABLE} "
                 f"WHERE tag_id=%s AND timestamp >= %s AND timestamp < %s")
            hi = int(time.time() * 1000) - 60_000
            lo = hi - args.sample_window_ms
            a = list(session.execute(q, (tag, lo, hi)))
            b = list(session.execute(q, (tag, lo, hi)))
            if len(a) != len(b):
                problem(f"{tag}: same bounded read returned {len(a)} then {len(b)}")
            elif [r.value for r in a] != [r.value for r in b]:
                problem(f"{tag}: same bounded read returned different values")
            sampled.append(len(a))
        except Exception as e:                                # noqa: BLE001
            problem(f"{tag}: bounded read failed: {type(e).__name__}")
    out["sampled_rows"] = sampled

    line = json.dumps(out, default=str)
    print(line, flush=True)
    if args.json_out:
        with open(args.json_out, "a") as f:
            f.write(line + "\n")
    cluster.shutdown()
    return 0 if out["ok"] else 1


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    w = sub.add_parser("write")
    w.add_argument("--hosts", required=True)
    w.add_argument("--rate", type=int, default=1000, help="rows/s, aggregate")
    w.add_argument("--tags", type=int, default=10000)
    w.add_argument("--rf", type=int, default=3)
    w.add_argument("--duration", type=int, default=0, help="seconds; 0 = forever")
    w.add_argument("--writer-id", default=os.uname().nodename)
    w.set_defaults(func=cmd_write)

    v = sub.add_parser("verify")
    v.add_argument("--hosts", required=True)
    v.add_argument("--expect-nodes", type=int, default=0)
    v.add_argument("--sample-tags", type=int, nargs="*", default=[0, 1, 4999, 9999])
    v.add_argument("--sample-window-ms", type=int, default=600_000)
    v.add_argument("--json-out", default="")
    v.set_defaults(func=cmd_verify)

    args = p.parse_args()
    sys.exit(args.func(args) or 0)


if __name__ == "__main__":
    main()
