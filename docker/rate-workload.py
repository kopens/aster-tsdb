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
"""Paced production-shape load for docker/rate-test.sh: `tags` tags at 1 Hz into tm_tag_point with scale-workload.py's value
distribution, reads running alongside, then an exact per-tag count and sum against what was acknowledged.

  rate-workload.py run    --host H --seconds N --tags 10000 --writers 10 --out /out
  rate-workload.py verify --host H --out /out
"""
import argparse, array, importlib.util, json, multiprocessing, os, random, threading, time, datetime
spec = importlib.util.spec_from_file_location('sw', '/tmp/scale-workload.py'); sw = importlib.util.module_from_spec(spec); spec.loader.exec_module(sw)
from cassandra.cluster import Cluster
KS, T = 'bench', 'tm_tag_point'
UTC = datetime.timezone.utc

def connect(host):
    c = Cluster([host], connect_timeout=60); s = c.connect(); s.default_timeout = 30
    return c, s

def dt(sec): return datetime.datetime.fromtimestamp(sec, UTC)

def writer(args):
    w, a, start = args
    c, s = connect(a.host)
    ins = {k: s.prepare(q % (KS, T)) for k, q in sw.INSERT_BY_KIND.items()}
    st = s.prepare(sw.INSERT_STATICS % (KS, T))
    mine = list(range(w, a.tags, a.writers))
    for i in mine: s.execute(st, sw.statics_for(i))
    gens = {i: sw.partition_rows(i, 10**9) for i in mine}
    kinds = {i: ins[sw.kind_of(sw.tag_type(i))] for i in mine}
    names = {i: sw.tag_id(i) for i in mine}
    sem = threading.Semaphore(512); lock = threading.Lock()
    ledger = {i: [0, 0] for i in mine}
    lat = {}; done = {}; err = {'n': 0, 'first': ''}
    def ok(_, i, l, t0):
        now = time.time(); m = int(now - start) // 60
        with lock:
            ledger[i][0] += 1; ledger[i][1] += l
            lat.setdefault(m, array.array('f')).append((now - t0) * 1000); done[m] = done.get(m, 0) + 1
        sem.release()
    def bad(e, i, l, t0):
        with lock:
            err['n'] += 1; err['first'] = err['first'] or repr(e)[:300]
        sem.release()
    late = 0; tick = int(start); lag = []
    slices = [mine[k::10] for k in range(10)]
    while tick < start + a.seconds:
        for k, part in enumerate(slices):
            d = tick + k / 10 - time.time()
            if d > 0: time.sleep(d)
            for i in part:
                row = next(gens[i])                      # (ts, attribute, error_code, latency, quality, value[, extra])
                sem.acquire(); t0 = time.time()
                f = s.execute_async(kinds[i], (names[i], dt(tick)) + row[1:])
                f.add_callbacks(ok, bad, callback_args=(i, row[3], t0), errback_args=(i, row[3], t0))
        lag.append((tick, round(time.time() - tick - 0.9, 3), 512 - sem._value))
        tick += 1
        if time.time() > tick + 1: late += 1
    for _ in range(512): sem.acquire()
    c.shutdown()
    return dict(ledger=ledger, lat={m: v.tobytes() for m, v in lat.items()}, done=done, err=err, late=late, lag=lag)

def reader(args):
    r, a, start = args
    c, s = connect(a.host); rnd = random.Random(r)
    p = dict(latest=s.prepare('SELECT * FROM %s.%s WHERE tag_id=? LIMIT 1' % (KS, T)),
             range10m=s.prepare('SELECT timestamp, value, value_numeric FROM %s.%s WHERE tag_id=? AND timestamp>=?' % (KS, T)),
             agg_all=s.prepare('SELECT count(*), avg(value_numeric), max(value_numeric) FROM %s.%s WHERE tag_id=? AND timestamp>=?' % (KS, T)),
             bucket1m=s.prepare("SELECT time_bucket(1m, timestamp), avg(value_numeric) FROM %s.%s WHERE tag_id=? AND timestamp>=? GROUP BY tag_id, time_bucket(1m, timestamp)" % (KS, T)))
    out = {k: array.array('f') for k in p}; err = {'n': 0, 'first': ''}; rows = 0
    numeric = [i for i in range(a.tags) if sw.kind_of(sw.tag_type(i)) == 'numeric']
    n = 0
    while time.time() < start + a.seconds:
        k = ('latest', 'range10m', 'agg_all', 'bucket1m')[n % 4]; n += 1
        tag = sw.tag_id(rnd.choice(numeric))
        args_ = (tag,) if k == 'latest' else (tag, dt(time.time() - 600 if k == 'range10m' else start - 1))
        t0 = time.time()
        try:
            rs = list(s.execute(p[k], args_)); out[k].append((time.time() - t0) * 1000); rows += len(rs)
        except Exception as e:
            err['n'] += 1; err['first'] = err['first'] or repr(e)[:300]
        time.sleep(max(0, 0.1 - (time.time() - t0)))
    c.shutdown()
    return dict(lat={k: v.tobytes() for k, v in out.items()}, err=err, rows=rows)

def pct(v, q): return round(v[min(len(v) - 1, int(len(v) * q))], 2) if v else None
def unpack(b):
    x = array.array('f'); x.frombytes(b); return x

def run(a):
    start = time.time() + 45
    wp = multiprocessing.Pool(a.writers); rp = multiprocessing.Pool(a.readers)
    wr = wp.map_async(writer, [(w, a, start) for w in range(a.writers)], chunksize=1)
    rr = rp.map_async(reader, [(r, a, start) for r in range(a.readers)], chunksize=1)
    W, R = wr.get(), rr.get()
    ledger = {}; lat = {}; done = {}
    for x in W:
        ledger.update(x['ledger'])
        for m, b in x['lat'].items(): lat.setdefault(m, array.array('f')).extend(unpack(b))
        for m, n in x['done'].items(): done[m] = done.get(m, 0) + n
    json.dump({str(k): v for k, v in ledger.items()}, open(a.out + '/ledger.json', 'w'))
    json.dump([x['lag'] for x in W], open(a.out + '/lag.json', 'w'))
    full = sorted(m for m in done if 0 < m < a.seconds // 60)
    per = [dict(minute=m, rows_s=round(done[m] / 60), p50=pct(sorted(lat[m]), .5), p99=pct(sorted(lat[m]), .99), max=round(max(lat[m]), 1)) for m in full]
    alll = sorted(x for m in full for x in lat[m])
    reads = {}
    for x in R:
        for k, b in x['lat'].items(): reads.setdefault(k, []).extend(unpack(b))
    res = dict(written=sum(v[0] for v in ledger.values()), write_errors=sum(x['err']['n'] for x in W), write_first=next((x['err']['first'] for x in W if x['err']['first']), ''),
               late_ticks=sum(x['late'] for x in W), min_rate=min(p['rows_s'] for p in per), avg_rate=round(sum(p['rows_s'] for p in per) / len(per)),
               write_ms=dict(p50=pct(alll, .5), p99=pct(alll, .99), p999=pct(alll, .999), max=round(alll[-1], 1)),
               reads={k: dict(n=len(v), p50=pct(sorted(v), .5), p99=pct(sorted(v), .99), max=round(max(v), 1)) for k, v in reads.items() if v},
               read_errors=sum(x['err']['n'] for x in R), read_first=next((x['err']['first'] for x in R if x['err']['first']), ''), read_rows=sum(x['rows'] for x in R), minutes=per)
    json.dump(res, open(a.out + '/run.json', 'w'), indent=1); print(json.dumps({k: v for k, v in res.items() if k != 'minutes'}))

def verify(a):
    ledger = json.load(open(a.out + '/ledger.json'))
    c, s = connect(a.host); s.default_timeout = 120
    q = s.prepare('SELECT count(*), sum(latency) FROM %s.%s WHERE tag_id=?' % (KS, T))
    st = s.prepare('SELECT DISTINCT tag_id, tag_name, type FROM %s.%s WHERE tag_id=?' % (KS, T))
    bad = []; rows = 0; lock = threading.Lock(); items = sorted(ledger.items(), key=lambda kv: int(kv[0]))
    def part(k):
        n = 0; b = []
        for i, (cnt, tot) in items[k::8]:
            i = int(i); r = s.execute(q, (sw.tag_id(i),)).one(); n += r[0]
            if r[0] != cnt or r[1] != tot: b.append((i, cnt, r[0], tot, r[1]))
            m = s.execute(st, (sw.tag_id(i),)).one()
            if m is None or m[1] != sw.statics_for(i)[6] or m[2] != sw.tag_type(i): b.append((i, 'static', m and m[1]))
        with lock:
            nonlocal rows
            rows += n; bad.extend(b)
    ts = [threading.Thread(target=part, args=(k,)) for k in range(8)]
    [t.start() for t in ts]; [t.join() for t in ts]
    res = dict(tags=len(items), rows=rows, expected=sum(v[0] for v in ledger.values()), mismatches=len(bad), sample=bad[:5])
    json.dump(res, open(a.out + '/verify.json', 'w')); print(json.dumps(res)); c.shutdown()

if __name__ == '__main__':
    p = argparse.ArgumentParser(); p.add_argument('cmd'); p.add_argument('--host'); p.add_argument('--seconds', type=int, default=1800)
    p.add_argument('--tags', type=int, default=10000); p.add_argument('--writers', type=int, default=10); p.add_argument('--readers', type=int, default=2); p.add_argument('--out', default='/out')
    a = p.parse_args(); (run if a.cmd == 'run' else verify)(a)
