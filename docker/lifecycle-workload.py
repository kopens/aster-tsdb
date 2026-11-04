#!/usr/bin/env python3
# The whole TSCS life of a table, compressed: writes at a steady rate into 5-minute windows that freeze to tscol after 5
# minutes, expire by TTL at 20 and drop by retention at 30, with late overwrites and deletes that force refreezes.
# Every worker owns its tags and keeps a ledger of what it wrote, so a lost row, a resurrected one or a window that
# outlived its retention shows up as a count that does not match. Pauses while the control file exists (node restart).
import argparse, glob, json, multiprocessing, os, random, sys, time
for z in glob.glob('/opt/cassandra/lib/cassandra-driver-internal-only-*.zip'):
    v = os.path.basename(z)[len('cassandra-driver-internal-only-'):-len('.zip')]
    sys.path.insert(0, os.path.join(z, 'cassandra-driver-' + v))
for e in ('geomet-0.1.0.zip', 'futures-2.1.6-py2.py3-none-any.zip'):
    p = '/opt/cassandra/lib/' + e
    if os.path.exists(p): sys.path.insert(0, p)
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import SimpleStatement

TTL_TABLE, RET_TABLE = 'life.ttl_t', 'life.ret_t'
MIN = 60_000

def connect(host):
    while True:
        try:
            c = Cluster([host], connect_timeout=30)
            s = c.connect(); s.default_timeout = 60
            return c, s
        except Exception:
            time.sleep(3)

def worker(args):
    wid, a = args
    random.seed(wid)
    cluster, s = connect(a.host)
    ins = {t: s.prepare('INSERT INTO %s (tag_id, timestamp, value_numeric, quality) VALUES (?, ?, ?, 192) USING TIMESTAMP ?' % t) for t in (TTL_TABLE, RET_TABLE)}
    tags = ['w%d-tag-%03d' % (wid, i) for i in range(a.tags)]
    per_second = a.rate // a.workers
    ledger = {}                    # (table, tag, minute) -> rows that should be there
    samples = {}                   # (table, tag, minute) -> a few timestamps, for late writes
    overwritten = []               # (table, tag, ts_ms, minute)
    stats = dict(written=0, retries=0, late=0, deleted=0, checked=0, mismatches=0, expired_checked=0, resurrected=0, overwrite_lost=0, read_ms=[])
    last = {t: 0 for t in tags}
    end = time.time() + a.minutes * 60
    next_minute = time.time() + 60
    while time.time() < end:
        if os.path.exists(a.control):
            time.sleep(1); continue
        started = time.time()
        now = int(started * 1000)
        rows = {TTL_TABLE: [], RET_TABLE: []}
        for i in range(per_second):
            tag = tags[i % len(tags)]
            ts = max(now, last[tag] + 1); last[tag] = ts
            table = TTL_TABLE if i % 2 == 0 else RET_TABLE
            rows[table].append((tag, ts, random.random() * 100))
        for table, batch in rows.items():
            # The write timestamp is the row's own, so a late overwrite or delete a microsecond above it really is newer.
            # Whole-batch retry: the same rows at the same timestamps change nothing, and the driver can raise from
            # execute_concurrent itself while a node restarts, so no per-row result is trustworthy until all succeed.
            while True:
                try:
                    results = execute_concurrent_with_args(s, ins[table], [(t, dt(ts), v, ts * 1000) for t, ts, v in batch],
                                                           concurrency=64, raise_on_first_error=True)
                    break
                except Exception:
                    stats['retries'] += 1
                    time.sleep(1)
            for tag, ts, _v in batch:
                key = (table, tag, ts // MIN)
                ledger[key] = ledger.get(key, 0) + 1
                keep = samples.setdefault(key, [])
                if len(keep) < 4: keep.append(ts)
                stats['written'] += 1

        if time.time() >= next_minute:
            next_minute += 60
            minute_now = int(time.time() * 1000) // MIN
            try:
                late_and_delete(s, stats, ledger, samples, overwritten, minute_now)
                verify(s, a, stats, ledger, overwritten, minute_now)
            except Exception as e:
                stats['retries'] += 1
                print('worker %d check failed: %r' % (wid, e), flush=True)
            for key in [k for k in ledger if minute_now - k[2] > 50]:
                ledger.pop(key); samples.pop(key, None)
            print(json.dumps(dict(worker=wid, t=int(time.time()), **{k: v for k, v in stats.items() if k != 'read_ms'})), flush=True)
        time.sleep(max(0, 1 - (time.time() - started)))
    cluster.shutdown()
    stats['read_ms'] = sorted(stats['read_ms'])
    return stats

def dt(ms):
    import datetime
    return datetime.datetime.utcfromtimestamp(ms / 1000)

def late_and_delete(s, stats, ledger, samples, overwritten, minute_now):
    # A closed, frozen window: an overwrite and a delete at timestamps inside it make it refreeze.
    keys = [k for k in samples if 11 <= minute_now - k[2] <= 13 and len(samples[k]) >= 2]
    random.shuffle(keys)
    for key in keys[:3]:
        table, tag, minute = key
        ts = samples[key].pop()
        s.execute(SimpleStatement('UPDATE %s USING TIMESTAMP %d SET value_numeric = 999 WHERE tag_id = %%s AND timestamp = %%s' % (table, ts * 1000 + 1)), (tag, dt(ts)))
        overwritten.append((table, tag, ts, minute)); stats['late'] += 1
        ts = samples[key].pop()
        s.execute(SimpleStatement('DELETE FROM %s USING TIMESTAMP %d WHERE tag_id = %%s AND timestamp = %%s' % (table, ts * 1000 + 2)), (tag, dt(ts)))
        ledger[key] -= 1; stats['deleted'] += 1

def count(s, table, tag, minute):
    q = 'SELECT count(*) FROM %s WHERE tag_id = %%s AND timestamp >= %%s AND timestamp < %%s' % table
    return s.execute(SimpleStatement(q), (tag, dt(minute * MIN), dt((minute + 1) * MIN))).one()[0]

def verify(s, a, stats, ledger, overwritten, minute_now):
    live, gone = [], []
    # An overwrite gives its cell a fresh TTL from the moment it was written (age <= 13), so that key is only gone at 33.
    refreshed = {(o[0], o[1], o[3]) for o in overwritten}
    for key in ledger:
        table, _, minute = key
        age = minute_now - minute
        if 2 <= age <= (17 if table == TTL_TABLE else 27): live.append(key)
        # TTL 20m; retention drops a window once it starts more than retention + window_size ago.
        if age >= ((35 if key in refreshed else 23) if table == TTL_TABLE else 43): gone.append(key)
    for key in random.sample(live, min(12, len(live))):
        got = count(s, *key)
        stats['checked'] += 1
        if got != ledger[key]:
            stats['mismatches'] += 1
            print('MISMATCH %s expected %d got %d (age %d min)' % (key, ledger[key], got, minute_now - key[2]), flush=True)
    for key in random.sample(gone, min(6, len(gone))):
        got = count(s, *key)
        stats['expired_checked'] += 1
        if got != 0:
            stats['resurrected'] += 1
            print('NOT GONE %s still has %d rows (age %d min)' % (key, got, minute_now - key[2]), flush=True)
    for table, tag, ts, minute in [o for o in overwritten if 0 < minute_now - o[3] <= (17 if o[0] == TTL_TABLE else 27)][-5:]:
        row = s.execute(SimpleStatement('SELECT value_numeric FROM %s WHERE tag_id = %%s AND timestamp = %%s' % table), (tag, dt(ts))).one()
        if row is None or row[0] != 999:
            stats['overwrite_lost'] += 1
            print('OVERWRITE LOST %s %s %d: %r' % (table, tag, ts, row), flush=True)
    # The shape the screens use: one tag, a range across hot and frozen windows.
    t0 = time.time()
    tag = random.choice(list({k[1] for k in ledger}))
    n = sum(1 for _ in s.execute(SimpleStatement('SELECT timestamp, value_numeric, quality FROM %s WHERE tag_id = %%s AND timestamp >= %%s' % RET_TABLE, fetch_size=5000),
                                 (tag, dt((minute_now - 25) * MIN))))
    stats['read_ms'].append(int((time.time() - t0) * 1000))

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--host', default='127.0.0.1'); p.add_argument('--minutes', type=int, default=60)
    p.add_argument('--rate', type=int, default=10000); p.add_argument('--workers', type=int, default=8)
    p.add_argument('--tags', type=int, default=25); p.add_argument('--control', default='/ctl/pause')
    a = p.parse_args()
    with multiprocessing.Pool(a.workers) as pool:
        results = pool.map(worker, [(i, a) for i in range(a.workers)])
    total = {k: sum(r[k] for r in results) for k in results[0] if k != 'read_ms'}
    reads = sorted(x for r in results for x in r['read_ms'])
    total['read_ms_p50'] = reads[len(reads) // 2] if reads else None
    total['read_ms_max'] = reads[-1] if reads else None
    print('SUMMARY ' + json.dumps(total), flush=True)
