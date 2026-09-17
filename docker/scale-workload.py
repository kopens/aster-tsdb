#!/usr/bin/env python3
"""Loader + timed query workload for the aster-tsdb scale / tiering benchmark.

Models the *production* table shape (`tm_tag_point`): one wide row per sample with static
per-tag metadata, several columns that are constant in practice, one column that is entirely
null, one high-entropy int, one slowly-varying numeric-looking text reading and a state bit.
That mix -- not a single high-entropy double -- is what the columnar (v3) chunk format is
designed for, so it is the shape the tiering trade-off has to be measured on.

Runs *inside* the container (see docker/scale-test.sh) using the python driver that ships with
cqlsh, so the measured query times are the real client-observed CQL execution times without any
cqlsh/docker-exec startup in the way.

  python3 scale-workload.py ddl
  python3 scale-workload.py load  --rows 20000000 --series 500 --loaders 12
  python3 scale-workload.py query --rows 20000000 --series 500 > report.html
"""
import argparse
import datetime
import json
import glob
import html
import math
import multiprocessing
import os
import random
import sys
import time

# cqlsh ships the driver as a zip; mirror what bin/cqlsh.py does to import it.
for _zip in glob.glob('/opt/cassandra/lib/cassandra-driver-internal-only-*.zip'):
    _ver = os.path.basename(_zip)[len('cassandra-driver-internal-only-'):-len('.zip')]
    sys.path.insert(0, os.path.join(_zip, 'cassandra-driver-' + _ver))
for _extra in ('geomet-0.1.0.zip', 'futures-2.1.6-py2.py3-none-any.zip'):
    _p = os.path.join('/opt/cassandra/lib', _extra)
    if os.path.exists(_p):
        sys.path.insert(0, _p)

KEYSPACE = 'scale'
TABLE = 'tm_tag_point'     # overridden by --table
EPOCH = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
SAMPLE_SECONDS = 1         # one sample per tag per second
BATCH_ROWS = 100           # rows per unlogged batch (stays under batch_size_warn_threshold)
IN_FLIGHT = 96             # async batches in flight per loader process

# Constant-in-production values. These are what make the CONSTANT / ALL_NULL encodings pay:
# each costs a full cell (value + per-cell metadata) per row in the base table and O(1) bytes
# per chunk once tiered.
QUALITY_GOOD = 192
ERROR_CODE_NONE = 0
ATTRIBUTE_EMPTY = {}       # frozen<map<text,text>>, empty in every production row

# ---------------------------------------------------------------------------------------------
# PRODUCTION VALUE DISTRIBUTION (measured read-only on the production cluster, 3,000 distinct
# tags sampled with PER PARTITION LIMIT 1).
#
# The static `type` column decides WHICH value column carries the reading. Because `type` is
# static it is fixed per tag, so within one chunk (one tag x one window) each value column is
# uniformly populated or uniformly absent -- never mixed. A chunk's value_numeric is therefore
# either fully populated (Chimp128) or entirely absent (ALL_NULL, 0 bytes), and the *mix of
# tags* is what sets the real compression ratio.
#
#     boolean 79.2%   int 8.3%   double 6.4%   long 2.8%   string 2.7%   float 0.5%
#
# Per-type population, verified by sampling real rows:
#     type=boolean : value_boolean = true/false | value_numeric = null | value = "true"/"false"
#     type=long    : value_numeric = 157        | value_boolean = null | value = "157"
#     type=double  : value_numeric = 20.76      | value_boolean = null | value = "20.76"
# i.e. `value` (text) is a REDUNDANT STRING MIRROR of whichever typed column holds the reading.
# The same information is stored twice, once typed and once as text; that is a real property of
# this schema and it matters for the compression number.
#
# type=string (2.7%) was NOT verified against production -- it is modelled here as `value` = the
# text reading with both typed columns null, and the report says so.
TYPE_WEIGHTS_PER_MILLE = (('boolean', 793), ('int', 83), ('double', 64),
                          ('long', 28), ('string', 27), ('float', 5))
NUMERIC_TYPES = frozenset(('int', 'long', 'float', 'double'))
# ---------------------------------------------------------------------------------------------

_TYPE_BY_SLOT = []
for _t, _w in TYPE_WEIGHTS_PER_MILLE:
    _TYPE_BY_SLOT.extend([_t] * _w)
assert len(_TYPE_BY_SLOT) == 1000, len(_TYPE_BY_SLOT)


def tag_type(i):
    """The static `type` of tag i. Multiplying by a large coprime spreads the types across
    consecutive tag indices, so any contiguous slice of tags (e.g. the 100 the multi-partition
    queries hit) carries a representative mix rather than one solid block of booleans."""
    return _TYPE_BY_SLOT[(i * 7919) % 1000]

DDL = """
CREATE KEYSPACE IF NOT EXISTS {ks} WITH replication =
    {{'class':'SimpleStrategy','replication_factor':1}};
CREATE TABLE IF NOT EXISTS {ks}.{tbl} (
    tag_id text,
    timestamp timestamp,
    area_id text static,
    asset_id text static,
    line_id text static,
    opc_id text static,
    site_id text static,
    tag_name text static,
    type text static,
    attribute frozen<map<text,text>>,
    error_code int,
    latency int,
    quality int,
    value text,
    value_boolean boolean,
    value_numeric double,
    PRIMARY KEY (tag_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
   AND compaction = {{'class':'UnifiedCompactionStrategy',
                     'scaling_parameters':'T4',
                     'target_sstable_size':'1GiB',
                     'expired_sstable_check_frequency_seconds':600}};
"""

INSERT_STATICS = ("INSERT INTO %s.%s (tag_id, area_id, asset_id, line_id, opc_id, site_id, "
                  "tag_name, type) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
_COMMON = "tag_id, timestamp, attribute, error_code, latency, quality, value"
# One prepared statement per type family, so a column that production leaves absent is genuinely
# absent. Binding None would write a NULL *tombstone* instead -- that is not what production does
# and it would put a per-row deletion into every chunk's ALL_NULL column.
INSERT_BY_KIND = {
    'boolean': "INSERT INTO %s.%s (" + _COMMON + ", value_boolean) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    'numeric': "INSERT INTO %s.%s (" + _COMMON + ", value_numeric) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    'string': "INSERT INTO %s.%s (" + _COMMON + ") VALUES (?, ?, ?, ?, ?, ?, ?)",
}

SITES = ['seoul', 'busan', 'ulsan', 'gwangju']
AREAS = ['area-a', 'area-b', 'area-c']
LINES = ['line-01', 'line-02', 'line-03', 'line-04', 'line-05']


def connect(timeout=900):
    from cassandra.cluster import Cluster
    cluster = Cluster(['127.0.0.1'], connect_timeout=60)
    session = cluster.connect()
    session.default_timeout = timeout
    return cluster, session


def tag_id(i):
    return 'tag-%06d' % i


def statics_for(i):
    return (tag_id(i),
            AREAS[i % len(AREAS)],
            'asset-%04d' % (i % 250),
            LINES[i % len(LINES)],
            'opc-%02d' % (i % 8),
            SITES[i % len(SITES)],
            'PLANT/%s/%s/TAG_%06d' % (SITES[i % len(SITES)], LINES[i % len(LINES)], i),
            tag_type(i))


def kind_of(tag_type_name):
    """Which prepared statement (and therefore which value column) a tag's static type selects."""
    if tag_type_name == 'boolean':
        return 'boolean'
    return 'numeric' if tag_type_name in NUMERIC_TYPES else 'string'


def partition_rows(i, rows_per_series):
    """Yield the per-row values of tag i, reproducing the measured production distribution.

    Constant in every row and every type: quality=192, error_code=0, attribute={}.
    latency is a high-entropy small int, present for every type.

    The reading itself is a *slowly varying* signal (a bounded random walk -- a real sensor, not
    white noise), and which column carries it is decided by the tag's static `type`:
      boolean  -> value_boolean, a state bit that flips every few hundred samples
      numeric  -> value_numeric (double); int/long are integer-valued, double/float have decimals
      string   -> neither typed column (assumed, not verified against production)
    `value` (text) always mirrors the reading as a string, exactly as production does.

    Yields (timestamp, attribute, error_code, latency, quality, value, extra) where `extra` is
    the value_boolean / value_numeric argument, or absent for string-typed tags.
    """
    kind_name = tag_type(i)
    kind = kind_of(kind_name)
    rnd = random.Random(0x5EED ^ (i * 2654435761))
    flag = bool(i & 1)
    # int/long read as whole numbers; double/float and the text-only case carry decimals.
    integral = kind_name in ('int', 'long')
    reading = float(rnd.randrange(10, 90)) if integral else 10.0 + rnd.random() * 80.0
    steps = (-1.0, 0.0, 0.0, 1.0) if integral else (-0.01, 0.0, 0.0, 0.01)
    lo, hi = (0.0, 1000.0) if integral else (0.0, 100.0)
    for n in range(rows_per_series):
        reading += steps[rnd.getrandbits(2)]
        if reading < lo:
            reading = lo
        elif reading > hi:
            reading = hi
        if n % 500 == 499:
            flag = not flag
        if kind == 'boolean':
            text, extra = ('true', True) if flag else ('false', False)
        elif integral:
            text, extra = '%d' % reading, reading
        else:
            text, extra = '%.2f' % reading, round(reading, 2)
        row = (EPOCH + datetime.timedelta(seconds=n * SAMPLE_SECONDS),
               ATTRIBUTE_EMPTY,
               ERROR_CODE_NONE,
               rnd.randint(1, 999),
               QUALITY_GOOD,
               text)
        yield row if kind == 'string' else row + (extra,)


def load_slice(args):
    from cassandra.query import BatchStatement, BatchType
    worker, total_series, rows_per_series, table = args
    loaders = LOADERS
    cluster, session = connect()
    statics = session.prepare(INSERT_STATICS % (KEYSPACE, table))
    inserts = {k: session.prepare(q % (KEYSPACE, table)) for k, q in INSERT_BY_KIND.items()}
    started = time.time()
    for s in range(worker, total_series, loaders):
        name = tag_id(s)
        session.execute(statics, statics_for(s))
        insert = inserts[kind_of(tag_type(s))]
        pending = []
        batch = BatchStatement(batch_type=BatchType.UNLOGGED)
        n = 0
        for row in partition_rows(s, rows_per_series):
            batch.add(insert, (name,) + row)
            n += 1
            if n == BATCH_ROWS:
                pending.append(session.execute_async(batch))
                batch = BatchStatement(batch_type=BatchType.UNLOGGED)
                n = 0
                if len(pending) >= IN_FLIGHT:
                    for f in pending:
                        f.result()
                    pending = []
        if n:
            pending.append(session.execute_async(batch))
        for f in pending:
            f.result()
        if worker == 0:
            done = (s // loaders) + 1
            per = (time.time() - started) or 1
            sys.stderr.write('   loader0: %d/%d partitions, %.0f rows/s (this worker)\n'
                             % (done, math.ceil(total_series / loaders),
                                done * rows_per_series / per))
            sys.stderr.flush()
    cluster.shutdown()
    return worker


def cmd_ddl(a):
    sys.stdout.write(DDL.format(ks=KEYSPACE, tbl=a.table))


def cmd_load(a):
    global LOADERS
    LOADERS = a.loaders
    rows_per_series = a.rows // a.series
    print('   %d tags x %d rows = %d rows (1 sample / %ds / tag)'
          % (a.series, rows_per_series, a.series * rows_per_series, SAMPLE_SECONDS))
    started = time.time()
    with multiprocessing.Pool(a.loaders, initializer=_init_loaders, initargs=(a.loaders,)) as pool:
        pool.map(load_slice, [(w, a.series, rows_per_series, a.table) for w in range(a.loaders)])
    elapsed = time.time() - started
    total = a.series * rows_per_series
    print('   loaded %d rows in %.0fs (%.0f rows/s)' % (total, elapsed, total / elapsed))
    if a.json_out:
        with open(a.json_out, 'w') as fh:
            json.dump({'rows': total, 'seconds': elapsed, 'rows_per_sec': total / elapsed}, fh)


def _init_loaders(n):
    global LOADERS
    LOADERS = n


# --------------------------------------------------------------------------- query phase

def fmt_rows(rows, limit=6, width=26):
    if not rows:
        return '(0 rows)'
    out = []
    cols = rows[0]._fields
    out.append(' | '.join(cols))
    out.append('-+-'.join('-' * len(c) for c in cols))
    for r in rows[:limit]:
        vals = []
        for v in r:
            if isinstance(v, float):
                s = '%.6g' % v
            elif isinstance(v, datetime.datetime):
                s = v.strftime('%Y-%m-%d %H:%M:%S%z')
            else:
                s = str(v)
            vals.append(s if len(s) <= width else s[:width - 1] + '~')
        out.append(' | '.join(vals))
    if len(rows) > limit:
        out.append('... (%d rows total)' % len(rows))
    else:
        out.append('(%d rows)' % len(rows))
    return '\n'.join(out)


def timed(session, cql):
    started = time.perf_counter()
    try:
        rows = list(session.execute(cql))
        ms = (time.perf_counter() - started) * 1000.0
        return ms, fmt_rows(rows), None
    except Exception as exc:                                  # noqa: BLE001 - reported in the HTML
        ms = (time.perf_counter() - started) * 1000.0
        return ms, '', '%s: %s' % (type(exc).__name__, exc)


def first_tag_of_kind(kind, total_series):
    for i in range(total_series):
        if kind_of(tag_type(i)) == kind:
            return i
    return 0


def build_plan(a, rows_per_series):
    # Aggregates run on value_numeric -- the actual sensor reading. It is populated only when the
    # tag's static `type` is numeric (int/long/float/double), which is ~18% of tags, so every
    # aggregate query is aimed at numeric-typed tags. `latency` (collection latency, a metadata
    # column present for every type) is measured once, as a secondary comparison, not as the
    # headline. Row-shaped queries are type-independent and cover both a boolean and a numeric tag.
    numeric = [i for i in range(a.series) if kind_of(tag_type(i)) == 'numeric']
    if not numeric:
        numeric = [0]
    agg_i = numeric[0]
    agg = tag_id(agg_i)
    agg_kind = tag_type(agg_i)
    bool_i = first_tag_of_kind('boolean', a.series)
    bool_tag = tag_id(bool_i)
    bool_kind = tag_type(bool_i)
    ten_ids = numeric[:10]
    hundred_ids = numeric[:100]
    ten = ', '.join("'%s'" % tag_id(i) for i in ten_ids)
    hundred = ', '.join("'%s'" % tag_id(i) for i in hundred_ids)
    # int/long tags walk over 0..1000; double/float tags over 0..100.
    hist_hi = 1000 if agg_kind in ('int', 'long') else 100
    span = datetime.timedelta(seconds=rows_per_series * SAMPLE_SECONDS)
    fmt = '%Y-%m-%d %H:%M:%S+0000'
    t0 = EPOCH.strftime(fmt)
    t1 = (EPOCH + span).strftime(fmt)
    t1h = (EPOCH + datetime.timedelta(hours=1)).strftime(fmt)
    t6h = (EPOCH + datetime.timedelta(hours=6)).strftime(fmt)
    tbl = '%s.%s' % (KEYSPACE, a.table)
    hours = max(1, int(span.total_seconds() // 3600))
    scan1 = rows_per_series
    return [
        ("single partition, aggregates over value_numeric (%s, type='%s', %s rows scanned)"
         % (agg, agg_kind, f'{scan1:,}'), [
            ("count(*) [%s rows]" % f'{scan1:,}',
             "SELECT count(*) FROM %s WHERE tag_id='%s';" % (tbl, agg)),
            ("time_bucket 1h + avg/min/max(value_numeric) [%s rows]" % f'{scan1:,}',
             "SELECT time_bucket(1h, timestamp), avg(value_numeric), min(value_numeric), "
             "max(value_numeric) FROM %s WHERE tag_id='%s' "
             "GROUP BY tag_id, time_bucket(1h, timestamp);" % (tbl, agg)),
            ("time_bucket 5m + avg(value_numeric) [%s rows]" % f'{scan1:,}',
             "SELECT time_bucket(5m, timestamp), avg(value_numeric) FROM %s WHERE tag_id='%s' "
             "GROUP BY tag_id, time_bucket(5m, timestamp);" % (tbl, agg)),
            ("first/last/delta/rate(value_numeric) per hour [%s rows]" % f'{scan1:,}',
             "SELECT time_bucket(1h, timestamp), first(value_numeric, timestamp), "
             "last(value_numeric, timestamp), delta(value_numeric, timestamp), "
             "rate(value_numeric, timestamp) FROM %s WHERE tag_id='%s' "
             "GROUP BY tag_id, time_bucket(1h, timestamp);" % (tbl, agg)),
            ("derivative(value_numeric) per hour [%s rows]" % f'{scan1:,}',
             "SELECT time_bucket(1h, timestamp), derivative(value_numeric, timestamp) FROM %s "
             "WHERE tag_id='%s' GROUP BY tag_id, time_bucket(1h, timestamp);" % (tbl, agg)),
            ("percentile p50/p95/p99(value_numeric) [%s rows]" % f'{scan1:,}',
             "SELECT percentile(value_numeric, 0.5), percentile(value_numeric, 0.95), "
             "percentile(value_numeric, 0.99) FROM %s WHERE tag_id='%s';" % (tbl, agg)),
            ("variance/stddev(value_numeric) [%s rows]" % f'{scan1:,}',
             "SELECT variance(value_numeric), stddev(value_numeric) FROM %s WHERE tag_id='%s';"
             % (tbl, agg)),
            ("histogram(value_numeric, 0, %d, 20) [%s rows]" % (hist_hi, f'{scan1:,}'),
             "SELECT histogram(value_numeric, 0, %d, 20) FROM %s WHERE tag_id='%s';"
             % (hist_hi, tbl, agg)),
            ("approx_count_distinct(value_numeric) [%s rows]" % f'{scan1:,}',
             "SELECT approx_count_distinct(value_numeric) FROM %s WHERE tag_id='%s';" % (tbl, agg)),
            ("integral + time_weighted_average(value_numeric) [%s rows]" % f'{scan1:,}',
             "SELECT integral(value_numeric, timestamp), "
             "time_weighted_average(value_numeric, timestamp) FROM %s WHERE tag_id='%s';"
             % (tbl, agg)),
            ("SECONDARY: time_bucket 1h + avg/min/max(latency) [%s rows]" % f'{scan1:,}',
             "SELECT time_bucket(1h, timestamp), avg(latency), min(latency), max(latency) FROM %s "
             "WHERE tag_id='%s' GROUP BY tag_id, time_bucket(1h, timestamp);" % (tbl, agg)),
        ]),
        # Row reads are where the wide shape actually costs something: a tiered read has to rebuild
        # all 8 regular columns out of one chunk payload, and a projection should be able to skip
        # the columns it did not ask for.
        ('row reads (all 16 columns vs. projections) -- type-independent', [
            ("newest 1000 rows, SELECT * -- type='%s' tag (%s)" % (bool_kind, bool_tag),
             "SELECT * FROM %s WHERE tag_id='%s' LIMIT 1000;" % (tbl, bool_tag)),
            ("newest 1000 rows, SELECT * -- type='%s' tag (%s)" % (agg_kind, agg),
             "SELECT * FROM %s WHERE tag_id='%s' LIMIT 1000;" % (tbl, agg)),
            ("1 hour of rows, SELECT * (all columns) [3,600 rows]",
             "SELECT * FROM %s WHERE tag_id='%s' AND timestamp >= '%s' AND timestamp < '%s';"
             % (tbl, agg, t0, t1h)),
            ("1 hour of rows, project timestamp+value only [3,600 rows]",
             "SELECT timestamp, value FROM %s WHERE tag_id='%s' AND timestamp >= '%s' "
             "AND timestamp < '%s';" % (tbl, agg, t0, t1h)),
            ("1 hour of rows, project timestamp+value_numeric only [3,600 rows]",
             "SELECT timestamp, value_numeric FROM %s WHERE tag_id='%s' AND timestamp >= '%s' "
             "AND timestamp < '%s';" % (tbl, agg, t0, t1h)),
            ("static columns only (1 row)",
             "SELECT tag_id, site_id, area_id, line_id, asset_id, opc_id, tag_name, type "
             "FROM %s WHERE tag_id='%s' LIMIT 1;" % (tbl, agg)),
            ("column presence, type='%s' tag [%s rows]" % (bool_kind, f'{scan1:,}'),
             "SELECT count(latency) AS latency, count(value) AS value, "
             "count(value_numeric) AS value_numeric, count(value_boolean) AS value_boolean, "
             "count(quality) AS quality, count(attribute) AS attribute "
             "FROM %s WHERE tag_id='%s';" % (tbl, bool_tag)),
            ("column presence, type='%s' tag [%s rows]" % (agg_kind, f'{scan1:,}'),
             "SELECT count(latency) AS latency, count(value) AS value, "
             "count(value_numeric) AS value_numeric, count(value_boolean) AS value_boolean, "
             "count(quality) AS quality, count(attribute) AS attribute "
             "FROM %s WHERE tag_id='%s';" % (tbl, agg)),
        ]),
        # ORDER BY timestamp ASC: the table is clustered DESC (production idiom), and the gap-fill
        # densifier walks buckets forwards, so ascending order has to be asked for explicitly.
        ('gap-fill (locf/interpolate over avg(value_numeric), numeric tag %s)' % agg, [
            ("gapfill 1h + locf over the full %dh span [%s rows]" % (hours, f'{scan1:,}'),
             "SELECT time_bucket_gapfill(1h, timestamp, '%s', '%s'), locf(avg(value_numeric)) "
             "FROM %s WHERE tag_id='%s' "
             "GROUP BY tag_id, time_bucket_gapfill(1h, timestamp, '%s', '%s') "
             "ORDER BY timestamp ASC;" % (t0, t1, tbl, agg, t0, t1)),
            ("gapfill 5m + interpolate over 6h [21,600 rows]",
             "SELECT time_bucket_gapfill(5m, timestamp, '%s', '%s'), "
             "interpolate(avg(value_numeric)) FROM %s WHERE tag_id='%s' "
             "AND timestamp >= '%s' AND timestamp < '%s' "
             "GROUP BY tag_id, time_bucket_gapfill(5m, timestamp, '%s', '%s') "
             "ORDER BY timestamp ASC;" % (t0, t6h, tbl, agg, t0, t6h, t0, t6h)),
        ]),
        ('multi-partition (numeric-typed tags only -- %d of %d tags are numeric)'
         % (len(numeric), a.series), [
            ("%d numeric tags, hourly avg(value_numeric) [%s rows]"
             % (len(ten_ids), f'{len(ten_ids) * rows_per_series:,}'),
             "SELECT tag_id, time_bucket(1h, timestamp), avg(value_numeric) FROM %s "
             "WHERE tag_id IN (%s) GROUP BY tag_id, time_bucket(1h, timestamp);" % (tbl, ten)),
            ("%d numeric tags, hourly avg(value_numeric) [%s rows]"
             % (len(hundred_ids), f'{len(hundred_ids) * rows_per_series:,}'),
             "SELECT tag_id, time_bucket(1h, timestamp), avg(value_numeric) FROM %s "
             "WHERE tag_id IN (%s) GROUP BY tag_id, time_bucket(1h, timestamp);" % (tbl, hundred)),
            ("%d numeric tags, p95(value_numeric) per tag [%s rows]"
             % (len(hundred_ids), f'{len(hundred_ids) * rows_per_series:,}'),
             "SELECT tag_id, percentile(value_numeric, 0.95) FROM %s WHERE tag_id IN (%s) "
             "GROUP BY tag_id;" % (tbl, hundred)),
        ]),
        ('dashboard query (numeric tag %s)' % agg, [
            ("OHLC + change + p95 of value_numeric per hour [%s rows]" % f'{scan1:,}',
             "SELECT time_bucket(1h, timestamp) AS bucket, count(value_numeric) AS samples, "
             "first(value_numeric, timestamp) AS open, last(value_numeric, timestamp) AS close, "
             "min(value_numeric) AS low, max(value_numeric) AS high, avg(value_numeric) AS mean, "
             "delta(value_numeric, timestamp) AS change, "
             "rate(value_numeric, timestamp) AS per_second, "
             "percentile(value_numeric, 0.95) AS p95 FROM %s WHERE tag_id='%s' "
             "GROUP BY tag_id, time_bucket(1h, timestamp);" % (tbl, agg)),
        ]),
        # KNOWN LIMITATION, not a benchmark result: a range scan does not merge chunk data back in,
        # so on a fully tiered table this returns only the surviving static rows. A near-instant
        # answer below is a WRONG ANSWER, never a speedup.
        ('full table scan (%s rows) -- WRONG ANSWER on a tiered table (known limitation)'
         % f'{a.rows:,}', [
            ("count(*) over the whole table",
             "SELECT count(*) FROM %s;" % tbl),
        ]),
    ]


def cmd_query(a):
    rows_per_series = a.rows // a.series
    plan = build_plan(a, rows_per_series)
    cluster, session = connect()
    results = []
    for section, items in plan:
        results.append(('section', section, '', 0.0, None))
        for desc, cql in items:
            sys.stderr.write('   timing: %s\n' % desc)
            sys.stderr.flush()
            ms, out, err = timed(session, cql)
            sys.stderr.write('      %.0f ms\n' % ms)
            sys.stderr.flush()
            results.append(('row', desc, cql, ms, (out, err)))
    cluster.shutdown()
    emit_html(a, results, rows_per_series)
    if a.json_out:
        with open(a.json_out, 'w') as fh:
            json.dump({'image': a.image, 'rows': a.rows, 'series': a.series,
                       'table': a.table, 'rows_per_series': rows_per_series,
                       'sample_seconds': SAMPLE_SECONDS,
                       'load_secs': a.load_secs,
                       'queries': [{'section': None if k == 'row' else d,
                                    'desc': d, 'cql': c, 'ms': ms,
                                    'result': (p[0] if p else ''), 'error': (p[1] if p else None),
                                    'kind': k}
                                   for k, d, c, ms, p in results]}, fh, indent=1)
    if a.md_out:
        with open(a.md_out, 'w') as fh:
            emit_md(a, results, rows_per_series, fh)
        sys.stderr.write('   markdown report written to %s\n' % a.md_out)


def headline(a, rows_per_series):
    total = a.series * rows_per_series
    hours = rows_per_series * SAMPLE_SECONDS / 3600.0
    return total, hours


def emit_html(a, results, rows_per_series):
    total, hours = headline(a, rows_per_series)
    thr = total / a.load_secs if a.load_secs else 0
    w = sys.stdout.write
    w("""<!doctype html>
<meta charset="utf-8">
<title>aster-tsdb · tm_tag_point scale test</title>
<style>
  :root { color-scheme: light dark; --bg:#fff; --fg:#1a1a1a; --muted:#666; --line:#e3e3e3;
          --accent:#0a6cbd; --warn:#c62828; --code:#f6f7f9; }
  @media (prefers-color-scheme: dark) {
    :root { --bg:#14161a; --fg:#e6e6e6; --muted:#9aa0a6; --line:#2c3036;
            --accent:#6cb6ff; --warn:#ff6b6b; --code:#1c1f24; }
  }
  body { background:var(--bg); color:var(--fg); margin:0 auto; padding:2rem 1.25rem; max-width:1180px;
         font:14px/1.5 ui-sans-serif,-apple-system,"Segoe UI",Roboto,sans-serif; }
  h1 { font-size:1.35rem; margin:0 0 .25rem; }
  .meta { color:var(--muted); font-size:.85rem; margin-bottom:1rem; }
  .meta code { background:var(--code); padding:.1rem .35rem; border-radius:4px; }
  .cards { display:flex; flex-wrap:wrap; gap:1rem; margin:1rem 0 1.5rem; }
  .card { background:var(--code); border-radius:8px; padding:.8rem 1.1rem; min-width:150px; }
  .card b { display:block; font-size:1.45rem; line-height:1.2; }
  .card span { color:var(--muted); font-size:.8rem; }
  table { border-collapse:collapse; width:100%; }
  td, th { border-top:1px solid var(--line); padding:.55rem .6rem; vertical-align:top; text-align:left; }
  th { color:var(--muted); font-weight:600; font-size:.8rem; text-transform:uppercase; letter-spacing:.04em; }
  tr.section td { background:var(--code); font-weight:600; }
  td.ms { white-space:nowrap; font-variant-numeric:tabular-nums; font-weight:600; color:var(--accent); text-align:right; }
  td.err { color:var(--warn); }
  pre { margin:0; background:var(--code); padding:.5rem .6rem; border-radius:6px; overflow-x:auto;
        font:12px/1.45 ui-monospace,SFMono-Regular,Menlo,monospace; white-space:pre; }
  td:nth-child(2) { width:30%; } td:nth-child(3) { width:34%; }
</style>
""")
    w('<h1>tm_tag_point scale test</h1>\n')
    w('<div class="meta">image <code>%s</code> · table <code>%s.%s</code> · single node in a '
      'container · %s</div>\n'
      % (html.escape(a.image), KEYSPACE, html.escape(a.table),
         time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())))
    w('<div class="cards">')
    w('<div class="card"><b>%s</b><span>rows loaded</span></div>' % f'{total:,}')
    w('<div class="card"><b>%s</b><span>tags (partitions)</span></div>' % f'{a.series:,}')
    w('<div class="card"><b>%s</b><span>rows per tag (%.1f h @ 1 sample/%ds)</span></div>'
      % (f'{rows_per_series:,}', hours, SAMPLE_SECONDS))
    w('<div class="card"><b>%s s</b><span>load time (%s rows/s)</span></div>'
      % (f'{a.load_secs:,}', f'{int(thr):,}'))
    w('</div>\n')
    w('<table><tr><th>assertion</th><th>query</th><th>result</th><th>CQL time</th></tr>\n')
    for kind, desc, cql, ms, payload in results:
        if kind == 'section':
            w('<tr class="section"><td colspan="4">%s</td></tr>\n' % html.escape(desc))
            continue
        out, err = payload
        body = ('<td class="err"><pre>%s</pre></td>' % html.escape(err)) if err \
            else ('<td><pre>%s</pre></td>' % html.escape(out))
        w('<tr><td>%s</td><td><pre>%s</pre></td>%s<td class="ms">%s ms</td></tr>\n'
          % (html.escape(desc), html.escape(cql), body, f'{ms:,.0f}'))
    w('</table>\n')


def emit_md(a, results, rows_per_series, out):
    total, hours = headline(a, rows_per_series)
    thr = total / a.load_secs if a.load_secs else 0
    w = out.write
    w('# tm_tag_point scale test\n\n')
    w('image `%s` · table `%s.%s` · single node in a container · %s\n\n'
      % (a.image, KEYSPACE, a.table, time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())))
    w('- **%s** rows loaded\n' % f'{total:,}')
    w('- **%s** tags (partitions), **%s rows per tag** = %.1f h of history at 1 sample/%ds\n'
      % (f'{a.series:,}', f'{rows_per_series:,}', hours, SAMPLE_SECONDS))
    w('- **%s s** load time (%s rows/s)\n' % (f'{a.load_secs:,}', f'{int(thr):,}'))
    w('\n## Query times\n\n| section | query | first result row | CQL time |\n|---|---|---|---|\n')
    section = ''
    for kind, desc, cql, ms, payload in results:
        if kind == 'section':
            section = desc
            continue
        out_text, err = payload
        lines = (err or out_text).splitlines()
        value = lines[2] if len(lines) > 2 else (lines[0] if lines else '')
        w('| %s | %s | `%s` | **%s ms** |\n'
          % (section, desc, value.replace('|', '\\|')[:70], f'{ms:,.0f}'))
    w('\n## Details\n')
    section = ''
    for kind, desc, cql, ms, payload in results:
        if kind == 'section':
            section = desc
            w('\n### %s\n' % section)
            continue
        out_text, err = payload
        w('\n**%s** — `%s ms`\n' % (desc, f'{ms:,.0f}'))
        w('\n```sql\n%s\n```\n' % cql)
        w('\n```\n%s\n```\n' % (err or out_text))


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest='cmd', required=True)
    for name in ('ddl', 'load', 'query'):
        s = sub.add_parser(name)
        s.add_argument('--rows', type=int, default=20_000_000)
        s.add_argument('--series', type=int, default=500)
        s.add_argument('--loaders', type=int, default=12)
        s.add_argument('--load-secs', type=int, default=0)
        s.add_argument('--image', default='aster-tsdb:6.0.0')
        s.add_argument('--md-out', default='', help='also write a Markdown report to this path')
        s.add_argument('--json-out', default='', help='also write machine-readable results here')
        s.add_argument('--table', default=TABLE, help='table to load into / query')
    a = p.parse_args()
    {'ddl': cmd_ddl, 'load': cmd_load, 'query': cmd_query}[a.cmd](a)


if __name__ == '__main__':
    main()
