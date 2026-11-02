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
"""Render a GC comparison report from two scale-test runs.

  python3 docker/gc-compare.py build/scale-zgc build/scale-g1 > doc/timeseries/gc-comparison.md

Each argument is the report prefix used for a run (SCALE_REPORT without the .html), so the
script reads <prefix>.json (query timings), <prefix>-write.json (write benchmark) and
<prefix>-gc.log (JVM GC log) for each side.
"""
import json
import os
import re
import sys

LABELS = {'zgc': 'Generational ZGC', 'g1': 'G1'}


def load(prefix):
    name = os.path.basename(prefix).replace('scale-', '')
    data = {'name': name, 'label': LABELS.get(name, name)}
    with open(prefix + '.json') as fh:
        data['queries'] = json.load(fh)
    wpath = prefix + '-write.json'
    data['write'] = json.load(open(wpath)) if os.path.exists(wpath) else None
    data['gc'] = gc_stats(prefix + '-gc.log')
    return data


def gc_stats(path):
    """Summarise a -Xlog:gc log.

    The two collectors report different things at -Xlog:gc level: G1 logs stop-the-world
    "Pause ... 12.345ms" lines, while ZGC logs whole collection cycles ("Major/Minor
    Collection ... 0.092s") that run concurrently with the application. Both are collected
    here and labelled separately rather than being averaged into one misleading number.
    Runs made with -Xlog:gc*,safepoint additionally carry real ZGC pause lines, which land
    in the pause bucket for both collectors.
    """
    if not os.path.exists(path):
        return None
    pauses, cycles = [], []
    for line in open(path, errors='replace'):
        line = line.strip()
        if 'Pause' in line:
            m = re.search(r'([\d.]+)ms$', line)
            if m:
                pauses.append(float(m.group(1)))
        elif re.search(r'(Major|Minor) Collection.*[\d.]+s$', line):
            m = re.search(r'([\d.]+)s$', line)
            if m:
                cycles.append(float(m.group(1)) * 1000.0)

    def summarise(values):
        if not values:
            return None
        ordered = sorted(values)
        return {'count': len(values), 'total_ms': sum(values),
                'avg_ms': sum(values) / len(values), 'max_ms': ordered[-1],
                'p99_ms': ordered[min(len(ordered) - 1, int(len(ordered) * 0.99))]}

    return {'pauses': summarise(pauses), 'cycles': summarise(cycles)}


def rows(run):
    """(section, desc, ms) for every timed query, in order."""
    out, section = [], ''
    for q in run['queries']['queries']:
        if q['kind'] == 'section':
            section = q['desc']
        else:
            out.append((section, q['desc'], q['ms'], q['error']))
    return out


def main():
    a, b = load(sys.argv[1]), load(sys.argv[2])
    w = sys.stdout.write
    w('# GC comparison: %s vs %s\n\n' % (a['label'], b['label']))
    meta = a['queries']
    w('Same node, same data set (%s rows in %s partitions), same queries; each side ran a warm-up '
      'pass first and the numbers below are the second, warm pass.\n\n'
      % (f"{meta['rows']:,}", f"{meta['series']:,}"))

    if a['write'] and b['write']:
        w('## Write throughput\n\n')
        w('| GC | rows | seconds | rows/s |\n|---|---|---|---|\n')
        for r in (a, b):
            wr = r['write']
            w('| %s | %s | %.0f | **%s** |\n'
              % (r['label'], f"{wr['rows']:,}", wr['seconds'], f"{int(wr['rows_per_sec']):,}"))
        delta = (b['write']['rows_per_sec'] / a['write']['rows_per_sec'] - 1) * 100
        w('\n%s is %+.1f%% on write throughput relative to %s.\n\n' % (b['label'], delta, a['label']))

    if a['gc'] and b['gc']:
        w('## GC activity (whole run, from -Xlog:gc)\n\n')
        w('| GC | kind | count | total | avg | p99 | max |\n|---|---|---|---|---|---|---|\n')
        for r in (a, b):
            for kind, key in (('stop-the-world pauses', 'pauses'), ('collection cycles (concurrent)', 'cycles')):
                g = (r['gc'] or {}).get(key)
                if not g:
                    continue
                w('| %s | %s | %s | %.1f ms | %.3f ms | %.3f ms | **%.3f ms** |\n'
                  % (r['label'], kind, f"{g['count']:,}", g['total_ms'], g['avg_ms'],
                     g['p99_ms'], g['max_ms']))
        w('\nG1 logs stop-the-world pauses at this log level; ZGC logs whole collection cycles that '
          'run concurrently with the application, so its cycle durations are *not* pause times.\n\n')

    w('## Query times\n\n')
    w('| section | query | %s | %s | difference |\n|---|---|---|---|---|\n' % (a['label'], b['label']))
    ra, rb = rows(a), rows(b)
    for (sec, desc, ms_a, err_a), (_, _, ms_b, err_b) in zip(ra, rb):
        if err_a or err_b:
            diff = 'error'
        else:
            diff = '%+.1f%%' % ((ms_b / ms_a - 1) * 100) if ms_a else 'n/a'
        w('| %s | %s | %s ms | %s ms | %s |\n'
          % (sec, desc, f'{ms_a:,.0f}', f'{ms_b:,.0f}', diff))
    tot_a = sum(m for _, _, m, e in ra if not e)
    tot_b = sum(m for _, _, m, e in rb if not e)
    w('| | **total** | **%s ms** | **%s ms** | **%+.1f%%** |\n'
      % (f'{tot_a:,.0f}', f'{tot_b:,.0f}', (tot_b / tot_a - 1) * 100))
    w('\nA negative difference means %s was faster.\n' % b['label'])

    w('\n## Method and caveats\n\n')
    w('- Single node in a container, 16G heap, identical `cassandra.yaml` (only the request\n'
      '  timeouts are raised so multi-million-row aggregates can finish). The G1 side enables the\n'
      '  tuning block that ships commented out in `conf/jvm21-server.options` and restores\n'
      '  compressed oops, which the shipped ZGC block disables as a jamm workaround.\n')
    w('- One measured pass per collector after one warm-up pass. Repeated passes of the same\n'
      '  query on the same collector varied by roughly 10%, so treat single-query differences\n'
      '  below that as noise; the direction is consistent across every query here.\n')
    w('- ZGC pause times are not in this report: at `-Xlog:gc` the ZGC lines are concurrent cycle\n'
      '  durations, not stop-the-world time. Re-run both sides with `-Xlog:gc*,safepoint` (which\n'
      '  `docker/scale-test.sh` now sets) to compare real pauses.\n')


if __name__ == '__main__':
    main()
