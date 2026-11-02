<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# GC comparison: Generational ZGC vs G1

> 측정 시점: 2026-07-30, 구형 24코어 컨테이너(X5670급), v3 청크 시절. **GC 간 상대 비교**로는
> 여전히 유효하지만 절대 수치는 현재 기준이 아니다 — 현행 수치는
> [tiering-benchmark.md](tiering-benchmark.md) 참조. 결론(Generational ZGC 채택)은 현행 설정이다.

Same node, same data set (100,000,000 rows in 1,000 partitions), same queries; each side ran a warm-up pass first and the numbers below are the second, warm pass.

## Write throughput

| GC | rows | seconds | rows/s |
|---|---|---|---|
| Generational ZGC | 10,000,000 | 34 | **296,636** |
| G1 | 10,000,000 | 35 | **287,793** |

G1 is -3.0% on write throughput relative to Generational ZGC.

## GC activity (whole run, from -Xlog:gc)

| GC | kind | count | total | avg | p99 | max |
|---|---|---|---|---|---|---|
| Generational ZGC | collection cycles (concurrent) | 75 | 290960.0 ms | 3879.467 ms | 18668.000 ms | **18668.000 ms** |
| G1 | stop-the-world pauses | 31 | 1477.5 ms | 47.660 ms | 459.059 ms | **459.059 ms** |

G1 logs stop-the-world pauses at this log level; ZGC logs whole collection cycles that run concurrently with the application, so its cycle durations are *not* pause times.

## Query times

| section | query | Generational ZGC | G1 | difference |
|---|---|---|---|---|
| single partition (100000 rows) | count(*) | 227 ms | 301 ms | +32.3% |
| single partition (100000 rows) | time_bucket 1h + avg/min/max | 350 ms | 462 ms | +32.2% |
| single partition (100000 rows) | time_bucket 5m + avg | 334 ms | 436 ms | +30.5% |
| single partition (100000 rows) | first/last/delta/rate per hour | 375 ms | 499 ms | +32.9% |
| single partition (100000 rows) | derivative per hour | 338 ms | 431 ms | +27.6% |
| single partition (100000 rows) | percentile p50/p95/p99 (whole partition) | 306 ms | 377 ms | +23.3% |
| single partition (100000 rows) | variance/stddev (whole partition) | 265 ms | 326 ms | +22.7% |
| single partition (100000 rows) | histogram 20 buckets | 274 ms | 329 ms | +20.2% |
| single partition (100000 rows) | approx_count_distinct | 280 ms | 333 ms | +18.9% |
| single partition (100000 rows) | integral + time_weighted_average | 287 ms | 346 ms | +20.8% |
| gap-fill | gapfill 1h + locf over the full span | 376 ms | 431 ms | +14.7% |
| gap-fill | gapfill 5m + interpolate over 6h | 91 ms | 99 ms | +8.9% |
| multi-partition | 10 series, hourly avg (1000000 rows) | 3,362 ms | 4,268 ms | +27.0% |
| multi-partition | 100 series, hourly avg (10000000 rows) | 33,333 ms | 44,303 ms | +32.9% |
| multi-partition | 100 series, p95 per series | 28,785 ms | 35,878 ms | +24.6% |
| dashboard query | OHLC + change + p95 per hour | 437 ms | 657 ms | +50.2% |
| full table scan (100000000 rows) | count(*) over the whole table | 205,983 ms | 261,894 ms | +27.1% |
| | **total** | **275,404 ms** | **351,370 ms** | **+27.6%** |

A negative difference means G1 was faster.

## Method and caveats

- Single node in a container, 16G heap, identical `cassandra.yaml` (only the request
  timeouts are raised so multi-million-row aggregates can finish). The G1 side enables the
  tuning block that ships commented out in `conf/jvm21-server.options` and restores
  compressed oops, which the shipped ZGC block disables as a jamm workaround.
- One measured pass per collector after one warm-up pass. Repeated passes of the same
  query on the same collector varied by roughly 10%, so treat single-query differences
  below that as noise; the direction is consistent across every query here.
- ZGC pause times are not in this report: at `-Xlog:gc` the ZGC lines are concurrent cycle
  durations, not stop-the-world time. Re-run both sides with `-Xlog:gc*,safepoint` (which
  `docker/scale-test.sh` now sets) to compare real pauses.
