# 계층화 벤치마크 원자료 — 비계층 베이스라인

**호스트 234** — Xeon Silver 4114T (Skylake-SP, 40 threads, 250 GB RAM) · **2026-08-04** ·
도커 이미지 `aster-tsdb:6.0.0-v4wired` (CI와 동일 베이스, chunk format **v4**, master `5cbdf914fa`) ·
하네스 [`docker/scale-test.sh`](../../docker/scale-test.sh) (12 로더 · 100행 unlogged batch · 힙 16G · generational ZGC) ·
**20,000,000행 / 500태그**.

요약과 해석은 [tiering-benchmark.md](tiering-benchmark.md)에 있습니다. 아래는 하네스 출력 전문입니다.

---

# tm_tag_point scale test

image `aster-tsdb:6.0.0-v4wired` · table `scale.tm_tag_point` · single node in a container · 2026-08-03 19:59 UTC

- **20,000,000** rows loaded
- **500** tags (partitions), **40,000 rows per tag** = 11.1 h of history at 1 sample/1s
- **86 s** load time (232,558 rows/s)

## Query times

| section | query | first result row | CQL time |
|---|---|---|---|
| single partition, aggregates over value_numeric (tag-000001, type='double', 40,000 rows scanned) | count(*) [40,000 rows] | `40000` | **303 ms** |
| single partition, aggregates over value_numeric (tag-000001, type='double', 40,000 rows scanned) | time_bucket 1h + avg/min/max(value_numeric) [40,000 rows] | `2024-01-01 11:00:00 \| 14.3501 \| 14.18 \| 14.44` | **270 ms** |
| single partition, aggregates over value_numeric (tag-000001, type='double', 40,000 rows scanned) | time_bucket 5m + avg(value_numeric) [40,000 rows] | `2024-01-01 11:05:00 \| 14.3752` | **178 ms** |
| single partition, aggregates over value_numeric (tag-000001, type='double', 40,000 rows scanned) | first/last/delta/rate(value_numeric) per hour [40,000 rows] | `2024-01-01 11:00:00 \| 14.19 \| 14.34 \| 0.15 \| 0.00037594` | **198 ms** |
| single partition, aggregates over value_numeric (tag-000001, type='double', 40,000 rows scanned) | derivative(value_numeric) per hour [40,000 rows] | `2024-01-01 11:00:00 \| 0.0003569` | **181 ms** |
| single partition, aggregates over value_numeric (tag-000001, type='double', 40,000 rows scanned) | percentile p50/p95/p99(value_numeric) [40,000 rows] | `13.38 \| 14.12 \| 14.28` | **194 ms** |
| single partition, aggregates over value_numeric (tag-000001, type='double', 40,000 rows scanned) | variance/stddev(value_numeric) [40,000 rows] | `0.149982 \| 0.387275` | **150 ms** |
| single partition, aggregates over value_numeric (tag-000001, type='double', 40,000 rows scanned) | histogram(value_numeric, 0, 100, 20) [40,000 rows] | `[0, 0, 0, 40000, 0, 0, 0,~` | **169 ms** |
| single partition, aggregates over value_numeric (tag-000001, type='double', 40,000 rows scanned) | approx_count_distinct(value_numeric) [40,000 rows] | `186` | **161 ms** |
| single partition, aggregates over value_numeric (tag-000001, type='double', 40,000 rows scanned) | integral + time_weighted_average(value_numeric) [40,000 rows] | `537779 \| 13.4448` | **171 ms** |
| single partition, aggregates over value_numeric (tag-000001, type='double', 40,000 rows scanned) | SECONDARY: time_bucket 1h + avg/min/max(latency) [40,000 rows] | `2024-01-01 11:00:00 \| 501 \| 5 \| 998` | **171 ms** |
| row reads (all 16 columns vs. projections) -- type-independent | newest 1000 rows, SELECT * -- type='boolean' tag (tag-000000) | `tag-000000 \| 2024-01-01 11:06:39 \| area-a \| asset-0000 \| line-01 \` | **56 ms** |
| row reads (all 16 columns vs. projections) -- type-independent | newest 1000 rows, SELECT * -- type='double' tag (tag-000001) | `tag-000001 \| 2024-01-01 11:06:39 \| area-b \| asset-0001 \| line-02 \` | **52 ms** |
| row reads (all 16 columns vs. projections) -- type-independent | 1 hour of rows, SELECT * (all columns) [3,600 rows] | `tag-000001 \| 2024-01-01 00:59:59 \| area-b \| asset-0001 \| line-02 \` | **159 ms** |
| row reads (all 16 columns vs. projections) -- type-independent | 1 hour of rows, project timestamp+value only [3,600 rows] | `2024-01-01 00:59:59 \| 13.58` | **56 ms** |
| row reads (all 16 columns vs. projections) -- type-independent | 1 hour of rows, project timestamp+value_numeric only [3,600 rows] | `2024-01-01 00:59:59 \| 13.58` | **55 ms** |
| row reads (all 16 columns vs. projections) -- type-independent | static columns only (1 row) | `tag-000001 \| busan \| area-b \| line-02 \| asset-0001 \| opc-01 \| PL` | **7 ms** |
| row reads (all 16 columns vs. projections) -- type-independent | column presence, type='boolean' tag [40,000 rows] | `40000 \| 40000 \| 0 \| 40000 \| 40000 \| 40000` | **200 ms** |
| row reads (all 16 columns vs. projections) -- type-independent | column presence, type='double' tag [40,000 rows] | `40000 \| 40000 \| 40000 \| 0 \| 40000 \| 40000` | **195 ms** |
| gap-fill (locf/interpolate over avg(value_numeric), numeric tag tag-000001) | gapfill 1h + locf over the full 11h span [40,000 rows] | `2024-01-01 00:00:00 \| 13.63` | **284 ms** |
| gap-fill (locf/interpolate over avg(value_numeric), numeric tag tag-000001) | gapfill 5m + interpolate over 6h [21,600 rows] | `2024-01-01 00:00:00 \| 13.7812` | **162 ms** |
| multi-partition (numeric-typed tags only -- 90 of 500 tags are numeric) | 10 numeric tags, hourly avg(value_numeric) [400,000 rows] | `tag-000001 \| 2024-01-01 11:00:00 \| 14.3501` | **1,620 ms** |
| multi-partition (numeric-typed tags only -- 90 of 500 tags are numeric) | 90 numeric tags, hourly avg(value_numeric) [3,600,000 rows] | `tag-000001 \| 2024-01-01 11:00:00 \| 14.3501` | **14,940 ms** |
| multi-partition (numeric-typed tags only -- 90 of 500 tags are numeric) | 90 numeric tags, p95(value_numeric) per tag [3,600,000 rows] | `tag-000001 \| 14.12` | **14,173 ms** |
| dashboard query (numeric tag tag-000001) | OHLC + change + p95 of value_numeric per hour [40,000 rows] | `2024-01-01 11:00:00 \| 400 \| 14.19 \| 14.34 \| 14.18 \| 14.44 \| 14.3` | **199 ms** |
| full table scan (20,000,000 rows) -- WRONG ANSWER on a tiered table (known limitation) | count(*) over the whole table | `20000000` | **82,050 ms** |

## Details

### single partition, aggregates over value_numeric (tag-000001, type='double', 40,000 rows scanned)

**count(*) [40,000 rows]** — `303 ms`

```sql
SELECT count(*) FROM scale.tm_tag_point WHERE tag_id='tag-000001';
```

```
count
-----
40000
(1 rows)
```

**time_bucket 1h + avg/min/max(value_numeric) [40,000 rows]** — `270 ms`

```sql
SELECT time_bucket(1h, timestamp), avg(value_numeric), min(value_numeric), max(value_numeric) FROM scale.tm_tag_point WHERE tag_id='tag-000001' GROUP BY tag_id, time_bucket(1h, timestamp);
```

```
system_time_bucket_1h__timestamp | system_avg_value_numeric | system_min_value_numeric | system_max_value_numeric
---------------------------------+--------------------------+--------------------------+-------------------------
2024-01-01 11:00:00 | 14.3501 | 14.18 | 14.44
2024-01-01 10:00:00 | 14.0014 | 13.68 | 14.35
2024-01-01 09:00:00 | 13.9908 | 13.79 | 14.21
2024-01-01 08:00:00 | 13.7399 | 13.4 | 14.08
2024-01-01 07:00:00 | 13.0183 | 12.59 | 13.42
2024-01-01 06:00:00 | 13.1566 | 12.67 | 13.63
... (12 rows total)
```

**time_bucket 5m + avg(value_numeric) [40,000 rows]** — `178 ms`

```sql
SELECT time_bucket(5m, timestamp), avg(value_numeric) FROM scale.tm_tag_point WHERE tag_id='tag-000001' GROUP BY tag_id, time_bucket(5m, timestamp);
```

```
system_time_bucket_5m__timestamp | system_avg_value_numeric
---------------------------------+-------------------------
2024-01-01 11:05:00 | 14.3752
2024-01-01 11:00:00 | 14.3417
2024-01-01 10:55:00 | 14.2353
2024-01-01 10:50:00 | 14.1967
2024-01-01 10:45:00 | 14.144
2024-01-01 10:40:00 | 14.1441
... (134 rows total)
```

**first/last/delta/rate(value_numeric) per hour [40,000 rows]** — `198 ms`

```sql
SELECT time_bucket(1h, timestamp), first(value_numeric, timestamp), last(value_numeric, timestamp), delta(value_numeric, timestamp), rate(value_numeric, timestamp) FROM scale.tm_tag_point WHERE tag_id='tag-000001' GROUP BY tag_id, time_bucket(1h, timestamp);
```

```
system_time_bucket_1h__timestamp | system_first_value_numeric__timestamp | system_last_value_numeric__timestamp | system_delta_value_numeric__timestamp | system_rate_value_numeric__timestamp
---------------------------------+---------------------------------------+--------------------------------------+---------------------------------------+-------------------------------------
2024-01-01 11:00:00 | 14.19 | 14.34 | 0.15 | 0.00037594
2024-01-01 10:00:00 | 13.9 | 14.19 | 0.29 | 8.05779e-05
2024-01-01 09:00:00 | 14.04 | 13.91 | -0.13 | -3.61211e-05
2024-01-01 08:00:00 | 13.4 | 14.05 | 0.65 | 0.000180606
2024-01-01 07:00:00 | 12.77 | 13.4 | 0.63 | 0.000175049
2024-01-01 06:00:00 | 13.63 | 12.76 | -0.87 | -0.000241734
... (12 rows total)
```

**derivative(value_numeric) per hour [40,000 rows]** — `181 ms`

```sql
SELECT time_bucket(1h, timestamp), derivative(value_numeric, timestamp) FROM scale.tm_tag_point WHERE tag_id='tag-000001' GROUP BY tag_id, time_bucket(1h, timestamp);
```

```
system_time_bucket_1h__timestamp | system_derivative_value_numeric__timestamp
---------------------------------+-------------------------------------------
2024-01-01 11:00:00 | 0.0003569
2024-01-01 10:00:00 | 0.000131532
2024-01-01 09:00:00 | -2.67674e-05
2024-01-01 08:00:00 | 0.000128848
2024-01-01 07:00:00 | 0.000180678
2024-01-01 06:00:00 | -0.00026808
... (12 rows total)
```

**percentile p50/p95/p99(value_numeric) [40,000 rows]** — `194 ms`

```sql
SELECT percentile(value_numeric, 0.5), percentile(value_numeric, 0.95), percentile(value_numeric, 0.99) FROM scale.tm_tag_point WHERE tag_id='tag-000001';
```

```
system_percentile_value_numeric__0_5 | system_percentile_value_numeric__0_95 | system_percentile_value_numeric__0_99
-------------------------------------+---------------------------------------+--------------------------------------
13.38 | 14.12 | 14.28
(1 rows)
```

**variance/stddev(value_numeric) [40,000 rows]** — `150 ms`

```sql
SELECT variance(value_numeric), stddev(value_numeric) FROM scale.tm_tag_point WHERE tag_id='tag-000001';
```

```
system_variance_value_numeric | system_stddev_value_numeric
------------------------------+----------------------------
0.149982 | 0.387275
(1 rows)
```

**histogram(value_numeric, 0, 100, 20) [40,000 rows]** — `169 ms`

```sql
SELECT histogram(value_numeric, 0, 100, 20) FROM scale.tm_tag_point WHERE tag_id='tag-000001';
```

```
system_histogram_value_numeric__0__100__20
------------------------------------------
[0, 0, 0, 40000, 0, 0, 0,~
(1 rows)
```

**approx_count_distinct(value_numeric) [40,000 rows]** — `161 ms`

```sql
SELECT approx_count_distinct(value_numeric) FROM scale.tm_tag_point WHERE tag_id='tag-000001';
```

```
system_approx_count_distinct_value_numeric
------------------------------------------
186
(1 rows)
```

**integral + time_weighted_average(value_numeric) [40,000 rows]** — `171 ms`

```sql
SELECT integral(value_numeric, timestamp), time_weighted_average(value_numeric, timestamp) FROM scale.tm_tag_point WHERE tag_id='tag-000001';
```

```
system_integral_value_numeric__timestamp | system_time_weighted_average_value_numeric__timestamp
-----------------------------------------+------------------------------------------------------
537779 | 13.4448
(1 rows)
```

**SECONDARY: time_bucket 1h + avg/min/max(latency) [40,000 rows]** — `171 ms`

```sql
SELECT time_bucket(1h, timestamp), avg(latency), min(latency), max(latency) FROM scale.tm_tag_point WHERE tag_id='tag-000001' GROUP BY tag_id, time_bucket(1h, timestamp);
```

```
system_time_bucket_1h__timestamp | system_avg_latency | system_min_latency | system_max_latency
---------------------------------+--------------------+--------------------+-------------------
2024-01-01 11:00:00 | 501 | 5 | 998
2024-01-01 10:00:00 | 496 | 1 | 999
2024-01-01 09:00:00 | 501 | 1 | 999
2024-01-01 08:00:00 | 498 | 1 | 999
2024-01-01 07:00:00 | 494 | 1 | 999
2024-01-01 06:00:00 | 503 | 1 | 999
... (12 rows total)
```

### row reads (all 16 columns vs. projections) -- type-independent

**newest 1000 rows, SELECT * -- type='boolean' tag (tag-000000)** — `56 ms`

```sql
SELECT * FROM scale.tm_tag_point WHERE tag_id='tag-000000' LIMIT 1000;
```

```
tag_id | timestamp | area_id | asset_id | line_id | opc_id | site_id | tag_name | type | attribute | error_code | latency | quality | value | value_boolean | value_numeric
-------+-----------+---------+----------+---------+--------+---------+----------+------+-----------+------------+---------+---------+-------+---------------+--------------
tag-000000 | 2024-01-01 11:06:39 | area-a | asset-0000 | line-01 | opc-00 | seoul | PLANT/seoul/line-01/TAG_0~ | boolean | {} | 0 | 653 | 192 | false | False | None
tag-000000 | 2024-01-01 11:06:38 | area-a | asset-0000 | line-01 | opc-00 | seoul | PLANT/seoul/line-01/TAG_0~ | boolean | {} | 0 | 698 | 192 | true | True | None
tag-000000 | 2024-01-01 11:06:37 | area-a | asset-0000 | line-01 | opc-00 | seoul | PLANT/seoul/line-01/TAG_0~ | boolean | {} | 0 | 992 | 192 | true | True | None
tag-000000 | 2024-01-01 11:06:36 | area-a | asset-0000 | line-01 | opc-00 | seoul | PLANT/seoul/line-01/TAG_0~ | boolean | {} | 0 | 813 | 192 | true | True | None
tag-000000 | 2024-01-01 11:06:35 | area-a | asset-0000 | line-01 | opc-00 | seoul | PLANT/seoul/line-01/TAG_0~ | boolean | {} | 0 | 10 | 192 | true | True | None
tag-000000 | 2024-01-01 11:06:34 | area-a | asset-0000 | line-01 | opc-00 | seoul | PLANT/seoul/line-01/TAG_0~ | boolean | {} | 0 | 404 | 192 | true | True | None
... (1000 rows total)
```

**newest 1000 rows, SELECT * -- type='double' tag (tag-000001)** — `52 ms`

```sql
SELECT * FROM scale.tm_tag_point WHERE tag_id='tag-000001' LIMIT 1000;
```

```
tag_id | timestamp | area_id | asset_id | line_id | opc_id | site_id | tag_name | type | attribute | error_code | latency | quality | value | value_boolean | value_numeric
-------+-----------+---------+----------+---------+--------+---------+----------+------+-----------+------------+---------+---------+-------+---------------+--------------
tag-000001 | 2024-01-01 11:06:39 | area-b | asset-0001 | line-02 | opc-01 | busan | PLANT/busan/line-02/TAG_0~ | double | {} | 0 | 225 | 192 | 14.34 | None | 14.34
tag-000001 | 2024-01-01 11:06:38 | area-b | asset-0001 | line-02 | opc-01 | busan | PLANT/busan/line-02/TAG_0~ | double | {} | 0 | 739 | 192 | 14.35 | None | 14.35
tag-000001 | 2024-01-01 11:06:37 | area-b | asset-0001 | line-02 | opc-01 | busan | PLANT/busan/line-02/TAG_0~ | double | {} | 0 | 367 | 192 | 14.35 | None | 14.35
tag-000001 | 2024-01-01 11:06:36 | area-b | asset-0001 | line-02 | opc-01 | busan | PLANT/busan/line-02/TAG_0~ | double | {} | 0 | 614 | 192 | 14.36 | None | 14.36
tag-000001 | 2024-01-01 11:06:35 | area-b | asset-0001 | line-02 | opc-01 | busan | PLANT/busan/line-02/TAG_0~ | double | {} | 0 | 195 | 192 | 14.36 | None | 14.36
tag-000001 | 2024-01-01 11:06:34 | area-b | asset-0001 | line-02 | opc-01 | busan | PLANT/busan/line-02/TAG_0~ | double | {} | 0 | 930 | 192 | 14.36 | None | 14.36
... (1000 rows total)
```

**1 hour of rows, SELECT * (all columns) [3,600 rows]** — `159 ms`

```sql
SELECT * FROM scale.tm_tag_point WHERE tag_id='tag-000001' AND timestamp >= '2024-01-01 00:00:00+0000' AND timestamp < '2024-01-01 01:00:00+0000';
```

```
tag_id | timestamp | area_id | asset_id | line_id | opc_id | site_id | tag_name | type | attribute | error_code | latency | quality | value | value_boolean | value_numeric
-------+-----------+---------+----------+---------+--------+---------+----------+------+-----------+------------+---------+---------+-------+---------------+--------------
tag-000001 | 2024-01-01 00:59:59 | area-b | asset-0001 | line-02 | opc-01 | busan | PLANT/busan/line-02/TAG_0~ | double | {} | 0 | 64 | 192 | 13.58 | None | 13.58
tag-000001 | 2024-01-01 00:59:58 | area-b | asset-0001 | line-02 | opc-01 | busan | PLANT/busan/line-02/TAG_0~ | double | {} | 0 | 618 | 192 | 13.57 | None | 13.57
tag-000001 | 2024-01-01 00:59:57 | area-b | asset-0001 | line-02 | opc-01 | busan | PLANT/busan/line-02/TAG_0~ | double | {} | 0 | 31 | 192 | 13.56 | None | 13.56
tag-000001 | 2024-01-01 00:59:56 | area-b | asset-0001 | line-02 | opc-01 | busan | PLANT/busan/line-02/TAG_0~ | double | {} | 0 | 148 | 192 | 13.56 | None | 13.56
tag-000001 | 2024-01-01 00:59:55 | area-b | asset-0001 | line-02 | opc-01 | busan | PLANT/busan/line-02/TAG_0~ | double | {} | 0 | 182 | 192 | 13.55 | None | 13.55
tag-000001 | 2024-01-01 00:59:54 | area-b | asset-0001 | line-02 | opc-01 | busan | PLANT/busan/line-02/TAG_0~ | double | {} | 0 | 127 | 192 | 13.54 | None | 13.54
... (3600 rows total)
```

**1 hour of rows, project timestamp+value only [3,600 rows]** — `56 ms`

```sql
SELECT timestamp, value FROM scale.tm_tag_point WHERE tag_id='tag-000001' AND timestamp >= '2024-01-01 00:00:00+0000' AND timestamp < '2024-01-01 01:00:00+0000';
```

```
timestamp | value
----------+------
2024-01-01 00:59:59 | 13.58
2024-01-01 00:59:58 | 13.57
2024-01-01 00:59:57 | 13.56
2024-01-01 00:59:56 | 13.56
2024-01-01 00:59:55 | 13.55
2024-01-01 00:59:54 | 13.54
... (3600 rows total)
```

**1 hour of rows, project timestamp+value_numeric only [3,600 rows]** — `55 ms`

```sql
SELECT timestamp, value_numeric FROM scale.tm_tag_point WHERE tag_id='tag-000001' AND timestamp >= '2024-01-01 00:00:00+0000' AND timestamp < '2024-01-01 01:00:00+0000';
```

```
timestamp | value_numeric
----------+--------------
2024-01-01 00:59:59 | 13.58
2024-01-01 00:59:58 | 13.57
2024-01-01 00:59:57 | 13.56
2024-01-01 00:59:56 | 13.56
2024-01-01 00:59:55 | 13.55
2024-01-01 00:59:54 | 13.54
... (3600 rows total)
```

**static columns only (1 row)** — `7 ms`

```sql
SELECT tag_id, site_id, area_id, line_id, asset_id, opc_id, tag_name, type FROM scale.tm_tag_point WHERE tag_id='tag-000001' LIMIT 1;
```

```
tag_id | site_id | area_id | line_id | asset_id | opc_id | tag_name | type
-------+---------+---------+---------+----------+--------+----------+-----
tag-000001 | busan | area-b | line-02 | asset-0001 | opc-01 | PLANT/busan/line-02/TAG_0~ | double
(1 rows)
```

**column presence, type='boolean' tag [40,000 rows]** — `200 ms`

```sql
SELECT count(latency) AS latency, count(value) AS value, count(value_numeric) AS value_numeric, count(value_boolean) AS value_boolean, count(quality) AS quality, count(attribute) AS attribute FROM scale.tm_tag_point WHERE tag_id='tag-000000';
```

```
latency | value | value_numeric | value_boolean | quality | attribute
--------+-------+---------------+---------------+---------+----------
40000 | 40000 | 0 | 40000 | 40000 | 40000
(1 rows)
```

**column presence, type='double' tag [40,000 rows]** — `195 ms`

```sql
SELECT count(latency) AS latency, count(value) AS value, count(value_numeric) AS value_numeric, count(value_boolean) AS value_boolean, count(quality) AS quality, count(attribute) AS attribute FROM scale.tm_tag_point WHERE tag_id='tag-000001';
```

```
latency | value | value_numeric | value_boolean | quality | attribute
--------+-------+---------------+---------------+---------+----------
40000 | 40000 | 40000 | 0 | 40000 | 40000
(1 rows)
```

### gap-fill (locf/interpolate over avg(value_numeric), numeric tag tag-000001)

**gapfill 1h + locf over the full 11h span [40,000 rows]** — `284 ms`

```sql
SELECT time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-01 11:06:40+0000'), locf(avg(value_numeric)) FROM scale.tm_tag_point WHERE tag_id='tag-000001' GROUP BY tag_id, time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-01 11:06:40+0000') ORDER BY timestamp ASC;
```

```
system_time_bucket_gapfill_1h__timestamp___2024_01_01_00_00_00_0000____2024_01_01_11_06_40_0000 | system_locf_system_avg_value_numeric
------------------------------------------------------------------------------------------------+-------------------------------------
2024-01-01 00:00:00 | 13.63
2024-01-01 01:00:00 | 13.422
2024-01-01 02:00:00 | 13.3907
2024-01-01 03:00:00 | 13.0522
2024-01-01 04:00:00 | 13.1257
2024-01-01 05:00:00 | 13.265
... (12 rows total)
```

**gapfill 5m + interpolate over 6h [21,600 rows]** — `162 ms`

```sql
SELECT time_bucket_gapfill(5m, timestamp, '2024-01-01 00:00:00+0000', '2024-01-01 06:00:00+0000'), interpolate(avg(value_numeric)) FROM scale.tm_tag_point WHERE tag_id='tag-000001' AND timestamp >= '2024-01-01 00:00:00+0000' AND timestamp < '2024-01-01 06:00:00+0000' GROUP BY tag_id, time_bucket_gapfill(5m, timestamp, '2024-01-01 00:00:00+0000', '2024-01-01 06:00:00+0000') ORDER BY timestamp ASC;
```

```
system_time_bucket_gapfill_5m__timestamp___2024_01_01_00_00_00_0000____2024_01_01_06_00_00_0000 | system_interpolate_system_avg_value_numeric
------------------------------------------------------------------------------------------------+--------------------------------------------
2024-01-01 00:00:00 | 13.7812
2024-01-01 00:05:00 | 13.7593
2024-01-01 00:10:00 | 13.7099
2024-01-01 00:15:00 | 13.7551
2024-01-01 00:20:00 | 13.6845
2024-01-01 00:25:00 | 13.5115
... (72 rows total)
```

### multi-partition (numeric-typed tags only -- 90 of 500 tags are numeric)

**10 numeric tags, hourly avg(value_numeric) [400,000 rows]** — `1,620 ms`

```sql
SELECT tag_id, time_bucket(1h, timestamp), avg(value_numeric) FROM scale.tm_tag_point WHERE tag_id IN ('tag-000001', 'tag-000002', 'tag-000013', 'tag-000014', 'tag-000026', 'tag-000027', 'tag-000038', 'tag-000039', 'tag-000050', 'tag-000051') GROUP BY tag_id, time_bucket(1h, timestamp);
```

```
tag_id | system_time_bucket_1h__timestamp | system_avg_value_numeric
-------+----------------------------------+-------------------------
tag-000001 | 2024-01-01 11:00:00 | 14.3501
tag-000001 | 2024-01-01 10:00:00 | 14.0014
tag-000001 | 2024-01-01 09:00:00 | 13.9908
tag-000001 | 2024-01-01 08:00:00 | 13.7399
tag-000001 | 2024-01-01 07:00:00 | 13.0183
tag-000001 | 2024-01-01 06:00:00 | 13.1566
... (120 rows total)
```

**90 numeric tags, hourly avg(value_numeric) [3,600,000 rows]** — `14,940 ms`

```sql
SELECT tag_id, time_bucket(1h, timestamp), avg(value_numeric) FROM scale.tm_tag_point WHERE tag_id IN ('tag-000001', 'tag-000002', 'tag-000013', 'tag-000014', 'tag-000026', 'tag-000027', 'tag-000038', 'tag-000039', 'tag-000050', 'tag-000051', 'tag-000063', 'tag-000064', 'tag-000075', 'tag-000076', 'tag-000087', 'tag-000088', 'tag-000100', 'tag-000101', 'tag-000112', 'tag-000113', 'tag-000124', 'tag-000125', 'tag-000126', 'tag-000137', 'tag-000138', 'tag-000149', 'tag-000150', 'tag-000161', 'tag-000162', 'tag-000163', 'tag-000174', 'tag-000175', 'tag-000186', 'tag-000187', 'tag-000198', 'tag-000199', 'tag-000200', 'tag-000211', 'tag-000212', 'tag-000223', 'tag-000224', 'tag-000235', 'tag-000236', 'tag-000237', 'tag-000248', 'tag-000249', 'tag-000260', 'tag-000261', 'tag-000273', 'tag-000274', 'tag-000284', 'tag-000285', 'tag-000286', 'tag-000297', 'tag-000298', 'tag-000310', 'tag-000311', 'tag-000321', 'tag-000322', 'tag-000323', 'tag-000334', 'tag-000335', 'tag-000347', 'tag-000348', 'tag-000359', 'tag-000360', 'tag-000371', 'tag-000372', 'tag-000384', 'tag-000385', 'tag-000396', 'tag-000397', 'tag-000408', 'tag-000409', 'tag-000421', 'tag-000422', 'tag-000433', 'tag-000434', 'tag-000445', 'tag-000446', 'tag-000447', 'tag-000458', 'tag-000459', 'tag-000470', 'tag-000471', 'tag-000482', 'tag-000483', 'tag-000484', 'tag-000495', 'tag-000496') GROUP BY tag_id, time_bucket(1h, timestamp);
```

```
tag_id | system_time_bucket_1h__timestamp | system_avg_value_numeric
-------+----------------------------------+-------------------------
tag-000001 | 2024-01-01 11:00:00 | 14.3501
tag-000001 | 2024-01-01 10:00:00 | 14.0014
tag-000001 | 2024-01-01 09:00:00 | 13.9908
tag-000001 | 2024-01-01 08:00:00 | 13.7399
tag-000001 | 2024-01-01 07:00:00 | 13.0183
tag-000001 | 2024-01-01 06:00:00 | 13.1566
... (1080 rows total)
```

**90 numeric tags, p95(value_numeric) per tag [3,600,000 rows]** — `14,173 ms`

```sql
SELECT tag_id, percentile(value_numeric, 0.95) FROM scale.tm_tag_point WHERE tag_id IN ('tag-000001', 'tag-000002', 'tag-000013', 'tag-000014', 'tag-000026', 'tag-000027', 'tag-000038', 'tag-000039', 'tag-000050', 'tag-000051', 'tag-000063', 'tag-000064', 'tag-000075', 'tag-000076', 'tag-000087', 'tag-000088', 'tag-000100', 'tag-000101', 'tag-000112', 'tag-000113', 'tag-000124', 'tag-000125', 'tag-000126', 'tag-000137', 'tag-000138', 'tag-000149', 'tag-000150', 'tag-000161', 'tag-000162', 'tag-000163', 'tag-000174', 'tag-000175', 'tag-000186', 'tag-000187', 'tag-000198', 'tag-000199', 'tag-000200', 'tag-000211', 'tag-000212', 'tag-000223', 'tag-000224', 'tag-000235', 'tag-000236', 'tag-000237', 'tag-000248', 'tag-000249', 'tag-000260', 'tag-000261', 'tag-000273', 'tag-000274', 'tag-000284', 'tag-000285', 'tag-000286', 'tag-000297', 'tag-000298', 'tag-000310', 'tag-000311', 'tag-000321', 'tag-000322', 'tag-000323', 'tag-000334', 'tag-000335', 'tag-000347', 'tag-000348', 'tag-000359', 'tag-000360', 'tag-000371', 'tag-000372', 'tag-000384', 'tag-000385', 'tag-000396', 'tag-000397', 'tag-000408', 'tag-000409', 'tag-000421', 'tag-000422', 'tag-000433', 'tag-000434', 'tag-000445', 'tag-000446', 'tag-000447', 'tag-000458', 'tag-000459', 'tag-000470', 'tag-000471', 'tag-000482', 'tag-000483', 'tag-000484', 'tag-000495', 'tag-000496') GROUP BY tag_id;
```

```
tag_id | system_percentile_value_numeric__0_95
-------+--------------------------------------
tag-000001 | 14.12
tag-000002 | 169
tag-000013 | 120
tag-000014 | 252
tag-000026 | 88.3
tag-000027 | 76
... (90 rows total)
```

### dashboard query (numeric tag tag-000001)

**OHLC + change + p95 of value_numeric per hour [40,000 rows]** — `199 ms`

```sql
SELECT time_bucket(1h, timestamp) AS bucket, count(value_numeric) AS samples, first(value_numeric, timestamp) AS open, last(value_numeric, timestamp) AS close, min(value_numeric) AS low, max(value_numeric) AS high, avg(value_numeric) AS mean, delta(value_numeric, timestamp) AS change, rate(value_numeric, timestamp) AS per_second, percentile(value_numeric, 0.95) AS p95 FROM scale.tm_tag_point WHERE tag_id='tag-000001' GROUP BY tag_id, time_bucket(1h, timestamp);
```

```
bucket | samples | open | close | low | high | mean | change | per_second | p95
-------+---------+------+-------+-----+------+------+--------+------------+----
2024-01-01 11:00:00 | 400 | 14.19 | 14.34 | 14.18 | 14.44 | 14.3501 | 0.15 | 0.00037594 | 14.42
2024-01-01 10:00:00 | 3600 | 13.9 | 14.19 | 13.68 | 14.35 | 14.0014 | 0.29 | 8.05779e-05 | 14.25
2024-01-01 09:00:00 | 3600 | 14.04 | 13.91 | 13.79 | 14.21 | 13.9908 | -0.13 | -3.61211e-05 | 14.15
2024-01-01 08:00:00 | 3600 | 13.4 | 14.05 | 13.4 | 14.08 | 13.7399 | 0.65 | 0.000180606 | 14
2024-01-01 07:00:00 | 3600 | 12.77 | 13.4 | 12.59 | 13.42 | 13.0183 | 0.63 | 0.000175049 | 13.33
2024-01-01 06:00:00 | 3600 | 13.63 | 12.76 | 12.67 | 13.63 | 13.1566 | -0.87 | -0.000241734 | 13.52
... (12 rows total)
```

### full table scan (20,000,000 rows) -- WRONG ANSWER on a tiered table (known limitation)

**count(*) over the whole table** — `82,050 ms`

```sql
SELECT count(*) FROM scale.tm_tag_point;
```

```
count
-----
20000000
(1 rows)
```
