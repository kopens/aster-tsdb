# time-series CQL integration test

image `aster-tsdb:6.0.0` · runtime `docker` · 2026-07-31 07:13 UTC

**52 passed, 0 failed** — assertions run against a live node booted from the image.

> Times are the full cqlsh round trip for one query; a trivial query on this host costs
> 752 ms of that (process startup + connect), so subtract roughly that much to read the CQL
> execution time. Server-side timings on a 100M-row data set are in the scale-test report.

## schema & fixture data

## time_bucket / downsampling

### ✅ time_bucket(1h) hourly avg = 20 / 60 — `790 ms`

```sql
SELECT time_bucket(1h, ts), avg(value) FROM it.metrics WHERE series='cpu' GROUP BY series, time_bucket(1h, ts);
```

```
 system.time_bucket(1h, ts)      | system.avg(value)
---------------------------------+-------------------
 2024-01-01 09:00:00.000000+0000 |                20
 2024-01-01 10:00:00.000000+0000 |                60
(2 rows)
```

### ✅ time_bucket(1h) second bucket = 60 — `753 ms`

```sql
SELECT time_bucket(1h, ts), avg(value) FROM it.metrics WHERE series='cpu' GROUP BY series, time_bucket(1h, ts);
```

```
 system.time_bucket(1h, ts)      | system.avg(value)
---------------------------------+-------------------
 2024-01-01 09:00:00.000000+0000 |                20
 2024-01-01 10:00:00.000000+0000 |                60
(2 rows)
```

### ✅ time_bucket scalar assigns bucket start — `733 ms`

```sql
SELECT ts, time_bucket(1h, ts) AS b FROM it.metrics WHERE series='cpu' LIMIT 1;
```

```
 ts                              | b
---------------------------------+---------------------------------
 2024-01-01 09:05:00.000000+0000 | 2024-01-01 09:00:00.000000+0000
(1 rows)
```

## first / last / delta / rate / derivative

### ✅ first(value, ts) = 10 — `730 ms`

```sql
SELECT first(value, ts) FROM it.metrics WHERE series='cpu';
```

```
 system.first(value, ts)
-------------------------
                      10
(1 rows)
```

### ✅ last(value, ts) = 70 — `732 ms`

```sql
SELECT last(value, ts) FROM it.metrics WHERE series='cpu';
```

```
 system.last(value, ts)
------------------------
                     70
(1 rows)
```

### ✅ delta(value, ts) = 60 — `733 ms`

```sql
SELECT delta(value, ts) FROM it.metrics WHERE series='cpu';
```

```
 system.delta(value, ts)
-------------------------
                      60
(1 rows)
```

### ✅ rate(value, ts) = 60/6000s = 0.01 — `723 ms`

```sql
SELECT rate(value, ts) FROM it.metrics WHERE series='cpu';
```

```
 system.rate(value, ts)
------------------------
                   0.01
(1 rows)
```

### ✅ derivative(value, ts) ~ 0.00977 (least squares, != rate) — `766 ms`

```sql
SELECT derivative(value, ts) FROM it.metrics WHERE series='cpu';
```

```
 system.derivative(value, ts)
------------------------------
                      0.00977
(1 rows)
```

## counters (reset aware)

### ✅ counter_delta detects the reset (= 250) — `749 ms`

```sql
SELECT counter_delta(total, ts) FROM it.counters WHERE series='api';
```

```
 system.counter_delta(total, ts)
---------------------------------
                             250
(1 rows)
```

### ✅ counter_rate = 250/120s ~ 2.083 — `724 ms`

```sql
SELECT counter_rate(total, ts) FROM it.counters WHERE series='api';
```

```
 system.counter_rate(total, ts)
--------------------------------
                        2.08333
(1 rows)
```

## percentile / spread / distribution

### ✅ percentile(value, 0.5) = 40 — `764 ms`

```sql
SELECT percentile(value, 0.5) FROM it.metrics WHERE series='cpu';
```

```
 system.percentile(value, 0.5)
-------------------------------
                            40
(1 rows)
```

### ✅ percentile(value, 0.95) = 67 — `727 ms`

```sql
SELECT percentile(value, 0.95) FROM it.metrics WHERE series='cpu';
```

```
 system.percentile(value, 0.95)
--------------------------------
                             67
(1 rows)
```

### ✅ variance(value) = 666.66 (sample) — `746 ms`

```sql
SELECT variance(value) FROM it.metrics WHERE series='cpu';
```

```
 system.variance(value)
------------------------
              666.66667
(1 rows)
```

### ✅ stddev(value) = 25.81 — `747 ms`

```sql
SELECT stddev(value) FROM it.metrics WHERE series='cpu';
```

```
 system.stddev(value)
----------------------
             25.81989
(1 rows)
```

### ✅ approx_count_distinct = 4 — `755 ms`

```sql
SELECT approx_count_distinct(value) FROM it.metrics WHERE series='cpu';
```

```
 system.approx_count_distinct(value)
-------------------------------------
                                   4
(1 rows)
```

### ✅ histogram returns nbuckets+2 entries — `744 ms`

```sql
SELECT histogram(value, 0, 100, 10) FROM it.metrics WHERE series='cpu';
```

```
 system.histogram(value, 0, 100, 10)
--------------------------------------
 [0, 0, 1, 0, 1, 0, 1, 0, 1, 0, 0, 0]
(1 rows)
```

## integral / time-weighted average

### ✅ integral(value, ts) = 240000 value-seconds — `751 ms`

```sql
SELECT integral(value, ts) FROM it.metrics WHERE series='cpu';
```

```
 system.integral(value, ts)
----------------------------
                    2.4e+05
(1 rows)
```

### ✅ time_weighted_average = 240000/6000 = 40 — `736 ms`

```sql
SELECT time_weighted_average(value, ts) FROM it.metrics WHERE series='cpu';
```

```
 system.time_weighted_average(value, ts)
-----------------------------------------
                                      40
(1 rows)
```

## two-variable statistics / regression (y = 2x + 1)

### ✅ regr_slope(y, x) = 2 — `734 ms`

```sql
SELECT regr_slope(y, x) FROM it.xy WHERE k='r';
```

```
 system.regr_slope(y, x)
-------------------------
                       2
(1 rows)
```

### ✅ regr_intercept(y, x) = 1 — `725 ms`

```sql
SELECT regr_intercept(y, x) FROM it.xy WHERE k='r';
```

```
 system.regr_intercept(y, x)
-----------------------------
                           1
(1 rows)
```

### ✅ regr_r2(y, x) = 1 — `756 ms`

```sql
SELECT regr_r2(y, x) FROM it.xy WHERE k='r';
```

```
 system.regr_r2(y, x)
----------------------
                    1
(1 rows)
```

### ✅ corr(y, x) = 1 — `721 ms`

```sql
SELECT corr(y, x) FROM it.xy WHERE k='r';
```

```
 system.corr(y, x)
-------------------
                 1
(1 rows)
```

### ✅ covar_pop(y, x) = 2.5 — `741 ms`

```sql
SELECT covar_pop(y, x) FROM it.xy WHERE k='r';
```

```
 system.covar_pop(y, x)
------------------------
                    2.5
(1 rows)
```

### ✅ covar_samp(y, x) = 3.33 — `744 ms`

```sql
SELECT covar_samp(y, x) FROM it.xy WHERE k='r';
```

```
 system.covar_samp(y, x)
-------------------------
                 3.33333
(1 rows)
```

## gap-fill: time_bucket_gapfill + locf + interpolate

### ✅ gapfill materialises every bucket (6 rows) — `746 ms`

```sql
SELECT time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000'), avg(value) FROM it.metrics WHERE series='gap' GROUP BY series, time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000');
```

```
 system.time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000') | system.avg(value)
--------------------------------------------------------------------------------------------+-------------------
                                                            2024-01-01 08:00:00.000000+0000 |              null
                                                            2024-01-01 09:00:00.000000+0000 |                60
                                                            2024-01-01 10:00:00.000000+0000 |              null
                                                            2024-01-01 11:00:00.000000+0000 |              null
                                                            2024-01-01 12:00:00.000000+0000 |               120
                                                            2024-01-01 13:00:00.000000+0000 |              null
(6 rows)
```

### ✅ gapfill empty bucket is null by default — `746 ms`

```sql
SELECT time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000'), avg(value) FROM it.metrics WHERE series='gap' GROUP BY series, time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000');
```

```
 system.time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000') | system.avg(value)
--------------------------------------------------------------------------------------------+-------------------
                                                            2024-01-01 08:00:00.000000+0000 |              null
                                                            2024-01-01 09:00:00.000000+0000 |                60
                                                            2024-01-01 10:00:00.000000+0000 |              null
                                                            2024-01-01 11:00:00.000000+0000 |              null
                                                            2024-01-01 12:00:00.000000+0000 |               120
                                                            2024-01-01 13:00:00.000000+0000 |              null
(6 rows)
```

### ✅ locf carries the previous bucket forward (10:00 -> 60) — `757 ms`

```sql
SELECT time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000'), locf(avg(value)) FROM it.metrics WHERE series='gap' GROUP BY series, time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000');
```

```
 system.time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000') | system.locf(system.avg(value))
--------------------------------------------------------------------------------------------+--------------------------------
                                                            2024-01-01 08:00:00.000000+0000 |                           null
                                                            2024-01-01 09:00:00.000000+0000 |                             60
                                                            2024-01-01 10:00:00.000000+0000 |                             60
                                                            2024-01-01 11:00:00.000000+0000 |                             60
                                                            2024-01-01 12:00:00.000000+0000 |                            120
                                                            2024-01-01 13:00:00.000000+0000 |                            120
(6 rows)
```

### ✅ locf leaves buckets before the first value null (08:00) — `753 ms`

```sql
SELECT time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000'), locf(avg(value)) FROM it.metrics WHERE series='gap' GROUP BY series, time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000');
```

```
 system.time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000') | system.locf(system.avg(value))
--------------------------------------------------------------------------------------------+--------------------------------
                                                            2024-01-01 08:00:00.000000+0000 |                           null
                                                            2024-01-01 09:00:00.000000+0000 |                             60
                                                            2024-01-01 10:00:00.000000+0000 |                             60
                                                            2024-01-01 11:00:00.000000+0000 |                             60
                                                            2024-01-01 12:00:00.000000+0000 |                            120
                                                            2024-01-01 13:00:00.000000+0000 |                            120
(6 rows)
```

### ✅ interpolate ramps 60 -> 120 linearly (10:00 -> 80) — `736 ms`

```sql
SELECT time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000'), interpolate(avg(value)) FROM it.metrics WHERE series='gap' GROUP BY series, time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000');
```

```
 system.time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000') | system.interpolate(system.avg(value))
--------------------------------------------------------------------------------------------+---------------------------------------
                                                            2024-01-01 08:00:00.000000+0000 |                                  null
                                                            2024-01-01 09:00:00.000000+0000 |                                    60
                                                            2024-01-01 10:00:00.000000+0000 |                                    80
                                                            2024-01-01 11:00:00.000000+0000 |                                   100
                                                            2024-01-01 12:00:00.000000+0000 |                                   120
                                                            2024-01-01 13:00:00.000000+0000 |                                  null
(6 rows)
```

### ✅ interpolate ramps 60 -> 120 linearly (11:00 -> 100) — `724 ms`

```sql
SELECT time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000'), interpolate(avg(value)) FROM it.metrics WHERE series='gap' GROUP BY series, time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000');
```

```
 system.time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000') | system.interpolate(system.avg(value))
--------------------------------------------------------------------------------------------+---------------------------------------
                                                            2024-01-01 08:00:00.000000+0000 |                                  null
                                                            2024-01-01 09:00:00.000000+0000 |                                    60
                                                            2024-01-01 10:00:00.000000+0000 |                                    80
                                                            2024-01-01 11:00:00.000000+0000 |                                   100
                                                            2024-01-01 12:00:00.000000+0000 |                                   120
                                                            2024-01-01 13:00:00.000000+0000 |                                  null
(6 rows)
```

### ✅ interpolate leaves the trailing bucket null (13:00) — `745 ms`

```sql
SELECT time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000'), interpolate(avg(value)) FROM it.metrics WHERE series='gap' GROUP BY series, time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000');
```

```
 system.time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000') | system.interpolate(system.avg(value))
--------------------------------------------------------------------------------------------+---------------------------------------
                                                            2024-01-01 08:00:00.000000+0000 |                                  null
                                                            2024-01-01 09:00:00.000000+0000 |                                    60
                                                            2024-01-01 10:00:00.000000+0000 |                                    80
                                                            2024-01-01 11:00:00.000000+0000 |                                   100
                                                            2024-01-01 12:00:00.000000+0000 |                                   120
                                                            2024-01-01 13:00:00.000000+0000 |                                  null
(6 rows)
```

## dashboard query (all together)

### ✅ combined OHLC + change + p95 query runs — `726 ms`

```sql
SELECT time_bucket(1h, ts) AS bucket, count(value) AS samples, first(value, ts) AS open, last(value, ts) AS close, min(value) AS low, max(value) AS high, avg(value) AS mean, delta(value, ts) AS change, rate(value, ts) AS per_second, percentile(value, 0.95) AS p95 FROM it.metrics WHERE series='cpu' AND ts >= '2024-01-01 00:00:00+0000' AND ts < '2024-01-02 00:00:00+0000' GROUP BY series, time_bucket(1h, ts);
```

```
 bucket                          | samples | open | close | low | high | mean | change | per_second | p95
---------------------------------+---------+------+-------+-----+------+------+--------+------------+-----
 2024-01-01 09:00:00.000000+0000 |       2 |   10 |    30 |  10 |   30 |   20 |     20 |   0.011111 |  29
 2024-01-01 10:00:00.000000+0000 |       2 |   50 |    70 |  50 |   70 |   60 |     20 |   0.011111 |  69
(2 rows)
```

## full-text search - SAI LIKE with index_analyzer

### ✅ LIKE '%timeout%' within partition + time range — `778 ms`

```sql
SELECT ts, msg FROM it.logs WHERE device='pump-01' AND ts >= '2024-01-01 09:00:00+0000' AND ts < '2024-01-01 10:00:00+0000' AND msg LIKE '%timeout%';
```

```
 ts                              | msg
---------------------------------+---------------------------------
 2024-01-01 09:00:00.000000+0000 | connection timeout on port 9042
(1 rows)
```

### ✅ LIKE mid-word fragment '%imeou%' (true substring) — `734 ms`

```sql
SELECT msg FROM it.logs WHERE device='pump-01' AND msg LIKE '%imeou%';
```

```
 msg
---------------------------------
 connection timeout on port 9042
(1 rows)
```

### ✅ LIKE korean fragment '%정지%' — `734 ms`

```sql
SELECT msg FROM it.logs WHERE device='pump-01' AND msg LIKE '%정지%';
```

```
 msg
----------------
 펌프 정지 알림
(1 rows)
```

### ✅ LIKE korean fragment crossing a space '%프 정%' — `736 ms`

```sql
SELECT msg FROM it.logs WHERE device='pump-01' AND msg LIKE '%프 정%';
```

```
 msg
----------------
 펌프 정지 알림
(1 rows)
```

### ✅ LIKE excludes non-matching rows (1 row only) — `744 ms`

```sql
SELECT msg FROM it.logs WHERE device='pump-01' AND msg LIKE '%refused%';
```

```
 msg
--------------------
 connection refused
(1 rows)
```

### ✅ LIKE prefix 'connection%' — `751 ms`

```sql
SELECT count(*) FROM it.logs WHERE device='pump-01' AND msg LIKE 'connection%';
```

```
 count
-------
     2
(1 rows)
```

### ✅ LIKE suffix '%9042' — `734 ms`

```sql
SELECT msg FROM it.logs WHERE device='pump-01' AND msg LIKE '%9042';
```

```
 msg
---------------------------------
 connection timeout on port 9042
(1 rows)
```

### ✅ '=' keeps exact whole-value semantics on the analyzed column — `732 ms`

```sql
SELECT count(*) FROM it.logs WHERE device='pump-01' AND msg = 'connection refused';
```

```
 count
-------
     1
(1 rows)
```

### ✅ LIKE combines with time_bucket aggregation — `744 ms`

```sql
SELECT time_bucket(1h, ts), count(*) FROM it.logs WHERE device='pump-01' AND ts >= '2024-01-01 00:00:00+0000' AND ts < '2024-01-02 00:00:00+0000' AND msg LIKE '%connection%' GROUP BY device, time_bucket(1h, ts);
```

```
 system.time_bucket(1h, ts)      | count
---------------------------------+-------
 2024-01-01 09:00:00.000000+0000 |     2
(1 rows)
```

## tiered storage: policy, retier, chunk re-encode, late merge

### ✅ ALTER TABLE installs the tiering policy (hex JSON) — `886 ms`

```sql
ALTER TABLE it.sensor WITH extensions = {'timeseries_tiering': 0x7b22686f745f77696e646f77223a223168222c226368756e6b5f77696e646f77223a223168222c22696e74657276616c223a22356d227d}; SELECT extensions FROM system_schema.tables WHERE keyspace_name='it' AND table_name='sensor';
```

```
 extensions
------------------------------------------------------------------------------------------------------------------------------------------
 {'timeseries_tiering': 0x7b22686f745f77696e646f77223a223168222c226368756e6b5f77696e646f77223a223168222c22696e74657276616c223a22356d227d}
(1 rows)
```

### ✅ nodetool retier runs one re-encode cycle — `1490 ms`

```sql
nodetool retier it sensor
```

```

```

### ✅ chunk row created for the window (samples = 3) — `717 ms`

```sql
SELECT samples FROM it.sensor__chunks WHERE tag_id='pump-01' AND window_start=1785466800000;
```

```
 samples
---------
       3
(1 rows)
```

### ✅ re-encoded base rows are deleted — `755 ms`

```sql
SELECT count(*) FROM it.sensor WHERE tag_id='pump-01' AND timestamp >= 1785466800000 AND timestamp < 1785470400000;
```

```
 count
-------
     0
(1 rows)
```

### ✅ window remains queryable via the chunk table — `734 ms`

```sql
SELECT tag_id, window_start, codec, samples FROM it.sensor__chunks WHERE tag_id='pump-01';
```

```
 tag_id  | window_start                    | codec | samples
---------+---------------------------------+-------+---------
 pump-01 | 2026-07-31 03:00:00.000000+0000 |     1 |       3
(1 rows)
```

### ✅ late row survives the first cycle's range tombstone — `733 ms`

```sql
SELECT count(*) FROM it.sensor WHERE tag_id='pump-01' AND timestamp >= 1785466800000 AND timestamp < 1785470400000;
```

```
 count
-------
     1
(1 rows)
```

### ✅ nodetool retier merges the late row — `1350 ms`

```sql
nodetool retier it sensor
```

```

```

### ✅ chunk re-encoded merged (samples 3 -> 4) — `723 ms`

```sql
SELECT samples FROM it.sensor__chunks WHERE tag_id='pump-01' AND window_start=1785466800000;
```

```
 samples
---------
       4
(1 rows)
```

### ✅ merged late row's base copy is deleted — `748 ms`

```sql
SELECT count(*) FROM it.sensor WHERE tag_id='pump-01' AND timestamp >= 1785466800000 AND timestamp < 1785470400000;
```

```
 count
-------
     0
(1 rows)
```

### ✅ nodetool tieringstatus lists it.sensor (interval 5m) — `1336 ms`

```sql
nodetool tieringstatus
```

```
Keyspace Table  Interval (ms) Last Run At   Windows Encoded Rows Encoded Late Merges Chunks Expired
it       sensor 300000        1785481987248 1               1            1           0             
```

### ✅ system_views.timeseries_tiering exposes policy and run stats — `731 ms`

```sql
SELECT keyspace_name, table_name, windows_encoded, rows_encoded, late_merges FROM system_views.timeseries_tiering;
```

```
 keyspace_name | table_name | windows_encoded | rows_encoded | late_merges
---------------+------------+-----------------+--------------+-------------
            it |     sensor |               1 |            1 |           1
(1 rows)
```
