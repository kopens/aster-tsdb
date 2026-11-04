/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.cql3.functions;

import java.util.Date;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

import static org.junit.Assert.assertTrue;

/**
 * Tests for the time-series native functions defined in {@link TimeSeriesFcts}
 * ({@code time_bucket}, {@code first}, {@code last}), exercised through real CQL.
 */
public class TimeSeriesFctsTest extends CQLTester
{
    private static Date date(String iso)
    {
        return new Date(java.time.Instant.parse(iso).toEpochMilli());
    }

    @Test
    public void testTimeBucketScalar() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v int, PRIMARY KEY (k, ts))");

        // 09:17 and 09:43 both fall into the 09:00 hour bucket; 10:05 into the 10:00 bucket.
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:17:00+0000', 1)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:43:00+0000', 2)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 10:05:00+0000', 3)");

        assertRows(execute("SELECT v, time_bucket(1h, ts) FROM %s WHERE k = 1"),
                   row(1, date("2024-01-01T09:00:00Z")),
                   row(2, date("2024-01-01T09:00:00Z")),
                   row(3, date("2024-01-01T10:00:00Z")));

        // time_bucket is the same computation as the equivalent floor() call.
        Object[][] expectedBuckets = { row(date("2024-01-01T09:00:00Z")),
                                       row(date("2024-01-01T09:00:00Z")),
                                       row(date("2024-01-01T10:00:00Z")) };
        assertRows(execute("SELECT time_bucket(1h, ts) FROM %s WHERE k = 1"), expectedBuckets);
        assertRows(execute("SELECT floor(ts, 1h) FROM %s WHERE k = 1"), expectedBuckets);
    }

    @Test
    public void testTimeBucketWithOrigin() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v int, PRIMARY KEY (k, ts))");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:17:00+0000', 1)");

        // Bucketing into 1-hour windows offset by 30 minutes: 09:17 lands in the [08:30, 09:30) bucket.
        assertRows(execute("SELECT time_bucket(1h, ts, '2024-01-01 00:30:00+0000') FROM %s WHERE k = 1"),
                   row(date("2024-01-01T08:30:00Z")));
    }

    @Test
    public void testTimeBucketGroupBy() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v int, PRIMARY KEY (k, ts))");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:17:00+0000', 1)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:43:00+0000', 3)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 10:05:00+0000', 10)");

        // Downsample to hourly buckets and aggregate within each bucket.
        assertRows(execute("SELECT time_bucket(1h, ts) AS bucket, sum(v), count(v) " +
                           "FROM %s WHERE k = 1 GROUP BY k, time_bucket(1h, ts)"),
                   row(date("2024-01-01T09:00:00Z"), 4, 2L),
                   row(date("2024-01-01T10:00:00Z"), 10, 1L));
    }

    @Test
    public void testFirstAndLast() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v text, PRIMARY KEY (k, ts))");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 'a')");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 10:00:00+0000', 'b')");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 11:00:00+0000', 'c')");

        assertRows(execute("SELECT first(v, ts), last(v, ts) FROM %s WHERE k = 1"),
                   row("a", "c"));
    }

    @Test
    public void testFirstAndLastIgnoreInsertionOrder() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v int, PRIMARY KEY (k, ts))");

        // Insert out of time order to prove the result depends on the timestamp, not arrival order.
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 11:00:00+0000', 30)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 10)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 10:00:00+0000', 20)");

        assertRows(execute("SELECT first(v, ts), last(v, ts) FROM %s WHERE k = 1"),
                   row(10, 30));
    }

    @Test
    public void testFirstAndLastPerBucket() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v int, PRIMARY KEY (k, ts))");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:10:00+0000', 1)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:50:00+0000', 2)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 10:10:00+0000', 3)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 10:50:00+0000', 4)");

        assertRows(execute("SELECT first(v, ts), last(v, ts) " +
                           "FROM %s WHERE k = 1 GROUP BY k, time_bucket(1h, ts)"),
                   row(1, 2),
                   row(3, 4));
    }

    @Test
    public void testFirstAndLastWithNoRows() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v int, PRIMARY KEY (k, ts))");
        assertRows(execute("SELECT first(v, ts), last(v, ts) FROM %s WHERE k = 1"),
                   row(null, null));
    }

    @Test
    public void testDeltaRateDerivativeOnLinearSeries() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        // Perfectly linear: +10 every 10 seconds over a 30-second span.
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 0)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:10+0000', 10)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:20+0000', 20)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:30+0000', 30)");

        // delta = 30 - 0; rate = 30 / 30s = 1.0/s; regression slope of a line = its slope = 1.0/s.
        assertRows(execute("SELECT delta(v, ts), rate(v, ts), derivative(v, ts) FROM %s WHERE k = 1"),
                   row(30.0, 1.0, 1.0));
    }

    @Test
    public void testDerivativeDiffersFromRateOnNonLinearSeries() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        // Flat then a jump: endpoint rate and least-squares slope diverge.
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 0)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:10+0000', 0)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:20+0000', 0)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:30+0000', 40)");

        // delta = 40; rate = 40 / 30s; least-squares slope = 1.2/s (computed from sums).
        assertRows(execute("SELECT delta(v, ts), rate(v, ts), derivative(v, ts) FROM %s WHERE k = 1"),
                   row(40.0, 40.0 / 30.0, 2400.0 / 2000.0));
    }

    @Test
    public void testRatePerBucketIgnoresInsertionOrder() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        // Out-of-order inserts within one hourly bucket; rate must use timestamp endpoints, not arrival order.
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:20+0000', 20)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 0)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:10+0000', 10)");

        assertRows(execute("SELECT delta(v, ts), rate(v, ts) " +
                           "FROM %s WHERE k = 1 GROUP BY k, time_bucket(1h, ts)"),
                   row(20.0, 1.0));
    }

    @Test
    public void testSeriesFunctionsWithNoRows() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        assertRows(execute("SELECT delta(v, ts), rate(v, ts), derivative(v, ts) FROM %s WHERE k = 1"),
                   row(null, null, null));
    }

    @Test
    public void testRateAndDerivativeSinglePointAreNull() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 5)");
        // A single sample has zero time span, so rate and derivative are undefined; delta is 0.
        assertRows(execute("SELECT delta(v, ts), rate(v, ts), derivative(v, ts) FROM %s WHERE k = 1"),
                   row(0.0, null, null));
    }

    @Test
    public void testCounterDeltaAndRateWithReset() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v int, PRIMARY KEY (k, ts))");
        // Monotonic counter with a reset at 09:00:20 (10 -> 5): increase = 10 + 5(after reset) + 10 = 25 over 30s.
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 0)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:10+0000', 10)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:20+0000', 5)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:30+0000', 15)");

        assertRows(execute("SELECT counter_delta(v, ts), counter_rate(v, ts) FROM %s WHERE k = 1"),
                   row(25.0, 25.0 / 30.0));
    }

    @Test
    public void testCounterMonotonicNoReset() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v int, PRIMARY KEY (k, ts))");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 0)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:10+0000', 10)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:30+0000', 30)");

        // No reset: increase = 30 over 30s -> rate 1.0/s.
        assertRows(execute("SELECT counter_delta(v, ts), counter_rate(v, ts) FROM %s WHERE k = 1"),
                   row(30.0, 1.0));
    }

    @Test
    public void testCounterEdgeCases() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v int, PRIMARY KEY (k, ts))");
        // Empty: both null.
        assertRows(execute("SELECT counter_delta(v, ts), counter_rate(v, ts) FROM %s WHERE k = 1"),
                   row(null, null));
        // Single sample: delta 0, rate undefined (no span).
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 7)");
        assertRows(execute("SELECT counter_delta(v, ts), counter_rate(v, ts) FROM %s WHERE k = 1"),
                   row(0.0, null));
    }

    @Test
    public void testCorrelationAndCovariance() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, x double, y double, PRIMARY KEY (k, ts))");
        // Perfect positive linear relationship y = 2x over (1,2),(2,4),(3,6).
        execute("INSERT INTO %s (k, ts, x, y) VALUES (1, '2024-01-01 09:00:00+0000', 1, 2)");
        execute("INSERT INTO %s (k, ts, x, y) VALUES (1, '2024-01-01 09:00:01+0000', 2, 4)");
        execute("INSERT INTO %s (k, ts, x, y) VALUES (1, '2024-01-01 09:00:02+0000', 3, 6)");

        // corr = 1.0; covar_pop = 4/3; covar_samp = 2.0.
        assertRows(execute("SELECT corr(y, x), covar_pop(y, x), covar_samp(y, x) FROM %s WHERE k = 1"),
                   row(1.0, 4.0 / 3.0, 2.0));

        // Linear regression of y on x for y = 2x: slope 2, intercept 0, r2 1.
        assertRows(execute("SELECT regr_slope(y, x), regr_intercept(y, x), regr_r2(y, x) FROM %s WHERE k = 1"),
                   row(2.0, 0.0, 1.0));
    }

    @Test
    public void testCorrelationEdgeCases() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, x double, y double, PRIMARY KEY (k, ts))");
        // Empty: all null.
        assertRows(execute("SELECT corr(y, x), covar_pop(y, x), covar_samp(y, x) FROM %s WHERE k = 1"),
                   row(null, null, null));
        // Single row: corr and covar_samp undefined; covar_pop = 0.
        execute("INSERT INTO %s (k, ts, x, y) VALUES (1, '2024-01-01 09:00:00+0000', 5, 9)");
        assertRows(execute("SELECT corr(y, x), covar_pop(y, x), covar_samp(y, x) FROM %s WHERE k = 1"),
                   row(null, 0.0, null));
        // Zero variance in x (constant) -> corr undefined (null).
        execute("INSERT INTO %s (k, ts, x, y) VALUES (1, '2024-01-01 09:00:01+0000', 5, 12)");
        assertRows(execute("SELECT corr(y, x) FROM %s WHERE k = 1"), row((Object) null));
    }

    @Test
    public void testPercentile() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        // Values 0,10,20,...,100 across 11 rows.
        for (int i = 0; i <= 10; i++)
            execute("INSERT INTO %s (k, ts, v) VALUES (1, ?, ?)",
                    new java.util.Date(1_700_000_000_000L + i * 1000L), (double) (i * 10));

        // Continuous percentile with linear interpolation over [0..100].
        assertRows(execute("SELECT percentile(v, 0) FROM %s WHERE k = 1"), row(0.0));
        assertRows(execute("SELECT percentile(v, 1) FROM %s WHERE k = 1"), row(100.0));
        assertRows(execute("SELECT percentile(v, 0.5) FROM %s WHERE k = 1"), row(50.0));
        assertRows(execute("SELECT percentile(v, 0.95) FROM %s WHERE k = 1"), row(95.0));
    }

    @Test
    public void testPercentileInterpolatesBetweenValues() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        // Four values 1,2,3,4: the 0.5 quantile sits between 2 and 3 -> 2.5 (linear interpolation).
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 1)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:01+0000', 2)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:02+0000', 3)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:03+0000', 4)");

        assertRows(execute("SELECT percentile(v, 0.5) FROM %s WHERE k = 1"), row(2.5));
    }

    @Test
    public void testPercentileRejectsOutOfRangeQuantile() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 1)");
        assertInvalidThrowMessage("between 0 and 1",
                                  org.apache.cassandra.exceptions.InvalidRequestException.class,
                                  "SELECT percentile(v, 2) FROM %s WHERE k = 1");
    }

    @Test
    public void testPercentileWithNoRows() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        assertRows(execute("SELECT percentile(v, 0.5) FROM %s WHERE k = 1"), row((Object) null));
    }

    @Test
    public void testTimeWeightedAverageEqualsMeanForUniformSpacing() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        // Linear values at uniform 10s spacing: time-weighted average equals the plain mean (10).
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 0)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:10+0000', 10)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:20+0000', 20)");

        assertRows(execute("SELECT time_weighted_average(v, ts) FROM %s WHERE k = 1"), row(10.0));
    }

    @Test
    public void testIntegral() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        // Constant value 10 held over 30 seconds: integral = 10 * 30 = 300 (value-seconds); TWA = 10.
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 10)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:10+0000', 10)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:20+0000', 10)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:30+0000', 10)");

        assertRows(execute("SELECT integral(v, ts), time_weighted_average(v, ts) FROM %s WHERE k = 1"),
                   row(300.0, 10.0));
    }

    /**
     * The ordering the buffered (value, timestamp) aggregates do at finalization has to be correct AND cheap on a
     * DESC-clustered table, which is the ordinary time-series shape: rows then arrive newest-first, i.e. exactly
     * reversed. An insertion sort is O(n²) there — measured at 14.4 s for one 100,000-row partition against ~0.5 s
     * for every other aggregate over the same scan.
     *
     * <p>Correctness and cost are asserted together on purpose. The answer alone cannot fail on a quadratic sort,
     * and a wall-clock bound alone is a flaky test; the bound here is deliberately loose (10 s for 20,000 rows,
     * where the quadratic version needs minutes and the merge sort needs milliseconds) so it fails only on a
     * genuine complexity regression, not on a slow machine.
     */
    @Test
    public void testBufferedAggregatesOnDescendingClusteringAreNotQuadratic() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts)) " +
                    "WITH CLUSTERING ORDER BY (ts DESC)");
        // Constant 10 over 19,999 uniform 1s steps: integral = 10 * 19,999, TWA = 10 -- values chosen so the
        // expected result is exact in double arithmetic whatever order the samples are folded in.
        int rows = 20_000;
        long base = 1_704_099_600_000L;                 // 2024-01-01 09:00:00Z
        for (int i = 0; i < rows; i++)
            execute("INSERT INTO %s (k, ts, v) VALUES (1, ?, 10)", new Date(base + i * 1000L));

        long startedAt = System.nanoTime();
        assertRows(execute("SELECT integral(v, ts), time_weighted_average(v, ts) FROM %s WHERE k = 1"),
                   row(10.0 * (rows - 1), 10.0));
        long elapsedMs = (System.nanoTime() - startedAt) / 1_000_000L;
        assertTrue("integral/TWA over " + rows + " descending rows took " + elapsedMs + " ms -- the finalization "
                   + "sort has gone quadratic again", elapsedMs < 10_000);

        // counter_delta/counter_rate buffer and order through the same helper, so they regress together.
        startedAt = System.nanoTime();
        execute("SELECT counter_delta(v, ts) FROM %s WHERE k = 1");
        elapsedMs = (System.nanoTime() - startedAt) / 1_000_000L;
        assertTrue("counter_delta over " + rows + " descending rows took " + elapsedMs + " ms", elapsedMs < 10_000);
    }

    /**
     * The reversal fast path is only stable when no two timestamps tie, and ties are reachable: a re-written sample
     * keeps its clustering, so the buffer can hold the same timestamp twice. Ordering must still put the later
     * arrival last, which is what makes the newest value win at the seam.
     */
    @Test
    public void testBufferedAggregateOrderingIsStableAcrossTiedTimestamps() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts)) " +
                    "WITH CLUSTERING ORDER BY (ts DESC)");
        // Descending arrival with a duplicated clustering: the second write to 09:00:10 replaces the first, so the
        // series is 0 @00s, 20 @10s, 20 @20s -- integral = trapezoid(0,20,10s) + trapezoid(20,20,10s) = 100 + 200.
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:20+0000', 20)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:10+0000', 10)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:10+0000', 20)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 0)");

        assertRows(execute("SELECT integral(v, ts) FROM %s WHERE k = 1"), row(300.0));
    }

    @Test
    public void testIntegralEdgeCases() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        assertRows(execute("SELECT integral(v, ts) FROM %s WHERE k = 1"), row((Object) null));
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 7)");
        // A single sample has no time span -> integral 0.
        assertRows(execute("SELECT integral(v, ts) FROM %s WHERE k = 1"), row(0.0));
    }

    @Test
    public void testTimeWeightedAverageWeightsByDuration() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        // 0 held for 1s, then ramps to 10 over 10s. Trapezoidal area = 0*1 + (0+10)/2*10 = 50 over 11s.
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 0)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:01+0000', 0)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:11+0000', 10)");

        // Time-weighted (50/11) differs from the plain mean of the values (10/3).
        assertRows(execute("SELECT time_weighted_average(v, ts) FROM %s WHERE k = 1"),
                   row(50.0 / 11.0));
    }

    @Test
    public void testTimeWeightedAverageSinglePointAndEmpty() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        assertRows(execute("SELECT time_weighted_average(v, ts) FROM %s WHERE k = 1"), row((Object) null));

        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 7)");
        assertRows(execute("SELECT time_weighted_average(v, ts) FROM %s WHERE k = 1"), row(7.0));
    }

    @Test
    public void testVarianceAndStddev() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        // Values 2,4,6: mean 4, sample variance = (4+0+4)/2 = 4, stddev = 2.
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 2)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:01+0000', 4)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:02+0000', 6)");

        assertRows(execute("SELECT variance(v), stddev(v) FROM %s WHERE k = 1"), row(4.0, 2.0));
    }

    @Test
    public void testVarianceNeedsTwoValues() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 5)");
        // Sample variance is undefined for a single value.
        assertRows(execute("SELECT variance(v), stddev(v) FROM %s WHERE k = 1"), row(null, null));
    }

    @Test
    public void testHistogram() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        // Buckets over [0, 20) with nbuckets=4 (width 5). Result is [underflow, b1, b2, b3, b4, overflow].
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', -1)");  // underflow
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:01+0000', 0)");   // bucket 1 [0,5)
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:02+0000', 1)");   // bucket 1
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:03+0000', 2)");   // bucket 1
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:04+0000', 7)");   // bucket 2 [5,10)
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:05+0000', 100)"); // overflow

        assertRows(execute("SELECT histogram(v, 0, 20, 4) FROM %s WHERE k = 1"),
                   row(java.util.Arrays.asList(1L, 3L, 1L, 0L, 0L, 1L)));
    }

    @Test
    public void testHistogramRejectsBadBounds() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 1)");
        assertInvalidThrowMessage("nbuckets > 0",
                                  org.apache.cassandra.exceptions.InvalidRequestException.class,
                                  "SELECT histogram(v, 0, 20, 0) FROM %s WHERE k = 1");
        assertInvalidThrowMessage("max > min",
                                  org.apache.cassandra.exceptions.InvalidRequestException.class,
                                  "SELECT histogram(v, 20, 0, 4) FROM %s WHERE k = 1");
    }

    @Test
    public void testApproxCountDistinct() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v int, PRIMARY KEY (k, ts))");
        // Six rows but only three distinct values; HyperLogLog++ is exact at this tiny cardinality.
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 1)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:01+0000', 1)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:02+0000', 2)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:03+0000', 3)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:04+0000', 3)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:05+0000', 2)");

        assertRows(execute("SELECT approx_count_distinct(v) FROM %s WHERE k = 1"), row(3L));
    }

    @Test
    public void testApproxCountDistinctEmptyIsZero() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v int, PRIMARY KEY (k, ts))");
        assertRows(execute("SELECT approx_count_distinct(v) FROM %s WHERE k = 1"), row(0L));
    }

    @Test
    public void testTimeBucketGapfillGroupsLikeTimeBucket() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v int, PRIMARY KEY (k, ts))");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:05:00+0000', 1)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:35:00+0000', 3)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 10:15:00+0000', 10)");

        // Scalar: returns the bucket start aligned to the start argument.
        assertRows(execute("SELECT time_bucket_gapfill(1h, ts, '2024-01-01 00:00:00+0000', " +
                           "'2024-01-01 12:00:00+0000') FROM %s WHERE k = 1"),
                   row(date("2024-01-01T09:00:00Z")),
                   row(date("2024-01-01T09:00:00Z")),
                   row(date("2024-01-01T10:00:00Z")));

        // As a GROUP BY selector it buckets exactly like time_bucket (gap-fill of empty buckets is layered on later).
        assertRows(execute("SELECT time_bucket_gapfill(1h, ts, '2024-01-01 00:00:00+0000', " +
                           "'2024-01-01 12:00:00+0000') AS bucket, sum(v) " +
                           "FROM %s WHERE k = 1 GROUP BY k, time_bucket_gapfill(1h, ts, '2024-01-01 00:00:00+0000', " +
                           "'2024-01-01 12:00:00+0000')"),
                   row(date("2024-01-01T09:00:00Z"), 4),
                   row(date("2024-01-01T10:00:00Z"), 10));
    }

    @Test
    public void testTimeBucketGapfillDensifiesEmptyBuckets() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v int, PRIMARY KEY (k, ts))");
        // Data in the 09:00 and 11:00 buckets; the 10:00 bucket is empty.
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:30:00+0000', 5)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 11:15:00+0000', 7)");

        String gf = "time_bucket_gapfill(1h, ts, '2024-01-01 09:00:00+0000', '2024-01-01 12:00:00+0000')";
        // The empty 10:00 bucket is materialized with a null aggregate (do not alias the bucket column in v1).
        assertRows(execute("SELECT " + gf + ", sum(v) FROM %s WHERE k = 1 GROUP BY k, " + gf),
                   row(date("2024-01-01T09:00:00Z"), 5),
                   row(date("2024-01-01T10:00:00Z"), null),
                   row(date("2024-01-01T11:00:00Z"), 7));
    }

    @Test
    public void testTimeBucketGapfillWithLocf() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v int, PRIMARY KEY (k, ts))");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:30:00+0000', 5)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 11:15:00+0000', 7)");

        String gf = "time_bucket_gapfill(1h, ts, '2024-01-01 09:00:00+0000', '2024-01-01 12:00:00+0000')";
        // locf(sum(v)) carries the previous non-empty bucket's value into the empty 10:00 bucket (5, not null).
        assertRows(execute("SELECT " + gf + ", locf(sum(v)) FROM %s WHERE k = 1 GROUP BY k, " + gf),
                   row(date("2024-01-01T09:00:00Z"), 5),
                   row(date("2024-01-01T10:00:00Z"), 5),
                   row(date("2024-01-01T11:00:00Z"), 7));
    }

    @Test
    public void testTimeBucketGapfillWithInterpolate() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v int, PRIMARY KEY (k, ts))");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:30:00+0000', 0)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 11:15:00+0000', 20)");

        String gf = "time_bucket_gapfill(1h, ts, '2024-01-01 09:00:00+0000', '2024-01-01 12:00:00+0000')";
        // interpolate(sum(v)) linearly fills the empty 10:00 bucket between 0 and 20 -> 10.0 (result is double).
        assertRows(execute("SELECT " + gf + ", interpolate(sum(v)) FROM %s WHERE k = 1 GROUP BY k, " + gf),
                   row(date("2024-01-01T09:00:00Z"), 0.0),
                   row(date("2024-01-01T10:00:00Z"), 10.0),
                   row(date("2024-01-01T11:00:00Z"), 20.0));
    }

    /**
     * Gap-fill on a {@code CLUSTERING ORDER BY (ts DESC)} table -- the industrial idiom.
     * {@link org.apache.cassandra.db.aggregation.TimeBucketGapFiller}
     * requires rows grouped by partition and <em>ascending</em> bucket within a partition, and nothing
     * enforces it, so this combination has to be pinned rather than assumed.
     */
    @Test
    public void testTimeBucketGapfillOnDescClusteredTable() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v int, PRIMARY KEY (k, ts)) " +
                    "WITH CLUSTERING ORDER BY (ts DESC)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:30:00+0000', 5)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 11:15:00+0000', 7)");

        String gf = "time_bucket_gapfill(1h, ts, '2024-01-01 09:00:00+0000', '2024-01-01 12:00:00+0000')";

        // ORDER BY ts ASC cancels the table's DESC declaration, so the filler sees the ascending
        // buckets it documents as its precondition: the empty 10:00 bucket carries 5 forward.
        assertRows(execute("SELECT " + gf + ", locf(sum(v)) FROM %s WHERE k = 1 GROUP BY k, " + gf +
                           " ORDER BY ts ASC"),
                   row(date("2024-01-01T09:00:00Z"), 5),
                   row(date("2024-01-01T10:00:00Z"), 5),
                   row(date("2024-01-01T11:00:00Z"), 7));
    }

    /**
     * The time-series functions against the production table shape, aggregating the column a real
     * dashboard aggregates. In {@code tm_tag_point} the reading lives in {@code value_numeric} when the
     * tag's static {@code type} is numeric (and in {@code value_boolean} when it is boolean); {@code value}
     * is a text mirror of the same reading and cannot be aggregated numerically, and {@code latency} is
     * collection metadata, not the measurement. Every function test above uses a toy
     * {@code (k int, ts timestamp, v int)} table, so nothing proved the functions work over this shape --
     * static columns, a frozen map, mixed types and DESC clustering all present at once.
     */
    @Test
    public void testFunctionsOverProductionShapeAggregateValueNumeric() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "tag_id text, timestamp timestamp, " +
                    "site_id text static, tag_name text static, type text static, " +
                    "attribute frozen<map<text, text>>, error_code int, latency int, quality int, " +
                    "value text, value_boolean boolean, value_numeric double, " +
                    "PRIMARY KEY (tag_id, timestamp)) WITH CLUSTERING ORDER BY (timestamp DESC)");

        execute("INSERT INTO %s (tag_id, site_id, tag_name, type) VALUES ('TAG-1', 'GN', 'temp', 'double')");
        // A numeric-typed tag: value_numeric carries the reading, value mirrors it as text,
        // value_boolean stays null, and quality/error_code/attribute are constant -- as in production.
        Object[][] samples = { { "2024-01-01T09:00:00Z", 10.0 },
                               { "2024-01-01T09:30:00Z", 20.0 },
                               { "2024-01-01T10:00:00Z", 30.0 },
                               { "2024-01-01T10:30:00Z", 40.0 } };
        for (Object[] s : samples)
            execute("INSERT INTO %s (tag_id, timestamp, attribute, error_code, latency, quality, " +
                    "value, value_boolean, value_numeric) VALUES ('TAG-1', ?, {}, 0, 42, 192, ?, null, ?)",
                    date((String) s[0]), String.valueOf(s[1]), s[1]);

        // Hourly bucketing plus the order-dependent family, on the real column. DESC clustering means an
        // unordered read arrives newest-first, so the ASC request is what first/last/delta/rate need.
        assertRows(execute("SELECT time_bucket(1h, timestamp), avg(value_numeric), min(value_numeric), " +
                           "max(value_numeric), first(value_numeric, timestamp), last(value_numeric, timestamp), " +
                           "delta(value_numeric, timestamp) FROM %s WHERE tag_id = 'TAG-1' " +
                           "GROUP BY tag_id, time_bucket(1h, timestamp) ORDER BY timestamp ASC"),
                   row(date("2024-01-01T09:00:00Z"), 15.0, 10.0, 20.0, 10.0, 20.0, 10.0),
                   row(date("2024-01-01T10:00:00Z"), 35.0, 30.0, 40.0, 30.0, 40.0, 10.0));

        // Whole-partition statistics on the same column.
        // percentile interpolates between neighbours, so p50 of {10,20,30,40} is (20+30)/2, not a rank pick.
        assertRows(execute("SELECT count(*), avg(value_numeric), percentile(value_numeric, 0.5) " +
                           "FROM %s WHERE tag_id = 'TAG-1'"),
                   row(4L, 25.0, 25.0));

        // Statics and the constant columns survive alongside the aggregates.
        assertRows(execute("SELECT site_id, type FROM %s WHERE tag_id = 'TAG-1' LIMIT 1"),
                   row("GN", "double"));
    }

    @Test
    public void testTimeBucketGapfillMultiPartition() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v int, PRIMARY KEY (k, ts))");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:30:00+0000', 5)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 11:15:00+0000', 7)");
        execute("INSERT INTO %s (k, ts, v) VALUES (2, '2024-01-01 09:30:00+0000', 1)");

        String gf = "time_bucket_gapfill(1h, ts, '2024-01-01 09:00:00+0000', '2024-01-01 12:00:00+0000')";
        // Each series is gap-filled independently (k identifies the partition).
        assertRowsIgnoringOrder(execute("SELECT k, " + gf + ", sum(v) FROM %s WHERE k IN (1, 2) GROUP BY k, " + gf),
                                row(1, date("2024-01-01T09:00:00Z"), 5),
                                row(1, date("2024-01-01T10:00:00Z"), null),
                                row(1, date("2024-01-01T11:00:00Z"), 7),
                                row(2, date("2024-01-01T09:00:00Z"), 1),
                                row(2, date("2024-01-01T10:00:00Z"), null),
                                row(2, date("2024-01-01T11:00:00Z"), null));
    }

    @Test
    public void testTimeBucketGapfillBucketCountGuardrail() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v int, PRIMARY KEY (k, ts))");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 00:00:00+0000', 1)");
        // 12 days at 1-second buckets is ~1.04M buckets, over the 1,000,000 cap.
        String gf = "time_bucket_gapfill(1s, ts, '2024-01-01 00:00:00+0000', '2024-01-13 00:00:00+0000')";
        assertInvalidThrowMessage("exceeding the limit",
                                  org.apache.cassandra.exceptions.InvalidRequestException.class,
                                  "SELECT " + gf + ", sum(v) FROM %s WHERE k = 1 GROUP BY k, " + gf);
    }

    @Test
    public void testTimeBucketGapfillRejectsBadRange() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v int, PRIMARY KEY (k, ts))");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:05:00+0000', 1)");
        assertInvalidThrowMessage("finish must be greater than start",
                                  org.apache.cassandra.exceptions.InvalidRequestException.class,
                                  "SELECT time_bucket_gapfill(1h, ts, '2024-01-01 12:00:00+0000', " +
                                  "'2024-01-01 00:00:00+0000') FROM %s WHERE k = 1");
    }
}
