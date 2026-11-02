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

package org.apache.cassandra.db.timeseries.tiering;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.InvalidQueryException;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Range scans on a tiered table merge chunk rows (range-scan-merge-design.md section 1), checked
 * against an <b>oracle</b>: a twin table holding the same rows that was never tiered. Every query is
 * run on both through the native protocol -- the coordinator path, where the merge lives -- and the
 * answers must be identical, row for row and in order.
 * <p>
 * The fixture mixes the three partition shapes the merge join distinguishes: chunk-only partitions
 * (every row encoded, base rows deleted), mixed ones (old windows encoded, a hot row left) and hot-only
 * ones; plus late rows written into encoded windows after the cycle. Paging is exercised at fetch
 * sizes small enough that page boundaries fall inside chunk-only partitions.
 */
public class TransparentRangeReadTest extends CQLTester
{
    private static final long HOUR = 3_600_000L;
    private static final long MINUTE = 60_000L;
    private static final int TAGS_PER_SHAPE = 12;
    private static final int[] FETCH_SIZES = { 1, 2, 7, 100 };

    private String twin;

    @BeforeClass
    public static void acceptPastTimestampsAndStartNetwork()
    {
        // Not named setUpClass: that would hide CQLTester.setUpClass, which loads the config.
        // Fixtures pin explicit 1970 writetimes, the shape the past-timestamp guard refuses on a tiered table.
        TieredWrites.acceptPastTimestampsIntoColdDataForTesting = true;
        requireNetwork();
    }

    private void setPolicy(String json) throws Throwable
    {
        String hex = ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(json));
        alterTable("ALTER TABLE %s WITH extensions = {'" + TieringPolicy.EXTENSION_KEY + "': 0x" + hex + "};");
    }

    /**
     * Twin first, tiered table second (so {@code %s} is the tiered one). Rows at hours 0-2 are cold
     * and encoded by the cycle at a synthetic now of 6h (hot_window 2h); rows at hour 5 stay hot.
     */
    private void loadTwinsAndReencode(String clusteringOrder) throws Throwable
    {
        String schema = "CREATE TABLE %s (tag text, ts timestamp, value double, label text, PRIMARY KEY (tag, ts)) " +
                        "WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}" + clusteringOrder;
        twin = createTable(schema);
        createTable(schema);
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        long writetime = 1000;
        for (int i = 0; i < TAGS_PER_SHAPE; i++)
        {
            for (int hour : new int[]{ 0, 1, 2 })
                for (int minute : new int[]{ 5, 25, 45 })
                    writeBoth(String.format("c%02d", i), hour * HOUR + minute * MINUTE, i * 100 + hour * 10 + minute / 20, writetime++);
            for (int hour : new int[]{ 0, 1, 5 })
                writeBoth(String.format("m%02d", i), hour * HOUR + 15 * MINUTE, 5000 + i * 100 + hour, writetime++);
            writeBoth(String.format("h%02d", i), 5 * HOUR + 35 * MINUTE, 9000 + i, writetime++);
        }

        TieredStorageService.TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), 6 * HOUR);
        assertTrue("the cycle must encode the cold windows: " + stats, stats.windowsEncoded > 0);
        // The deletion gate: every comparison below would also pass if the re-encoder had left the base rows in
        // place. So the cold rows must be gone from the base table -- the chunk the only copy -- before the twin
        // comparisons mean anything.
        assertEquals("a chunk-only partition has no base rows left", 0, rawBaseRows("c05"));
        assertEquals("a mixed partition keeps only its hot row", 1, rawBaseRows("m05"));
        assertEquals("a hot-only partition is untouched", 1, rawBaseRows("h05"));

        // Late rows into encoded windows: an overwrite of an encoded row, and a new row beside encoded ones.
        writeBoth("c00", 25 * MINUTE, -1.5, 900_000);
        writeBoth("m03", HOUR + 40 * MINUTE, -2.5, 900_001);
    }

    /** Rows the tiered table's base table physically holds for {@code tag}, bypassing the chunk merge. */
    private int rawBaseRows(String tag) throws Throwable
    {
        TransparentReads.enterInternalBypass();
        try
        {
            return execute("SELECT ts FROM %s WHERE tag = ?", tag).size();
        }
        finally
        {
            TransparentReads.exitInternalBypass();
        }
    }

    private void writeBoth(String tag, long ts, double value, long writetime) throws Throwable
    {
        String insert = "INSERT INTO %s (tag, ts, value, label) VALUES (?, ?, ?, ?) USING TIMESTAMP " + writetime;
        execute(insert, tag, new Date(ts), value, tag + '@' + ts);
        execute(String.format(insert, KEYSPACE + '.' + twin), tag, new Date(ts), value, tag + '@' + ts);
    }

    private List<String> rows(String table, String query, int fetchSize, Object... values)
    {
        String cql = query.replace("%s", KEYSPACE + '.' + table);
        ResultSet result = sessionNet().execute(new SimpleStatement(cql, values).setFetchSize(fetchSize));
        List<String> out = new ArrayList<>();
        for (Row row : result)
            out.add(row.toString());
        return out;
    }

    private void assertSameAsTwin(String query, Object... values)
    {
        List<String> expected = rows(twin, query, 5000, values);
        assertEquals("tiered scan vs never-tiered twin: " + query, expected, rows(currentTable(), query, 5000, values));
        for (int fetchSize : FETCH_SIZES)
            assertEquals("fetch size " + fetchSize + ": " + query, expected, rows(currentTable(), query, fetchSize, values));
    }

    private void assertTheWholeSuiteMatches()
    {
        assertSameAsTwin("SELECT tag, ts, value, label FROM %s");
        assertSameAsTwin("SELECT * FROM %s");
        assertSameAsTwin("SELECT count(*) FROM %s");
        assertSameAsTwin("SELECT DISTINCT tag FROM %s");
        assertSameAsTwin("SELECT tag, value FROM %s LIMIT 7");
        assertSameAsTwin("SELECT tag, value FROM %s LIMIT 50");
        assertSameAsTwin("SELECT tag, ts FROM %s PER PARTITION LIMIT 2");
        assertSameAsTwin("SELECT tag, ts FROM %s PER PARTITION LIMIT 1 LIMIT 9");
        assertSameAsTwin("SELECT max(value), min(ts), count(label) FROM %s");
        assertSameAsTwin("SELECT label FROM %s");
        assertSameAsTwin("SELECT tag, ts, value FROM %s WHERE ts < ? ALLOW FILTERING", new Date(HOUR));
        assertSameAsTwin("SELECT tag, ts, value FROM %s WHERE ts >= ? AND ts < ? ALLOW FILTERING",
                         new Date(30 * MINUTE), new Date(HOUR + 30 * MINUTE));
        assertSameAsTwin("SELECT tag, value FROM %s WHERE ts = ? ALLOW FILTERING", new Date(HOUR + 25 * MINUTE));
        assertSameAsTwin("SELECT tag, value FROM %s WHERE ts > ? ALLOW FILTERING", new Date(4 * HOUR));
    }

    @Test
    public void rangeScansReturnWhatTheNeverTieredTwinReturns() throws Throwable
    {
        loadTwinsAndReencode("");
        assertTheWholeSuiteMatches();
    }

    @Test
    public void rangeScansOnADescendingTableReturnWhatTheTwinReturns() throws Throwable
    {
        loadTwinsAndReencode(" AND CLUSTERING ORDER BY (ts DESC)");
        assertTheWholeSuiteMatches();
    }

    /** Spark's shape: the union of token-range splits is the full scan, with no partition twice or missing. */
    @Test
    public void tokenRangeSplitsUnionToTheFullScan() throws Throwable
    {
        loadTwinsAndReencode("");
        String split = "SELECT tag, ts, value FROM %s WHERE token(tag) > ? AND token(tag) <= ?";
        BigInteger min = BigInteger.valueOf(Long.MIN_VALUE);
        BigInteger width = BigInteger.valueOf(Long.MAX_VALUE).subtract(min).divide(BigInteger.valueOf(16));
        List<String> union = new ArrayList<>();
        for (int i = 0; i < 16; i++)
        {
            long left = min.add(width.multiply(BigInteger.valueOf(i))).longValueExact();
            long right = i == 15 ? Long.MAX_VALUE : min.add(width.multiply(BigInteger.valueOf(i + 1))).longValueExact();
            assertSameAsTwin(split, left, right);
            union.addAll(rows(currentTable(), split, 3, left, right));
        }
        assertEquals(rows(twin, "SELECT tag, ts, value FROM %s", 5000), union);
    }

    /** The join enumerates chunk keys once per sub-range and never lists windows for a hot-only key. */
    @Test
    public void aFullScanListsEachChunkedPartitionOnce() throws Throwable
    {
        loadTwinsAndReencode("");
        long keysBefore = ChunkRowSource.chunkKeysListed.get();
        long mergedBefore = TieringMetrics.count(KEYSPACE, currentTable(), TieringMetrics.Event.RANGE_SCAN_MERGED);

        assertEquals(rows(twin, "SELECT * FROM %s", 5000), rows(currentTable(), "SELECT * FROM %s", 5000));

        assertEquals("one key per chunked partition (the c* and m* tags), none for the h* tags",
                     2L * TAGS_PER_SHAPE, ChunkRowSource.chunkKeysListed.get() - keysBefore);
        assertTrue("merged scans are counted",
                   TieringMetrics.count(KEYSPACE, currentTable(), TieringMetrics.Event.RANGE_SCAN_MERGED) > mergedBefore);
    }

    /** Replicas filter hot rows by a non-key column; chunk rows join after that, so such a scan still refuses. */
    @Test
    public void aScanFilteringOnARegularColumnIsStillRefused() throws Throwable
    {
        loadTwinsAndReencode("");
        assertRefused("SELECT * FROM %s WHERE value > 1 ALLOW FILTERING", "non-key column");
        assertRefused("SELECT * FROM %s WHERE label = 'c00@300000' ALLOW FILTERING", "non-key column");
    }

    @Test
    public void rangeScanRefuseInThePolicyRestoresTheRefusal() throws Throwable
    {
        loadTwinsAndReencode("");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\",\"range_scan\":\"refuse\"}");
        assertRefused("SELECT * FROM %s", "range_scan to 'refuse'");
        assertRefused("SELECT count(*) FROM %s", "range_scan to 'refuse'");

        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\",\"range_scan\":\"merge\"}");
        assertSameAsTwin("SELECT count(*) FROM %s");
    }

    @Test
    public void theNodeSwitchRefusesEveryMergeUntilTurnedBackOn() throws Throwable
    {
        loadTwinsAndReencode("");
        TieredStorageService service = new TieredStorageService();
        service.setRangeScanMergeEnabled(false);
        try
        {
            assertRefused("SELECT * FROM %s", "switched off");
        }
        finally
        {
            service.setRangeScanMergeEnabled(true);
        }
        assertSameAsTwin("SELECT * FROM %s");
    }

    @Test
    public void anUnknownRangeScanValueIsRejected() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) " +
                    "WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        try
        {
            setPolicy("{\"hot_window\":\"2h\",\"range_scan\":\"sometimes\"}");
            fail("range_scan accepts only 'merge' or 'refuse'");
        }
        catch (Throwable e)
        {
            assertTrue(String.valueOf(e.getMessage()), String.valueOf(e.getMessage()).contains("range_scan"));
        }
    }

    private void assertRefused(String query, String reason)
    {
        long refusedBefore = TieringMetrics.count(KEYSPACE, currentTable(), TieringMetrics.Event.RANGE_SCAN_REFUSED);
        try
        {
            int n = rows(currentTable(), query, 5000).size();
            fail("expected the scan to be refused (" + reason + "), got " + n + " row(s): " + query);
        }
        catch (InvalidQueryException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains(reason));
            assertTrue(e.getMessage(), e.getMessage().contains("partition key"));
        }
        assertEquals(refusedBefore + 1, TieringMetrics.count(KEYSPACE, currentTable(), TieringMetrics.Event.RANGE_SCAN_REFUSED));
    }
}
