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

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.timeseries.ColumnarChunkCodec;
import org.apache.cassandra.db.timeseries.ColumnarCursor;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Stage 4 of the time-series memtable: a memtable flush encodes windows already colder than
 * {@code hot_window} straight into the {@code __chunks} table and omits their rows from the base
 * sstable (docs/superpowers/specs/2026-08-02-timeseries-memtable-design.md section 5.1).
 *
 * <p>The two claims that matter, and how they are held:
 * <ul>
 *   <li><b>Correctness of the chunks</b>: a cold flush must produce exactly the chunk the re-encoder
 *       ({@link TieredStorageService}) would have produced from the same rows -- asserted by running
 *       both paths over identical data and comparing the chunk rows byte for byte, then decoding
 *       them ({@link #coldWindowFlushMatchesReencoderOutput}).</li>
 *   <li><b>The durability ordering</b>: rows may only be omitted after their chunk write succeeded
 *       (and the ledger was widened). Asserted by injecting a chunk-write failure and requiring the
 *       affected window's rows to still be in the base sstable after the flush
 *       ({@link #chunkInsertFailureKeepsThatWindowsRowsInBase}).</li>
 * </ul>
 *
 * <p>Timestamp convention (as in {@link TransparentReadTest}): event times are small epoch-millis
 * values, so every tested window is far below the real-clock hot boundary the flush path computes;
 * "hot" rows use timestamps near the actual current time. Every query names its table explicitly
 * (never CQLTester's {@code %s}), because several tests hold two tables at once.
 */
public class ColdWindowChunkFlushTest extends CQLTester
{
    private static final long HOUR = 3_600_000L;
    private static final String TSCS_1H =
        "{'class':'TimeSeriesCompactionStrategy','window_size':'1h','freeze_after':'2h'}";

    @After
    public void clearInjection()
    {
        ColdWindowChunkFlush.beforeChunkInsertForTesting = null;
        ColdWindowChunkFlush.budgetMillisForTesting = -1;
    }

    /**
     * The cold-window encode runs on the memtable flush thread and issues a quorum read and a write
     * per window, one after another. A failure was caught per window and the loop moved on -- so
     * when the chunk writes were failing (the case: a backfill has the memtable pool near full, so
     * the chunk-table write itself blocks until its timeout), every remaining window paid its own
     * timeout while the flush thread, and every table queued behind it, waited. The first failure
     * must end chunk I/O for the rest of the flush; the remaining windows flush as rows.
     */
    @Test
    public void firstChunkFailureEndsChunkIoForTheRestOfTheFlush() throws Throwable
    {
        String table = createTieredMemtableTable();
        for (String tag : new String[]{ "a", "b", "c" })
            for (int w = 0; w < 3; w++)
                execute("INSERT INTO " + qualified(table) + " (tag, ts, value) VALUES (?, ?, ?) USING TIMESTAMP ?",
                        tag, new Date(w * HOUR + 60_000L), (double) w, 100L + w);

        AtomicInteger attempts = new AtomicInteger();
        ColdWindowChunkFlush.beforeChunkInsertForTesting = window -> {
            attempts.incrementAndGet();
            throw new RuntimeException("injected chunk-write timeout");
        };
        flush();

        assertEquals("nine windows, one failure: no further chunk write may be attempted", 1, attempts.get());
        for (String tag : new String[]{ "a", "b", "c" })
            assertEquals(3, raw("SELECT * FROM " + qualified(table) + " WHERE tag = ?", tag).size());
        assertEquals(0, execute("SELECT * FROM " + coverageTable(table)).size());
    }

    /**
     * Slow rather than failing chunk I/O must not hold the flush thread indefinitely either: past
     * its budget a flush stops encoding and writes the remaining cold windows as rows, which the
     * re-encoder picks up on its own schedule.
     */
    @Test
    public void flushPastItsChunkBudgetWritesTheRestAsRows() throws Throwable
    {
        String table = createTieredMemtableTable();
        insertTwoColdWindows(table);

        ColdWindowChunkFlush.budgetMillisForTesting = 0;
        AtomicInteger attempts = new AtomicInteger();
        ColdWindowChunkFlush.beforeChunkInsertForTesting = window -> attempts.incrementAndGet();
        flush();

        assertEquals("an exhausted budget must stop chunk writes", 0, attempts.get());
        assertEquals(6, raw("SELECT * FROM " + qualified(table) + " WHERE tag = 't1'").size());
        assertNull(chunkRow(table, 0L));
        assertEquals(6, execute("SELECT * FROM " + qualified(table) + " WHERE tag = 't1'").size());
    }

    @Test
    public void coldWindowFlushMatchesReencoderOutput() throws Throwable
    {
        String flushed = createTieredMemtableTable();
        insertTwoColdWindows(flushed);
        flush();

        // The cold rows were omitted from the base sstable: nothing of them is left as base rows.
        assertEquals(0, raw("SELECT * FROM " + qualified(flushed) + " WHERE tag = 't1'").size());

        // The same data through the re-encoder: a plain table, same rows, same writetimes, one cycle.
        String reencoded = createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        insertTwoColdWindows(reencoded);
        new TieredStorageService().runOnce(KEYSPACE, reencoded, System.currentTimeMillis());

        for (long windowStart : new long[]{ 0L, HOUR })
        {
            UntypedResultSet.Row fromFlush = chunkRow(flushed, windowStart);
            UntypedResultSet.Row fromReencoder = chunkRow(reencoded, windowStart);
            assertNotNull("cold flush wrote no chunk for window " + windowStart, fromFlush);
            assertNotNull("re-encoder wrote no chunk for window " + windowStart, fromReencoder);

            assertEquals("samples differ in window " + windowStart,
                         fromReencoder.getInt("samples"), fromFlush.getInt("samples"));
            assertEquals("max_row_writetime differs in window " + windowStart,
                         fromReencoder.getLong("max_row_writetime"), fromFlush.getLong("max_row_writetime"));
            assertEquals("codec differs in window " + windowStart,
                         fromReencoder.getByte("codec"), fromFlush.getByte("codec"));
            ByteBuffer flushPayload = fromFlush.getBytes("payload");
            ByteBuffer reencoderPayload = fromReencoder.getBytes("payload");
            assertEquals("payload bytes differ in window " + windowStart, reencoderPayload, flushPayload);

            // Byte equality already implies it, but assert the decoded content too -- that is the
            // form of the correctness claim, and it fails with a readable message if the bytes drift.
            ColumnarCursor expected = ColumnarChunkCodec.cursor(reencoderPayload, null);
            ColumnarCursor actual = ColumnarChunkCodec.cursor(flushPayload, null);
            while (expected.advance())
            {
                assertTrue(actual.advance());
                assertEquals(expected.timestamp(), actual.timestamp());
                assertEquals(DoubleType.instance.compose(expected.getBytes("value")),
                             DoubleType.instance.compose(actual.getBytes("value")), 0.0);
            }
            assertFalse(actual.advance());
        }
    }

    @Test
    public void hotWindowIsNotChunked() throws Throwable
    {
        String table = createTieredMemtableTable();
        // Three cold rows in window [0, 1h)...
        insertRow(table, 10 * 60_000L, 1.0, 101);
        insertRow(table, 20 * 60_000L, 2.0, 102);
        insertRow(table, 30 * 60_000L, 3.0, 103);
        // ...and two hot rows, timestamped within the last half hour of real time: their windows sit
        // at or above the cutoff the flush computes from the real clock.
        long now = System.currentTimeMillis();
        execute("INSERT INTO " + qualified(table) + " (tag, ts, value) VALUES ('t1', ?, 4.0)",
                new Date(now - 30 * 60_000L));
        execute("INSERT INTO " + qualified(table) + " (tag, ts, value) VALUES ('t1', ?, 5.0)",
                new Date(now - 20 * 60_000L));
        flush();

        // The hot rows were flushed as ordinary base rows...
        assertEquals(2, raw("SELECT * FROM " + qualified(table) + " WHERE tag = 't1' AND ts >= ?",
                            new Date(now - HOUR)).size());
        // ...the cold rows were not...
        assertEquals(0, raw("SELECT * FROM " + qualified(table) + " WHERE tag = 't1' AND ts < ?",
                            new Date(HOUR)).size());
        // ...and the only chunk written is the cold window's.
        UntypedResultSet chunks = execute("SELECT window_start FROM " + chunkTable(table) + " WHERE tag = 't1'");
        assertEquals(1, chunks.size());
        assertEquals(0L, chunks.one().getTimestamp("window_start").getTime());

        // Everything remains visible to an ordinary SELECT.
        assertEquals(5, execute("SELECT * FROM " + qualified(table) + " WHERE tag = 't1'").size());
    }

    @Test
    public void readsMergeChunkFlushedRowsAcrossHotColdBoundary() throws Throwable
    {
        String table = createTieredMemtableTable();
        insertRow(table, 10 * 60_000L, 1.0, 101);
        insertRow(table, 20 * 60_000L, 2.0, 102);
        insertRow(table, 30 * 60_000L, 3.0, 103);
        insertRow(table, HOUR + 10 * 60_000L, 4.0, 104);
        insertRow(table, HOUR + 20 * 60_000L, 5.0, 105);
        long hotTs = System.currentTimeMillis() - 30 * 60_000L;
        execute("INSERT INTO " + qualified(table) + " (tag, ts, value) VALUES ('t1', ?, 6.0)", new Date(hotTs));
        flush();

        // Sanity: the cold rows really are only in chunks now (nothing left to answer from base).
        assertEquals(0, raw("SELECT * FROM " + qualified(table) + " WHERE tag = 't1' AND ts < ?",
                            new Date(2 * HOUR)).size());

        // Time-range SELECT spanning the whole partition: hot and cold merged, in order.
        UntypedResultSet all = execute("SELECT value FROM " + qualified(table) + " WHERE tag = 't1'");
        assertEquals(6, all.size());
        double[] expected = { 1.0, 2.0, 3.0, 4.0, 5.0, 6.0 };
        int i = 0;
        for (UntypedResultSet.Row row : all)
            assertEquals(expected[i++], row.getDouble("value"), 0.0);

        // Cold-only time range.
        assertEquals(3, execute("SELECT value FROM " + qualified(table) + " WHERE tag = 't1' AND ts >= ? AND ts < ?",
                                new Date(0), new Date(HOUR)).size());

        // Point lookup on a chunked timestamp.
        UntypedResultSet point = execute("SELECT value FROM " + qualified(table) + " WHERE tag = 't1' AND ts = ?",
                                         new Date(20 * 60_000L));
        assertEquals(1, point.size());
        assertEquals(2.0, point.one().getDouble("value"), 0.0);

        // Aggregate over hot and cold together.
        UntypedResultSet aggregate = execute("SELECT count(value) AS c, avg(value) AS a FROM " + qualified(table) +
                                             " WHERE tag = 't1'");
        assertEquals(6L, aggregate.one().getLong("c"));
        assertEquals(3.5, aggregate.one().getDouble("a"), 0.0);

        // DESC with a LIMIT reaching across the boundary: the hot row first, then the newest cold ones.
        UntypedResultSet newest = execute("SELECT value FROM " + qualified(table) +
                                          " WHERE tag = 't1' ORDER BY ts DESC LIMIT 4");
        assertEquals(4, newest.size());
        double[] expectedDesc = { 6.0, 5.0, 4.0, 3.0 };
        i = 0;
        for (UntypedResultSet.Row row : newest)
            assertEquals(expectedDesc[i++], row.getDouble("value"), 0.0);

        // ASC LIMIT served entirely from chunks.
        UntypedResultSet oldest = execute("SELECT value FROM " + qualified(table) + " WHERE tag = 't1' LIMIT 2");
        assertEquals(2, oldest.size());
        assertEquals(1.0, oldest.iterator().next().getDouble("value"), 0.0);
    }

    /**
     * The ordering rule under failure: a window whose chunk write fails must keep its rows in the
     * base sstable -- omitting them first (or despite the failure) is exactly the data-loss bug the
     * durability order exists to prevent. Other windows of the same flush are unaffected.
     */
    @Test
    public void chunkInsertFailureKeepsThatWindowsRowsInBase() throws Throwable
    {
        String table = createTieredMemtableTable();
        insertTwoColdWindows(table);

        ColdWindowChunkFlush.beforeChunkInsertForTesting = window -> {
            if (window == HOUR)
                throw new RuntimeException("injected chunk-write failure");
        };
        flush();  // must succeed: a failed window falls back to rows, it does not fail the flush

        // The failed window's rows are still there, as ordinary base rows in the flushed sstable.
        assertEquals(3, raw("SELECT * FROM " + qualified(table) + " WHERE tag = 't1' AND ts >= ? AND ts < ?",
                            new Date(HOUR), new Date(2 * HOUR)).size());
        // The window whose chunk write succeeded was omitted as designed.
        assertEquals(0, raw("SELECT * FROM " + qualified(table) + " WHERE tag = 't1' AND ts < ?",
                            new Date(HOUR)).size());
        // Chunks exist only where the write succeeded.
        assertNotNull(chunkRow(table, 0L));
        assertNull(chunkRow(table, HOUR));
        // Nothing is lost either way: a plain SELECT still sees every row.
        assertEquals(6, execute("SELECT * FROM " + qualified(table) + " WHERE tag = 't1'").size());
        // The ledger was widened over the successful window only.
        UntypedResultSet coverage = execute("SELECT min_window_start, max_window_start FROM " + coverageTable(table));
        assertEquals(1, coverage.size());
        assertEquals(0L, coverage.one().getTimestamp("min_window_start").getTime());
        assertEquals(0L, coverage.one().getTimestamp("max_window_start").getTime());
    }

    /** As above, with every chunk write failing: the flush degrades to a plain row flush. */
    @Test
    public void chunkInsertFailureOnEveryWindowLeavesEverythingAsRows() throws Throwable
    {
        String table = createTieredMemtableTable();
        insertTwoColdWindows(table);

        ColdWindowChunkFlush.beforeChunkInsertForTesting = window -> {
            throw new RuntimeException("injected chunk-write failure");
        };
        flush();

        assertEquals(6, raw("SELECT * FROM " + qualified(table) + " WHERE tag = 't1'").size());
        assertNull(chunkRow(table, 0L));
        assertNull(chunkRow(table, HOUR));
        // Nothing durable was written, so the ledger must not claim anything.
        assertEquals(0, execute("SELECT * FROM " + coverageTable(table)).size());
        assertEquals(6, execute("SELECT * FROM " + qualified(table) + " WHERE tag = 't1'").size());
    }

    @Test
    public void coverageLedgerReflectsChunkedRange() throws Throwable
    {
        String table = createTieredMemtableTable();
        insertTwoColdWindows(table);
        flush();

        UntypedResultSet coverage = execute("SELECT min_window_start, max_window_start, max_chunk_window FROM " +
                                            coverageTable(table));
        assertEquals(1, coverage.size());
        UntypedResultSet.Row row = coverage.one();
        assertEquals(0L, row.getTimestamp("min_window_start").getTime());
        assertEquals(HOUR, row.getTimestamp("max_window_start").getTime());
        assertEquals(HOUR, row.getLong("max_chunk_window"));

        // And the read path's cached view agrees: cold data reaches up to the end of the last window.
        ChunkCoverage.Coverage cached = ChunkCoverage.forTable(Schema.instance.getTableMetadata(KEYSPACE, table),
                                                               null);
        assertTrue(cached.anyChunks());
        assertEquals(2 * HOUR, cached.topExclusiveMs());
    }

    /**
     * The schema gate is {@link TieringPolicy#unsupportedSchemaError} and no stricter: a table whose
     * shape tiering cannot encode (here, a non-frozen collection) simply flushes as rows, as today.
     */
    @Test
    public void unsupportedSchemaFallsBackToRowFlush() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (tag text, ts timestamp, value double, vals list<double>, " +
                                   "PRIMARY KEY (tag, ts)) WITH compaction = " + TSCS_1H +
                                   " AND memtable = 'timeseries'");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        ChunkTables.ensureChunkTable(Schema.instance.getTableMetadata(KEYSPACE, table));
        ChunkCoverage.invalidateAll();

        insertRow(table, 10 * 60_000L, 1.0, 101);
        insertRow(table, 20 * 60_000L, 2.0, 102);
        flush();

        assertEquals(2, raw("SELECT * FROM " + qualified(table) + " WHERE tag = 't1'").size());
        assertEquals(0, execute("SELECT * FROM " + chunkTable(table) + " WHERE tag = 't1'").size());
    }

    /** A table without a tiering policy is entirely unaffected: rows flush as rows, no chunk table appears. */
    @Test
    public void tableWithoutTieringPolicyIsUntouched() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) " +
                                   "WITH compaction = " + TSCS_1H + " AND memtable = 'timeseries'");
        insertRow(table, 10 * 60_000L, 1.0, 101);
        insertRow(table, 20 * 60_000L, 2.0, 102);
        flush();

        assertEquals(2, execute("SELECT * FROM " + qualified(table) + " WHERE tag = 't1'").size());
        assertNull(Schema.instance.getTableMetadata(KEYSPACE, ChunkTables.chunkTableName(table)));
    }

    /**
     * Regression: an sstable holding only static rows covers an EMPTY clustering range, and that
     * range's bounds are neither BOTTOM nor TOP yet have no clustering component. The encoder's
     * intersection guard read {@code bufferAt(0)} from one and threw AIOOBE, which sent the whole
     * flush down the row fallback -- production hit this on 2026-08-02 at the first cold flush of a
     * table whose seed writes were static-only. The chunk flush must survive the static-only
     * sstable and still chunk the cold window.
     */
    @Test
    public void staticOnlySSTableDoesNotBreakTheColdFlush() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (tag text, ts timestamp, unit text static, " +
                                   "value double, PRIMARY KEY (tag, ts)) " +
                                   "WITH compaction = " + TSCS_1H + " AND memtable = 'timeseries'");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        ChunkTables.ensureChunkTable(Schema.instance.getTableMetadata(KEYSPACE, table));
        ChunkCoverage.invalidateAll();

        // A static-only write, flushed alone: its sstable covers an empty clustering range.
        execute("INSERT INTO " + qualified(table) + " (tag, unit) VALUES ('t1', 'kPa')");
        flush();

        // Now a cold window. Before the fix this flush logged
        // "Cold-window chunk flush failed ... AIOOBE" and fell back to rows.
        insertRow(table, 10 * 60_000L, 1.0, 101);
        insertRow(table, 20 * 60_000L, 2.0, 102);
        flush();

        assertFalse("the cold window must reach the chunk table despite the static-only sstable",
                    execute("SELECT * FROM " + chunkTable(table) + " WHERE tag = 't1'").isEmpty());
        // And the data stays correct through the transparent read, statics included.
        assertEquals(2, execute("SELECT ts, value FROM " + qualified(table) + " WHERE tag = 't1'").size());
        assertEquals("kPa", execute("SELECT unit FROM " + qualified(table) + " WHERE tag = 't1' LIMIT 1")
                            .one().getString("unit"));
    }

    // ------------------------------------------------------------------------------------ helpers

    /** A TSCS table on the time-series memtable with a tiering policy and its chunk table in place. */
    private String createTieredMemtableTable() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) " +
                                   "WITH compaction = " + TSCS_1H + " AND memtable = 'timeseries'");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        // The flush path never issues DDL; the chunk table is normally created by the re-encoder's
        // first cycle, so stand in for that here.
        ChunkTables.ensureChunkTable(Schema.instance.getTableMetadata(KEYSPACE, table));
        ChunkCoverage.invalidateAll();
        return table;
    }

    /** Six rows over windows [0, 1h) and [1h, 2h), with explicit, deterministic writetimes. */
    private void insertTwoColdWindows(String table) throws Throwable
    {
        insertRow(table, 10 * 60_000L, 1.0, 101);
        insertRow(table, 20 * 60_000L, 2.0, 102);
        insertRow(table, 30 * 60_000L, 3.0, 103);
        insertRow(table, HOUR + 10 * 60_000L, 4.0, 104);
        insertRow(table, HOUR + 20 * 60_000L, 5.0, 105);
        insertRow(table, HOUR + 40 * 60_000L, 6.0, 106);
    }

    private void insertRow(String table, long tsMillis, double value, long writetime) throws Throwable
    {
        execute("INSERT INTO " + qualified(table) + " (tag, ts, value) VALUES ('t1', ?, ?) USING TIMESTAMP ?",
                new Date(tsMillis), value, writetime);
    }

    private void setPolicy(String json) throws Throwable
    {
        String hex = ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(json));
        alterTable("ALTER TABLE %s WITH extensions = {'" + TieringPolicy.EXTENSION_KEY + "': 0x" + hex + "};");
    }

    private String qualified(String table)
    {
        return KEYSPACE + '.' + table;
    }

    private String chunkTable(String table)
    {
        return KEYSPACE + '.' + ChunkTables.chunkTableName(table);
    }

    private String coverageTable(String table)
    {
        return KEYSPACE + '.' + ChunkTables.coverageTableName(table);
    }

    /** @return the chunk row of {@code (t1, windowStart)}, or null if none was written. */
    private UntypedResultSet.Row chunkRow(String table, long windowStart) throws Throwable
    {
        UntypedResultSet rows = execute("SELECT codec, samples, max_row_writetime, payload FROM " + chunkTable(table) +
                                        " WHERE tag = 't1' AND window_start = ?", new Date(windowStart));
        return rows.isEmpty() ? null : rows.one();
    }

    /** Executes {@code query} with the transparent-read merge bypassed: base-table rows only. */
    private UntypedResultSet raw(String query, Object... values) throws Throwable
    {
        TransparentReads.enterInternalBypass();
        try
        {
            return execute(query, values);
        }
        finally
        {
            TransparentReads.exitInternalBypass();
        }
    }
}
