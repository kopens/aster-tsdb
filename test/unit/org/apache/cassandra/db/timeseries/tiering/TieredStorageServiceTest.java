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
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.AbstractCompactionTask;
import org.apache.cassandra.db.compaction.ActiveCompactionsTracker;
import org.apache.cassandra.db.compaction.FreezeCompactionTask;
import org.apache.cassandra.db.compaction.TimeSeriesCompactionStrategy;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.timeseries.ChunkV4Codec;
import org.apache.cassandra.db.timeseries.ChunkV4Directory;
import org.apache.cassandra.db.timeseries.ColumnarChunkCodec;
import org.apache.cassandra.db.timeseries.ColumnarCursor;
import org.apache.cassandra.db.timeseries.StatOrder;
import org.apache.cassandra.db.timeseries.tiering.TieredStorageService.TierRunStats;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.reads.thresholds.CoordinatorWarnings;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration tests for {@link TieredStorageService#runOnce}, exercising the whole re-encode cycle
 * against a real (single-node) schema and real CQL queries -- see
 * docs/superpowers/plans/2026-07-31-chunk-store-sp2.md, Task 2, for the scenarios this covers.
 */
public class TieredStorageServiceTest extends CQLTester
{
    private static final long HOUR = 3_600_000L;

    // NOT named setUpClass(): that exact name would shadow CQLTester's own @BeforeClass setUpClass()
    // (same name+signature in the hierarchy -- only the most-derived one runs), skipping the server/
    // schema setup it performs and breaking every test in this class, not just the ones added here.
    @BeforeClass
    public static void setUpVirtualKeyspace()
    {
        addVirtualKeyspace(); // registers system_views, for virtualTableShowsPolicyAndStats
    }

    @Test
    public void encodeClosedWindowsAndDeleteRows() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        String[] tags = { "a", "b", "c" };
        long now = 5 * HOUR;
        long wt = 1;
        for (String tag : tags)
        {
            for (int w = 0; w < 3; w++)
            {
                long windowStart = w * HOUR;
                for (int r = 0; r < 4; r++)
                    insertRow(tag, windowStart + r * 600_000L, w * 100.0 + r, wt++);
            }
            insertRow(tag, 4 * HOUR, 999.0, wt++); // hot row -- must survive untouched
        }

        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), now);

        assertEquals(9, stats.windowsEncoded); // 3 tags * 3 closed windows
        assertEquals(36, stats.rowsEncoded);   // 3 tags * 3 windows * 4 rows
        assertEquals(0, stats.lateMerges);
        assertEquals(0, stats.chunksExpired);
        assertTrue(stats.bytesWritten > 0);

        for (String tag : tags)
        {
            for (int w = 0; w < 3; w++)
            {
                long windowStart = w * HOUR;
                UntypedResultSet chunkRows = execute(chunkSelectQuery(), tag, new Date(windowStart));
                assertEquals(1, chunkRows.size());
                UntypedResultSet.Row chunkRow = chunkRows.one();
                assertEquals(4, chunkRow.getInt("samples"));
                assertEquals(ColumnarChunkCodec.VERSION, chunkRow.getByte("codec"));

                assertEquals(0, raw("SELECT * FROM %s WHERE tag = ? AND ts >= ? AND ts < ?",
                                        tag, new Date(windowStart), new Date(windowStart + HOUR)).size());
            }

            assertEquals(1, raw("SELECT * FROM %s WHERE tag = ? AND ts = ?", tag, new Date(4 * HOUR)).size());
        }
    }

    @Test
    public void hotWindowRowsSurviveDenseIngestion() throws Throwable
    {
        // Regression for a round-2 review finding: the window walk's cutoff check must be enforced on
        // EVERY path that can advance windowStart, including the dense "windowStart = windowEnd"
        // continuation after a successfully-processed window -- not just on initial discovery and the
        // empty-window jump. Continuous, gap-free ingestion across the cutoff boundary is required to
        // exercise this: an empty window anywhere would route through nextClosedWindowStart's own
        // cutoff check and mask the bug, which is exactly how the round-1 test suite missed it (every
        // hot marker row there sat past an empty window).
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        long now = 5 * HOUR;
        long cutoff = 3 * HOUR; // windowStartFor(now - hot_window) = windowStartFor(3h) = 3h

        insertRow("dense", 0L, 10.0, 1);          // window [0,1h)      -- closed
        insertRow("dense", HOUR, 11.0, 2);        // window [1h,2h)     -- closed
        insertRow("dense", cutoff - 1, 12.0, 3);  // window [2h,3h)     -- closed; pins cutoff-1ms
        insertRow("dense", cutoff, 13.0, 4);      // window [3h,4h)     -- hot; pins ts == cutoff exactly
        insertRow("dense", 4 * HOUR, 14.0, 5);    // window [4h,5h)     -- hot ("current" window)

        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), now);

        assertEquals(3, stats.windowsEncoded); // only the 3 closed windows
        assertEquals(3, stats.rowsEncoded);

        // Every row at or after cutoff -- including the one exactly at ts == cutoff -- must still be
        // in base, completely untouched.
        assertEquals(2, raw("SELECT * FROM %s WHERE tag = ? AND ts >= ?", "dense", new Date(cutoff)).size());
        assertEquals(1, raw("SELECT * FROM %s WHERE tag = ? AND ts = ?", "dense", new Date(cutoff)).size());
        assertEquals(1, raw("SELECT * FROM %s WHERE tag = ? AND ts = ?", "dense", new Date(4 * HOUR)).size());

        // The closed rows (ts < cutoff) are gone.
        assertEquals(0, raw("SELECT * FROM %s WHERE tag = ? AND ts < ?", "dense", new Date(cutoff)).size());

        // No chunk was ever written for a window at or after cutoff.
        assertEquals(0, execute(chunkSelectQuery(), "dense", new Date(cutoff)).size());
        assertEquals(0, execute(chunkSelectQuery(), "dense", new Date(4 * HOUR)).size());

        // The three closed windows were encoded normally.
        for (long windowStart : new long[]{ 0L, HOUR, 2 * HOUR })
            assertEquals(1, execute(chunkSelectQuery(), "dense", new Date(windowStart)).size());
    }

    @Test
    public void roundtripThroughChunks() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        long[] tsValues = { 0L, 600_000L, 1_200_000L, 1_800_000L, 2_400_000L };
        double[] values = { 1.5, -2.25, 3.0, 0.0, 42.125 };
        long wt = 1;
        for (int i = 0; i < tsValues.length; i++)
            insertRow("solo", tsValues[i], values[i], wt++);
        insertRow("solo", 4 * HOUR, 999.0, wt++);

        new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);

        UntypedResultSet.Row chunkRow = execute(chunkSelectQuery(), "solo", new Date(0L)).one();
        ColumnarCursor cursor = ColumnarChunkCodec.cursor(chunkRow.getBytes("payload"), null);
        for (int i = 0; i < tsValues.length; i++)
        {
            assertTrue(cursor.advance());
            assertEquals(tsValues[i], cursor.timestamp());
            assertEquals(values[i], doubleAt(cursor, "value"), 0.0);
        }
        assertFalse(cursor.advance());
    }

    @Test
    public void deleteTimestampPreservesLateRows() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        for (int i = 0; i < 4; i++)
            insertRow("t", i * 600_000L, i * 1.0, 100 + i); // writetimes 100..103
        insertRow("t", 4 * HOUR, 999.0, 200); // hot row -- keeps the tag enumerated after the window is cleared

        TieredStorageService service = new TieredStorageService();
        TierRunStats first = service.runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertEquals(1, first.windowsEncoded);
        assertEquals(4, first.rowsEncoded);
        assertEquals(0, first.lateMerges);

        assertEquals(0, raw("SELECT * FROM %s WHERE tag = ? AND ts >= ? AND ts < ?",
                                "t", new Date(0L), new Date(HOUR)).size());

        // Late row: newer writetime than the tombstone the first run issued (USING TIMESTAMP 103),
        // which is all this test needs -- every read here runs under enterInternalBypass() (see
        // raw()), so it asserts on PHYSICAL rows and never exercises the hot+chunk merge. 110 rather
        // than 104 only so the number does not sit on max_row_writetime + 1, where the merge's
        // documented tie would apply if this test were ever changed to read through it;
        // TieredStorageColumnsTest#aLateRowAtExactlyMaxWritetimePlusOneTiesWithTheChunk covers that.
        long lateTs = 2_400_000L;
        insertRow("t", lateTs, 42.0, 110);

        UntypedResultSet survived = raw("SELECT * FROM %s WHERE tag = ? AND ts = ?", "t", new Date(lateTs));
        assertEquals(1, survived.size());
        assertEquals(42.0, survived.one().getDouble("value"), 0.0);

        TierRunStats second = service.runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertEquals(1, second.windowsEncoded);
        assertEquals(1, second.rowsEncoded);
        assertEquals(1, second.lateMerges);

        UntypedResultSet.Row chunkRow = execute(chunkSelectQuery(), "t", new Date(0L)).one();
        assertEquals(5, chunkRow.getInt("samples"));

        ColumnarCursor cursor = ColumnarChunkCodec.cursor(chunkRow.getBytes("payload"), null);
        boolean foundLate = false;
        while (cursor.advance())
        {
            if (cursor.timestamp() == lateTs)
            {
                foundLate = true;
                assertEquals(42.0, doubleAt(cursor, "value"), 0.0);
            }
        }
        assertTrue("expected the late sample to be present in the merged chunk", foundLate);

        assertEquals(0, raw("SELECT * FROM %s WHERE tag = ? AND ts = ?", "t", new Date(lateTs)).size());
    }

    @Test
    public void idempotentWhenInterrupted() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        TableMetadata base = getCurrentColumnFamilyStore().metadata();
        ChunkTables.ensureChunkTable(base);

        long[] tsValues = { 0L, 600_000L, 1_200_000L, 1_800_000L };
        double[] values = { 1.0, 2.0, 3.0, 4.0 };
        for (int i = 0; i < tsValues.length; i++)
            insertRow("i", tsValues[i], values[i], 297 + i); // writetimes 297..300
        insertRow("i", 4 * HOUR, 999.0, 400); // hot row -- keeps the tag enumerated

        // Simulate "wrote the chunk, crashed before the delete": pre-write a chunk matching the
        // still-live rows, at the same timestamp (301 = maxWt+1 = 300+1) a genuine first pass would
        // have used -- NOT the wall-clock timestamp a plain execute() would default to (which, being
        // far larger than anything runOnce ever writes, would make every cell of this pre-written row
        // permanently win over runOnce's re-merge and pass the assertions below vacuously).
        ByteBuffer payload = encodeDoubleChunk(tsValues, values, tsValues.length);
        execute(chunkInsertQuery(), "i", new Date(0L), tsValues.length, 300L, payload, 301L);

        assertEquals(4, raw("SELECT * FROM %s WHERE tag = ? AND ts >= ? AND ts < ?",
                                "i", new Date(0L), new Date(HOUR)).size());

        // The re-encode is deterministic, so the resumed cycle rebuilds byte-for-byte what is already
        // stored and recognises it has nothing new to write: it writes NO chunk (all stats zero) but
        // still issues the delete the crashed cycle never got to.
        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertEquals(0, stats.windowsEncoded);
        assertEquals(0, stats.lateMerges);
        assertEquals(0, stats.rowsEncoded);
        assertEquals(0, stats.bytesWritten);

        UntypedResultSet.Row chunkRow = execute("SELECT samples, payload, WRITETIME(payload) AS chunk_wt FROM " +
                                                chunkTableRef() + " WHERE tag = ? AND window_start = ?",
                                                "i", new Date(0L)).one();
        assertEquals(4, chunkRow.getInt("samples")); // converged, not duplicated
        // Untouched, not rewritten: the chunk row still carries the pre-written cycle's timestamp.
        assertEquals(301L, chunkRow.getLong("chunk_wt"));

        // Decode and check the actual (ts, value) content, not just the sample count.
        ColumnarCursor cursor = ColumnarChunkCodec.cursor(chunkRow.getBytes("payload"), null);
        for (int i = 0; i < tsValues.length; i++)
        {
            assertTrue(cursor.advance());
            assertEquals(tsValues[i], cursor.timestamp());
            assertEquals(values[i], doubleAt(cursor, "value"), 0.0);
        }
        assertFalse(cursor.advance());

        // The interrupted cycle's delete now completes.
        assertEquals(0, raw("SELECT * FROM %s WHERE tag = ? AND ts >= ? AND ts < ?",
                                "i", new Date(0L), new Date(HOUR)).size());
    }

    @Test
    public void runningTheSameCycleTwiceIsANoOp() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, extra int, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        for (int i = 0; i < 4; i++)
            execute("INSERT INTO %s (tag, ts, value, extra) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                    "t", new Date(i * 600_000L), i * 1.5, i, 100L + i);
        insertRow("t", 4 * HOUR, 999.0, 200); // hot row -- keeps the tag enumerated

        TieredStorageService service = new TieredStorageService();
        assertEquals(1, service.runOnce(KEYSPACE, currentTable(), 5 * HOUR).windowsEncoded);

        UntypedResultSet.Row first = execute("SELECT payload, WRITETIME(payload) AS chunk_wt FROM " +
                                             chunkTableRef() + " WHERE tag = ? AND window_start = ?",
                                             "t", new Date(0L)).one();
        ByteBuffer firstPayload = ByteBufferUtil.clone(first.getBytes("payload"));
        long firstWritetime = first.getLong("chunk_wt");

        TierRunStats second = service.runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertEquals(0, second.windowsEncoded);
        assertEquals(0, second.rowsEncoded);
        assertEquals(0, second.lateMerges);
        assertEquals(0, second.bytesWritten);

        UntypedResultSet.Row after = execute("SELECT payload, WRITETIME(payload) AS chunk_wt FROM " +
                                             chunkTableRef() + " WHERE tag = ? AND window_start = ?",
                                             "t", new Date(0L)).one();
        assertEquals("the chunk must be byte-identical after a second cycle", firstPayload, after.getBytes("payload"));
        assertEquals(firstWritetime, after.getLong("chunk_wt"));
    }

    @Test
    public void coldWindowExpiry() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"1h\",\"chunk_window\":\"1h\",\"cold_window\":\"2h\"}");

        TableMetadata base = getCurrentColumnFamilyStore().metadata();
        ChunkTables.ensureChunkTable(base);

        long now = 10 * HOUR;
        insertRow("cold", now - 100_000L, 1.0, 1); // hot row -- keeps the tag enumerated

        ByteBuffer expiring = encodeDoubleChunk(new long[]{ 0L }, new double[]{ 1.0 }, 1);
        execute(chunkInsertQuery(), "cold", new Date(0L), 1, 500L, expiring, 1L);

        ByteBuffer surviving = encodeDoubleChunk(new long[]{ 9 * HOUR }, new double[]{ 2.0 }, 1);
        execute(chunkInsertQuery(), "cold", new Date(9 * HOUR), 1, 600L, surviving, 2L);

        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), now);
        assertEquals(1, stats.chunksExpired);

        assertEquals(0, execute(chunkSelectQuery(), "cold", new Date(0L)).size());
        assertEquals(1, execute(chunkSelectQuery(), "cold", new Date(9 * HOUR)).size());
    }

    @Test
    public void deadTagColdChunksStillExpire() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"1h\",\"chunk_window\":\"1h\",\"cold_window\":\"2h\"}");

        TableMetadata base = getCurrentColumnFamilyStore().metadata();
        ChunkTables.ensureChunkTable(base);

        // SP3 R6: this tag has NO base rows at all (fully re-encoded, then the base rows aged away) -
        // it exists only in the chunk table. Chunk-table-driven expiry enumeration must still find
        // and expire its cold chunk; the old base-DISTINCT enumeration never would have.
        ByteBuffer expiring = encodeDoubleChunk(new long[]{ 0L }, new double[]{ 1.0 }, 1);
        execute(chunkInsertQuery(), "dead", new Date(0L), 1, 500L, expiring, 1L);

        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), 10 * HOUR);
        assertEquals(1, stats.chunksExpired);
        assertEquals(0, execute(chunkSelectQuery(), "dead", new Date(0L)).size());
    }

    @Test
    public void unsupportedSchemaSkipsWithError() throws Throwable
    {
        // A second clustering column: no time axis a chunk could encode. See TieringPolicyTest /
        // TieringSchemaSupportTest for the whole accept/reject matrix.
        createTable("CREATE TABLE %s (tag text, ts timestamp, seq int, value double, PRIMARY KEY (tag, ts, seq)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        assertSkippedWithError("clustering column");
    }

    /** Runs one cycle and asserts it did nothing but log an ERROR containing {@code expectedInMessage}. */
    private void assertSkippedWithError(String expectedInMessage) throws Throwable
    {
        Logger serviceLogger = (Logger) LoggerFactory.getLogger(TieredStorageService.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        serviceLogger.addAppender(appender);
        TierRunStats stats;
        try
        {
            stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);
            assertTrue("expected an ERROR log containing '" + expectedInMessage + "', got: " + appender.list,
                       appender.list.stream().anyMatch(e -> e.getLevel() == Level.ERROR &&
                                                            e.getFormattedMessage().contains(expectedInMessage)));
        }
        finally
        {
            serviceLogger.detachAppender(appender);
        }

        assertEquals(0, stats.windowsEncoded);
        assertEquals(0, stats.rowsEncoded);
        assertEquals(0, stats.lateMerges);
        assertEquals(0, stats.chunksExpired);
        assertEquals(0, stats.bytesWritten);
        assertNull(Schema.instance.getTableMetadata(KEYSPACE, ChunkTables.chunkTableName(currentTable())));
    }

    @Test
    public void everyChunkIsWrittenWithTheColumnarCodecVersion() throws Throwable
    {
        // The columnar format is the only chunk format written, so the chunk row's `codec` column is
        // a fixed 4 for every pattern -- constant series and quantized walks alike. (This replaces
        // the per-window gorilla/chimp bake-off that used to make this column vary.)
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        int n = 100;
        long wt = 1;

        for (int i = 0; i < n; i++)
            insertRow("const", i * 30_000L, 5.0, wt++);

        Random random = new Random(17);
        double walk = 50.0;
        for (int i = 0; i < n; i++)
        {
            walk += (random.nextInt(3) - 1) * 0.1;
            insertRow("quant", i * 30_000L, Math.round(walk * 10.0) / 10.0, wt++);
        }

        insertRow("const", 4 * HOUR, 999.0, wt++);
        insertRow("quant", 4 * HOUR, 999.0, wt++);

        new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);

        byte constCodec = execute(chunkSelectQuery(), "const", new Date(0L)).one().getByte("codec");
        byte quantCodec = execute(chunkSelectQuery(), "quant", new Date(0L)).one().getByte("codec");

        assertEquals(ColumnarChunkCodec.VERSION, constCodec);
        assertEquals(ColumnarChunkCodec.VERSION, quantCodec);
    }

    @Test
    public void rowWithNoLiveCellIsEncodedRatherThanDeletedUnencoded() throws Throwable
    {
        // A row whose every regular column is null still EXISTS, and the range delete would take it,
        // so it must go into the chunk (as a timestamp with all columns null) rather than be skipped.
        // Skipping it -- what the single-column re-encoder did -- silently destroyed the row.
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        insertRow("n", 0L, 7.5, 50);
        // Bare key insert -- a live row with no value cell at all, so WRITETIME(value) is null too.
        execute("INSERT INTO %s (tag, ts) VALUES (?, ?) USING TIMESTAMP 10", "n", new Date(600_000L));
        insertRow("n", 4 * HOUR, 999.0, 200); // hot row -- keeps the tag enumerated

        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);

        assertEquals(1, stats.windowsEncoded);
        assertEquals(2, stats.rowsEncoded); // BOTH rows, the value-less one included

        UntypedResultSet.Row chunkRow = execute(chunkSelectQuery(), "n", new Date(0L)).one();
        assertEquals(2, chunkRow.getInt("samples"));
        ColumnarCursor cursor = ColumnarChunkCodec.cursor(chunkRow.getBytes("payload"), null);
        assertTrue(cursor.advance());
        assertEquals(0L, cursor.timestamp());
        assertEquals(7.5, doubleAt(cursor, "value"), 0.0);
        assertTrue(cursor.advance());
        assertEquals(600_000L, cursor.timestamp());
        assertTrue("the value-less row must round-trip as null, not as 0.0", cursor.isNull("value"));
        assertFalse(cursor.advance());

        // The range delete covers the whole window regardless of which individual rows had a value.
        assertEquals(0, raw("SELECT * FROM %s WHERE tag = ? AND ts >= ? AND ts < ?",
                                "n", new Date(0L), new Date(HOUR)).size());

        // ...and a plain (merged) SELECT still sees both rows, the second with a null value.
        UntypedResultSet merged = execute("SELECT ts, value FROM %s WHERE tag = ? AND ts >= ? AND ts < ?",
                                          "n", new Date(0L), new Date(HOUR));
        assertEquals(2, merged.size());
        UntypedResultSet.Row[] rows = merged.stream().toArray(UntypedResultSet.Row[]::new);
        assertEquals(7.5, rows[0].getDouble("value"), 0.0);
        assertFalse("the reconstructed row must have no value cell", rows[1].has("value"));
    }

    @Test
    public void windowWithNoCellWritetimeIsLeftCompletelyUntouched() throws Throwable
    {
        // Every row in the window is a bare primary-key insert, so no WRITETIME exists anywhere in
        // it. There is then no timestamp the range delete could use that is provably not newer than
        // some row it would destroy, so the cycle must leave the window entirely alone -- not encode
        // it and delete at a guessed timestamp.
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        execute("INSERT INTO %s (tag, ts) VALUES (?, ?) USING TIMESTAMP 10", "n", new Date(0L));
        execute("INSERT INTO %s (tag, ts) VALUES (?, ?) USING TIMESTAMP 11", "n", new Date(600_000L));
        insertRow("n", 4 * HOUR, 999.0, 200); // hot row -- keeps the tag enumerated

        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);

        assertEquals(0, stats.windowsEncoded);
        assertEquals(0, execute(chunkSelectQuery(), "n", new Date(0L)).size());
        assertEquals(2, raw("SELECT ts FROM %s WHERE tag = ? AND ts >= ? AND ts < ?",
                            "n", new Date(0L), new Date(HOUR)).size());
    }

    @Test
    public void corruptChunkOnOneTagDoesNotAbortOtherTags() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        TableMetadata base = getCurrentColumnFamilyStore().metadata();
        ChunkTables.ensureChunkTable(base);

        // "bad": an existing chunk row whose payload is not a valid codec payload at all, so decoding
        // it for the merge throws mid-cycle.
        insertRow("bad", 0L, 1.0, 10);
        insertRow("bad", 4 * HOUR, 999.0, 20);
        ByteBuffer garbage = ByteBufferUtil.bytes("not a valid chunk payload");
        execute(chunkInsertQuery(), "bad", new Date(0L), 1, 5L, garbage, 6L);

        // "good": an ordinary closed window that must still get encoded despite "bad" throwing.
        insertRow("good", 0L, 2.0, 10);
        insertRow("good", 4 * HOUR, 999.0, 20);

        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);

        assertEquals(1, stats.windowsEncoded); // only "good"'s window succeeded
        assertEquals(1, execute(chunkSelectQuery(), "good", new Date(0L)).size());
        // ...and the cycle SAYS it skipped one. Without this the run returns all-clear stats while
        // having silently under-encoded the table, which is an availability failure reported as
        // success -- the same defect class as swallowing a chunk-read timeout on the read path.
        assertEquals(1, stats.tagsSkipped);
    }

    @Test
    public void retierFailsWhenTheCycleSkippedTags() throws Throwable
    {
        // nodetool retier is a one-shot operator instruction, not a background tick: a cycle that
        // could not finish some tags did not do what it was asked, so it must exit non-zero rather
        // than print nothing and return 0.
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        TableMetadata base = getCurrentColumnFamilyStore().metadata();
        ChunkTables.ensureChunkTable(base);

        // retier() drives the cycle off real wall-clock time, so use a window that is unambiguously
        // closed whenever the test runs.
        long windowStart = (System.currentTimeMillis() / HOUR - 3) * HOUR;
        insertRow("bad", windowStart, 1.0, 10);
        execute(chunkInsertQuery(), "bad", new Date(windowStart), 1, 5L,
                ByteBufferUtil.bytes("not a valid chunk payload"), 6L);

        String table = currentTable();
        try
        {
            TieredStorageService.instance.retier(KEYSPACE, table);
            fail("expected retier to fail after skipping a tag");
        }
        catch (IllegalStateException e)
        {
            throw e;                                      // gate contention, not what this asserts
        }
        catch (RuntimeException expected)
        {
            assertTrue(expected.getMessage(), expected.getMessage().contains("tag(s) skipped"));
        }

        // The stats are still recorded, so the virtual table / tieringstatus can show the skip.
        assertEquals(1, TieredStorageService.instance.lastStats(KEYSPACE, table).tagsSkipped);
    }

    @Test
    public void oversizedWindowAbortsThatTagOnlyWithError() throws Throwable
    {
        // Regression for a final-review finding: a window holding more samples than the service will
        // encode (TieredStorageService.maxSamplesPerWindow in production; shrunk to 3 here via that
        // same seam) must be detected while paging -- never fully materialized -- and abort that tag's walk
        // with an actionable ERROR, instead of blowing up in encode and re-reading the giant window
        // every cycle forever. Other tags in the same run must still encode.
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        // "big": 5 rows in one closed window -- over the injected 3-sample cap.
        for (int i = 0; i < 5; i++)
            insertRow("big", i * 60_000L, i * 1.0, 10 + i);
        insertRow("big", 4 * HOUR, 999.0, 99);

        // "small": 2 rows in one closed window -- under the cap, must encode normally.
        insertRow("small", 0L, 1.0, 10);
        insertRow("small", 60_000L, 2.0, 11);
        insertRow("small", 4 * HOUR, 999.0, 99);

        TieredStorageService service = new TieredStorageService();
        service.maxSamplesPerWindow = 3;

        Logger serviceLogger = (Logger) LoggerFactory.getLogger(TieredStorageService.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        serviceLogger.addAppender(appender);
        TierRunStats stats;
        try
        {
            stats = service.runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        }
        finally
        {
            serviceLogger.detachAppender(appender);
        }

        // "big" encoded nothing: no chunk row, and every source row is still in base, untouched.
        assertEquals(0, execute(chunkSelectQuery(), "big", new Date(0L)).size());
        assertEquals(5, raw("SELECT * FROM %s WHERE tag = ? AND ts < ?", "big", new Date(HOUR)).size());

        // The abort was logged at ERROR, naming the tag and telling the operator what to change.
        assertTrue("expected an ERROR naming tag 'big' and pointing at chunk_window",
                   appender.list.stream().anyMatch(e -> e.getLevel() == Level.ERROR &&
                                                        e.getFormattedMessage().contains("big") &&
                                                        e.getFormattedMessage().contains("chunk_window")));

        // "small" still encoded on the very same run.
        assertEquals(1, stats.windowsEncoded);
        assertEquals(2, stats.rowsEncoded);
        assertEquals(1, execute(chunkSelectQuery(), "small", new Date(0L)).size());
        assertEquals(0, raw("SELECT * FROM %s WHERE tag = ? AND ts < ?", "small", new Date(HOUR)).size());
    }

    @Test
    public void descClusteredTableDrainsBacklogInOneRun() throws Throwable
    {
        // Regression for a final-review finding: with CLUSTERING ORDER BY (ts DESC) -- the dominant
        // time-series idiom -- an order-less LIMIT 1 probe returns the NEWEST row in range, so the
        // empty-window jump would leap over every window between the gap and the cutoff and the
        // backlog would drain a couple of windows per cycle instead of completely. The deliberate
        // empty window [1h,2h) below forces the walk through that jump (nextClosedWindowStart): it
        // must land on the OLDEST remaining row's window (2h), not the newest's (3h).
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) " +
                    "WITH compaction = {'class': 'TimeSeriesCompactionStrategy'} AND CLUSTERING ORDER BY (ts DESC)");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        long now = 6 * HOUR; // cutoff = windowStartFor(6h - 2h) = 4h
        insertRow("d", 0L, 1.0, 1);        // window [0,1h)  -- closed
        insertRow("d", 600_000L, 1.5, 2);  // window [0,1h)  -- closed
        insertRow("d", 2 * HOUR, 2.0, 3);  // window [2h,3h) -- closed, after the empty [1h,2h) gap
        insertRow("d", 3 * HOUR, 3.0, 4);  // window [3h,4h) -- closed
        insertRow("d", 5 * HOUR, 999.0, 5); // hot -- must survive untouched

        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), now);

        // The whole multi-window backlog drained in this single run...
        assertEquals(3, stats.windowsEncoded);
        assertEquals(4, stats.rowsEncoded);
        for (long windowStart : new long[]{ 0L, 2 * HOUR, 3 * HOUR })
            assertEquals("expected a chunk for window starting at " + windowStart,
                         1, execute(chunkSelectQuery(), "d", new Date(windowStart)).size());
        assertEquals(2, execute(chunkSelectQuery(), "d", new Date(0L)).one().getInt("samples"));

        // ...every closed source row is gone, and the hot row survived.
        assertEquals(0, raw("SELECT * FROM %s WHERE tag = ? AND ts < ?", "d", new Date(4 * HOUR)).size());
        assertEquals(1, raw("SELECT * FROM %s WHERE tag = ? AND ts = ?", "d", new Date(5 * HOUR)).size());
    }

    @Test
    public void virtualTableShowsPolicyAndStats() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        // retier() (unlike runOnce() in the other tests here) drives the cycle off real wall-clock
        // time, so use a window safely in the past -- hour-aligned, a few hours ago -- rather than a
        // synthetic small timestamp, so it is unambiguously closed no matter when the test runs.
        long windowStart = (System.currentTimeMillis() / HOUR - 3) * HOUR;
        insertRow("v", windowStart, 1.0, 1);

        String table = currentTable();
        TieredStorageService.instance.retier(KEYSPACE, table);

        UntypedResultSet rows = execute("SELECT * FROM system_views.timeseries_tiering WHERE keyspace_name = ? AND table_name = ?",
                                        KEYSPACE, table);
        assertEquals(1, rows.size());
        UntypedResultSet.Row row = rows.one();
        assertEquals(2 * HOUR, row.getLong("hot_window_ms"));
        assertEquals(HOUR, row.getLong("chunk_window_ms"));
        assertEquals(1, row.getLong("windows_encoded"));
        assertEquals(1, row.getLong("rows_encoded"));
        assertEquals(0, row.getLong("late_merges"));
        assertEquals(0, row.getLong("chunks_expired"));
        assertEquals(0, row.getLong("tags_skipped"));
        assertTrue("last_run_at should be a real timestamp once retier has run", row.getLong("last_run_at") > 0);
    }

    @Test
    public void sweepIsolatesPerTableFailures() throws Throwable
    {
        // Regression for a review finding: one table's run failing inside the global sweep (e.g. an
        // UnavailableException because its keyspace cannot meet the policy's consistency level) must
        // not abort the tick for every table iterated after it, and must be logged rather than
        // escaping into the scheduled executor (whose failure wrapper swallows request-failure
        // exceptions silently). Two policy-bearing tables; one's run deterministically throws via the
        // preRunHookForTesting seam (a healthy single-node cluster cannot provoke the real failure).
        // The test is order-independent: without the per-table catch the injected throwable escapes
        // sweep() and fails this test no matter which table the schema walk visits first.
        String badTable = createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        String goodTable = createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        TieredStorageService service = new TieredStorageService(); // fresh instance: both tables are due (never run)
        service.preRunHookForTesting = (ks, table) ->
        {
            if (badTable.equals(table))
                throw new RuntimeException("injected failure for " + ks + '.' + table);
        };

        Logger serviceLogger = (Logger) LoggerFactory.getLogger(TieredStorageService.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        serviceLogger.addAppender(appender);
        try
        {
            service.sweep(); // must complete without throwing
        }
        finally
        {
            serviceLogger.detachAppender(appender);
        }

        // The failing table was logged (keyspace.table named), not silently swallowed...
        assertTrue("expected a WARN naming the failed table",
                   appender.list.stream().anyMatch(e -> e.getLevel() == Level.WARN &&
                                                        e.getFormattedMessage().contains(KEYSPACE + "." + badTable)));
        // ...its run never completed...
        assertNull(service.lastRunAtMillis(KEYSPACE, badTable));
        assertNull(service.lastStats(KEYSPACE, badTable));
        // ...and the other table still got its run on the same tick.
        assertNotNull("the second table's run must survive the first table's failure",
                      service.lastRunAtMillis(KEYSPACE, goodTable));
        assertNotNull(service.lastStats(KEYSPACE, goodTable));
    }

    @Test
    public void shutdownStopsTheCycleInsteadOfGrindingThroughEveryTag() throws Throwable
    {
        // Regression for what the first shutdown-hook deploy actually did in production. Cancelling
        // the sweep interrupts its thread, but runOnce's per-tag handler catches every RuntimeException
        // and moves on -- by design, so one bad tag cannot wedge a table. During shutdown EVERY
        // remaining tag fails, so the cycle walked the entire backlog at shutdown speed and logged one
        // ERROR per tag: thousands of lines in seconds, burying anything real. The cycle must notice it
        // is stopping and leave, quietly.
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        long wt = 1;
        for (int t = 0; t < 5; t++)
            for (int r = 0; r < 4; r++)
                insertRow("tag" + t, r * 600_000L, r, wt++);

        Logger serviceLogger = (Logger) LoggerFactory.getLogger(TieredStorageService.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        serviceLogger.addAppender(appender);

        TierRunStats stats;
        boolean previous = TieredStorageService.setSweepStoppingForTesting(true);
        try
        {
            stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        }
        finally
        {
            TieredStorageService.setSweepStoppingForTesting(previous);
            serviceLogger.detachAppender(appender);
        }

        // Nothing was encoded, and -- the point of the fix -- nothing was logged as a failure.
        assertEquals(0, stats.windowsEncoded);
        assertFalse("shutdown must not report per-tag failures as errors",
                    appender.list.stream().anyMatch(e -> e.getLevel() == Level.ERROR));
        assertTrue("the cycle should say once that it stopped because tiering is shutting down",
                   appender.list.stream().anyMatch(e -> e.getFormattedMessage().contains("shutting down")));

        // And it left the data alone: every source row is still there for the next startup to encode.
        for (int t = 0; t < 5; t++)
            assertEquals(4, raw("SELECT * FROM %s WHERE tag = ?", "tag" + t).size());
    }

    @Test
    public void shutdownDuringTheBaseTableTagScanIsNotReportedAsAnError() throws Throwable
    {
        // The same shutdown failure mode as above, one layer down. The fallback base-table scan
        // (taken while the registry is still empty) catches each token range's failure and logs it
        // as an ERROR with a stack trace, so a shutdown that lands mid-scan produced one incident per
        // remaining range -- and kept scanning ranges the query path could no longer serve.
        TagRegistry.resetForTesting();
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        insertRow("tag0", 0L, 1.0, 1);

        Logger serviceLogger = (Logger) LoggerFactory.getLogger(TieredStorageService.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        serviceLogger.addAppender(appender);

        // No walk pages: the registry stays empty, so enumeration falls through to scanTags. The
        // first range scan is where shutdown arrives -- drain has begun, and the query fails.
        TieredStorageService service = new TieredStorageService();
        int previousBudget = service.setScanPagesPerCycleForTesting(0);
        AtomicInteger rangeScans = new AtomicInteger();
        boolean previous = TieredStorageService.setSweepStoppingForTesting(false);
        TieredStorageService.tagRangeScanHookForTesting = range ->
        {
            rangeScans.incrementAndGet();
            TieredStorageService.setSweepStoppingForTesting(true);
            throw new RuntimeException("injected: query path dismantled by drain");
        };
        TierRunStats stats;
        try
        {
            stats = service.runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        }
        finally
        {
            TieredStorageService.tagRangeScanHookForTesting = null;
            TieredStorageService.setSweepStoppingForTesting(previous);
            service.setScanPagesPerCycleForTesting(previousBudget);
            serviceLogger.detachAppender(appender);
        }

        assertFalse("a range scan failing because tiering is shutting down is not an error",
                    appender.list.stream().anyMatch(e -> e.getLevel() == Level.ERROR));
        assertTrue("the scan should say it stopped because tiering is shutting down",
                   appender.list.stream().anyMatch(e -> e.getFormattedMessage().contains("shutting down")));
        assertEquals("no further range may be scanned once shutdown is seen", 1, rangeScans.get());
        // Still counted: the cycle did not enumerate everything, and must not report that it did.
        assertTrue(stats.tagsSkipped > 0);
        assertEquals(0, stats.windowsEncoded);
        assertEquals(1, raw("SELECT * FROM %s WHERE tag = ?", "tag0").size());
    }

    @Test
    public void tagsTheWalkDiscoversAreRegisteredPastTheWritePathCacheCeiling() throws Throwable
    {
        // Past MAX_CACHED_TAGS_PER_TABLE the write path stops registering tags -- deliberately, since
        // registering uncached from there would cost a distributed INSERT per mutation. The walk is
        // what makes that safe, but it registered through the same capped call, so on a table past
        // the ceiling a newly discovered tag was dropped too: never registered, never enumerated,
        // never encoded, and nothing said so. The walk's own rate is bounded (scanPagesPerCycle pages
        // per cycle), so it must register what it finds whether or not the cache has room.
        TagRegistry.resetForTesting();
        int previousCeiling = TagRegistry.setMaxCachedTagsPerTableForTesting(2);
        try
        {
            createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
            setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

            long wt = 1;
            for (int t = 0; t < 6; t++)
                insertRow("tag" + t, 0L, t, wt++);

            new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);

            assertEquals("every tag the walk found must be registered, cache ceiling or not", 6, registeredTagCount());
            for (int t = 0; t < 6; t++)
                assertEquals("tag" + t + " should have been chunked", 1, execute(chunkSelectQuery(), "tag" + t, new Date(0L)).size());
        }
        finally
        {
            TagRegistry.setMaxCachedTagsPerTableForTesting(previousCeiling);
        }
    }

    @Test
    public void tscsFreezesAWindowAsSoonAsTieringHasEncodedItPurgingTheShadowedRows() throws Throwable
    {
        // End to end: the re-encoder chunks a closed window and range-deletes its rows in their own TSCS
        // window. Until a rewrite merges the two, every read over that range reads the dead rows as well
        // (6x slower per row in the 2026-09-25 soak). With freeze_after 1h that window would wait an hour;
        // tiering-aware freeze must hand it to the freezer as soon as the coverage ledger reaches its end,
        // and the freeze must leave no base rows behind.
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) " +
                    "WITH compaction = {'class':'TimeSeriesCompactionStrategy', 'window_size':'1m', 'freeze_after':'1h'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        setPolicy("{\"hot_window\":\"2m\",\"chunk_window\":\"1m\"}");

        long minute = 60_000L;
        long window = (System.currentTimeMillis() - 5 * minute) / minute * minute;   // closed, inside freeze_after
        for (int i = 0; i < 4; i++)
            insertRow("tag", window + i * 1000, i, (window + i * 1000) * 1000);        // writetime in the window (micros)
        flush();                                                                        // the rows' own sstable

        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), System.currentTimeMillis());
        assertEquals(1, stats.windowsEncoded);
        flush();                                                                        // the range tombstone's sstable
        assertEquals(2, cfs.getLiveSSTables().size());
        assertEquals(4, cfs.getLiveSSTables().stream().mapToLong(SSTableReader::getTotalRows).sum());

        TimeSeriesCompactionStrategy tscs = (TimeSeriesCompactionStrategy)
            cfs.getCompactionStrategyManager().getCompactionStrategyFor(cfs.getLiveSSTables().iterator().next());
        long nowSec = FBUtilities.nowInSeconds();
        AbstractCompactionTask task = Iterables.getOnlyElement(tscs.getNextBackgroundTasks(cfs.gcBefore(nowSec)), null);
        assertTrue("a fully encoded window must go to the freezer, not wait out freeze_after: " + task,
                   task instanceof FreezeCompactionTask);
        task.execute(ActiveCompactionsTracker.NOOP);

        assertEquals(1, cfs.getLiveSSTables().size());
        assertEquals("the freeze must drop every row the re-encoder deleted",
                     0, cfs.getLiveSSTables().stream().mapToLong(SSTableReader::getTotalRows).sum());
        // And the data is still all there, from the chunk.
        assertEquals(4, execute("SELECT * FROM %s WHERE tag = 'tag'").size());
    }

    @Test
    public void incrementalScanCoversTheRingAcrossCyclesInsteadOfTimingOut() throws Throwable
    {
        // The registry made enumeration cheap for tags the write path has seen, but the backlog -- tags
        // whose rows predate tiering and that nothing writes to any more -- is only discoverable by
        // scanning the base table, and on the table this exists for that scan cannot finish. Paging
        // does not bound it: DISTINCT's LIMIT counts only tags that still have a live row, so a page
        // asked for 256 tags walks past however many already-tiered or static-only partitions lie
        // between them. Measured in production: every token range failed, every cycle, forever.
        //
        // So the scan stops trying to finish. Each cycle advances a cursor by a bounded number of
        // pages and registers what it found; the ring is covered over many cycles instead of one.
        TagRegistry.resetForTesting();
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        long wt = 1;
        for (int t = 0; t < 6; t++)
            insertRow("tag" + t, 0L, t, wt++);

        TieredStorageService service = new TieredStorageService();
        int previousBudget = service.setScanPagesPerCycleForTesting(1);
        int previousPage = TieredStorageService.setTagPageSizeForTesting(2);
        try
        {
            // One page of two tags per cycle: the ring takes three cycles to cover, and each cycle
            // registers strictly more than the last without ever running an unbounded scan.
            service.runOnce(KEYSPACE, currentTable(), 5 * HOUR);
            assertEquals(2, registeredTagCount());
            service.runOnce(KEYSPACE, currentTable(), 5 * HOUR);
            assertEquals(4, registeredTagCount());
            service.runOnce(KEYSPACE, currentTable(), 5 * HOUR);
            assertEquals(6, registeredTagCount());

            // A further cycle finds the ring exhausted, wraps, and does not lose what it knows.
            service.runOnce(KEYSPACE, currentTable(), 5 * HOUR);
            assertEquals(6, registeredTagCount());
        }
        finally
        {
            service.setScanPagesPerCycleForTesting(previousBudget);
            TieredStorageService.setTagPageSizeForTesting(previousPage);
        }

        // And every tag the walk discovered was encoded -- the point of discovering them.
        for (int t = 0; t < 6; t++)
            assertEquals("tag" + t + " should have been chunked", 1, execute(chunkSelectQuery(), "tag" + t, new Date(0L)).size());
    }

    /**
     * The tag walk's failure lever is the token span, not the LIMIT: a DISTINCT page pays for every
     * partition it walks past, not the ones it returns, so shrinking the LIMIT of a too-expensive
     * stretch shrinks the answer and never the work. {@code boundedScanUpper} is that lever's maths:
     * each consecutive failure halves the stretch the next page covers, from wherever the cursor is.
     */
    @Test
    public void boundedScanUpperHalvesTheStretchPerFailure()
    {
        IPartitioner p = Murmur3Partitioner.instance;

        // No failures: the machinery costs nothing -- unbounded, exactly the pre-existing walk.
        assertNull(TieredStorageService.boundedScanUpper(p, null, 0));

        // Each failure halves the stretch: strictly shrinking upper bounds, all inside the ring.
        Token min = p.getMinimumToken();
        Token max = p.getMaximumTokenForSplitting();
        Token previous = max;
        for (int failures = 1; failures <= 6; failures++)
        {
            Token upper = TieredStorageService.boundedScanUpper(p, null, failures);
            assertNotNull(upper);
            assertTrue("upper must lie inside the ring", min.compareTo(upper) < 0 && upper.compareTo(max) < 0);
            assertTrue("failure " + failures + " must shrink the stretch", upper.compareTo(previous) < 0);
            previous = upper;
        }

        // From a mid-ring cursor the stretch starts there, not at the ring's start.
        Token cursor = p.getToken(UTF8Type.instance.decompose("mid-ring-cursor"));
        Token fromCursor = TieredStorageService.boundedScanUpper(p, cursor, 3);
        assertNotNull(fromCursor);
        assertTrue("the bound must lie beyond the cursor", cursor.compareTo(fromCursor) < 0);

        // The halving is capped, not unbounded: absurd failure counts still yield a usable span.
        assertNotNull(TieredStorageService.boundedScanUpper(p, null, 1000));

        // A partitioner that cannot split token ranges keeps the original LIMIT-only behaviour.
        assertNull(TieredStorageService.boundedScanUpper(ByteOrderedPartitioner.instance, null, 3));
    }

    /** @return how many tags are in the current table's registry. */
    private int registeredTagCount() throws Throwable
    {
        return execute(String.format("SELECT tag FROM %s.%s WHERE scope = 'tags'",
                                     KEYSPACE, ChunkTables.tagsTableName(currentTable()))).size();
    }

    @Test
    public void tagRegistryIsPopulatedAndThenDrivesEnumeration() throws Throwable
    {
        // The registry exists so the steady-state cycle stops paying for SELECT DISTINCT over the
        // base table (~19ms per partition on the production table this was built for -- ~220s a
        // cycle against a 300s interval). Two things have to hold for that to be safe: the first
        // cycle's authoritative base-table scan must be written into the registry, and a later cycle
        // reading the registry instead must enumerate exactly the same tags -- encoding new closed
        // windows for them just as the scan would have.
        TagRegistry.resetForTesting();
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        String[] tags = { "a", "b", "c" };
        long wt = 1;
        for (String tag : tags)
            for (int r = 0; r < 4; r++)
                insertRow(tag, r * 600_000L, r, wt++); // window 0

        TieredStorageService service = new TieredStorageService();
        TierRunStats first = service.runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertEquals(3, first.windowsEncoded);
        assertEquals(0, first.tagsSkipped);

        // The scan's result was persisted -- one clustering row per tag, in one partition.
        UntypedResultSet registered = execute(String.format("SELECT tag FROM %s.%s WHERE scope = 'tags'",
                                                            KEYSPACE, ChunkTables.tagsTableName(currentTable())));
        assertEquals(3, registered.size());
        Set<String> registeredTags = new HashSet<>();
        for (UntypedResultSet.Row row : registered)
            registeredTags.add(row.getString("tag"));
        assertEquals(new HashSet<>(Arrays.asList(tags)), registeredTags);

        // Second cycle: the registry is populated, so enumeration comes from the
        // registry. New closed windows for the same tags must still be found and encoded.
        for (String tag : tags)
            for (int r = 0; r < 4; r++)
                insertRow(tag, HOUR + r * 600_000L, r, wt++); // window 1

        TierRunStats second = service.runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertEquals("the registry-backed cycle must find every tag the scan would have",
                     3, second.windowsEncoded);
        assertEquals(0, second.tagsSkipped);
        for (String tag : tags)
            assertEquals(1, execute(chunkSelectQuery(), tag, new Date(HOUR)).size());
    }

    @Test
    public void sweepSpacesRetriesOfAFailedTableByItsInterval() throws Throwable
    {
        // Regression for a production incident: the sweep gated on the last *completed* run, so a
        // table whose run always throws never recorded a timestamp, was permanently "never run", and
        // was therefore re-attempted on every 60s tick regardless of its own (here: 1h) interval. The
        // failure that provoked it was a read timeout on a full-table DISTINCT scan, so each retry
        // was also the most expensive thing the cycle can do -- a failing table generating the load
        // that kept it failing. Attempts, not completions, are what the interval spaces out.
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\",\"interval\":\"1h\"}");

        TieredStorageService service = new TieredStorageService(); // fresh instance: the table is due
        int[] attempts = { 0 };
        service.preRunHookForTesting = (ks, table) ->
        {
            attempts[0]++;
            throw new RuntimeException("injected failure for " + ks + '.' + table);
        };

        service.sweep();
        service.sweep(); // second tick, well inside the 1h interval

        assertEquals("a failed run must still count as an attempt, so the interval spaces the retry",
                     1, attempts[0]);
        // The failure is still not a completion: status keeps reporting "never successfully run".
        assertNull(service.lastRunAtMillis(KEYSPACE, currentTable()));
        assertNull(service.lastStats(KEYSPACE, currentTable()));
    }

    @Test
    public void reentryGuardRejectsConcurrentRun() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        String table = currentTable();
        TieredStorageService service = TieredStorageService.instance;

        assertTrue("expected the gate to be free before this test acquires it",
                   service.acquireGateForTesting(KEYSPACE, table));
        try
        {
            service.retier(KEYSPACE, table);
            fail("expected retier to throw IllegalStateException while the gate is held");
        }
        catch (IllegalStateException expected)
        {
            // expected -- a run for this table is (fictitiously) already in flight
        }
        finally
        {
            service.releaseGateForTesting(KEYSPACE, table);
        }

        // Gate released -- a normal retier now runs to completion instead of throwing.
        service.retier(KEYSPACE, table);
    }

    /**
     * A column added while a cycle runs must not be destroyed by that cycle.
     * <p>
     * The cycle builds its encoder and its window query from the table metadata it read at the top,
     * so it selects only the columns that existed then. The source delete, though, is a whole-row
     * range delete at the window's maximum cell writetime. A cold row written after the ALTER, with a
     * value in the new column, is therefore read without that value, encoded without it, and then
     * deleted with it: the value is gone, and nothing reports it. The fix re-checks the schema
     * before a window's chunk is written and its rows deleted, and leaves the table for the next
     * cycle (which reads the new schema) when it has changed.
     */
    @Test
    public void columnAddedDuringACycleIsNotDestroyedByIt() throws Throwable
    {
        TagRegistry.resetForTesting();
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        insertRow("t", 0L, 1.0, 100);
        insertRow("t", 600_000L, 2.0, 101);

        // The seam fires during tag enumeration: after the cycle has taken its metadata snapshot,
        // before it reads the tag's first window. Registry walk off, so enumeration scans ranges.
        TieredStorageService service = new TieredStorageService();
        int previousBudget = service.setScanPagesPerCycleForTesting(0);
        AtomicInteger altered = new AtomicInteger();
        TieredStorageService.tagRangeScanHookForTesting = range ->
        {
            if (altered.getAndIncrement() > 0)
                return;
            try
            {
                alterTable("ALTER TABLE %s ADD extra int");
                execute("INSERT INTO %s (tag, ts, value, extra) VALUES ('t', ?, 3.0, 42) USING TIMESTAMP 150",
                        new Date(1_200_000L));
            }
            catch (Throwable t)
            {
                throw new RuntimeException(t);
            }
        };
        TierRunStats stats;
        try
        {
            stats = service.runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        }
        finally
        {
            TieredStorageService.tagRangeScanHookForTesting = null;
            service.setScanPagesPerCycleForTesting(previousBudget);
        }
        assertTrue("the seam must have run, or this test proves nothing", altered.get() > 0);

        UntypedResultSet.Row added = execute("SELECT extra FROM %s WHERE tag = 't' AND ts = ?", new Date(1_200_000L)).one();
        assertTrue("the value written to the new column was destroyed by the cycle", added.has("extra"));
        assertEquals(42, added.getInt("extra"));
        assertEquals("nothing may be encoded against a stale column list", 0, stats.windowsEncoded);
        assertEquals(3, raw("SELECT * FROM %s WHERE tag = 't'").size());

        // The next cycle reads the new schema and carries the new column into the chunk.
        assertEquals(1, new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR).windowsEncoded);
        assertEquals(0, raw("SELECT * FROM %s WHERE tag = 't' AND ts < ?", new Date(HOUR)).size());
        assertEquals(42, execute("SELECT extra FROM %s WHERE tag = 't' AND ts = ?", new Date(1_200_000L))
                             .one().getInt("extra"));
    }

    /**
     * The first write to a tag on a node registers it in the tag registry. That INSERT ran
     * synchronously on the client's own write path, so a slow registry replica added its latency to
     * the client's write -- up to a full write timeout -- and after a restart every tag's first write
     * paid it again. Registration is bookkeeping for the re-encoder; the client must not wait on it.
     */
    @Test
    public void writePathDoesNotWaitForTheRegistryInsert() throws Throwable
    {
        TagRegistry.resetForTesting();
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        ChunkTables.ensureChunkTable(Schema.instance.getTableMetadata(KEYSPACE, currentTable()));

        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger registrations = new AtomicInteger();
        TagRegistry.writeHookForTesting = tag ->
        {
            registrations.incrementAndGet();
            Uninterruptibles.awaitUninterruptibly(release, 30, TimeUnit.SECONDS);
        };
        ExecutorService client = Executors.newSingleThreadExecutor();
        try
        {
            Future<?> write = client.submit(() ->
            {
                try
                {
                    insertRow("fresh", 4 * HOUR, 1.0, 1);
                }
                catch (Throwable t)
                {
                    throw new RuntimeException(t);
                }
            });
            try
            {
                write.get(5, TimeUnit.SECONDS);
            }
            catch (TimeoutException e)
            {
                fail("a client write blocked behind the tag-registry INSERT");
            }
        }
        finally
        {
            release.countDown();
            TagRegistry.awaitPendingWritesForTesting();
            TagRegistry.writeHookForTesting = null;
            client.shutdownNow();
        }
        assertEquals("the tag must still be registered, just not on the client's time", 1, registrations.get());
        assertEquals(1, registeredTagCount());
    }

    /**
     * A failed registration released its claim so a later write would retry -- which meant EVERY
     * later write to that tag retried, each one another distributed INSERT against a registry that
     * had just proved it could not take one. A failure must back off, not multiply.
     */
    @Test
    public void aFailedRegistrationBacksOffInsteadOfRetryingOnEveryWrite() throws Throwable
    {
        TagRegistry.resetForTesting();
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        ChunkTables.ensureChunkTable(Schema.instance.getTableMetadata(KEYSPACE, currentTable()));

        AtomicInteger attempts = new AtomicInteger();
        TagRegistry.writeHookForTesting = tag ->
        {
            attempts.incrementAndGet();
            throw new RuntimeException("injected: registry replica unavailable");
        };
        try
        {
            for (int i = 0; i < 20; i++)
            {
                insertRow("flaky", 4 * HOUR + i, i, i + 1);
                TagRegistry.awaitPendingWritesForTesting();
            }
        }
        finally
        {
            TagRegistry.writeHookForTesting = null;
        }
        assertEquals("twenty writes after one failed registration must not issue twenty INSERTs", 1, attempts.get());
    }

    /**
     * The re-encoder and the cold-window flush both read-modify-write the same chunk rows. The flush
     * serializes itself per table ({@code ColdWindowChunkFlush.lockFor}), but the re-encoder never
     * took that lock, so on one node the two could read the same old chunk, each merge their own
     * rows into it, and write -- the later write dropping the earlier one's rows, whose base copies
     * were already gone. The re-encoder must hold the same lock across its read-merge-write.
     */
    @Test
    public void reencoderHoldsTheFlushLockWhileItRewritesAChunk() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        insertRow("t", 0L, 1.0, 1);

        AtomicInteger inserts = new AtomicInteger();
        AtomicInteger unlocked = new AtomicInteger();
        TieredStorageService.beforeChunkInsertForTesting = base ->
        {
            inserts.incrementAndGet();
            if (!Thread.holdsLock(ColdWindowChunkFlush.lockFor(base.id)))
                unlocked.incrementAndGet();
        };
        try
        {
            new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        }
        finally
        {
            TieredStorageService.beforeChunkInsertForTesting = null;
        }
        assertEquals("the seam must have seen the chunk write, or this test proves nothing", 1, inserts.get());
        assertEquals("the chunk was rewritten without the lock the cold-window flush serializes on", 0, unlocked.get());
    }

    /**
     * Review item: the oldest-row probe has no lower bound, so it walks every range tombstone the
     * re-encoder has left in the tag's partition. This pins down why that is NOT a failure mode:
     * tombstones past gc_grace_seconds are dropped before the read counts them
     * ({@code ReadCommand.withoutPurgeableTombstones}), so the probe only ever counts the markers of
     * windows encoded within the last gc_grace -- two per window. Production uses gc_grace 1 day and
     * chunk_window 15m, i.e. at most 192 counted markers against a failure threshold of 1,000,000
     * (warn 10,000). The second half shows the bound is real: markers younger than gc_grace do count.
     */
    @Test
    public void oldestRowProbeOnlyCountsTombstonesYoungerThanGcGrace() throws Throwable
    {
        int previousThreshold = DatabaseDescriptor.getTombstoneFailureThreshold();
        try
        {
            TierRunStats purgeable = encodeThirtyWindowsThenMergeALateRow(0);
            assertEquals("purgeable tombstones must not stop the probe", 0, purgeable.tagsSkipped);
            assertEquals(1, purgeable.lateMerges);

            TierRunStats young = encodeThirtyWindowsThenMergeALateRow(864000);
            // The tag walk's DISTINCT page over the same partition trips the threshold as well, so
            // more than one thing is skipped; what matters is that the probe is stopped at all.
            assertTrue("tombstones younger than gc_grace are counted -- this is the real bound",
                       young.tagsSkipped >= 1);
            assertEquals(0, young.lateMerges);
        }
        finally
        {
            DatabaseDescriptor.setTombstoneFailureThreshold(previousThreshold);
        }
    }

    private TierRunStats encodeThirtyWindowsThenMergeALateRow(int gcGraceSeconds) throws Throwable
    {
        DatabaseDescriptor.setTombstoneFailureThreshold(100_000);
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) " +
                    "WITH compaction = {'class': 'TimeSeriesCompactionStrategy'} AND gc_grace_seconds = " + gcGraceSeconds);
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        long wt = 1;
        for (int w = 0; w < 30; w++)
            insertRow("deep", w * HOUR, w, wt++);
        TieredStorageService service = new TieredStorageService();
        assertEquals(30, service.runOnce(KEYSPACE, currentTable(), 40 * HOUR).windowsEncoded);

        // A late row in the newest encoded window: reaching it walks past 29 windows' range tombstones.
        insertRow("deep", 29 * HOUR + 60_000L, 99.0, 1000);
        Thread.sleep(1100); // a tombstone is purgeable once its deletion time is strictly before gcBefore
        DatabaseDescriptor.setTombstoneFailureThreshold(10);
        // A read that trips a threshold reports it through the coordinator's per-thread warnings,
        // which the native-protocol path initializes and a test thread must initialize itself.
        CoordinatorWarnings.init();
        try
        {
            return service.runOnce(KEYSPACE, currentTable(), 40 * HOUR);
        }
        finally
        {
            CoordinatorWarnings.reset();
        }
    }

    /**
     * The tag walk halves its token span after each failed page, and forgot the span that worked
     * after a single success: the next page went back to the full remaining ring, failed (a read
     * timeout, ~12s of coordinator and replica work) and the ladder started over. On node 41 that
     * was one failed scan every cycle ("has failed N cycle(s) at its current position"). The walk
     * must keep the span that works and only try a wider one occasionally.
     */
    @Test
    public void tagWalkKeepsTheSpanThatWorks() throws Throwable
    {
        TagRegistry.resetForTesting();
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        long wt = 1;
        for (int t = 0; t < 40; t++)
            insertRow("tag" + t, 0L, t, wt++);

        TieredStorageService service = new TieredStorageService();
        int previousBudget = service.setScanPagesPerCycleForTesting(4);
        int previousPage = TieredStorageService.setTagPageSizeForTesting(2);
        AtomicInteger failedPages = new AtomicInteger();
        AtomicInteger goodPages = new AtomicInteger();
        // Anything wider than a quarter of the remaining ring "times out".
        TieredStorageService.tagWalkPageHookForTesting = level ->
        {
            if (level < 2)
            {
                failedPages.incrementAndGet();
                throw new RuntimeException("injected: page too expensive");
            }
            goodPages.incrementAndGet();
        };
        try
        {
            for (int cycle = 0; cycle < 10; cycle++)
                service.runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        }
        finally
        {
            TieredStorageService.tagWalkPageHookForTesting = null;
            service.setScanPagesPerCycleForTesting(previousBudget);
            TieredStorageService.setTagPageSizeForTesting(previousPage);
        }
        assertTrue("the walk must make progress at the span that works", goodPages.get() >= 10);
        assertTrue("ten cycles re-paid the failure ladder " + failedPages.get() + " times; two to find the " +
                   "working span and one occasional probe wider are all that is needed", failedPages.get() <= 3);
    }

    /**
     * A table whose tiering stopped working was easy to miss: an invalid policy made the virtual
     * table drop the table's row entirely, and a run that kept failing left only a stale
     * last_run_at. The view must show both, with the error.
     */
    @Test
    public void virtualTableShowsTablesWhoseTieringIsFailing() throws Throwable
    {
        String broken = createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"not-a-duration\"}");
        UntypedResultSet invalid = execute("SELECT * FROM system_views.timeseries_tiering WHERE keyspace_name = ? AND table_name = ?",
                                           KEYSPACE, broken);
        assertEquals("a table with an invalid policy must stay visible", 1, invalid.size());
        assertTrue(invalid.one().getString("last_error").contains("invalid"));

        String failing = createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        TieredStorageService service = TieredStorageService.instance;
        service.preRunHookForTesting = (ks, table) ->
        {
            if (table.equals(failing))
                throw new RuntimeException("injected: replicas unavailable");
        };
        try
        {
            for (int i = 0; i < 2; i++)
            {
                try
                {
                    service.retier(KEYSPACE, failing);
                    fail("the injected failure must propagate out of retier");
                }
                catch (RuntimeException expected)
                {
                }
            }
        }
        finally
        {
            service.preRunHookForTesting = null;
        }
        UntypedResultSet.Row row = execute("SELECT * FROM system_views.timeseries_tiering WHERE keyspace_name = ? AND table_name = ?",
                                           KEYSPACE, failing).one();
        assertEquals(2L, row.getLong("consecutive_failures"));
        assertTrue(row.getString("last_error").contains("injected: replicas unavailable"));
        assertTrue(row.getLong("last_attempt_at") > 0);
        assertEquals(-1L, row.getLong("last_run_at"));

        // One success clears the failure streak.
        service.retier(KEYSPACE, failing);
        row = execute("SELECT * FROM system_views.timeseries_tiering WHERE keyspace_name = ? AND table_name = ?",
                      KEYSPACE, failing).one();
        assertEquals(0L, row.getLong("consecutive_failures"));
        assertTrue(row.getLong("last_run_at") > 0);
    }

    private void insertRow(String tag, long tsMillis, double value, long writetime) throws Throwable
    {
        execute("INSERT INTO %s (tag, ts, value) VALUES (?, ?, ?) USING TIMESTAMP ?",
                tag, new Date(tsMillis), value, writetime);
    }

    /** The current row's {@code name} column, decoded as a double. */
    private static double doubleAt(ColumnarCursor cursor, String name)
    {
        return DoubleType.instance.compose(cursor.getBytes(name));
    }

    /** A v4 payload holding one {@code double} column called {@code value} -- what the re-encoder writes. */
    private static ByteBuffer encodeDoubleChunk(long[] timestamps, double[] values, int count)
    {
        ByteBuffer[] cells = new ByteBuffer[count];
        for (int i = 0; i < count; i++)
            cells[i] = DoubleType.instance.decompose(values[i]);
        SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
        columns.put("value", new ChunkV4Codec.ColumnInput(ChunkV4Directory.TYPE_DOUBLE,
                                                          StatOrder.IEEE754_TOTAL, cells));
        return ColumnarChunkCodec.encode(timestamps, count, columns);
    }

    /**
     * Base-table verification reads must see the PHYSICAL rows, not SP3's transparent hot+chunk
     * merge - these tests assert that the re-encoder actually deleted/kept raw rows. Logical
     * (merged) visibility is TransparentReadTest's job.
     */
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

    private void setPolicy(String json) throws Throwable
    {
        String hex = ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(json));
        alterTable("ALTER TABLE %s WITH extensions = {'" + TieringPolicy.EXTENSION_KEY + "': 0x" + hex + "};");
    }

    private String chunkTableRef()
    {
        return KEYSPACE + "." + ChunkTables.chunkTableName(currentTable());
    }

    private String chunkSelectQuery()
    {
        return "SELECT * FROM " + chunkTableRef() + " WHERE tag = ? AND window_start = ?";
    }

    /** Bind order: tag, window_start, samples, max_row_writetime, payload, USING TIMESTAMP. */
    private String chunkInsertQuery()
    {
        return "INSERT INTO " + chunkTableRef() +
               " (tag, window_start, codec, samples, max_row_writetime, payload) VALUES (?, ?, 4, ?, ?, ?) " +
               "USING TIMESTAMP ?";
    }
}
