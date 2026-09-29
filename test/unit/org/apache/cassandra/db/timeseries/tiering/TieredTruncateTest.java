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

import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.timeseries.tiering.TieredStorageService.TierRunStats;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@code TRUNCATE} of a tiered base table removes its tiered data too.
 * <p>
 * Tiering deletes a window's base rows once it has encoded them, so after a cycle the chunk table
 * holds the only copy of that history, and transparent reads merge it back into every
 * {@code SELECT}. A {@code TRUNCATE} that only emptied the base table therefore left every tiered
 * row readable: the table looked truncated to nobody. The decided behaviour (2026-09-28) is to
 * cascade -- the chunk table, the coverage ledger and the tag registry are truncated with the base.
 */
public class TieredTruncateTest extends CQLTester
{
    private static final long HOUR = 3_600_000L;

    @BeforeClass
    public static void setUpNetwork()
    {
        requireNetwork();
    }

    @After
    public void clearSeams()
    {
        TieredStorageService.afterWindowReadHookForTesting = null;
    }

    @Test
    public void truncateRemovesTheTieredHistory() throws Throwable
    {
        loadTwoWindowsAndReencode();

        execute("TRUNCATE %s");

        assertEquals("a truncated table must not serve its tiered history",
                     0, execute("SELECT * FROM %s WHERE tag = 't1'").size());
        assertShadowTablesEmpty();
    }

    @Test
    public void truncateOverTheNativeProtocolRemovesTheTieredHistory() throws Throwable
    {
        // The coordinator path (StorageProxy.truncateBlocking), which is what a client reaches --
        // the in-process execute() above takes TruncateStatement.executeLocally instead.
        loadTwoWindowsAndReencode();

        executeNet("TRUNCATE %s");

        assertEquals("a truncated table must not serve its tiered history",
                     0, executeNet("SELECT * FROM %s WHERE tag = 't1'").all().size());
        assertShadowTablesEmpty();
    }

    @Test
    public void tableIsUsableAndTiersAgainAfterTruncate() throws Throwable
    {
        loadTwoWindowsAndReencode();
        execute("TRUNCATE %s");

        // New data after the truncate is tiered normally and is all that is visible.
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 7.0) USING TIMESTAMP 201", new Date(5 * 60_000L));
        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertEquals(1L, stats.windowsEncoded);
        assertEquals(0L, stats.tagsSkipped);

        UntypedResultSet rows = execute("SELECT value FROM %s WHERE tag = 't1'");
        assertEquals(1, rows.size());
        assertEquals(7.0, rows.one().getDouble("value"), 0.0);
    }

    @Test
    public void aCycleRacingTheTruncateDoesNotWriteTheOldRowsBack() throws Throwable
    {
        // The cycle reads a window's rows, then the table is truncated, then the cycle would write
        // the chunk it built from the rows it read -- after the chunk table was emptied, so the
        // truncated data would come back as a chunk. The cycle must notice and stop.
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR); // creates the shadow tables
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 1.0) USING TIMESTAMP 101", new Date(10 * 60_000L));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 2.0) USING TIMESTAMP 102", new Date(20 * 60_000L));

        AtomicInteger truncates = new AtomicInteger();
        TieredStorageService.afterWindowReadHookForTesting = () ->
        {
            if (truncates.getAndIncrement() > 0)
                return;
            try
            {
                execute("TRUNCATE %s");
            }
            catch (Throwable t)
            {
                throw new RuntimeException(t);
            }
        };
        new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertTrue("the seam must have run, or this test proves nothing", truncates.get() > 0);

        assertEquals("rows read before the truncate must not be written back as a chunk",
                     0, execute("SELECT * FROM %s WHERE tag = 't1'").size());
        assertEquals(0, execute("SELECT * FROM " + shadow(ChunkTables.chunkTableName(currentTable()))).size());
    }

    private void loadTwoWindowsAndReencode() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 1.0) USING TIMESTAMP 101", new Date(10 * 60_000L));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 2.0) USING TIMESTAMP 102", new Date(20 * 60_000L));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 3.0) USING TIMESTAMP 103", new Date(HOUR + 10 * 60_000L));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 4.0) USING TIMESTAMP 104", new Date(4 * HOUR + 10 * 60_000L));

        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertEquals(2L, stats.windowsEncoded);
        // Precondition: the history really is only in the chunk table, and a SELECT still sees it.
        assertEquals(2, execute("SELECT * FROM " + shadow(ChunkTables.chunkTableName(currentTable()))).size());
        assertEquals(4, execute("SELECT * FROM %s WHERE tag = 't1'").size());
    }

    private void assertShadowTablesEmpty() throws Throwable
    {
        String base = currentTable();
        assertEquals("chunk table", 0, execute("SELECT * FROM " + shadow(ChunkTables.chunkTableName(base))).size());
        assertEquals("coverage ledger", 0, execute("SELECT * FROM " + shadow(ChunkTables.coverageTableName(base))).size());
        assertEquals("tag registry", 0, execute("SELECT * FROM " + shadow(ChunkTables.tagsTableName(base))).size());
    }

    private static String shadow(String table)
    {
        return KEYSPACE + '.' + table;
    }

    private void setPolicy(String json) throws Throwable
    {
        String hex = ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(json));
        alterTable("ALTER TABLE %s WITH extensions = {'" + TieringPolicy.EXTENSION_KEY + "': 0x" + hex + "};");
    }
}
