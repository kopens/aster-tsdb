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

package org.apache.cassandra.db.compaction;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Random;

import com.google.common.collect.Iterables;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.IScrubber;
import org.apache.cassandra.io.sstable.IVerifier;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.bti.TscolFormat;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Time-series compaction with {@code frozen_format = 'columnar'} freezes a closed window into one {@code tscol}
 * sstable, and refreezes it -- reading the columnar sstable plus the late data's sstable -- into a new one, with every
 * answer unchanged. Late writes, overwrites and deletes into the frozen window are ordinary writes: nothing refuses
 * them and nothing tombstones the frozen data on the side.
 */
public class TscolFreezeTest extends CQLTester
{
    private static final String TSCS_COLUMNAR = "{'class': 'TimeSeriesCompactionStrategy', 'window_size': '1m', " +
                                                "'freeze_after': '1m', 'frozen_format': 'columnar'}";

    @Test
    public void closedWindowFreezesToColumnarAndRefreezesWithLateData() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, v double, q int, PRIMARY KEY (tag, ts)) WITH compaction = " + TSCS_COLUMNAR);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // One closed 1-minute window an hour ago (TSCS windows by write timestamp).
        long windowMicros = ((System.currentTimeMillis() - 3_600_000L) / 60_000L * 60_000L) * 1000L;
        for (int sstable = 0; sstable < 2; sstable++)
        {
            for (int i = 0; i < 300; i++)
                execute("INSERT INTO %s (tag, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                        "t" + (i % 3), new Date(sstable * 1_000_000L + i * 1000L), 20.0 + i % 7, i % 4, windowMicros + i);
            flush();
        }
        assertEquals(2, cfs.getLiveSSTables().size());
        List<String> queries = List.of("SELECT * FROM %s",
                                       "SELECT tag, ts, v, writetime(v) FROM %s WHERE tag = 't1' ORDER BY ts DESC LIMIT 50",
                                       "SELECT count(*), max(v) FROM %s WHERE tag = 't2'",
                                       "SELECT * FROM %s WHERE q = 3 ALLOW FILTERING");
        List<UntypedResultSet> expected = run(queries);

        runOnlyFreezeTask(cfs);
        assertEquals(1, cfs.getLiveSSTables().size());
        assertTrue(TscolFormat.is(Iterables.getOnlyElement(cfs.getLiveSSTables()).descriptor.getFormat()));
        assertSame(queries, expected, run(queries));

        // Late data into the frozen window: an overwrite, a new row and a delete -- all plain writes.
        execute("UPDATE %s USING TIMESTAMP ? SET v = 99.0 WHERE tag = 't1' AND ts = ?", windowMicros + 50_000, new Date(1000L));
        execute("INSERT INTO %s (tag, ts, v, q) VALUES ('t1', ?, 1.5, 3) USING TIMESTAMP ?", new Date(5_000_000L), windowMicros + 50_001);
        execute("DELETE FROM %s USING TIMESTAMP ? WHERE tag = 't2' AND ts = ?", windowMicros + 50_002, new Date(2000L));
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());
        List<UntypedResultSet> withLate = run(queries);

        runOnlyFreezeTask(cfs);
        assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader refrozen = Iterables.getOnlyElement(cfs.getLiveSSTables());
        assertTrue(TscolFormat.is(refrozen.descriptor.getFormat()));
        assertSame(queries, withLate, run(queries));
        assertEquals(99.0, execute("SELECT v FROM %s WHERE tag = 't1' AND ts = ?", new Date(1000L)).one().getDouble("v"), 0.0);
    }

    /**
     * Windows frozen before frozen_format = 'columnar' was set are single
     * row-format sstables that nothing would ever rewrite. Turning the option on makes each of them one freeze
     * candidate, converted once to tscol and then left alone.
     */
    @Test
    public void windowsFrozenInRowFormatConvertOnceTheOptionIsSet() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, v double, PRIMARY KEY (tag, ts)) WITH compaction = " +
                    "{'class': 'TimeSeriesCompactionStrategy', 'window_size': '1m', 'freeze_after': '1m'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        long windowMicros = ((System.currentTimeMillis() - 3_600_000L) / 60_000L * 60_000L) * 1000L;
        for (int sstable = 0; sstable < 2; sstable++)
        {
            for (int i = 0; i < 100; i++)
                execute("INSERT INTO %s (tag, ts, v) VALUES ('a', ?, ?) USING TIMESTAMP ?", new Date(sstable * 100_000L + i * 1000L), (double) i, windowMicros + i);
            flush();
        }
        runOnlyFreezeTask(cfs);
        assertFalse(TscolFormat.is(Iterables.getOnlyElement(cfs.getLiveSSTables()).descriptor.getFormat()));
        List<UntypedResultSet> expected = run(List.of("SELECT * FROM %s"));

        alterTable("ALTER TABLE %s WITH compaction = {'class': 'TimeSeriesCompactionStrategy', 'window_size': '1m', " +
                   "'freeze_after': '1m', 'frozen_format': 'columnar'}");
        cfs.disableAutoCompaction();
        runOnlyFreezeTask(cfs);
        assertTrue(TscolFormat.is(Iterables.getOnlyElement(cfs.getLiveSSTables()).descriptor.getFormat()));
        assertSame(List.of("SELECT * FROM %s"), expected, run(List.of("SELECT * FROM %s")));

        TimeSeriesCompactionStrategy tscs = (TimeSeriesCompactionStrategy)
            cfs.getCompactionStrategyManager().getCompactionStrategyFor(cfs.getLiveSSTables().iterator().next());
        assertTrue("a converted window is FROZEN: nothing more to do", tscs.getNextBackgroundTasks(FBUtilities.nowInSeconds()).isEmpty());
    }

    @Test
    public void rowFormatIsTheDefaultAndUnsupportedShapesStayRowFormat() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, seq int, v double, PRIMARY KEY (tag, ts, seq)) WITH compaction = " + TSCS_COLUMNAR);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        long windowMicros = ((System.currentTimeMillis() - 3_600_000L) / 60_000L * 60_000L) * 1000L;
        for (int sstable = 0; sstable < 2; sstable++)
        {
            execute("INSERT INTO %s (tag, ts, seq, v) VALUES ('a', ?, 1, 1.0) USING TIMESTAMP ?", new Date(sstable * 1000L), windowMicros + sstable);
            flush();
        }
        runOnlyFreezeTask(cfs);
        assertFalse("two clustering columns: the codec does not carry this shape",
                    TscolFormat.is(Iterables.getOnlyElement(cfs.getLiveSSTables()).descriptor.getFormat()));
    }

    @Test
    public void frozenFormatIsValidated() throws Throwable
    {
        assertInvalidMessage("frozen_format must be row or columnar",
                             "CREATE TABLE " + KEYSPACE + ".bad_frozen_format (k int, ts timestamp, v int, PRIMARY KEY (k, ts)) " +
                           "WITH compaction = {'class': 'TimeSeriesCompactionStrategy', 'frozen_format': 'parquet'}");
    }

    private static void runOnlyFreezeTask(ColumnFamilyStore cfs)
    {
        TimeSeriesCompactionStrategy tscs = (TimeSeriesCompactionStrategy)
            cfs.getCompactionStrategyManager().getCompactionStrategyFor(cfs.getLiveSSTables().iterator().next());
        Collection<AbstractCompactionTask> tasks = tscs.getNextBackgroundTasks(FBUtilities.nowInSeconds());
        AbstractCompactionTask task = Iterables.getOnlyElement(tasks, null);
        assertNotNull("a closed window with two sstables must be frozen", task);
        assertTrue(task.getClass().getName(), task instanceof FreezeCompactionTask);
        task.execute(ActiveCompactionsTracker.NOOP);
    }

    /**
     * A dropped column's cells stay in the frozen file until the next rewrite. Row format filters them by drop time as
     * it reads; so must this format, or re-adding the column under the same name would bring the old values back.
     */
    @Test
    public void droppedThenReAddedColumnDoesNotBringOldValuesBack() throws Throwable
    {
        ColumnFamilyStore cfs = frozenColumnarTable();
        List<String> rows = List.of("SELECT tag, ts, v FROM %s WHERE tag = 't1'");
        List<UntypedResultSet> expected = run(rows);

        alterTable("ALTER TABLE %s DROP q");
        alterTable("ALTER TABLE %s ADD q int");

        for (UntypedResultSet.Row row : execute("SELECT q FROM %s"))
            assertFalse("the old value is gone", row.has("q"));
        assertEquals(0, execute("SELECT * FROM %s WHERE q = 3 ALLOW FILTERING").size());
        assertSame(rows, expected, run(rows));
        assertEquals("a row made by UPDATE of the dropped column alone is gone", 0, execute("SELECT * FROM %s WHERE tag = 'updated'").size());

        // A write after the re-add is a value of the new column. It lands in today's window; a late row into the old
        // window refreezes it, which purges the old cells for good.
        execute("INSERT INTO %s (tag, ts, q) VALUES ('t1', ?, 7)", new Date(1000L));
        long windowMicros = ((System.currentTimeMillis() - 3_600_000L) / 60_000L * 60_000L) * 1000L;
        execute("INSERT INTO %s (tag, ts, v) VALUES ('late', ?, 1.0) USING TIMESTAMP ?", new Date(0L), windowMicros + 1000);
        flush();
        runOnlyFreezeTask(cfs);
        assertEquals(7, execute("SELECT q FROM %s WHERE tag = 't1' AND ts = ?", new Date(1000L)).one().getInt("q"));
        assertEquals(1, execute("SELECT * FROM %s WHERE q = 7 ALLOW FILTERING").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE q = 3 ALLOW FILTERING").size());
    }

    @Test
    public void scrubAndVerifyKeepTheData() throws Throwable
    {
        ColumnFamilyStore cfs = frozenColumnarTable();
        List<String> all = List.of("SELECT * FROM %s");
        List<UntypedResultSet> expected = run(all);

        SSTableReader frozen = Iterables.getOnlyElement(cfs.getLiveSSTables());
        try (IVerifier verifier = frozen.getVerifier(cfs, new OutputHandler.LogOutput(), false,
                                                     IVerifier.options().invokeDiskFailurePolicy(false).extendedVerification(true).build()))
        {
            verifier.verify();
        }

        assertEquals(CompactionManager.AllSSTableOpStatus.SUCCESSFUL,
                     cfs.scrub(true, IScrubber.options().checkData().build(), 1));
        assertSame(all, expected, run(all));
    }

    @Test
    public void saiBuildsOverAFrozenColumnarWindow() throws Throwable
    {
        ColumnFamilyStore cfs = frozenColumnarTable();
        long expected = execute("SELECT * FROM %s WHERE q = 3 ALLOW FILTERING").size();
        assertTrue(expected > 0);

        createIndex("CREATE INDEX ON %s(q) USING 'sai'");
        waitForTableIndexesQueryable();
        assertEquals(expected, execute("SELECT * FROM %s WHERE q = 3").size());
        assertTrue(TscolFormat.is(Iterables.getOnlyElement(cfs.getLiveSSTables()).descriptor.getFormat()));
    }

    /**
     * The format's contract in one test: the same random writes -- partial rows, rows made by UPDATE alone, overwrites,
     * cell and row deletes, a range delete, nulls, empty values, statics, TTLs, some already expired -- read the same
     * from a columnar window as from row format, in every query shape.
     */
    @Test
    public void randomWritesReadTheSameFromBothFormats() throws Throwable
    {
        for (long seed = 1; seed <= 6; seed++)
            compareFormats(seed);
    }

    private void compareFormats(long seed) throws Throwable
    {
        String order = seed % 2 == 0 ? " AND CLUSTERING ORDER BY (ts DESC)" : "";
        String schema = "CREATE TABLE %s (tag text, ts timestamp, a int, b double, c text, s text static, PRIMARY KEY (tag, ts)) WITH compaction = ";
        String rowTable = createTable(schema + "{'class': 'TimeSeriesCompactionStrategy', 'window_size': '1m', 'freeze_after': '1m'}" + order);
        ColumnFamilyStore rowStore = getCurrentColumnFamilyStore();
        rowStore.disableAutoCompaction();
        String columnarTable = createTable(schema + TSCS_COLUMNAR + order);
        ColumnFamilyStore columnarStore = getCurrentColumnFamilyStore();
        columnarStore.disableAutoCompaction();
        List<String> tables = List.of(KEYSPACE + '.' + rowTable, KEYSPACE + '.' + columnarTable);

        Random random = new Random(seed);
        long micros = ((System.currentTimeMillis() - 3_600_000L) / 60_000L * 60_000L) * 1000L;
        for (int i = 0; i < 800; i++)
        {
            if (i == 400)
            {
                rowStore.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
                columnarStore.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
            }
            // t0 and t1 take the deletes that keep a partition in row form; the others stay columnar.
            int partition = random.nextInt(6);
            String tag = "t" + partition;
            Date ts = new Date(random.nextInt(200) * 1000L);
            long at = micros + i;
            int value = random.nextInt(50);
            switch (random.nextInt(partition < 2 ? 12 : 9))
            {
                case 0: case 1: case 2:
                    both(tables, "INSERT INTO %t (tag, ts, a, b, c) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?", tag, ts, value, value / 2.0, "c" + value, at); break;
                case 3:
                    both(tables, "INSERT INTO %t (tag, ts, a) VALUES (?, ?, ?) USING TIMESTAMP ?", tag, ts, value, at); break;
                case 4:
                    both(tables, "UPDATE %t USING TIMESTAMP ? SET b = ? WHERE tag = ? AND ts = ?", at, value / 4.0, tag, ts); break;
                case 5:
                    both(tables, "INSERT INTO %t (tag, ts, a, c) VALUES (?, ?, ?, ?) USING TIMESTAMP ? AND TTL 100000", tag, ts, value, "ttl", at); break;
                case 6:
                    both(tables, "UPDATE %t USING TIMESTAMP ? AND TTL 1 SET c = 'gone' WHERE tag = ? AND ts = ?", at, tag, ts); break;
                case 7:
                    both(tables, "INSERT INTO %t (tag, ts, a, b, c) VALUES (?, ?, ?, null, '') USING TIMESTAMP ?", tag, ts, value, at); break;
                case 8:
                    both(tables, "UPDATE %t USING TIMESTAMP ? SET s = ? WHERE tag = ?", at, "s" + value, tag); break;
                case 9:
                    both(tables, "DELETE a FROM %t USING TIMESTAMP ? WHERE tag = ? AND ts = ?", at, tag, ts); break;
                case 10:
                    both(tables, "DELETE FROM %t USING TIMESTAMP ? WHERE tag = ? AND ts = ?", at, tag, ts); break;
                default:
                    both(tables, "DELETE FROM %t USING TIMESTAMP ? WHERE tag = ? AND ts >= ? AND ts < ?", at, tag, ts, new Date(ts.getTime() + 5000L));
            }
        }
        rowStore.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        columnarStore.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        Thread.sleep(1100);                                                   // the TTL 1 cells are expired on both sides

        runOnlyFreezeTask(columnarStore);
        assertTrue(TscolFormat.is(Iterables.getOnlyElement(columnarStore.getLiveSSTables()).descriptor.getFormat()));

        List<String> queries = new ArrayList<>(List.of("SELECT * FROM %t",
                                                       "SELECT tag, ts, a, writetime(a), writetime(c) FROM %t",
                                                       "SELECT b FROM %t",
                                                       "SELECT DISTINCT tag, s FROM %t",
                                                       "SELECT * FROM %t WHERE a = 7 ALLOW FILTERING",
                                                       "SELECT count(*), max(a), min(b) FROM %t"));
        for (int p = 0; p < 6; p++)
        {
            queries.add("SELECT * FROM %t WHERE tag = 't" + p + "' ORDER BY ts ASC");
            queries.add("SELECT ts, c FROM %t WHERE tag = 't" + p + "' ORDER BY ts DESC LIMIT 7");
            queries.add("SELECT ts, a FROM %t WHERE tag = 't" + p + "' AND ts >= 30000 AND ts < 90000");
            queries.add("SELECT count(*) FROM %t WHERE tag = 't" + p + "'");
        }
        for (String query : queries)
        {
            String label = "seed " + seed + ": " + query;
            assertSame(List.of(label), List.of(execute(query.replace("%t", tables.get(0)))), List.of(execute(query.replace("%t", tables.get(1)))));
        }
    }

    private void both(List<String> tables, String cql, Object... values) throws Throwable
    {
        for (String table : tables)
            execute(cql.replace("%t", table), values);
    }

    private ColumnFamilyStore frozenColumnarTable() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, v double, q int, PRIMARY KEY (tag, ts)) WITH compaction = " + TSCS_COLUMNAR);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        long windowMicros = ((System.currentTimeMillis() - 3_600_000L) / 60_000L * 60_000L) * 1000L;
        for (int i = 0; i < 300; i++)
            execute("INSERT INTO %s (tag, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                    "t" + (i % 3), new Date(i * 1000L), 20.0 + i % 7, i % 4, windowMicros + i);
        execute("UPDATE %s USING TIMESTAMP ? SET q = 1 WHERE tag = 'updated' AND ts = ?", windowMicros, new Date(0L));
        flush();
        runOnlyFreezeTask(cfs);
        assertTrue(TscolFormat.is(Iterables.getOnlyElement(cfs.getLiveSSTables()).descriptor.getFormat()));
        return cfs;
    }

    /**
     * The shadow-table tiering is gone, but a table it managed still carries its {@code timeseries_tiering}
     * extension (and TSCS's {@code tiering_aware_freeze} option), and its {@code __chunks} table still exists. The
     * value is ignored and the leftover table is an ordinary table: nothing refuses either, and the base table
     * drops without first dropping what used to be its shadow.
     */
    @Test
    public void leftoverTieringExtensionIsIgnored() throws Throwable
    {
        ByteBuffer policy = ByteBufferUtil.bytes("{\"hot_window\":\"7d\",\"chunk_window\":\"1h\"}");
        createTable("CREATE TABLE %s (tag text, ts timestamp, v double, PRIMARY KEY (tag, ts)) WITH compaction = " +
                    "{'class': 'TimeSeriesCompactionStrategy', 'window_size': '1m', 'freeze_after': '1m', " +
                    "'frozen_format': 'columnar', 'tiering_aware_freeze': 'true'} " +
                    "AND extensions = {'timeseries_tiering': 0x" + ByteBufferUtil.bytesToHex(policy) + "}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        assertEquals(policy, cfs.metadata().params.extensions.get("timeseries_tiering"));

        // Below the old hot_window, where a tiered read would have gone looking for chunks.
        long windowMicros = ((System.currentTimeMillis() - 30 * 86_400_000L) / 60_000L * 60_000L) * 1000L;
        for (int i = 0; i < 50; i++)
            execute("INSERT INTO %s (tag, ts, v) VALUES ('a', ?, ?) USING TIMESTAMP ?", new Date(i * 1000L), (double) i, windowMicros + i);
        flush();
        assertEquals(50, execute("SELECT * FROM %s WHERE tag = 'a'").size());
        assertEquals(50, execute("SELECT * FROM %s").size());

        runOnlyFreezeTask(cfs);
        assertTrue(TscolFormat.is(Iterables.getOnlyElement(cfs.getLiveSSTables()).descriptor.getFormat()));
        assertEquals(50, execute("SELECT * FROM %s WHERE tag = 'a'").size());
        assertEquals(7.0, execute("SELECT v FROM %s WHERE tag = 'a' AND ts = ?", new Date(7000L)).one().getDouble("v"), 0.0);

        // What the tiered-write guards used to refuse: a delete into cold data.
        execute("DELETE FROM %s WHERE tag = 'a' AND ts = ?", new Date(7000L));
        assertEquals(49, execute("SELECT * FROM %s WHERE tag = 'a'").size());

        String base = KEYSPACE + '.' + currentTable();
        String chunks = base + "__chunks";
        schemaChange("CREATE TABLE " + chunks + " (tag text, ts timestamp, chunk blob, PRIMARY KEY (tag, ts))");
        execute("INSERT INTO " + chunks + " (tag, ts, chunk) VALUES ('a', 0, 0x00)");
        execute("TRUNCATE " + base);
        assertEquals("the old base table's TRUNCATE must not reach it", 1, execute("SELECT * FROM " + chunks).size());
        schemaChange("DROP TABLE " + base);
        assertEquals("nor its DROP", 1, execute("SELECT * FROM " + chunks).size());
        schemaChange("DROP TABLE " + chunks);
    }

    private List<UntypedResultSet> run(List<String> queries) throws Throwable
    {
        List<UntypedResultSet> results = new ArrayList<>();
        for (String q : queries)
            results.add(execute(q));
        return results;
    }

    private static void assertSame(List<String> queries, List<UntypedResultSet> expected, List<UntypedResultSet> actual)
    {
        for (int i = 0; i < queries.size(); i++)
        {
            UntypedResultSet e = expected.get(i), a = actual.get(i);
            assertEquals(queries.get(i), e.size(), a.size());
            java.util.Iterator<UntypedResultSet.Row> ei = e.iterator(), ai = a.iterator();
            while (ei.hasNext())
            {
                UntypedResultSet.Row er = ei.next(), ar = ai.next();
                for (org.apache.cassandra.cql3.ColumnSpecification c : e.metadata())
                    assertEquals(queries.get(i) + " " + c.name, er.getBytes(c.name.toString()), ar.getBytes(c.name.toString()));
            }
        }
    }
}
