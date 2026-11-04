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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import com.google.common.collect.Iterables;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.bti.TscolFormat;
import org.apache.cassandra.utils.FBUtilities;

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
     * Windows frozen before frozen_format = 'columnar' was set -- or restored by nodetool untier -- are single
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
