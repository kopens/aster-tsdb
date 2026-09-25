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
package org.apache.cassandra.db.virtual;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * {@code system_views.timeseries_windows} shows, per TSCS window, the sstable shape an operator needs to spot
 * trouble -- above all window-spanning sstables and rows piling up in a window.
 */
public class TimeseriesWindowsTableTest extends CQLTester
{
    private static final long MINUTE = TimeUnit.MINUTES.toMillis(1);

    // Not setUpClass(): that would shadow CQLTester's own @BeforeClass of the same name.
    @BeforeClass
    public static void setUpVirtualKeyspace()
    {
        addVirtualKeyspace();
    }

    @Test
    public void reportsEachWindowWithItsSSTablesRowsAndSpanning() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) " +
                    "WITH compaction = {'class':'TimeSeriesCompactionStrategy', 'timestamp_resolution':'MILLISECONDS', " +
                    "'window_size':'1m', 'freeze_after':'1h'}");
        getCurrentColumnFamilyStore().disableAutoCompaction();

        // Two windows, placed by write timestamp: 3 rows in one flush, 2 rows in two more flushes.
        long older = (System.currentTimeMillis() - 10 * MINUTE) / MINUTE * MINUTE;
        long newer = older + MINUTE;
        for (int i = 0; i < 3; i++)
            execute("INSERT INTO %s (tag, ts, value) VALUES (?, ?, ?) USING TIMESTAMP " + (older + 1000 + i),
                    "t", new Date(older + i), (double) i);
        flush();
        for (int i = 0; i < 2; i++)
        {
            execute("INSERT INTO %s (tag, ts, value) VALUES (?, ?, ?) USING TIMESTAMP " + (newer + 1000 + i),
                    "t", new Date(newer + i), (double) i);
            flush();
        }

        UntypedResultSet rows = execute("SELECT * FROM system_views.timeseries_windows WHERE keyspace_name = ? AND table_name = ?",
                                        keyspace(), currentTable());
        List<UntypedResultSet.Row> windows = rows.stream().toList();
        assertEquals(2, windows.size());

        UntypedResultSet.Row first = windows.get(0);
        assertEquals(older, first.getTimestamp("window_start").getTime());
        assertEquals("CLOSING", first.getString("state"));          // closed, but within freeze_after
        assertFalse(first.getBoolean("parked"));
        assertEquals(1, first.getInt("sstables"));
        assertEquals(0, first.getInt("spanning_sstables"));
        assertEquals(3, first.getLong("rows"));
        assertTrue(first.getLong("bytes_on_disk") > 0);

        UntypedResultSet.Row second = windows.get(1);
        assertEquals(newer, second.getTimestamp("window_start").getTime());
        assertEquals(2, second.getInt("sstables"));
        assertEquals(0, second.getInt("spanning_sstables"));
        assertEquals(2, second.getLong("rows"));
    }

    @Test
    public void aSpanningSSTableIsCounted() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) " +
                    "WITH compaction = {'class':'TimeSeriesCompactionStrategy', 'timestamp_resolution':'MILLISECONDS', " +
                    "'window_size':'1m', 'freeze_after':'1h'}");
        getCurrentColumnFamilyStore().disableAutoCompaction();

        // Written before TSCS is set, so the flush is not window-split: one sstable across two windows --
        // the legacy shape (and the shape the UCS delegate used to produce) that this column exists to flag.
        alterTable("ALTER TABLE %s WITH compaction = {'class':'UnifiedCompactionStrategy'}");
        long older = (System.currentTimeMillis() - 10 * MINUTE) / MINUTE * MINUTE;
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t', ?, 1.0) USING TIMESTAMP " + (older + 1000), new Date(older));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t', ?, 2.0) USING TIMESTAMP " + (older + MINUTE + 1000), new Date(older + MINUTE));
        flush();
        alterTable("ALTER TABLE %s WITH compaction = {'class':'TimeSeriesCompactionStrategy', 'timestamp_resolution':'MILLISECONDS', " +
                   "'window_size':'1m', 'freeze_after':'1h'}");

        UntypedResultSet.Row only = execute("SELECT * FROM system_views.timeseries_windows WHERE keyspace_name = ? AND table_name = ?",
                                            keyspace(), currentTable()).one();
        assertEquals("filed under the window of its max timestamp", older + MINUTE, only.getTimestamp("window_start").getTime());
        assertEquals(1, only.getInt("sstables"));
        assertEquals(1, only.getInt("spanning_sstables"));
        assertEquals(2, only.getLong("rows"));
    }
}
