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

package org.apache.cassandra.distributed.test.timeseries;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.bti.TscolFormat;
import org.apache.cassandra.metrics.ReadRepairMetrics;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Freezing is node-local, so replicas of one window sit in different formats for a while, and the columnar one must not
 * look different from outside: not to a digest, not to repair. {@code stream_entire_sstables} is off because the path
 * under test is the one that ships Data.db sections and has the receiver parse them.
 */
public class TscolDistributedTest extends TestBaseImpl
{
    private static final String COMPACTION = "{'class':'TimeSeriesCompactionStrategy','window_size':'1m','freeze_after':'1m'," +
                                             "'frozen_format':'columnar','enabled':'false'}";
    private static final int ROWS = 300;
    private static Cluster CLUSTER;

    @BeforeClass
    public static void setUpCluster() throws IOException
    {
        CLUSTER = init(Cluster.build(2)
                              .withDataDirCount(1)                     // one window, one sstable
                              .withConfig(config -> config.set("hinted_handoff_enabled", false)
                                                          .set("stream_entire_sstables", false)
                                                          .with(GOSSIP).with(NETWORK))
                              .start(),
                       2);
    }

    @AfterClass
    public static void tearDownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    private static void createTable(String table)
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s." + table + " (tag text, ts timestamp, v double, q int, " +
                                          "PRIMARY KEY (tag, ts)) WITH compaction = " + COMPACTION));
    }

    private static long closedWindowMicros()
    {
        return ((System.currentTimeMillis() - 3_600_000L) / 60_000L * 60_000L) * 1000L;
    }

    private static void freeze(IInvokableInstance node, String table)
    {
        String ks = KEYSPACE;
        node.flush(ks);
        node.runOnInstance(() -> Keyspace.open(ks).getColumnFamilyStore(table).enableAutoCompaction());
        for (int i = 0; i < 120 && columnarFiles(node, table) == 0; i++)
            Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
        assertEquals("the window froze to columnar", 1, columnarFiles(node, table));
    }

    private static int columnarFiles(IInvokableInstance node, String table)
    {
        String ks = KEYSPACE;
        return node.callOnInstance(() -> {
            int count = 0;
            for (SSTableReader sstable : Keyspace.open(ks).getColumnFamilyStore(table).getLiveSSTables())
                if (TscolFormat.is(sstable.descriptor.getFormat()))
                    count++;
            return count;
        });
    }

    private static int sstables(IInvokableInstance node, String table)
    {
        String ks = KEYSPACE;
        return node.callOnInstance(() -> Keyspace.open(ks).getColumnFamilyStore(table).getLiveSSTables().size());
    }

    private static long reconcileReads(IInvokableInstance node)
    {
        return node.callOnInstance(() -> ReadRepairMetrics.repairedBlocking.getCount());
    }

    /**
     * One replica columnar, the other still row format. If a columnar read differed from a row-format one by so much as
     * a cell, a read at ALL would see a digest mismatch and start a read repair, which is what the meter counts.
     */
    @Test
    public void mixedFormatReplicasAgreeOnDigestsAndRepair()
    {
        String table = "mixed";
        createTable(table);
        long micros = closedWindowMicros();
        for (int i = 0; i < ROWS; i++)
            CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s." + table + " (tag, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?"),
                                           ConsistencyLevel.ALL, "t" + i % 3, new Date(i * 1000L), i / 2.0, i % 5, micros + i);
        CLUSTER.get(2).flush(KEYSPACE);
        freeze(CLUSTER.get(1), table);
        assertEquals("the other replica has not frozen", 0, columnarFiles(CLUSTER.get(2), table));

        for (int coordinator = 1; coordinator <= 2; coordinator++)
        {
            long before = reconcileReads(CLUSTER.get(coordinator));
            for (String select : Arrays.asList("SELECT * FROM %s." + table + " WHERE tag = 't1'",
                                               "SELECT v FROM %s." + table + " WHERE tag = 't1'",
                                               "SELECT v, writetime(v) FROM %s." + table + " WHERE tag = 't2' AND ts >= 30000 AND ts < 90000"))
                assertTrue(CLUSTER.coordinator(coordinator).execute(withKeyspace(select), ConsistencyLevel.ALL).length > 0);
            assertEquals("no digest mismatch through coordinator " + coordinator, before, reconcileReads(CLUSTER.get(coordinator)));
        }

        int first = sstables(CLUSTER.get(1), table), second = sstables(CLUSTER.get(2), table);
        CLUSTER.get(1).nodetoolResult("repair", "--full", KEYSPACE, table).asserts().success();
        assertEquals("repair found nothing to send to node 1", first, sstables(CLUSTER.get(1), table));
        assertEquals("repair found nothing to send to node 2", second, sstables(CLUSTER.get(2), table));
    }

    /**
     * Rows that only node 1 has, frozen columnar there, reach node 2 by repair as sections of the columnar Data.db --
     * including a partition the range delete keeps in row form inside the same file.
     */
    @Test
    public void repairStreamsSectionsOfAColumnarSSTable()
    {
        String table = "sections";
        createTable(table);
        long micros = closedWindowMicros();
        String insert = withKeyspace("INSERT INTO %s." + table + " (tag, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?");
        for (int i = 0; i < ROWS; i++)
            CLUSTER.get(1).executeInternal(insert, "t" + i % 3, new Date(i * 1000L), i / 2.0, i % 5, micros + i);
        CLUSTER.get(1).executeInternal(withKeyspace("DELETE FROM %s." + table + " USING TIMESTAMP ? WHERE tag = 't0' AND ts >= ? AND ts < ?"),
                                       micros + ROWS, new Date(0L), new Date(30_000L));
        freeze(CLUSTER.get(1), table);
        String count = withKeyspace("SELECT count(*) FROM %s." + table);
        Object expected = CLUSTER.get(1).executeInternal(count)[0][0];
        assertEquals("node 2 starts empty", 0L, CLUSTER.get(2).executeInternal(count)[0][0]);

        CLUSTER.get(1).nodetoolResult("repair", "--full", KEYSPACE, table).asserts().success();

        assertEquals(expected, CLUSTER.get(2).executeInternal(count)[0][0]);
        String select = withKeyspace("SELECT tag, ts, v, q, writetime(v) FROM %s." + table + " WHERE tag = 't1'");
        assertTrue("node 2 has every cell, writetimes included",
                   Arrays.deepEquals(CLUSTER.get(1).executeInternal(select), CLUSTER.get(2).executeInternal(select)));
    }
}
