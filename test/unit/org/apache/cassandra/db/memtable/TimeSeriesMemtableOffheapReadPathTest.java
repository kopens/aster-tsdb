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

package org.apache.cassandra.db.memtable;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.NativeDecoratedKey;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.memory.NativePool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The production condition the heap-based memtable tests cannot reach: {@code
 * memtable_allocation_type: offheap_objects} clones clusterings into {@code NativeClustering}s,
 * whose value accessor has no object factory — {@code NativeAccessor.factory()} throws
 * {@code UnsupportedOperationException}. Fabricating a {@code ClusteringBound} from a
 * memtable-owned clustering ({@code Slice.make} / {@code ClusteringBound.create}) therefore throws
 * on a native memtable, and the shard-pruning probe of the 2026-08-02 outage did exactly that.
 *
 * <p>Paging is what put the probe on the hot path: page one of a paged read carries
 * {@code Slices.ALL}, which skips pruning, but every later page carries the pager's
 * {@code Slices.forPaging} rewrite — never {@code ALL} — so every paged read of the tag table
 * failed from its second page while unpaged reads stayed green. The in-process
 * {@code CQLTester.execute()} path neither pages nor allocates natively, which is why the original
 * battery missed it; paged native-protocol reads over offheap_objects are therefore a required
 * part of this memtable's coverage.
 */
public class TimeSeriesMemtableOffheapReadPathTest extends CQLTester
{
    private static final long HOUR_MS = 3_600_000L;

    /** 2026-08-02T00:00:00Z — exactly on an hour boundary, so {@code +k hours} is window {@code k}. */
    private static final long BASE_MS = 1785628800000L;

    private static final String TSCS =
        "{'class':'TimeSeriesCompactionStrategy','window_size':'1h','freeze_after':'2h'}";

    /** Microsecond write timestamp {@code hours} windows after {@link #BASE_MS}. */
    private static long at(int hours)
    {
        return (BASE_MS + hours * HOUR_MS) * 1000L;
    }

    /**
     * Overrides {@link CQLTester#setUpClass()} so the allocation type is switched after
     * {@code daemonInitialization} but before the server — and with it the first memtable pool —
     * exists. Same pattern as {@code MemtableSizeTestBase#setup}.
     */
    @BeforeClass
    public static void setUpClass()
    {
        prePrepareServer();
        try
        {
            Field confField = DatabaseDescriptor.class.getDeclaredField("conf");
            confField.setAccessible(true);
            ((Config) confField.get(null)).memtable_allocation_type = Config.MemtableAllocationType.offheap_objects;
        }
        catch (NoSuchFieldException | IllegalAccessException e)
        {
            throw new RuntimeException(e);
        }
        prepareServer();
    }

    /**
     * A compound clustering declines the bare-long clustering store, so every stored clustering is
     * a {@code NativeClustering} — the shape the production outage hit. (A single DESC timestamp
     * clustering used to be the declining shape, but it now takes the long store with negated
     * comparisons, so this test pins the native-clustering path through a shape the long store
     * structurally cannot take.) Before the fix, page two of the plain paged read threw
     * {@code UnsupportedOperationException} out of the pruning probe.
     */
    @Test
    public void pagedReadsSurviveNativeClusteringsOnDescTable() throws Throwable
    {
        createTable("CREATE TABLE %s (series text, ts timestamp, seq int, v double, PRIMARY KEY (series, ts, seq)) " +
                    "WITH CLUSTERING ORDER BY (ts DESC, seq DESC) AND compaction = " + TSCS + " AND memtable = 'timeseries'");
        for (int i = 0; i < 40; i++)
            execute("INSERT INTO %s (series, ts, seq, v) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                    "S1", BASE_MS + i * 1000L, 0, i * 1.0, at(i < 20 ? 0 : 1) + i);
        assertOffheapTimeSeriesMemtable();

        assertEquals(40, count(executeNetWithPaging("SELECT * FROM %s WHERE series = 'S1'", 7)));
        assertEquals(40, count(executeNetWithPaging("SELECT * FROM %s WHERE series = 'S1' ORDER BY ts ASC", 7)));
        assertEquals(25, count(executeNetWithPaging("SELECT * FROM %s WHERE series = 'S1' AND ts >= " +
                                                    (BASE_MS + 5000L) + " LIMIT 25", 7)));
    }

    /**
     * An ASC timestamp clustering keeps the bare-long store (heap bounds), but overflow rows are
     * cloned whole — native clusterings. A row tombstone below every array clustering makes the
     * overflow map's native clustering the min bound, which is the other way production could
     * throw: any promoted or unrepresentable row whose clustering extends the array bounds.
     */
    @Test
    public void pagedReadsSurviveNativeOverflowClusteringsOnAscTable() throws Throwable
    {
        createTable("CREATE TABLE %s (series text, ts timestamp, v double, PRIMARY KEY (series, ts)) " +
                    "WITH compaction = " + TSCS + " AND memtable = 'timeseries'");
        for (int i = 0; i < 30; i++)
            execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                    "S1", BASE_MS + i * 1000L, i * 1.0, at(0) + i);
        // Promote one row (later write timestamp, same window)...
        execute("UPDATE %s USING TIMESTAMP ? SET v = ? WHERE series = ? AND ts = ?",
                at(0) + 60_000_000L, 7.5, "S1", BASE_MS + 7000L);
        // ...and leave a row tombstone BELOW every array clustering: overflow-only native bound.
        execute("DELETE FROM %s USING TIMESTAMP ? WHERE series = ? AND ts = ?",
                at(0) + 61_000_000L, "S1", BASE_MS - 60_000L);
        execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                "S1", BASE_MS + 40_000L, 40.0, at(1));
        assertOffheapTimeSeriesMemtable();

        assertEquals(31, count(executeNetWithPaging("SELECT * FROM %s WHERE series = 'S1'", 7)));
        assertEquals(10, count(executeNetWithPaging("SELECT * FROM %s WHERE series = 'S1' AND ts >= " +
                                                    BASE_MS + " AND ts <= " + (BASE_MS + 9000L), 3)));
    }

    /**
     * The unit-level pin of the outage: the bounds probe of a native partition, fed the pager's
     * rewritten slices directly. A DESC <b>text</b> clustering keeps every stored clustering a
     * {@code NativeClustering} — a single DESC timestamp no longer does (it takes the long store
     * with negated comparisons), so the probe is pinned through a shape the long store cannot
     * take. Before the fix every covering probe here threw {@code UnsupportedOperationException};
     * after it, covering shapes keep the shard and exhausted shapes prune it.
     */
    @Test
    public void nativeBoundsProbeHandlesPagerSlices() throws Throwable
    {
        createTable("CREATE TABLE %s (series text, name text, v double, PRIMARY KEY (series, name)) " +
                    "WITH CLUSTERING ORDER BY (name DESC) AND compaction = " + TSCS + " AND memtable = 'timeseries'");
        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (series, name, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                    "S1", String.format("k%02d", i), i * 1.0, at(0) + i);
        assertOffheapTimeSeriesMemtable();

        TimeSeriesMemtable memtable = (TimeSeriesMemtable) getCurrentColumnFamilyStore().getCurrentMemtable();
        TimeSeriesMemtable.ShardPartition partition = solePartition(memtable);
        ClusteringComparator comparator = getCurrentColumnFamilyStore().metadata().comparator;

        Clustering<?> mid = Clustering.make(ByteBufferUtil.bytes("k05"));
        assertTrue(partition.mayContainRowsIn(Slices.ALL.forPaging(comparator, mid, false, false)));
        assertTrue(partition.mayContainRowsIn(Slices.ALL.forPaging(comparator, mid, false, true)));
        assertTrue(partition.mayContainRowsIn(Slices.with(comparator, Slice.make(mid))));

        // Pruning must still prune: past the comparator-last row ("k00" on DESC) is nothing.
        assertFalse(partition.mayContainRowsIn(Slices.ALL.forPaging(comparator,
                                                                    Clustering.make(ByteBufferUtil.bytes("k00")),
                                                                    false, false)));
        assertFalse(partition.mayContainRowsIn(Slices.NONE));
        assertTrue(partition.mayContainRowsIn(Slices.ALL));
    }

    /**
     * Every read-path iterator of a columnar partition must hand out an on-heap partition key.
     *
     * <p>Under offheap_objects the partition key this memtable stores is a
     * {@link NativeDecoratedKey}: its length and bytes live in the memtable's native regions, which
     * are freed once the memtable is flushed and reclaimed. Rows already went through
     * {@code allocator.ensureOnHeap()}, but the key did not: {@code EnsureOnHeap.applyToPartitionKey}
     * only rewrites the transformation's cached field, and {@code BaseRows.partitionKey()} answers
     * from its input, so every columnar read iterator reported the raw native key. Upstream
     * {@code AtomicBTreePartition} is immune because it builds its iterators from its own
     * (cloning) {@code partitionKey()} accessor; the columnar partition built them from the stored
     * key. This is the unit-level pin of the 2026-09-28 SIGSEGV (see
     * {@link #inMemoryLocalResponseOutlivesTheMemtableItWasReadFrom}).
     */
    @Test
    public void readPathIteratorsNeverExposeTheNativePartitionKey() throws Throwable
    {
        createTable("CREATE TABLE %s (series text, ts timestamp, v double, PRIMARY KEY (series, ts)) " +
                    "WITH compaction = " + TSCS + " AND memtable = 'timeseries'");
        for (int i = 0; i < 20; i++)
            execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                    "S1", BASE_MS + i * 1000L, i * 1.0, at(0) + i);
        assertOffheapTimeSeriesMemtable();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        TimeSeriesMemtable memtable = (TimeSeriesMemtable) cfs.getCurrentMemtable();
        TimeSeriesMemtable.ShardPartition partition = solePartition(memtable);
        assertTrue("expected the columnar representation, got " + partition.getClass().getName(),
                   partition instanceof TimeSeriesColumnarPartition);
        assertTrue("the stored key must be native, or this test proves nothing",
                   ((TimeSeriesColumnarPartition) partition).storedKey() instanceof NativeDecoratedKey);

        DecoratedKey key = Util.dk("S1");
        ColumnFilter all = ColumnFilter.all(cfs.metadata());
        SSTableReadsListener noop = SSTableReadsListener.NOOP_LISTENER;

        assertOnHeapKey("sliced read (streaming path)", key,
                        memtable.rowIterator(key, Slices.ALL, all, false, noop));
        assertOnHeapKey("reversed sliced read (streaming path)", key,
                        memtable.rowIterator(key, Slices.ALL, all, true, noop));
        assertOnHeapKey("whole-partition read (rebuild path)", key, memtable.rowIterator(key));
        assertOnHeapKey("names read (rebuild path)", key,
                        partition.unfilteredIterator(all,
                                                     FBUtilities.singleton(Clustering.make(ByteBufferUtil.bytes(BASE_MS + 3000L)),
                                                                           cfs.metadata().comparator),
                                                     false));
        try (UnfilteredPartitionIterator range = memtable.partitionIterator(all, DataRange.allData(cfs.getPartitioner()), noop))
        {
            assertTrue(range.hasNext());
            assertOnHeapKey("range read", key, range.next());
        }
    }

    /**
     * The 2026-09-28 node-41 SIGSEGV, reproduced end to end. CASSANDRA-21354 (upstream, merged into
     * this fork 2026-09-24) keeps a coordinator-local single-partition response as an in-memory
     * partition instead of serializing it inside the read's {@code ReadExecutionController}. The
     * response therefore outlives the read's OpOrder group, and with it the memtable's protection
     * against reclaim. The coordinator digests it later, once the other replicas have answered; on
     * node 41 that was after the memtable had been flushed and its native regions freed, so
     * {@code NativeDecoratedKey.getKey()} read a garbage length out of freed memory and the digest's
     * copy ran into a thread-stack guard page ({@code SEGV_ACCERR} in
     * {@code jbyte_disjoint_arraycopy} under {@code Digest.update}).
     *
     * <p>The key's type is asserted first and before the flush, so the unfixed build fails here with
     * an assertion instead of reading freed memory and taking the test JVM down with it.
     */
    @Test
    public void inMemoryLocalResponseOutlivesTheMemtableItWasReadFrom() throws Throwable
    {
        createTable("CREATE TABLE %s (series text, ts timestamp, v double, PRIMARY KEY (series, ts)) " +
                    "WITH compaction = " + TSCS + " AND memtable = 'timeseries'");
        for (int i = 0; i < 20; i++)
            execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                    "S1", BASE_MS + i * 1000L, i * 1.0, at(0) + i);
        assertOffheapTimeSeriesMemtable();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SinglePartitionReadCommand command =
            SinglePartitionReadCommand.fullPartitionRead(cfs.metadata(), FBUtilities.nowInSeconds(), Util.dk("S1"));
        ReadResponse response;
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator iterator = command.executeLocally(controller))
        {
            response = command.createLocalObjectResponse(iterator, controller.getRepairedDataInfo(), false);
        }
        assertEquals("the read must take the CASSANDRA-21354 in-memory path, or this test proves nothing",
                     "InMemoryDataResponse", response.getClass().getSimpleName());

        try (UnfilteredPartitionIterator retained = response.makeIterator(command))
        {
            assertTrue(retained.hasNext());
            assertOnHeapKey("in-memory response", Util.dk("S1"), retained.next());
        }

        ByteBuffer before = response.digest(command);
        flush();   // flushes, discards and reclaims the memtable the response was read from
        assertTrue("the memtable must have been replaced", cfs.getCurrentMemtable().isClean());
        assertEquals(before, response.digest(command));
    }

    private static void assertOnHeapKey(String what, DecoratedKey expected, UnfilteredRowIterator iterator)
    {
        assertTrue(what + ": no iterator", iterator != null);
        try (UnfilteredRowIterator rows = iterator)
        {
            DecoratedKey key = rows.partitionKey();
            assertFalse(what + ": partition key is the memtable's native key " + key.getClass().getName(),
                        key instanceof NativeDecoratedKey);
            assertFalse(what + ": partition key bytes are off heap", key.getKey().isDirect());
            assertEquals(what, expected, key);
            while (rows.hasNext())
                rows.next();
        }
    }

    private void assertOffheapTimeSeriesMemtable()
    {
        assertTrue("this test must run on a NativePool (offheap_objects), or it proves nothing",
                   AbstractAllocatorMemtable.MEMORY_POOL instanceof NativePool);
        Memtable current = getCurrentColumnFamilyStore().getCurrentMemtable();
        assertTrue("expected a TimeSeriesMemtable, got " + current.getClass().getName(),
                   current instanceof TimeSeriesMemtable);
    }

    private static TimeSeriesMemtable.ShardPartition solePartition(TimeSeriesMemtable memtable)
    {
        List<TimeSeriesMemtable.ShardPartition> partitions = new ArrayList<>();
        for (TimeSeriesMemtable.WindowShard shard : memtable.shards().values())
            partitions.addAll(shard.partitions.values());
        assertEquals(1, partitions.size());
        return partitions.get(0);
    }

    private static int count(ResultSet resultSet)
    {
        int rows = 0;
        for (Row ignored : resultSet)
            rows++;
        return rows;
    }
}
