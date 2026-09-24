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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.timeseries.TimeWindowSplittingMultiWriter;
import org.apache.cassandra.db.compaction.timeseries.WindowFrozenListener;
import org.apache.cassandra.db.compaction.timeseries.WindowFrozenListeners;
import org.apache.cassandra.db.compaction.timeseries.WindowRoutingIterator;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.utils.FBUtilities.nowInSeconds;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * End-to-end coverage for the retention-driven whole-window drop, mirroring
 * {@link TimeWindowCompactionStrategyTest#testDropExpiredSSTables()}: real schema, real flushed sstables,
 * and (via {@link ColumnFamilyStore#setCompactionParameters}) the real, non-mocked
 * {@link TimeSeriesCompactionStrategy} two-arg constructor - the one reflection instantiates in production,
 * which the Mockito-based {@link TimeSeriesCompactionStrategyTest} never exercises.
 * <p>
 * This lives in its own class (rather than alongside {@link TimeSeriesCompactionStrategyTest}) because a
 * SchemaLoader-based fixture (real keyspace/schema init) and the Mockito-only fixture don't share static
 * init cleanly in one JUnit class.
 */
public class TimeSeriesCompactionStrategyE2ETest extends SchemaLoader
{
    private static final String KEYSPACE1 = "TimeSeriesCompactionStrategyE2ETest";
    private static final String CF_STANDARD1 = "Standard1";
    private static final String CF_GCGRACE0 = "StandardGcGrace0";
    private static final int TTL_SECONDS = 10;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        CassandraRelevantProperties.STREAMING_HISTOGRAM_ROUND_SECONDS.setInt(1);
        ServerTestUtils.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_GCGRACE0).gcGraceSeconds(0));
    }

    @Test
    public void testDropExpiredWindowWhole()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);

        // This sstable's window closed roughly an hour ago - well past the 2-minute retention cutoff
        // configured below. timestamp_resolution=MILLISECONDS below makes the strategy read this raw
        // millisecond value directly as the cell timestamp, same convention as TimeWindowCompactionStrategyTest.
        long expiredWriteMillis = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);
        DecoratedKey expiredKey = Util.dk("expired-window");
        new RowUpdateBuilder(cfs.metadata(), expiredWriteMillis, TTL_SECONDS, expiredKey.getKey())
            .clustering("column")
            .add("val", value).build().applyUnsafe();
        Util.flush(cfs);
        SSTableReader expiredSSTable = cfs.getLiveSSTables().iterator().next();

        // This one lands in the current window and must survive the drop.
        DecoratedKey liveKey = Util.dk("live-window");
        new RowUpdateBuilder(cfs.metadata(), System.currentTimeMillis(), liveKey.getKey())
            .clustering("column")
            .add("val", value).build().applyUnsafe();
        Util.flush(cfs);
        assertEquals(2, cfs.getLiveSSTables().size());

        // window_size=1m, freeze_after=1m, retention=2m (the configuration minimum, since retention must
        // be >= window_size + freeze_after): the expired-window sstable is an hour past that cutoff, the
        // live one is in the current window.
        cfs.setCompactionParameters(ImmutableMap.of("class", "TimeSeriesCompactionStrategy",
                                                    "timestamp_resolution", "MILLISECONDS",
                                                    "window_size", "1m",
                                                    "freeze_after", "1m",
                                                    "retention", "2m"));

        TimeSeriesCompactionStrategy tscs =
            (TimeSeriesCompactionStrategy) cfs.getCompactionStrategyManager().getCompactionStrategyFor(expiredSSTable);

        long gcBefore = nowInSeconds();
        Collection<AbstractCompactionTask> tasks = tscs.getNextBackgroundTasks(gcBefore);
        AbstractCompactionTask task = Iterables.getOnlyElement(tasks, null);
        assertNotNull(task);
        assertTrue(task instanceof TimeSeriesCompactionTask);
        assertEquals(1, Iterables.size(task.transaction.originals()));
        assertEquals(expiredSSTable, task.transaction.originals().iterator().next());

        task.execute(ActiveCompactionsTracker.NOOP);

        assertFalse(cfs.getLiveSSTables().contains(expiredSSTable));
        assertEquals(1, cfs.getLiveSSTables().size());
    }

    /**
     * getMaximalTasks builds one task per window directly (bypassing the delegate), so it needs its own
     * routing: a window that is itself expired must still go through {@link TimeSeriesCompactionTask} (so its
     * retention cutoff is honoured), while a live window must go through a plain {@link CompactionTask}.
     */
    @Test
    public void testGetMaximalTasksRoutesExpiredWindowThroughTimeSeriesCompactionTask()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);

        // Expired window: closed roughly an hour ago, well past the 2-minute retention cutoff configured below.
        long expiredWriteMillis = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);
        DecoratedKey expiredKey = Util.dk("expired-window-maximal");
        new RowUpdateBuilder(cfs.metadata(), expiredWriteMillis, expiredKey.getKey())
            .clustering("column")
            .add("val", value).build().applyUnsafe();
        Util.flush(cfs);
        SSTableReader expiredSSTable = cfs.getLiveSSTables().iterator().next();

        // Current window: must never be routed through the retention-drop task.
        DecoratedKey liveKey = Util.dk("live-window-maximal");
        new RowUpdateBuilder(cfs.metadata(), System.currentTimeMillis(), liveKey.getKey())
            .clustering("column")
            .add("val", value).build().applyUnsafe();
        Util.flush(cfs);
        assertEquals(2, cfs.getLiveSSTables().size());

        cfs.setCompactionParameters(ImmutableMap.of("class", "TimeSeriesCompactionStrategy",
                                                    "timestamp_resolution", "MILLISECONDS",
                                                    "window_size", "1m",
                                                    "freeze_after", "1m",
                                                    "retention", "2m"));

        TimeSeriesCompactionStrategy tscs =
            (TimeSeriesCompactionStrategy) cfs.getCompactionStrategyManager().getCompactionStrategyFor(expiredSSTable);

        long gcBefore = nowInSeconds();
        List<AbstractCompactionTask> tasks = tscs.getMaximalTasks(gcBefore, false);
        assertEquals(2, tasks.size());   // one task per window, never merged across windows

        try
        {
            for (AbstractCompactionTask task : tasks)
            {
                boolean coversExpired = Iterables.contains(task.transaction.originals(), expiredSSTable);
                if (coversExpired)
                    assertTrue("expired window must route through TimeSeriesCompactionTask", task instanceof TimeSeriesCompactionTask);
                else
                    assertFalse("live window must not route through TimeSeriesCompactionTask", task instanceof TimeSeriesCompactionTask);
            }
        }
        finally
        {
            for (AbstractCompactionTask task : tasks)
                task.transaction.abort();
        }
    }

    /** Records fire counts - used to verify idempotent-consumer semantics (exactly one event per freeze/re-freeze). */
    private static final class RecordingListener implements WindowFrozenListener
    {
        final List<Long> windowStarts = new CopyOnWriteArrayList<>();
        final List<SSTableReader> frozen = new CopyOnWriteArrayList<>();
        /** Whether the reported sstable was already in the tracker's live set at the moment of the call. */
        final List<Boolean> liveAtFireTime = new CopyOnWriteArrayList<>();
        volatile ColumnFamilyStore observed;

        @Override
        public void onWindowFrozen(TableMetadata table, long windowStartMillis, SSTableReader sstable)
        {
            windowStarts.add(windowStartMillis);
            frozen.add(sstable);
            if (observed != null)
                liveAtFireTime.add(observed.getLiveSSTables().contains(sstable));
        }
    }

    /**
     * T2-I5: pins the post-commit ordering of the frozen-window event, which until now nothing
     * discriminated - moving {@code WindowFrozenListeners.fire} above {@code super.finish(pipeline)} in
     * {@link FreezeCompactionTask} left every test green. {@code super.finish} is the point of no
     * return: it runs prepareToCommit + commit, so by the time it returns the output sstable is durably
     * committed and visible in the tracker. Firing earlier would hand consumers an sstable that is not
     * yet live (and might never become live), so asserting that the listener sees it ALREADY live is
     * exactly the discriminating observation.
     */
    @Test
    public void testFrozenEventFiresOnlyAfterTheOutputIsCommitted()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);
        long windowSizeMillis = 60_000L;
        long base = ((System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1)) / windowSizeMillis) * windowSizeMillis;
        for (int i = 0; i < 2; i++)
        {
            new RowUpdateBuilder(cfs.metadata(), base + 1000 + i, Util.dk("commit-" + i).getKey())
                .clustering("column").add("val", value).build().applyUnsafe();
            Util.flush(cfs);
        }
        assertEquals(2, cfs.getLiveSSTables().size());

        cfs.setCompactionParameters(ImmutableMap.of("class", "TimeSeriesCompactionStrategy",
                                                    "timestamp_resolution", "MILLISECONDS",
                                                    "window_size", "1m",
                                                    "freeze_after", "1m"));

        RecordingListener listener = new RecordingListener();
        listener.observed = cfs;
        WindowFrozenListeners.registerListener(listener);
        try
        {
            TimeSeriesCompactionStrategy tscs = (TimeSeriesCompactionStrategy)
                cfs.getCompactionStrategyManager().getCompactionStrategyFor(cfs.getLiveSSTables().iterator().next());
            AbstractCompactionTask task = Iterables.getOnlyElement(tscs.getNextBackgroundTasks(nowInSeconds()), null);
            assertNotNull(task);
            assertTrue(task instanceof FreezeCompactionTask);
            task.execute(ActiveCompactionsTracker.NOOP);

            assertEquals(1, listener.windowStarts.size());
            assertEquals(1, listener.liveAtFireTime.size());
            assertTrue("the frozen sstable must already be live when the event fires",
                       listener.liveAtFireTime.get(0));
        }
        finally
        {
            WindowFrozenListeners.unsafeClearListeners();
        }
    }

    /** An interrupted freeze commits nothing, so it must fire nothing. */
    @Test
    public void testInterruptedFreezeFiresNothing()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);
        long windowSizeMillis = 60_000L;
        long base = ((System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1)) / windowSizeMillis) * windowSizeMillis;
        for (int i = 0; i < 2; i++)
        {
            new RowUpdateBuilder(cfs.metadata(), base + 1000 + i, Util.dk("interrupt-" + i).getKey())
                .clustering("column").add("val", value).build().applyUnsafe();
            Util.flush(cfs);
        }

        cfs.setCompactionParameters(ImmutableMap.of("class", "TimeSeriesCompactionStrategy",
                                                    "timestamp_resolution", "MILLISECONDS",
                                                    "window_size", "1m",
                                                    "freeze_after", "1m"));

        RecordingListener listener = new RecordingListener();
        WindowFrozenListeners.registerListener(listener);
        try
        {
            TimeSeriesCompactionStrategy tscs = (TimeSeriesCompactionStrategy)
                cfs.getCompactionStrategyManager().getCompactionStrategyFor(cfs.getLiveSSTables().iterator().next());
            AbstractCompactionTask task = Iterables.getOnlyElement(tscs.getNextBackgroundTasks(nowInSeconds()), null);
            assertNotNull(task);

            // Stop the compaction the moment it registers itself, so it aborts mid-iteration.
            ActiveCompactionsTracker stopImmediately = new ActiveCompactionsTracker()
            {
                public void beginCompaction(CompactionInfo.Holder ci)
                {
                    ci.stop();
                }

                public void finishCompaction(CompactionInfo.Holder ci)
                {
                }
            };

            try
            {
                task.execute(stopImmediately);
                fail("interrupted freeze should have thrown");
            }
            catch (CompactionInterruptedException expected)
            {
                // the transaction rolls back; the window simply classifies FREEZING again next round
            }

            assertEquals("an interrupted freeze commits nothing and must fire nothing", 0, listener.windowStarts.size());
            assertEquals(2, cfs.getLiveSSTables().size());
        }
        finally
        {
            WindowFrozenListeners.unsafeClearListeners();
        }
    }

    /**
     * T2-C1: a freeze must run the disk-space pre-flight like any other compaction, and must refuse to
     * shrink its scope. Overriding {@code shouldReduceScopeForSpace()} to false expressed the second
     * intent but silently disabled the first, because {@link CompactionTask#runMayThrow} guards the
     * whole pre-flight with that flag - so a freeze larger than the free space started writing, hit
     * ENOSPC, and under the shipped {@code disk_failure_policy: stop} took the node out of the ring.
     */
    @Test
    public void testFreezeRunsDiskSpaceCheckAndRefusesToShrink()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);
        long windowSizeMillis = 60_000L;
        long base = ((System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1)) / windowSizeMillis) * windowSizeMillis;
        for (int i = 0; i < 2; i++)
        {
            new RowUpdateBuilder(cfs.metadata(), base + 1000 + i, Util.dk("space-" + i).getKey())
                .clustering("column").add("val", value).build().applyUnsafe();
            Util.flush(cfs);
        }

        cfs.setCompactionParameters(ImmutableMap.of("class", "TimeSeriesCompactionStrategy",
                                                    "timestamp_resolution", "MILLISECONDS",
                                                    "window_size", "1m",
                                                    "freeze_after", "1m"));

        TimeSeriesCompactionStrategy tscs = (TimeSeriesCompactionStrategy)
            cfs.getCompactionStrategyManager().getCompactionStrategyFor(cfs.getLiveSSTables().iterator().next());
        AbstractCompactionTask task = Iterables.getOnlyElement(tscs.getNextBackgroundTasks(nowInSeconds()), null);
        assertNotNull(task);
        assertTrue(task instanceof FreezeCompactionTask);

        try
        {
            FreezeCompactionTask freeze = (FreezeCompactionTask) task;

            // A freeze never drops its largest input to fit: that would leave the window at two
            // sstables, which is precisely NOT frozen.
            assertFalse("a freeze must never reduce its scope",
                        freeze.reduceScopeForLimitedSpace(new HashSet<>(freeze.transaction.originals()), Long.MAX_VALUE));

            // ...and it must still let the pre-flight run, which is what the old flag suppressed.
            freeze.transaction.abort();
            final boolean[] preflightRan = { false };
            LifecycleTransaction txn = cfs.getTracker().tryModify(cfs.getLiveSSTables(), OperationType.COMPACTION);
            assertNotNull(txn);
            FreezeCompactionTask instrumented = new FreezeCompactionTask(cfs, txn, nowInSeconds(), base)
            {
                @Override
                protected boolean buildCompactionCandidatesForAvailableDiskSpace(Set<SSTableReader> nonExpired,
                                                                                 boolean containsExpired,
                                                                                 org.apache.cassandra.utils.TimeUUID taskId)
                {
                    preflightRan[0] = true;
                    return super.buildCompactionCandidatesForAvailableDiskSpace(nonExpired, containsExpired, taskId);
                }
            };
            instrumented.execute(ActiveCompactionsTracker.NOOP);
            assertTrue("the disk-space pre-flight must actually run for a freeze", preflightRan[0]);
        }
        finally
        {
            WindowFrozenListeners.unsafeClearListeners();
        }
    }

    @Test
    public void testFreezeThenLateDataRefreeze()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);

        // Two sstables that both fall entirely within one 1-minute window (same min/max window): base is
        // that window's start, an hour in the past so the window is well closed.
        long windowSizeMillis = 60_000L;
        long base = ((System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1)) / windowSizeMillis) * windowSizeMillis;
        for (int i = 0; i < 2; i++)
        {
            new RowUpdateBuilder(cfs.metadata(), base + 1000 + i, Util.dk("frozen-" + i).getKey())
                .clustering("column")
                .add("val", value).build().applyUnsafe();
            Util.flush(cfs);
        }
        assertEquals(2, cfs.getLiveSSTables().size());

        // No retention configured: the expired-window branch must not sweep this window out from under the
        // freeze path, so this test exercises freeze in isolation.
        cfs.setCompactionParameters(ImmutableMap.of("class", "TimeSeriesCompactionStrategy",
                                                    "timestamp_resolution", "MILLISECONDS",
                                                    "window_size", "1m",
                                                    "freeze_after", "1m"));

        RecordingListener listener = new RecordingListener();
        WindowFrozenListeners.registerListener(listener);
        try
        {
            TimeSeriesCompactionStrategy tscs = (TimeSeriesCompactionStrategy)
                cfs.getCompactionStrategyManager().getCompactionStrategyFor(cfs.getLiveSSTables().iterator().next());

            // 1) Freeze: the closed window's 2 sstables -> one FreezeCompactionTask -> a single sstable and
            // exactly one fire.
            Collection<AbstractCompactionTask> tasks = tscs.getNextBackgroundTasks(nowInSeconds());
            AbstractCompactionTask task = Iterables.getOnlyElement(tasks, null);
            assertNotNull(task);
            assertTrue(task instanceof FreezeCompactionTask);
            assertEquals(2, Iterables.size(task.transaction.originals()));
            task.execute(ActiveCompactionsTracker.NOOP);

            assertEquals(1, cfs.getLiveSSTables().size());
            assertEquals(1, listener.windowStarts.size());
            assertEquals(base, (long) listener.windowStarts.get(0));
            assertEquals(cfs.getLiveSSTables().iterator().next(), listener.frozen.get(0));

            // 2) A FROZEN window is not reselected (stateless classification) and does not re-fire.
            Collection<AbstractCompactionTask> idle = tscs.getNextBackgroundTasks(nowInSeconds());
            for (AbstractCompactionTask t : idle)                    // clean up txns even on contract violation
                t.transaction.abort();
            assertTrue(idle.isEmpty());
            assertEquals(1, listener.windowStarts.size());

            // 3) Late data: a late write into the same window flips FROZEN back to FREEZING -> re-freeze and
            // a second fire (design spec section 4).
            new RowUpdateBuilder(cfs.metadata(), base + 5000, Util.dk("late").getKey())
                .clustering("column")
                .add("val", value).build().applyUnsafe();
            Util.flush(cfs);
            assertEquals(2, cfs.getLiveSSTables().size());

            Collection<AbstractCompactionTask> refreeze = tscs.getNextBackgroundTasks(nowInSeconds());
            AbstractCompactionTask refreezeTask = Iterables.getOnlyElement(refreeze, null);
            assertNotNull(refreezeTask);
            assertTrue(refreezeTask instanceof FreezeCompactionTask);
            refreezeTask.execute(ActiveCompactionsTracker.NOOP);

            assertEquals(1, cfs.getLiveSSTables().size());
            assertEquals(2, listener.windowStarts.size());           // re-freeze = exactly one additional fire
            assertEquals(base, (long) listener.windowStarts.get(1));
        }
        finally
        {
            WindowFrozenListeners.unsafeClearListeners();
        }
    }

    /**
     * The tiering re-encoder deletes the rows it has chunked with a range tombstone timestamped at their own
     * max writetime, so the tombstone routes into the same window as the rows -- which by then is often
     * already FROZEN. The tombstone's sstable must turn the window back to FREEZING, and the refreeze (a real
     * CompactionController merge) must drop the shadowed rows: gc_grace only protects the tombstone, not what
     * it shadows. Until that rewrite, every read over the range merges the dead rows too -- measured at 7-13x
     * slower on tiered aggregates -- so this is the property that bounds how long that lasts.
     */
    @Test
    public void rangeTombstoneIntoAFrozenWindowRefreezesAndDropsTheShadowedRows()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);   // default gc_grace: tombstone kept
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);
        long windowSizeMillis = 60_000L;
        long base = ((System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1)) / windowSizeMillis) * windowSizeMillis;
        for (int i = 0; i < 3; i++)
        {
            new RowUpdateBuilder(cfs.metadata(), base + 1000 + i, Util.dk("tag").getKey())
                .clustering("c" + i)
                .add("val", value).build().applyUnsafe();
            Util.flush(cfs);
        }
        cfs.setCompactionParameters(ImmutableMap.of("class", "TimeSeriesCompactionStrategy",
                                                    "timestamp_resolution", "MILLISECONDS",
                                                    "window_size", "1m",
                                                    "freeze_after", "1m"));
        TimeSeriesCompactionStrategy tscs = (TimeSeriesCompactionStrategy)
            cfs.getCompactionStrategyManager().getCompactionStrategyFor(cfs.getLiveSSTables().iterator().next());

        // gcBefore must honour gc_grace: passing "now" would make the tombstone purgeable as soon as the clock
        // ticks past the second it was written in, and the refreeze would then (correctly) drop everything.
        AbstractCompactionTask freeze = Iterables.getOnlyElement(tscs.getNextBackgroundTasks(cfs.gcBefore(nowInSeconds())), null);
        assertTrue(freeze instanceof FreezeCompactionTask);
        freeze.execute(ActiveCompactionsTracker.NOOP);
        assertEquals(1, cfs.getLiveSSTables().size());
        assertEquals(3, rowsIn(Iterables.getOnlyElement(cfs.getLiveSSTables())));

        // The re-encoder's delete: covers every row, stamped just after the newest of them -- inside the window.
        new RowUpdateBuilder(cfs.metadata(), base + 1003, Util.dk("tag").getKey())
            .addRangeTombstone("c0", "c9").build().applyUnsafe();
        Util.flush(cfs);
        assertEquals("the tombstone lands beside the frozen sstable", 2, cfs.getLiveSSTables().size());

        AbstractCompactionTask refreeze = Iterables.getOnlyElement(tscs.getNextBackgroundTasks(cfs.gcBefore(nowInSeconds())), null);
        assertTrue("a frozen window that gains a tombstone sstable must be refrozen", refreeze instanceof FreezeCompactionTask);
        refreeze.execute(ActiveCompactionsTracker.NOOP);

        SSTableReader refrozen = Iterables.getOnlyElement(cfs.getLiveSSTables());
        assertEquals("the refreeze must drop the rows the tombstone shadows", 0, rowsIn(refrozen));
        assertTrue("the tombstone itself is kept until gc_grace", rangeTombstoneMarkersIn(refrozen) > 0);
    }

    /**
     * Active windows are compacted by the UCS delegate, and every sstable it produces must still sit in one
     * window. The delegate is window-blind: handed the sstables of several active windows at once, it merged
     * across them into a spanning sstable, filed under its newest window. Fed by later compactions of that
     * window, such an sstable keeps its max timestamp recent, so it never leaves the active set, is never
     * frozen and never split -- and the rows in it from older windows never meet the deletions routed into
     * those windows. In production that left ~17M rows the tiering re-encoder had already deleted on disk,
     * and a count over one tag's recent day timed out.
     */
    @Test
    public void delegateCompactionNeverProducesAWindowSpanningSSTable()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);
        long windowSizeMillis = 60_000L;
        // Two adjacent, closed but still ACTIVE windows (freeze_after 1h): both belong to the delegate.
        long older = ((System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(10)) / windowSizeMillis) * windowSizeMillis;
        long newer = older + windowSizeMillis;
        for (int i = 0; i < 8; i++)
        {
            for (long window : new long[]{ older, newer })
            {
                new RowUpdateBuilder(cfs.metadata(), window + 1000 + i, Util.dk("tag").getKey())
                    .clustering("c" + window + '-' + i)
                    .add("val", value).build().applyUnsafe();
                Util.flush(cfs);
            }
        }
        cfs.setCompactionParameters(ImmutableMap.of("class", "TimeSeriesCompactionStrategy",
                                                    "timestamp_resolution", "MILLISECONDS",
                                                    "window_size", "1m",
                                                    "freeze_after", "1h",
                                                    "scaling_parameters", "T4"));
        assertTrue("every flushed sstable starts window-contained", allContained(cfs, windowSizeMillis));

        TimeSeriesCompactionStrategy tscs = (TimeSeriesCompactionStrategy)
            cfs.getCompactionStrategyManager().getCompactionStrategyFor(cfs.getLiveSSTables().iterator().next());
        int rounds = 0;
        for (Collection<AbstractCompactionTask> tasks = tscs.getNextBackgroundTasks(cfs.gcBefore(nowInSeconds()));
             !tasks.isEmpty() && rounds < 20;
             tasks = tscs.getNextBackgroundTasks(cfs.gcBefore(nowInSeconds())), rounds++)
        {
            for (AbstractCompactionTask task : tasks)
                task.execute(ActiveCompactionsTracker.NOOP);
        }
        assertTrue("the delegate must have compacted something for this test to mean anything", rounds > 0);
        assertTrue("delegate outputs must stay inside one window: " + describeSpans(cfs, windowSizeMillis),
                   allContained(cfs, windowSizeMillis));
        assertEquals(16, cfs.getLiveSSTables().stream().mapToInt(TimeSeriesCompactionStrategyE2ETest::rowsIn).sum());
    }

    private static boolean allContained(ColumnFamilyStore cfs, long windowSizeMillis)
    {
        for (SSTableReader sstable : cfs.getLiveSSTables())
            if (sstable.getMinTimestamp() / windowSizeMillis != sstable.getMaxTimestamp() / windowSizeMillis)
                return false;
        return true;
    }

    private static String describeSpans(ColumnFamilyStore cfs, long windowSizeMillis)
    {
        StringBuilder sb = new StringBuilder();
        for (SSTableReader sstable : cfs.getLiveSSTables())
            sb.append('[').append(sstable.getMinTimestamp() / windowSizeMillis).append("..")
              .append(sstable.getMaxTimestamp() / windowSizeMillis).append("] ");
        return sb.toString();
    }

    private static int rowsIn(SSTableReader sstable)
    {
        int rows = 0;
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    while (partition.hasNext())
                        rows += partition.next().isRow() ? 1 : 0;
                }
            }
        }
        return rows;
    }

    private static int rangeTombstoneMarkersIn(SSTableReader sstable)
    {
        int markers = 0;
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    while (partition.hasNext())
                        markers += partition.next().isRangeTombstoneMarker() ? 1 : 0;
                }
            }
        }
        return markers;
    }

    /**
     * Structural resolution of T1's caveat ("TTL reclaim of a closed window needs retention"): without any
     * retention configured, the freeze compaction's real CompactionController + gcBefore reclaims TTL'd data
     * in a closed window. If the whole window is expired, the output is zero sstables and the window itself
     * disappears - and in that case no event fires (there is no sstable to hand a consumer).
     */
    @Test
    public void testFreezeReclaimsTTLDataInClosedWindowWithoutRetention() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_GCGRACE0);   // gcGraceSeconds(0)
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);
        long windowSizeMillis = 60_000L;
        long base = ((System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1)) / windowSizeMillis) * windowSizeMillis;
        for (int i = 0; i < 2; i++)
        {
            new RowUpdateBuilder(cfs.metadata(), base + 1000 + i, 1 /* TTL 1s */, Util.dk("ttl-" + i).getKey())
                .clustering("column")
                .add("val", value).build().applyUnsafe();
            Util.flush(cfs);
        }
        assertEquals(2, cfs.getLiveSSTables().size());

        cfs.setCompactionParameters(ImmutableMap.of("class", "TimeSeriesCompactionStrategy",
                                                    "timestamp_resolution", "MILLISECONDS",
                                                    "window_size", "1m",
                                                    "freeze_after", "1m"));
        Thread.sleep(2000);   // TTL(1s) elapses: gcGrace 0 means gcBefore=now judges both sstables fully expired

        RecordingListener listener = new RecordingListener();
        WindowFrozenListeners.registerListener(listener);
        try
        {
            TimeSeriesCompactionStrategy tscs = (TimeSeriesCompactionStrategy)
                cfs.getCompactionStrategyManager().getCompactionStrategyFor(cfs.getLiveSSTables().iterator().next());

            Collection<AbstractCompactionTask> tasks = tscs.getNextBackgroundTasks(nowInSeconds());
            AbstractCompactionTask task = Iterables.getOnlyElement(tasks, null);
            assertNotNull(task);
            assertTrue(task instanceof FreezeCompactionTask);
            task.execute(ActiveCompactionsTracker.NOOP);

            assertTrue(cfs.getLiveSSTables().isEmpty());             // reclaimed with no retention configured
            assertEquals(0, listener.windowStarts.size());           // window vanished = no event
        }
        finally
        {
            WindowFrozenListeners.unsafeClearListeners();
        }
    }

    @Test
    public void testSplitRefreezeLegacySpanningSSTable()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();
        java.nio.ByteBuffer value = java.nio.ByteBuffer.wrap(new byte[16]);

        // setCompactionParameters is a LOCAL override that outlives the test that set it, and JUnit's
        // method order is not source order - so pin the pre-TSCS strategy explicitly here rather than
        // assuming no earlier test in this class has already switched this table to TSCS. Without this
        // the flush below would already be window-split and there would be no legacy spanning sstable
        // left to test.
        cfs.setCompactionParameters(ImmutableMap.of("class", "SizeTieredCompactionStrategy"));

        // Recreate a LEGACY spanning sstable: flush rows from two windows in one memtable while the
        // schema strategy is still the default (non-TSCS) - TSCS's own flush writer would split it.
        //
        // Deliberately NOT one-cell-per-row: the "c-straddle" row below has two cells written in
        // different windows, which is exactly the shape this test used to lack and which is why it
        // missed C1. Splitting must break that row up per cell, not send it whole to one window.
        long now = System.currentTimeMillis();
        DecoratedKey key = Util.dk("span");
        new RowUpdateBuilder(cfs.metadata(), now - TimeUnit.HOURS.toMillis(1), key.getKey())
            .clustering("c-old").add("val", value).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), now - TimeUnit.MINUTES.toMillis(10), key.getKey())
            .clustering("c-new").add("val", value).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), now - TimeUnit.HOURS.toMillis(1), key.getKey())
            .clustering("c-straddle").add("val", value).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), now - TimeUnit.MINUTES.toMillis(10), key.getKey())
            .clustering("c-straddle").add("val0", "x").build().applyUnsafe();
        Util.flush(cfs);
        assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader spanning = cfs.getLiveSSTables().iterator().next();

        cfs.setCompactionParameters(ImmutableMap.of("class", "TimeSeriesCompactionStrategy",
                                                    "timestamp_resolution", "MILLISECONDS",
                                                    "window_size", "1m",
                                                    "freeze_after", "1m"));
        TimeSeriesCompactionStrategy tscs =
            (TimeSeriesCompactionStrategy) cfs.getCompactionStrategyManager().getCompactionStrategyFor(spanning);

        Collection<AbstractCompactionTask> tasks = tscs.getNextBackgroundTasks(nowInSeconds());
        AbstractCompactionTask task = Iterables.getOnlyElement(tasks, null);
        assertNotNull(task);
        assertTrue(task instanceof SplitRefreezeCompactionTask);
        task.execute(ActiveCompactionsTracker.NOOP);

        // One contained sstable per window; both rows still readable; nothing left to do next round
        // (each window is now a single contained sstable = FROZEN - the freeze path never reselects it).
        assertEquals(2, cfs.getLiveSSTables().size());
        for (SSTableReader s : cfs.getLiveSSTables())
            assertEquals(s.toString(), windowOfMinute(s.getMinTimestamp()), windowOfMinute(s.getMaxTimestamp()));
        assertEquals(3, Util.getOnlyPartition(Util.cmd(cfs, key).build()).rowCount());
        assertTrue(tscs.getNextBackgroundTasksAt(System.currentTimeMillis(), nowInSeconds()).isEmpty());
    }

    /**
     * C2, second half: {@code SplitRefreezeCompactionTask} had no cap at all - its {@code rewriters} map
     * was an unbounded {@code TreeMap}, so splitting one legacy sstable spanning a year at
     * {@code window_size=1h} would have opened ~8,760 concurrent rewriters, each with its own writer and
     * file descriptors. It now applies the same per-element admission the flush writer does. Ten windows,
     * cap three: at most three outputs, all rows still readable.
     */
    @Test
    public void testSplitRefreezeRespectsTheWindowWriterCap()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();
        cfs.setCompactionParameters(ImmutableMap.of("class", "SizeTieredCompactionStrategy"));

        int saved = TimeWindowSplittingMultiWriter.maxWindowWriters;
        try
        {
            ByteBuffer value = ByteBuffer.wrap(new byte[16]);
            DecoratedKey key = Util.dk("cap-span");
            long now = System.currentTimeMillis();
            // One legacy partition spanning ten one-minute windows, flushed while the strategy is still
            // SizeTiered so nothing splits it on the way in. All ten are well past window_size +
            // freeze_after, so the newest window is closed and the sstable is a split candidate.
            for (int i = 0; i < 10; i++)
                new RowUpdateBuilder(cfs.metadata(), now - TimeUnit.MINUTES.toMillis(10L + i), key.getKey())
                    .clustering("c" + i).add("val", value).build().applyUnsafe();
            Util.flush(cfs);
            assertEquals(1, cfs.getLiveSSTables().size());
            SSTableReader spanning = cfs.getLiveSSTables().iterator().next();

            TimeWindowSplittingMultiWriter.maxWindowWriters = 3;
            cfs.setCompactionParameters(ImmutableMap.of("class", "TimeSeriesCompactionStrategy",
                                                        "timestamp_resolution", "MILLISECONDS",
                                                        "window_size", "1m",
                                                        "freeze_after", "1m"));
            TimeSeriesCompactionStrategy tscs =
                (TimeSeriesCompactionStrategy) cfs.getCompactionStrategyManager().getCompactionStrategyFor(spanning);

            AbstractCompactionTask task = Iterables.getOnlyElement(tscs.getNextBackgroundTasks(nowInSeconds()), null);
            assertNotNull(task);
            assertTrue(task instanceof SplitRefreezeCompactionTask);
            task.execute(ActiveCompactionsTracker.NOOP);

            assertEquals("a split must not open more rewriters than the cap", 3, cfs.getLiveSSTables().size());
            assertEquals(10, Util.getOnlyPartition(Util.cmd(cfs, key).build()).rowCount());
        }
        finally
        {
            TimeWindowSplittingMultiWriter.maxWindowWriters = saved;
        }
    }

    /**
     * The freeze &lt;-&gt; split alternation, end to end, through the real completion path.
     * <p>
     * A window whose single spanning sstable holds a partition too large to window-route <b>and</b>
     * ordinary data: the split writes the un-routable partition unsplit into one sstable and the
     * ordinary rows into their own, leaving the window with two sstables - which makes it a FREEZE
     * candidate - and the freeze merges them straight back into one still-spanning sstable, which makes
     * it a SPLIT candidate again. Both rewrites change the window's shape, so parking on "this rewrite
     * changed nothing" reset on every single one of them and the giant partition was rewritten end to
     * end forever.
     * <p>
     * Deliberately driven by real {@code task.execute(...)} calls rather than by running the guard
     * callbacks by hand: the strike is scored from inside {@code FreezeCompactionTask#finish} and
     * {@code SplitRefreezeCompactionTask#runMayThrow}, post-commit, and the unit tests' direct
     * {@code onCompleted().run()} could not tell a task that fires it from one that does not. Deleting
     * either call fails this test, because nothing scores and the loop never terminates.
     */
    @Test
    public void testFreezeSplitAlternationTerminatesThroughTheRealCompletionPath()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();
        cfs.setCompactionParameters(ImmutableMap.of("class", "SizeTieredCompactionStrategy"));

        long savedBudget = WindowRoutingIterator.maxBufferedBytesPerPartition;
        try
        {
            ByteBuffer value = ByteBuffer.wrap(new byte[16]);
            long now = System.currentTimeMillis();

            // One partition across eight one-minute windows, in clustering (= time) order, flushed while
            // the strategy is still SizeTiered so nothing splits it on the way in.
            DecoratedKey huge = Util.dk("alternation-unroutable");
            for (int i = 0; i < 8; i++)
                new RowUpdateBuilder(cfs.metadata(), now - TimeUnit.MINUTES.toMillis(10L - i), huge.getKey())
                    .clustering("c" + i).add("val", value).build().applyUnsafe();
            // Ordinary data in the newest of those windows, in its own partition. This is the half the
            // split CAN route, and therefore the second sstable that turns a futile split into a freeze.
            DecoratedKey ordinary = Util.dk("alternation-ordinary");
            new RowUpdateBuilder(cfs.metadata(), now - TimeUnit.MINUTES.toMillis(3), ordinary.getKey())
                .clustering("c").add("val", value).build().applyUnsafe();
            Util.flush(cfs);
            assertEquals(1, cfs.getLiveSSTables().size());
            SSTableReader spanning = cfs.getLiveSSTables().iterator().next();

            // Budget 1 byte: any partition of more than one unfiltered overflows, so the eight-window
            // partition is written unsplit every time while the one-row partition still routes normally.
            WindowRoutingIterator.maxBufferedBytesPerPartition = 1;
            cfs.setCompactionParameters(ImmutableMap.of("class", "TimeSeriesCompactionStrategy",
                                                        "timestamp_resolution", "MILLISECONDS",
                                                        "window_size", "1m",
                                                        "freeze_after", "1m"));
            TimeSeriesCompactionStrategy tscs =
                (TimeSeriesCompactionStrategy) cfs.getCompactionStrategyManager().getCompactionStrategyFor(spanning);

            int maxRounds = 4 * TimeSeriesCompactionStrategy.NO_PROGRESS_STRIKES + 4;
            int rewrites = 0;
            boolean sawSplit = false;
            boolean sawFreeze = false;
            for (int round = 0; round < maxRounds; round++)
            {
                AbstractCompactionTask task = Iterables.getOnlyElement(tscs.getNextBackgroundTasks(nowInSeconds()), null);
                if (task == null)
                    break;                                // parked: the alternation stopped on its own
                sawSplit |= task instanceof SplitRefreezeCompactionTask;
                sawFreeze |= task instanceof FreezeCompactionTask;
                rewrites++;
                task.execute(ActiveCompactionsTracker.NOOP);
            }

            assertTrue("both paths must take part, or this is not the alternation: split=" + sawSplit +
                       " freeze=" + sawFreeze, sawSplit && sawFreeze);
            assertTrue("the alternation must terminate; it ran " + rewrites + " rewrites", rewrites < maxRounds);
            assertFalse("the window must end up parked", tscs.getParkedWindows().isEmpty());
            assertEquals("...and it must be visible on the table's MBean",
                         tscs.getParkedWindows().keySet(), cfs.getParkedTimeSeriesWindows().keySet());

            // Nothing was lost on the way round the loop.
            assertEquals(8, Util.getOnlyPartition(Util.cmd(cfs, huge).build()).rowCount());
            assertEquals(1, Util.getOnlyPartition(Util.cmd(cfs, ordinary).build()).rowCount());
        }
        finally
        {
            WindowRoutingIterator.maxBufferedBytesPerPartition = savedBudget;
        }
    }

    /**
     * The other thing the strategy tracks for operators and could not previously be seen from outside
     * the JVM: sstables whose window is beyond {@code max_future_window}. They are excluded from the
     * UCS delegate, from freeze, from split and from retention, so they accumulate permanently - one
     * per flush for as long as the bad clock or {@code USING TIMESTAMP} input persists - while the
     * table reports nothing to do. Only a throttled WARN said so before this reached the MBean.
     */
    @Test
    public void testFarFutureSSTablesAreVisibleOnTheMBean()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();
        cfs.setCompactionParameters(ImmutableMap.of("class", "SizeTieredCompactionStrategy"));

        ByteBuffer value = ByteBuffer.wrap(new byte[16]);
        long farFuture = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(30);
        new RowUpdateBuilder(cfs.metadata(), farFuture, Util.dk("bad-clock").getKey())
            .clustering("c").add("val", value).build().applyUnsafe();
        Util.flush(cfs);
        assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader ahead = cfs.getLiveSSTables().iterator().next();

        cfs.setCompactionParameters(ImmutableMap.of("class", "TimeSeriesCompactionStrategy",
                                                    "timestamp_resolution", "MILLISECONDS",
                                                    "window_size", "1m",
                                                    "freeze_after", "1m",
                                                    "max_future_window", "1d"));
        TimeSeriesCompactionStrategy tscs =
            (TimeSeriesCompactionStrategy) cfs.getCompactionStrategyManager().getCompactionStrategyFor(ahead);

        assertTrue("nothing is excluded until a background round classifies it",
                   cfs.getFarFutureTimeSeriesSSTables().isEmpty());
        tscs.getNextBackgroundTasks(nowInSeconds());

        assertEquals(List.of(ahead.toString()), cfs.getFarFutureTimeSeriesSSTables());
    }

    private static long windowOfMinute(long ms)
    {
        return ms - Math.floorMod(ms, 60_000L);
    }
}
