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

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.LongUnaryOperator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.timeseries.TimeWindowSplittingMultiWriter;
import org.apache.cassandra.db.compaction.timeseries.WindowRoutingIterator;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.WrappedLifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;

/**
 * TSCS T3's local re-freeze for legacy SPANNING sstables (design spec sections 4 and 10): rewrites
 * exactly one sstable whose write timestamps straddle window boundaries into one output sstable per
 * window, each fully contained in its window. Once split, each window classifies FROZEN (single
 * contained sstable) or FREEZING on its own in later rounds - this task never fires
 * {@code WindowFrozenListener} itself (plan D7: freeze completion, and its event, belong to the
 * regular {@link FreezeCompactionTask} path).
 *
 * Write choreography follows anticompaction (CompactionManager.antiCompactGroup): several
 * {@link SSTableRewriter}s over one shared {@link LifecycleTransaction}, committed together after
 * the originals are obsoleted.
 * <p>
 * Most spanning sstables are legacy - pre-T3 data or a strategy switch - so this is normally a
 * one-shot migration per sstable. TSCS can also produce one deliberately, when a partition is too
 * large to window-route on heap or when a flush hits the per-writer cap.
 * <p>
 * <b>Bounded fan-out.</b> One rewriter per window would open one set of writer file descriptors per
 * window, and a legacy sstable backfilled over a year at {@code window_size=1h} spans ~8,760 of them -
 * so the same cap the flush writer applies ({@link TimeWindowSplittingMultiWriter#maxWindowWriters})
 * is applied here, by the same per-element admission: beyond the cap a window folds onto an
 * already-open rewriter. A folded output still spans windows and is re-selected for a later split,
 * and that sequence terminates because each pass emits at most {@code maxWindowWriters} non-empty
 * outputs, so every output covers a proper subset of this input's windows and the widest span
 * strictly shrinks. A partition too large to window-route on heap is split by reading it again
 * once per window ({@link #splitByRereading}), so it converges too. Anything else that does not
 * converge is parked by the strategy's no-progress guard instead of being rewritten forever.
 */
public class SplitRefreezeCompactionTask extends AbstractCompactionTask
{
    private static final Logger logger = LoggerFactory.getLogger(SplitRefreezeCompactionTask.class);

    private final long gcBefore;
    private final LongUnaryOperator windowStartOfMillis;
    private final TimeUnit tableResolution;
    private final Runnable onCompleted;
    /**
     * Chosen once in {@link #runMayThrow} and used by both the disk-space pre-flight and every writer.
     * Sizing the pre-flight against the originals' directories while writing somewhere else clears the
     * check on the wrong disk under JBOD, which is precisely the ENOSPC the pre-flight exists to avoid.
     */
    private File writeDirectory;

    public SplitRefreezeCompactionTask(ColumnFamilyStore cfs,
                                       LifecycleTransaction txn,
                                       long gcBefore,
                                       LongUnaryOperator windowStartOfMillis,
                                       TimeUnit tableResolution)
    {
        this(cfs, txn, gcBefore, windowStartOfMillis, tableResolution, () -> {});
    }

    /**
     * @param onCompleted run once the rewrite has durably committed, and never if it aborted or threw.
     *        {@link TimeSeriesCompactionStrategy} scores its no-progress guard from this, so a split
     *        stopped by {@code nodetool stop COMPACTION} or refused by the disk-space pre-flight must
     *        not reach it - a strike for work that never ran would park a healthy window.
     */
    public SplitRefreezeCompactionTask(ColumnFamilyStore cfs,
                                       LifecycleTransaction txn,
                                       long gcBefore,
                                       LongUnaryOperator windowStartOfMillis,
                                       TimeUnit tableResolution,
                                       Runnable onCompleted)
    {
        super(cfs, txn);
        this.gcBefore = gcBefore;
        this.windowStartOfMillis = windowStartOfMillis;
        this.tableResolution = tableResolution;
        this.onCompleted = onCompleted;
    }

    @VisibleForTesting
    Runnable onCompleted()
    {
        return onCompleted;
    }

    private ActiveCompactionsTracker activeCompactions;

    @Override
    protected void executeInternal(ActiveCompactionsTracker activeCompactions)
    {
        this.activeCompactions = activeCompactions == null ? ActiveCompactionsTracker.NOOP : activeCompactions;
        run();
    }

    @Override
    protected void runMayThrow()
    {
        Set<SSTableReader> originals = transaction.originals();
        // Checked before dereferencing, not after: asserts are disabled in production, so an assert
        // placed below the iterator().next() would never guard anything (M7).
        if (originals.size() != 1)
            throw new IllegalStateException("split-refreeze rewrites exactly one spanning sstable, got " + originals);
        SSTableReader spanning = originals.iterator().next();

        // Pick the output directory once, then check that same directory has room (M8).
        writeDirectory = cfs.getDirectories().getDirectoryForNewSSTables();
        checkDiskSpace(originals);

        long nowInSec = FBUtilities.nowInSeconds();
        long maxDataAge = CompactionTask.getMaxDataAge(originals);
        Map<Long, SSTableRewriter> rewriters = new TreeMap<>();
        Throwable err = null;

        RateLimiter limiter = CompactionManager.instance.getRateLimiter();
        double compressionRatio = spanning.getCompressionRatio();
        if (compressionRatio == MetadataCollector.NO_COMPRESSION_RATIO)
            compressionRatio = 1.0;
        long lastBytesScanned = 0;

        try (SharedTxn shared = new SharedTxn(transaction);
             ISSTableScanner scanner = spanning.getScanner();
             CompactionController controller = new CompactionController(cfs, originals, gcBefore);
             CompactionIterator ci = new CompactionIterator(OperationType.COMPACTION, List.of(scanner), controller,
                                                            nowInSec, TimeUUID.Generator.nextTimeUUID()))
        {
            activeCompactions.beginCompaction(ci);
            try
            {
                long[] rereadBytes = { 0 };
                long lastRereadBytes = 0;
                while (ci.hasNext())
                {
                    // One admission decision maker per partition, seeded from the rewriters already
                    // open: that is what keeps the fan-out capped WITHIN a partition too, not just
                    // across them (a single partition can span every window in the input).
                    WindowAdmission admission = new WindowAdmission(rewriters.keySet());
                    DecoratedKey overflowed = null;
                    try (UnfilteredRowIterator partition = ci.next())
                    {
                        NavigableMap<Long, UnfilteredRowIterator> slices =
                            WindowRoutingIterator.slicesWithinBudget(partition, admission, tableResolution);
                        if (slices == null)
                            overflowed = partition.partitionKey();
                        else
                            for (Map.Entry<Long, UnfilteredRowIterator> entry : slices.entrySet())
                                rewriterFor(rewriters, entry.getKey(), admission, shared, maxDataAge, originals).append(entry.getValue());
                    }
                    if (overflowed != null)
                        splitByRereading(spanning, overflowed, admission, rewriters, shared, maxDataAge, originals, rereadBytes);

                    // Obey compaction_throughput like every other compaction does (M6): this rewrites a
                    // whole sstable and would otherwise run at unthrottled disk speed. The re-reads of an
                    // over-budget partition bypass the scanner, so they are charged separately.
                    long bytesScanned = scanner.getBytesScanned();
                    CompactionManager.instance.compactionRateLimiterAcquire(limiter, bytesScanned, lastBytesScanned, compressionRatio);
                    lastBytesScanned = bytesScanned;
                    CompactionManager.instance.compactionRateLimiterAcquire(limiter, rereadBytes[0], lastRereadBytes, compressionRatio);
                    lastRereadBytes = rereadBytes[0];
                }

                for (SSTableRewriter rewriter : rewriters.values())
                    rewriter.prepareToCommit();
                transaction.checkpoint();
                transaction.obsoleteOriginals();
                transaction.prepareToCommit();
                Throwable t = null;
                for (SSTableRewriter rewriter : rewriters.values())
                    t = rewriter.commit(t);
                t = transaction.commit(t);
                Throwables.maybeFail(t);
                logger.info("Split spanning sstable {} of {}.{} into {} window-contained sstable(s)",
                            spanning, cfs.getKeyspaceName(), cfs.getTableName(), rewriters.size());
                // Strictly post-commit: the guard must score completed work only. Anything above that
                // throws - the disk-space pre-flight, a nodetool stop, an IO error - skips this.
                onCompleted.run();
            }
            finally
            {
                activeCompactions.finishCompaction(ci);
            }
        }
        catch (Throwable t)
        {
            err = t;
            throw t;
        }
        finally
        {
            // Committed rewriters no-op on close; on the error path close() aborts them.
            err = Throwables.close(err, rewriters.values());
            if (err != null)
                Throwables.maybeFail(err);
        }
    }

    /**
     * Splits one partition too large to window-route on heap by reading it back from {@code spanning}
     * once per window ({@link WindowRoutingIterator#rereadSlices}), so memory stays bounded and every
     * output is still window-contained. Writing it unsplit instead - the flush path's degraded mode -
     * would give this task nothing to converge on: the next split would overflow the same way, and the
     * window would be parked for good, which is how a production snapshot table came to have 23 parked
     * windows (node 41, 2026-10-01).
     * <p>
     * The re-reads bypass the {@link CompactionIterator}, so this partition is written without
     * tombstone purging. That only keeps garbage one rewrite longer: the window's freeze, which runs
     * next, purges it.
     */
    private void splitByRereading(SSTableReader spanning,
                                  DecoratedKey key,
                                  WindowAdmission admission,
                                  Map<Long, SSTableRewriter> rewriters,
                                  ILifecycleTransaction shared,
                                  long maxDataAge,
                                  Set<SSTableReader> originals,
                                  long[] bytesRead)
    {
        logger.info("Partition {} of {}.{} is over the {}-byte window-routing buffer; splitting it by reading it " +
                    "again once per window", key, cfs.getKeyspaceName(), cfs.getTableName(),
                    WindowRoutingIterator.maxBufferedBytesPerPartition);
        ColumnFilter columns = ColumnFilter.all(cfs.metadata());
        WindowRoutingIterator.rereadSlices(slices -> spanning.rowIterator(key, slices, columns, false, SSTableReadsListener.NOOP_LISTENER),
                                           admission, tableResolution,
                                           bytes -> bytesRead[0] += bytes,
                                           (window, slice) -> rewriterFor(rewriters, window, admission, shared, maxDataAge, originals).append(slice));
    }

    /**
     * The disk-space pre-flight an ordinary {@link CompactionTask} runs before writing a byte (M6).
     * A split rewrites the whole input, so it needs roughly the input's size free. Deliberately a plain
     * {@link RuntimeException} and not an {@code FSWriteError}: the latter is what
     * {@code disk_failure_policy: stop} reacts to by taking the node out of the ring, and running out of
     * room for an optional maintenance rewrite is not a disk failure. The window simply stays FREEZING
     * and the split is retried on a later round.
     * <p>
     * Sized against {@link #writeDirectory} - the directory {@link #createWriter} will actually write
     * to - and not against the originals' directories: those can sit on a different JBOD disk, whose
     * free space says nothing about whether the output fits (M8). The whole output goes to one
     * directory, so the whole expected size is charged to it rather than spread over several.
     */
    private void checkDiskSpace(Set<SSTableReader> originals)
    {
        if (!cfs.isCompactionDiskSpaceCheckEnabled())
            return;

        long writeSize = cfs.getExpectedCompactedFileSize(originals, OperationType.COMPACTION);
        if (!cfs.getDirectories().hasDiskSpaceForCompactionsAndStreams(Map.of(writeDirectory, writeSize),
                                                                       CompactionManager.instance.active.estimatedRemainingWriteToDiskBytes()))
        {
            CompactionManager.instance.incrementAborted();
            throw new RuntimeException(String.format("Not enough space for split-refreeze (%s) of %s.%s, expected write size = %d",
                                                     transaction.opIdString(), cfs.getKeyspaceName(), cfs.name, writeSize));
        }
    }

    /**
     * Decides, for the duration of one partition, which window keys may have a rewriter of their own -
     * the same admission {@link TimeWindowSplittingMultiWriter} applies on the flush path, and for the
     * same reason: routing consults it for every timestamped element, so one partition cannot open an
     * unbounded number of rewriters however many windows it spans. The mapping is stable within a
     * partition (the admitted set only grows until the cap and never after), so routing still yields at
     * most one slice per rewriter.
     */
    private final class WindowAdmission implements LongUnaryOperator
    {
        private final NavigableSet<Long> admitted;

        WindowAdmission(Set<Long> alreadyOpen)
        {
            this.admitted = new TreeSet<>(alreadyOpen);
        }

        @Override
        public long applyAsLong(long millis)
        {
            return admit(windowStartOfMillis.applyAsLong(millis));
        }

        long admit(long window)
        {
            if (admitted.contains(window))
                return window;
            if (admitted.size() < TimeWindowSplittingMultiWriter.maxWindowWriters())
            {
                admitted.add(window);
                return window;
            }

            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, "split-refreeze-writer-cap", 1, TimeUnit.MINUTES,
                             "Split-refreeze of {}.{} reached the cap of {} concurrent window writers; further " +
                             "windows are folded onto open ones, so this pass leaves window-spanning sstables " +
                             "that later passes have to break up. window_size is very likely far too small for " +
                             "the write-timestamp spread of this data.",
                             cfs.getKeyspaceName(), cfs.getTableName(), TimeWindowSplittingMultiWriter.maxWindowWriters());

            Long floor = admitted.floor(window);
            return floor != null ? floor : admitted.first();
        }
    }

    /**
     * The window key is re-admitted rather than trusted, for the reason spelled out on
     * {@code TimeWindowSplittingMultiWriter.writerFor}: the degraded whole-partition slice that routing
     * emits on a buffer overflow is keyed off its buckets and can name a key routing never asked about.
     * Folding it here keeps {@code rewriters.keySet()} inside the admitted set - which is what bounds
     * the fan-out - and it is a lone slice, so it cannot collide with a sibling of the same partition.
     */
    private SSTableRewriter rewriterFor(Map<Long, SSTableRewriter> rewriters,
                                        long windowStart,
                                        WindowAdmission admission,
                                        ILifecycleTransaction shared,
                                        long maxDataAge,
                                        Set<SSTableReader> originals)
    {
        long target = admission.admit(windowStart);
        SSTableRewriter rewriter = rewriters.get(target);
        if (rewriter == null)
        {
            rewriter = SSTableRewriter.constructWithoutEarlyOpening(shared, false, maxDataAge);
            rewriter.switchWriter(createWriter(shared, originals));
            rewriters.put(target, rewriter);
        }
        return rewriter;
    }

    private SSTableWriter createWriter(ILifecycleTransaction shared, Set<SSTableReader> originals)
    {
        Descriptor descriptor = cfs.newSSTableDescriptor(writeDirectory);
        return descriptor.getFormat().getWriterFactory().builder(descriptor)
                         .setKeyCount(originals.iterator().next().estimatedKeys())
                         .setRepairedAt(CompactionTask.getMinRepairedAt(originals))
                         .setPendingRepair(CompactionTask.getPendingRepair(originals))
                         .setTransientSSTable(CompactionTask.getIsTransient(originals))
                         .setTableMetadataRef(cfs.metadata)
                         .setMetadataCollector(new MetadataCollector(cfs.metadata().comparator))
                         .setSerializationHeader(SerializationHeader.make(cfs.metadata(), originals))
                         .addDefaultComponents(cfs.indexManager.listIndexGroups())
                         .setSecondaryIndexGroups(cfs.indexManager.listIndexGroups())
                         .setCompressionDictionaryManager(cfs.compressionDictionaryManager())
                         .build(shared, cfs);
    }

    /** Same shape as anticompaction's SharedTxn: lets several rewriters share one transaction whose lifecycle the task drives. */
    private static final class SharedTxn extends WrappedLifecycleTransaction implements AutoCloseable
    {
        SharedTxn(ILifecycleTransaction delegate)
        {
            super(delegate);
        }

        @Override
        public Throwable commit(Throwable accumulate)
        {
            return accumulate;
        }

        @Override
        public void prepareToCommit()
        {
        }

        @Override
        public void checkpoint()
        {
        }

        @Override
        public void obsoleteOriginals()
        {
        }

        @Override
        public void close()
        {
        }
    }
}
