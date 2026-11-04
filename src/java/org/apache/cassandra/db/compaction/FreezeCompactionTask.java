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

import java.util.Collection;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.timeseries.WindowFrozenListeners;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.DefaultCompactionWriter;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.timeseries.RowBlockCodec;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.bti.TscolFormat;
import org.apache.cassandra.io.util.File;

/**
 * Freezes one closed time window of {@link TimeSeriesCompactionStrategy}: a major compaction of every sstable
 * in the window (of this strategy-instance slice) down to a single sstable, after which the window classifies
 * FROZEN. Runs a real {@link CompactionController} with the caller's gcBefore, so TTL/tombstone data in the
 * closed window is purged here - this is what structurally closes T1's "closed windows need retention to
 * reclaim TTL'd data" gap. On failure the standard compaction transaction rolls back and the window simply
 * classifies FREEZING again next round (design spec section 8).
 */
public class FreezeCompactionTask extends CompactionTask
{
    private final long windowStartMillis;
    private final Runnable onCompleted;
    private boolean columnar;

    public FreezeCompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, long gcBefore, long windowStartMillis)
    {
        this(cfs, txn, gcBefore, windowStartMillis, () -> {});
    }

    /**
     * @param onCompleted run once the freeze has durably committed, and never if it aborted or threw.
     *        {@link TimeSeriesCompactionStrategy} scores its no-progress guard from this, so a freeze
     *        refused by the disk-space pre-flight or stopped by {@code nodetool stop COMPACTION} must
     *        not reach it - a strike for work that never ran would park a healthy window, and freeing
     *        the disk would not un-park it.
     */
    public FreezeCompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, long gcBefore,
                                long windowStartMillis, Runnable onCompleted)
    {
        super(cfs, txn, gcBefore);
        this.windowStartMillis = windowStartMillis;
        this.onCompleted = onCompleted;
    }

    /**
     * Write the frozen window as a {@code tscol} (columnar) sstable when {@code frozen_format} asks for it and the
     * table's shape is one the columnar codec carries; otherwise the selected format, as before.
     */
    public FreezeCompactionTask withColumnarOutput(boolean requested)
    {
        this.columnar = writesColumnar(cfs, requested);
        return this;
    }

    static boolean writesColumnar(ColumnFamilyStore cfs, boolean requested)
    {
        return requested && RowBlockCodec.supports(cfs.metadata());
    }

    @VisibleForTesting
    boolean columnar()
    {
        return columnar;
    }

    @Override
    public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs, Directories directories,
                                                          ILifecycleTransaction transaction, Set<SSTableReader> nonExpiredSSTables)
    {
        if (!columnar)
            return super.getCompactionAwareWriter(cfs, directories, transaction, nonExpiredSSTables);
        return new DefaultCompactionWriter(cfs, directories, transaction, nonExpiredSSTables, keepOriginals, getLevel())
        {
            @Override
            protected Descriptor newDescriptor(File directory)
            {
                return cfs.newSSTableDescriptor(directory, TscolFormat.instance());
            }
        };
    }

    @Override
    protected boolean cursorCompactionAllowed()
    {
        return !columnar;
    }

    @VisibleForTesting
    Runnable onCompleted()
    {
        return onCompleted;
    }

    /**
     * A freeze must be deterministic: silently dropping the largest input on low disk (the CompactionTask
     * default) breaks the whole-window-to-one-sstable contract. Better to fail whole and retry next round.
     * <p>
     * This is expressed by refusing <em>partial</em> compactions rather than by overriding
     * {@code shouldReduceScopeForSpace()}, which reads as if it meant the same thing and does not:
     * {@link CompactionTask#runMayThrow} guards the whole disk-space pre-flight with it
     * ({@code shouldReduceScopeForSpace() && !buildCompactionCandidatesForAvailableDiskSpace(...)}), so
     * returning false there skips the check entirely. A freeze of a window larger than the free space
     * would then start writing, hit ENOSPC, throw {@code FSWriteError}, and under the shipped
     * {@code disk_failure_policy: stop} take the node out of the ring. With
     * {@code partialCompactionsAcceptable() == false} the pre-flight runs, {@code reduceScopeForLimitedSpace}
     * refuses to shrink the freeze, and the task aborts cleanly with "Not enough space for compaction"
     * before writing a byte - the same choice {@link LeveledCompactionTask} and {@link SSTableSplitter} make.
     */
    @Override
    protected boolean partialCompactionsAcceptable()
    {
        return false;
    }

    /**
     * Fires the listeners strictly post-commit: {@code super.finish} is the "point of no return" in
     * {@link CompactionTask#runMayThrow} - once it returns, the single output sstable is durably committed.
     * Zero outputs means the whole window was expired data and no longer exists - no event (there is nothing
     * to hand to a consumer).
     * <p>
     * More than one output is unlikely but <b>possible</b>: {@code DefaultCompactionWriter} only suppresses
     * {@code shouldSwitchWriterInCurrentLocation}, while {@code CompactionAwareWriter.maybeSwitchLocation}
     * still switches at JBOD disk boundaries, and an sstable is assigned to a per-disk strategy instance by
     * its <em>first</em> key, so a boundary or topology change can leave one instance holding sstables that
     * cross the new boundary. The window then still has two sstables and classifies FREEZING again; the
     * strategy's no-progress guard is what stops that becoming a permanent refreeze loop
     * ({@code TimeSeriesCompactionStrategy#recordCompletedRewrite}). No event fires for a split output,
     * because no single sstable represents the window.
     * <p>
     * The guard callback fires here for the same reason the listeners do, and unconditionally: a freeze
     * that emitted two sstables, or none, is still a freeze that ran, and "ran and changed nothing" is
     * exactly what the guard has to count.
     */
    @Override
    protected Collection<SSTableReader> finish(AbstractCompactionPipeline pipeline)
    {
        Collection<SSTableReader> newSStables = super.finish(pipeline);
        onCompleted.run();
        if (newSStables.size() == 1)
            WindowFrozenListeners.fire(cfs.metadata(), windowStartMillis, newSStables.iterator().next());
        else if (newSStables.isEmpty())
            logger.debug("Freeze of window {} in {} produced no sstable (window fully expired); no event fired",
                         windowStartMillis, cfs.getTableName());
        else
            logger.warn("Freeze of window {} in {} produced {} sstables instead of 1 (a JBOD disk-boundary " +
                        "writer switch); no event fired. The window stays FREEZING; nodetool relocatesstables " +
                        "will realign it. Until then the strategy's no-progress guard stops it refreezing forever.",
                        windowStartMillis, cfs.getTableName(), newSStables.size());
        return newSStables;
    }
}
