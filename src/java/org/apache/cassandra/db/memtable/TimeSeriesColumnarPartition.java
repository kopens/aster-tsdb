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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;

import org.github.jamm.Unmetered;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.ArrayCell;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.WrappingUnfilteredRowIterator;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.BulkIterator;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.Cloner;
import org.apache.cassandra.utils.memory.EnsureOnHeap;
import org.apache.cassandra.utils.memory.HeapCloner;
import org.apache.cassandra.utils.memory.MemtableAllocator;

/**
 * One window shard's version of one partition, stored column-wise in primitive arrays instead of as
 * a tree of {@code Row} and {@code Cell} objects.
 *
 * <p><b>Why.</b> In the object representation every ingested row costs a {@code BTreeRow}, a
 * {@code Cell} per column and a {@code ByteBuffer} per value; for a typical industrial tag table
 * (a timestamp clustering plus a handful of numeric columns) that overhead is several times the
 * data itself. Here the common case — every cell of the row written at one timestamp, which is what
 * a plain {@code INSERT} produces — is stored as one slot across a set of parallel arrays: the
 * clustering as a {@code long} where the clustering is a single timestamp/bigint column, one write
 * timestamp, one TTL and one local-deletion-time for the whole row, and each column's value in a
 * primitive array of its fixed width. Objects are built only when the partition is read or flushed.
 *
 * <p><b>Two tiers.</b> Anything the arrays cannot represent falls back to plain {@code Row} objects
 * in a per-partition overflow map, <em>one row at a time</em> — never the whole partition:
 * <ul>
 *   <li>a later write that touches an existing row at a different timestamp promotes that row (the
 *       merged result no longer has one uniform timestamp);</li>
 *   <li>rows with cell tombstones, row deletions, expired liveness or non-uniform TTLs;</li>
 *   <li>rows whose clustering does not fit the primitive clustering store.</li>
 * </ul>
 * Static rows, partition deletions and range tombstones keep their existing object representation in
 * dedicated fields, exactly as the reference memtables hold them, and are merged back in on read.
 * The fallback is total: an unrepresentable shape is a missed optimisation, never a refusal.
 *
 * <p><b>Ordering.</b> Rows are appended; a sorted prefix and an unsorted tail are tracked and the
 * tail is folded in lazily, on read or flush — in-order ingest never sorts. The tail is bounded
 * ({@link #MAX_UNSORTED_TAIL}) only so the duplicate-clustering probe a write performs stays cheap.
 *
 * <p><b>Concurrency: the slots are append-only.</b> Writes hold {@link #writeLock} for their whole
 * duration; a read takes it only for the instant it captures a walk, and holds nothing while it
 * iterates.
 * A slot's row data is immutable once written: a re-write of a clustering the arrays already hold
 * appends a <em>new</em> slot with the merged row and marks the old slot {@link #FLAG_SUPERSEDED} —
 * a one-shot 0&rarr;1 flag that is never cleared — and a promotion to the overflow tier inserts into
 * the concurrent overflow map and then supersedes the slot the same way. Consolidation and growth
 * replace the arrays wholesale instead of mutating them, so a reader that captured the array
 * references and the row count under the lock (see {@link #captureWalk()}) walks a frozen prefix
 * that no later write can tear. Reads and flushes therefore <b>retain nothing</b>: there is no
 * snapshot and no cache of any kind; the streaming iterator ({@link TimeSeriesStreamingIterator})
 * assembles one row per pull and leaves nothing behind when it closes — which is how the hot read
 * path and the flush view ({@link #flushView()}) both read the arrays — and the full rebuild
 * ({@link #buildSnapshot()}), kept as the fail-open fallback and for the shapes streaming does not
 * serve, builds, is consumed, and is discarded.
 *
 * <p><b>The degradation valve.</b> Superseded slots are dead weight until flush. A pathologically
 * re-written partition would grow its arrays without bound, so when more than half of a partition's
 * slots are superseded (and the arrays are past their initial capacity — a handful of slots cannot
 * bloat), its next write demotes <em>that partition only</em> to the object tier: the arrays
 * freeze, an {@link TimeSeriesMemtable.ObjectShardPartition} sibling takes every later write, and
 * reads and flushes merge the two — demotion in the same spirit as fail-open, never refusal.
 */
public final class TimeSeriesColumnarPartition implements TimeSeriesMemtable.ShardPartition
{
    private static final int INITIAL_CAPACITY = 8;

    /**
     * Ceiling on the unsorted tail. A write below the current maximum clustering must check whether
     * that clustering already exists (binary search of the sorted prefix plus a linear scan of the
     * tail), so an unbounded tail would make heavily out-of-order ingest quadratic. When the tail
     * exceeds this, it is folded into the prefix during the write — which is the one case the
     * "sort lazily" rule allows sorting on the write path, and amortises to O(size / limit) per row.
     */
    private static final int MAX_UNSORTED_TAIL = 1024;

    private static final byte FLAG_HAS_LIVENESS = 1;
    /**
     * The slot's row is no longer the truth: it was either replaced by a later slot holding the
     * merged row (append-only re-write) or promoted to the overflow map. One-shot — set exactly once,
     * after its replacement is in place, and never cleared — so a lock-free reader sees each slot as
     * either live or superseded, never torn. The slot keeps its clustering for search order.
     */
    private static final byte FLAG_SUPERSEDED = 2;

    /**
     * Rows assembled from array slots into {@code BTreeRow}s, across every columnar partition. The
     * streaming read path's cost model is O(log n + rows returned); this is the counter that pins it
     * — a sliced read must move it by about the slice's row count, never by the partition's.
     */
    @VisibleForTesting
    public static final LongAdder rowsAssembled = new LongAdder();

    /**
     * Write-lock acquisitions that found the lock free, acquisitions that had to wait, and the total
     * nanoseconds the waiting ones spent blocked — across every columnar partition of every table in
     * the process, like {@link #rowsAssembled} above.
     *
     * <p><b>Why they exist.</b> After 6d5f2a3156 took ingest on this memtable from 93.6k to 155.6k
     * rows/s, nothing outside the process could say whether what remains is CPU in the write path or
     * threads queueing on one hot partition — the object monitor these replaced cannot be asked. The
     * ratio {@code contendedWrites / (contendedWrites + uncontendedWrites)} answers the first
     * question and {@code writeContentionNanos} sizes it; a low ratio means further work belongs in
     * the per-row cost, a high one means it belongs in the locking granularity.
     *
     * <p>Process-wide {@link LongAdder}s rather than a metrics MBean: this is a profiling counter for
     * a specific optimisation question, the class already publishes {@link #rowsAssembled} exactly
     * this way, and a per-table {@code MetricsView} would need a lifecycle (creation, release on drop)
     * that a static counter does not. Tests sample deltas, never absolutes.
     */
    @VisibleForTesting
    public static final LongAdder uncontendedWrites = new LongAdder();
    @VisibleForTesting
    public static final LongAdder contendedWrites = new LongAdder();
    @VisibleForTesting
    public static final LongAdder writeContentionNanos = new LongAdder();

    /**
     * The one lock. <b>Every</b> mutation of the arrays and every probe that must not see them
     * half-written takes it — the write path, {@link #captureWalk()}, {@link #readView()},
     * {@link #mayContainRowsIn} and the observability accessors — because the append-only slot
     * protocol is only safe if a reader either holds this lock or reads a capture made under it.
     * Two locks would be no lock at all: the walk a reader captured could then be torn by a
     * concurrent {@link #consolidate()} replacing the arrays underneath it.
     *
     * <p><b>Why not the object monitor it replaces.</b> A monitor cannot report contention, which is
     * the question above. {@link ReentrantLock} answers it and gives back everything the monitor
     * gave: the same happens-before on acquire/release, and reentrancy — which this class relies on
     * ({@link #put} holds it across {@link #apply} → {@link #replaceSlot} → {@link #materializeRow}).
     * Nothing in this class ever used {@code wait}/{@code notify}, and nothing outside it ever
     * synchronised on a partition instance, so the monitor had no other users to strand.
     *
     * <p>{@code @Unmetered} as {@code TrieMemtable}'s write lock is, and for a second reason here:
     * a held lock's AQS keeps an {@code exclusiveOwnerThread} reference, and TimeSeriesMemtableHeapTest
     * walks this object graph with jamm — a deep measurement that stepped into a live {@code Thread}
     * would measure the runtime, not the memtable.
     */
    @Unmetered
    @VisibleForTesting
    final ReentrantLock writeLock = new ReentrantLock();

    @Unmetered
    private final TableMetadataRef metadata;
    @Unmetered
    private final MemtableAllocator allocator;
    private final DecoratedKey key;

    /** Whether the clustering is a single timestamp/bigint column, stored as a bare {@code long}. */
    private final boolean longClusterings;

    /**
     * Whether that column is DESC ({@code ReversedType}-wrapped). {@code ReversedType} wraps
     * comparison only — the serialized form stays the base type's 8 bytes — so the raw value is
     * stored unchanged and every long comparison is negated instead. Storing a transformed key
     * would demand an inverse at every materialization site, where a miss corrupts silently.
     */
    private final boolean reversedLongKeys;

    // ---- row-major parallel arrays; all mutation is guarded by writeLock ----
    private int capacity;
    private int size;
    /** Rows [0, sortedCount) are in clustering order; rows [sortedCount, size) arrived out of order. */
    private int sortedCount;
    private long[] clusteringKeys;
    private Object[] clusteringObjects;
    private long[] timestamps;
    private long[] localDeletionTimes;
    private int[] ttls;
    private byte[] flags;
    /** Per-column stores, kept sorted by column so a materialized row's cells are already in order. */
    private final List<ColumnStore> stores = new ArrayList<>(4);

    /** Slots carrying {@link #FLAG_SUPERSEDED}; feeds the degradation valve. Guarded by {@link #writeLock}. */
    private int supersededCount;

    /** Appends admitted by the certainly-new bounds shortcut, i.e. without a slot search. Guarded by {@link #writeLock}. */
    private long certainNewAppends;

    /**
     * Index of the unsorted tail for long-keyed clusterings: clustering key → the newest slot
     * holding it, so the duplicate probe a write performs is one map lookup instead of a linear
     * scan of up to {@link #MAX_UNSORTED_TAIL} slots inside the lock. Writer-only state, guarded
     * by {@link #writeLock} like every array mutation; readers never see it. Cleared whenever
     * {@link #consolidate()} folds the tail into the sorted prefix (slot indices change there).
     * Bounded by the tail bound, so at most ~{@value #MAX_UNSORTED_TAIL} entries.
     */
    private HashMap<Long, Integer> tailIndex;

    /** Greatest clustering held by the arrays, so in-order ingest can append without searching. */
    private long maxClusteringKey;
    private Object maxClusteringObject;

    /** Least clustering held by the arrays, maintained on append for {@link #mayContainRowsIn}. */
    private long minClusteringKey;
    private Object minClusteringObject;

    // ---- object-tier structures ----
    /**
     * Concurrent so the streaming read path can merge it without ever taking the lock; volatile so a
     * reader that captured a {@code null} before the map existed simply misses entries added after
     * its capture, which is the same visibility rule the arrays follow.
     */
    private volatile ConcurrentSkipListMap<Clustering<?>, Row> overflow;
    private Row staticRow = Rows.EMPTY_STATIC_ROW;
    private DeletionInfo deletionInfo = DeletionInfo.LIVE;
    private RegularAndStaticColumns columns = RegularAndStaticColumns.NONE;

    /**
     * Conservative stats for the streaming iterator, merged from every applied update. Only ever
     * widens, which {@link EncodingStats}'s contract allows: stats tune the serialization deltas and
     * a loose bound costs bytes, never correctness. The rebuilt views collect exact stats instead.
     */
    private volatile EncodingStats cumulativeStats = EncodingStats.NO_STATS;

    /**
     * The degradation valve's outcome: once more than half the slots are superseded, every later
     * write lands here and reads merge this partition's frozen state with it. Set exactly once,
     * under the lock; never cleared.
     */
    private volatile TimeSeriesMemtable.ObjectShardPartition demotedTo;

    TimeSeriesColumnarPartition(TableMetadataRef metadata, DecoratedKey key, MemtableAllocator allocator)
    {
        this.metadata = metadata;
        this.key = key;
        this.allocator = allocator;
        ClusteringComparator comparator = metadata.get().comparator;
        AbstractType<?> subtype = comparator.size() == 1 ? comparator.subtype(0) : null;
        AbstractType<?> base = subtype == null ? null : subtype.unwrap();
        this.longClusterings = base == TimestampType.instance || base == LongType.instance;
        this.reversedLongKeys = longClusterings && subtype.isReversed();
    }

    /**
     * The fixed serialized width this column can be stored at, or 0 for the object fallback. Identity
     * comparison deliberately excludes {@code ReversedType} wrappers and anything not on this list —
     * an unknown type must land in the fallback, never fail.
     */
    private static int fixedWidthOf(AbstractType<?> type)
    {
        if (type == DoubleType.instance || type == LongType.instance || type == TimestampType.instance)
            return 8;
        if (type == FloatType.instance || type == Int32Type.instance || type == SimpleDateType.instance)
            return 4;
        if (type == BooleanType.instance)
            return 1;
        return 0;
    }

    // ------------------------------------------------------------------------------------- writes

    @Override
    public long put(PartitionUpdate update, UpdateTransaction indexer, Cloner cloner, OpOrder.Group opGroup, AtomicLong liveDataSize)
    {
        TimeSeriesMemtable.ObjectShardPartition demoted = demotedTo;
        if (demoted == null)
        {
            Put put = new Put(cloner, indexer);
            lockForWrite();
            try
            {
                demoted = demotedTo;
                if (demoted == null && size >= INITIAL_CAPACITY && supersededCount * 2 > size)
                {
                    // The degradation valve: over half the slots are dead weight, so this partition's
                    // re-write pattern defeats the arrays. Freeze them and give every later write to
                    // an object-tier sibling — including this one, applied below outside the lock.
                    // The floor exists because the valve is about array BLOAT: a partition of a few
                    // slots cannot bloat, and demoting it on its first promotion (1 of 1 slots
                    // superseded is over half) would throw healthy partitions to the object tier.
                    demoted = new TimeSeriesMemtable.ObjectShardPartition(metadata, key, allocator);
                    demotedTo = demoted;
                }
                if (demoted == null)
                {
                    cumulativeStats = cumulativeStats.mergeWith(update.stats());
                    mergeDeletionInfo(update.deletionInfo(), put);

                    RegularAndStaticColumns merged = update.columns().mergeTo(columns);
                    put.heapDelta += merged.unsharedHeapSize() - columns.unsharedHeapSize();
                    columns = merged;

                    Row updateStatic = update.staticRow();
                    if (!updateStatic.isEmpty())
                        mergeStatic(updateStatic, put);

                    for (Row row : update)
                    {
                        if (!row.isEmpty())
                            apply(row, put);
                    }
                }
            }
            finally
            {
                writeLock.unlock();
            }
            if (demoted == null)
            {
                // Outside the lock: acquiring memtable space may block on a flush, and the flush path
                // takes this partition's lock to build its view.
                allocator.onHeap().adjust(put.heapDelta, opGroup);
                liveDataSize.addAndGet(put.dataDelta);
                return put.colUpdateTimeDelta;
            }
        }
        // Demoted: the object tier owns every write from here on. Also outside the lock, for the
        // same allocation-under-lock reason as above.
        return demoted.put(update, indexer, cloner, opGroup, liveDataSize);
    }

    /**
     * Takes {@link #writeLock} and records whether it was free. {@code tryLock()} first and only then
     * the blocking acquire, with the wait timed — {@code TrieMemtable.MemtableShard.put}'s pattern
     * verbatim, so the two memtables' contention numbers mean the same thing and can be compared.
     * The uncontended path is one CAS and one increment; nothing here reads the clock unless a
     * thread actually blocked, because {@code nanoTime} on the hot path would be part of the cost it
     * is meant to measure.
     *
     * <p>Only the write path is instrumented. A read-side probe holding the lock still shows up here
     * — as blocked nanos on the writer it delayed — which is the number that matters; counting the
     * probes themselves would mix a flush's one acquisition per partition into the write ratio.
     */
    private void lockForWrite()
    {
        // Reentrant, so this also succeeds (and counts as uncontended) if this thread already holds
        // the lock — the same thing the monitor did for a nested acquisition.
        if (writeLock.tryLock())
        {
            uncontendedWrites.increment();
            return;
        }
        contendedWrites.increment();
        long lockStartTime = Clock.Global.nanoTime();
        writeLock.lock();
        writeContentionNanos.add(Clock.Global.nanoTime() - lockStartTime);
    }

    /** Mirrors {@code BTreePartitionUpdater#merge(DeletionInfo, DeletionInfo)}, including the indexer events. */
    private void mergeDeletionInfo(DeletionInfo update, Put put)
    {
        if (update.isLive() || !update.mayModify(deletionInfo))
            return;

        if (!update.getPartitionDeletion().isLive())
            put.indexer.onPartitionDeletion(update.getPartitionDeletion());
        if (update.hasRanges())
            update.rangeIterator(false).forEachRemaining(put.indexer::onRangeTombstone);

        DeletionInfo merged = deletionInfo.mutableCopy().add(update.clone(HeapCloner.instance));
        put.heapDelta += merged.unsharedHeapSize() - deletionInfo.unsharedHeapSize();
        deletionInfo = merged;
    }

    private void mergeStatic(Row update, Put put)
    {
        if (staticRow.isEmpty())
        {
            Row cloned = update.clone(put.cloner);
            put.indexer.onInserted(update);
            put.heapDelta += cloned.unsharedHeapSizeExcludingData();
            put.dataDelta += cloned.dataSize();
            staticRow = cloned;
        }
        else
        {
            Row merged = Rows.merge(staticRow, update, put);
            put.indexer.onUpdated(staticRow, merged);
            Row cloned = merged.clone(put.cloner);
            put.heapDelta += cloned.unsharedHeapSizeExcludingData() - staticRow.unsharedHeapSizeExcludingData();
            put.dataDelta += cloned.dataSize() - staticRow.dataSize();
            staticRow = cloned;
        }
    }

    private void apply(Row row, Put put)
    {
        Clustering<?> clustering = row.clustering();

        // A clustering that was promoted (or never fit the arrays) lives in the overflow map, and the
        // overflow owns it exclusively, so this lookup must come before any array search or append.
        Row inOverflow = overflow == null ? null : overflow.get(clustering);
        if (inOverflow != null)
        {
            Row merged = Rows.merge(inOverflow, row, put);
            put.indexer.onUpdated(inOverflow, merged);
            Row cloned = merged.clone(put.cloner);
            put.dataDelta += cloned.dataSize() - inOverflow.dataSize();
            putOverflow(cloned, inOverflow, put);
            return;
        }

        RowShape shape = classify(row);

        long longKey = 0;
        boolean representable = !longClusterings;
        if (longClusterings)
        {
            ByteBuffer raw = clustering.bufferAt(0);
            representable = raw != null && raw.remaining() == 8;
            if (representable)
                longKey = raw.getLong(raw.position());
        }
        boolean arrayable = shape != null && representable;

        // In-order fast path: strictly outside everything the arrays hold — above max or below min,
        // both bounds cover superseded slots too — and nothing in overflow that could collide, means
        // the clustering is certainly new: append without any lookup. The min side is what ascending
        // ingest into a DESC table (the production tm_tag_point pattern) takes on every row; without
        // it each such row pays a findSlot scan of the unsorted tail inside the partition lock.
        boolean overflowEmpty = overflow == null || overflow.isEmpty();
        if (arrayable && overflowEmpty
            && (size == 0 || compareToMax(clustering, longKey) > 0 || compareToMin(clustering, longKey) < 0))
        {
            certainNewAppends++;
            append(row, shape, longKey, null, put);
            put.indexer.onInserted(row);
            // shape.dataSize, not row.dataSize(): the same number, already summed by classify()
            // above out of a walk this path was making anyway.
            put.dataDelta += shape.dataSize;
            return;
        }

        int slot = representable ? findSlot(clustering, longKey) : -1;
        if (slot >= 0)
        {
            replaceSlot(slot, row, put);
            return;
        }

        if (arrayable)
        {
            append(row, shape, longKey, null, put);
            put.indexer.onInserted(row);
            put.dataDelta += shape.dataSize;
        }
        else
        {
            Row cloned = row.clone(put.cloner);
            put.indexer.onInserted(row);
            put.dataDelta += cloned.dataSize();
            putOverflow(cloned, null, put);
        }
    }

    /**
     * The row's fast-path parameters, or {@code null} if the row needs the object tier: a row
     * deletion, a cell tombstone, an expired liveness, a cell whose value does not fit its column's
     * fixed width, a complex column, or cells that do not all share one (timestamp, ttl,
     * localDeletionTime).
     *
     * <p>The row's {@code dataSize} rides along in the answer. On the columnar fast path a row was
     * being walked four times per write — {@code TimeSeriesMemtable.maxTimestamp} to route it, here,
     * {@link #append} to store it, and then {@code row.dataSize()} purely to account for it — and
     * this pass is already over exactly the cells that last walk visits, so the sum is free here and
     * one whole walk is not.
     *
     * <p><b>The accounting must not drift:</b> {@code liveDataSize} feeds the flush size estimate.
     * The sum below reproduces {@code BTreeRow.dataSize()}'s decomposition exactly — clustering plus
     * primary-key liveness plus <em>deletion, which is {@code time.dataSize() + 1} even when LIVE</em>,
     * plus every {@code ColumnData} — which is safe because it is computed from the same four parts
     * of the same row. The {@code instanceof} is the guard for the one thing that could break it: a
     * {@code Row} implementation whose {@code dataSize()} is not that decomposition. Today there are
     * only two in the tree ({@code BTreeRow}, and SAI's {@code RowWithSource}, a pure delegating
     * wrapper built only on the read path), and everything reaching a memtable write is a
     * {@code BTreeRow} — but a memtable is not the place to bet on that staying true, and the
     * fallback costs exactly what today costs.
     */
    private RowShape classify(Row row)
    {
        if (!row.deletion().isLive())
            return null;

        long ts = 0;
        int ttl = 0;
        long ldt = 0;
        boolean seen = false;
        boolean hasLiveness = false;
        LivenessInfo liveness = row.primaryKeyLivenessInfo();

        boolean fusedSize = row instanceof BTreeRow;
        long dataSize = fusedSize
                        ? row.clustering().dataSize() + liveness.dataSize() + row.deletion().dataSize()
                        : 0;

        if (!liveness.isEmpty())
        {
            if (liveness.ttl() == LivenessInfo.EXPIRED_LIVENESS_TTL)
                return null;
            hasLiveness = true;
            seen = true;
            ts = liveness.timestamp();
            ttl = liveness.ttl();
            ldt = liveness.localExpirationTime();
        }

        for (ColumnData data : row)
        {
            if (!data.column().isSimple())
                return null;
            Cell<?> cell = (Cell<?>) data;
            if (cell.isTombstone())
                return null;

            dataSize += cell.dataSize();

            int width = fixedWidthOf(cell.column().type);
            if (width > 0)
            {
                ByteBuffer value = cell.buffer();
                if (value == null || value.remaining() != width)
                    return null;
            }

            if (!seen)
            {
                seen = true;
                ts = cell.timestamp();
                ttl = cell.ttl();
                ldt = cell.localDeletionTime();
            }
            else if (cell.timestamp() != ts || cell.ttl() != ttl || cell.localDeletionTime() != ldt)
            {
                return null;
            }
        }

        if (!seen)
            return null;
        return new RowShape(ts, ttl, ldt, hasLiveness, fusedSize ? dataSize : row.dataSize());
    }

    /**
     * Appends {@code row} as a new slot. {@code reusedClustering} is non-null only for an append-only
     * re-write, whose clustering is already stored (and owned) by the superseded slot — sharing the
     * reference avoids a clone and, on a native allocator, avoids touching the value factory at all.
     */
    private void append(Row row, RowShape shape, long longKey, Clustering<?> reusedClustering, Put put)
    {
        ensureCapacity(put);
        int slot = size;

        Clustering<?> storedClustering = null;
        if (longClusterings)
        {
            clusteringKeys[slot] = longKey;
        }
        else if (reusedClustering != null)
        {
            storedClustering = reusedClustering;
            clusteringObjects[slot] = storedClustering;
        }
        else
        {
            storedClustering = put.cloner.clone(row.clustering());
            clusteringObjects[slot] = storedClustering;
            put.heapDelta += storedClustering.unsharedHeapSize();
        }
        timestamps[slot] = shape.timestamp;
        localDeletionTimes[slot] = shape.localDeletionTime;
        ttls[slot] = shape.ttl;
        flags[slot] = shape.hasLiveness ? FLAG_HAS_LIVENESS : 0;

        for (ColumnData data : row)
        {
            Cell<?> cell = (Cell<?>) data;
            storeFor(cell.column(), put).set(slot, cell, put);
        }

        // Non-strict comparison: an append-only re-write legitimately appends a clustering equal to
        // the previous slot's, and a non-decreasing prefix keeps every equal-clustering run in append
        // order — which is what lets a reader treat the run's last member as its newest.
        boolean inOrder = sortedCount == slot
                          && (slot == 0 || compareSlotTo(slot - 1, row.clustering(), longKey) <= 0);
        size++;
        if (inOrder)
        {
            sortedCount = size;
        }
        else
        {
            // Index the out-of-order slot so findSlot never scans the tail: newest slot wins the
            // entry (a re-append overwrites it just before the older slot is superseded), and the
            // whole index dies with the tail in consolidate().
            if (longClusterings)
            {
                if (tailIndex == null)
                    tailIndex = new HashMap<>();
                tailIndex.put(longKey, slot);
            }
            if (size - sortedCount > MAX_UNSORTED_TAIL)
                consolidate();
        }

        if (size == 1 || compareToMax(row.clustering(), longKey) > 0)
        {
            maxClusteringKey = longKey;
            maxClusteringObject = storedClustering;
        }
        if (size == 1 || compareToMin(row.clustering(), longKey) < 0)
        {
            minClusteringKey = longKey;
            minClusteringObject = storedClustering;
        }
    }

    /**
     * A write to a clustering the arrays already hold: materialize, reconcile through the standard
     * {@link Rows#merge} machinery, then replace <b>append-only</b> — the merged row goes to a new
     * slot (still one uniform timestamp) or to the overflow map (the design's cell slow path), and
     * only then is the old slot marked superseded. Slot data is never modified in place, which is
     * what lets readers walk the arrays without a lock.
     */
    private void replaceSlot(int slot, Row update, Put put)
    {
        Row existing = materializeRow(slot);
        Row merged = Rows.merge(existing, update, put);
        put.indexer.onUpdated(existing, merged);
        // A delta, not a size: the slot's old row is being replaced, so what leaves must be subtracted
        // and classify()'s fused size (below, and only when the merged row still fits the arrays)
        // cannot express it. This is the re-write path, not the ingest path it was fused for.
        put.dataDelta += merged.dataSize() - existing.dataSize();

        RowShape shape = classify(merged);
        if (shape != null)
        {
            long longKey = longClusterings ? clusteringKeys[slot] : 0L;
            Clustering<?> reused = longClusterings ? null : (Clustering<?>) clusteringObjects[slot];
            append(merged, shape, longKey, reused, put);
        }
        else
        {
            Row cloned = merged.clone(put.cloner);
            putOverflow(cloned, null, put);
        }
        // Strictly after the replacement is in place, so no reader can ever observe the flag set
        // while the truth is nowhere.
        setSuperseded(slot);
    }

    /** One-shot: 0&rarr;1 only, never cleared. */
    private void setSuperseded(int slot)
    {
        if ((flags[slot] & FLAG_SUPERSEDED) == 0)
        {
            flags[slot] |= FLAG_SUPERSEDED;
            supersededCount++;
        }
    }

    private void putOverflow(Row cloned, Row replaced, Put put)
    {
        if (overflow == null)
            overflow = new ConcurrentSkipListMap<>(metadata.get().comparator);
        overflow.put(cloned.clustering(), cloned);
        put.heapDelta += cloned.unsharedHeapSizeExcludingData()
                         - (replaced == null ? 0 : replaced.unsharedHeapSizeExcludingData());
    }

    private ColumnStore storeFor(ColumnMetadata column, Put put)
    {
        int lo = 0;
        int hi = stores.size() - 1;
        while (lo <= hi)
        {
            int mid = (lo + hi) >>> 1;
            int cmp = stores.get(mid).column.compareTo(column);
            if (cmp < 0)
                lo = mid + 1;
            else if (cmp > 0)
                hi = mid - 1;
            else
                return stores.get(mid);
        }
        ColumnStore created = new ColumnStore(column, Math.max(capacity, INITIAL_CAPACITY));
        stores.add(lo, created);
        put.heapDelta += created.heapSize();
        return created;
    }

    private void ensureCapacity(Put put)
    {
        if (size < capacity)
            return;

        int newCapacity = Math.max(INITIAL_CAPACITY, capacity * 2);
        if (longClusterings)
            clusteringKeys = clusteringKeys == null ? new long[newCapacity] : Arrays.copyOf(clusteringKeys, newCapacity);
        else
            clusteringObjects = clusteringObjects == null ? new Object[newCapacity] : Arrays.copyOf(clusteringObjects, newCapacity);
        timestamps = timestamps == null ? new long[newCapacity] : Arrays.copyOf(timestamps, newCapacity);
        localDeletionTimes = localDeletionTimes == null ? new long[newCapacity] : Arrays.copyOf(localDeletionTimes, newCapacity);
        ttls = ttls == null ? new int[newCapacity] : Arrays.copyOf(ttls, newCapacity);
        flags = flags == null ? new byte[newCapacity] : Arrays.copyOf(flags, newCapacity);
        for (ColumnStore store : stores)
            store.grow(newCapacity);

        // 8 (clustering or reference) + 8 (timestamp) + 8 (ldt) + 4 (ttl) + 1 (flags) per new slot,
        // plus what each store reports for itself; array headers are noise at these growth steps.
        put.heapDelta += (newCapacity - capacity) * 29L;
        for (ColumnStore store : stores)
            put.heapDelta += (newCapacity - capacity) * (long) (store.width == 0 ? 9 : store.width + 1);
        capacity = newCapacity;
    }

    // ------------------------------------------------------------------------------------ ordering

    /** {@code Long.compare} in comparator order: negated when the clustering column is DESC. */
    private int compareKeys(long a, long b)
    {
        int cmp = Long.compare(a, b);
        return reversedLongKeys ? -cmp : cmp;
    }

    private int compareToMax(Clustering<?> clustering, long longKey)
    {
        return longClusterings
               ? compareKeys(longKey, maxClusteringKey)
               : metadata.get().comparator.compare(clustering, (Clustering<?>) maxClusteringObject);
    }

    private int compareToMin(Clustering<?> clustering, long longKey)
    {
        return longClusterings
               ? compareKeys(longKey, minClusteringKey)
               : metadata.get().comparator.compare(clustering, (Clustering<?>) minClusteringObject);
    }

    private int compareSlotTo(int slot, Clustering<?> clustering, long longKey)
    {
        return longClusterings
               ? compareKeys(clusteringKeys[slot], longKey)
               : metadata.get().comparator.compare((Clustering<?>) clusteringObjects[slot], clustering);
    }

    private int compareSlots(int a, int b)
    {
        return longClusterings
               ? compareKeys(clusteringKeys[a], clusteringKeys[b])
               : metadata.get().comparator.compare((Clustering<?>) clusteringObjects[a], (Clustering<?>) clusteringObjects[b]);
    }

    /**
     * @return the <b>live</b> slot holding {@code clustering}, or -1. Since re-writes append, a
     * clustering can occupy a run of slots of which at most one is live — the search must never hand
     * a superseded slot back to the write path, or a merge would resurrect stale data.
     */
    private int findSlot(Clustering<?> clustering, long longKey)
    {
        // Long-keyed tails are fully indexed: a live hit is the answer (at most one slot per
        // clustering is ever live), and a superseded or missing entry falls through to the sorted
        // prefix — the tail cannot hold a live slot the index does not know about, so the linear
        // tail scan below is skipped entirely for this path.
        if (longClusterings && tailIndex != null)
        {
            Integer tailSlot = tailIndex.get(longKey);
            if (tailSlot != null && (flags[tailSlot] & FLAG_SUPERSEDED) == 0)
                return tailSlot;
        }

        int lo = 0;
        int hi = sortedCount - 1;
        int hit = -1;
        while (lo <= hi)
        {
            int mid = (lo + hi) >>> 1;
            int cmp = compareSlotTo(mid, clustering, longKey);
            if (cmp < 0)
                lo = mid + 1;
            else if (cmp > 0)
                hi = mid - 1;
            else
            {
                hit = mid;
                break;
            }
        }
        if (hit >= 0)
        {
            for (int i = hit; i >= 0 && compareSlotTo(i, clustering, longKey) == 0; i--)
            {
                if ((flags[i] & FLAG_SUPERSEDED) == 0)
                    return i;
            }
            for (int i = hit + 1; i < sortedCount && compareSlotTo(i, clustering, longKey) == 0; i++)
            {
                if ((flags[i] & FLAG_SUPERSEDED) == 0)
                    return i;
            }
        }
        if (!longClusterings)
        {
            for (int i = sortedCount; i < size; i++)
            {
                if (compareSlotTo(i, clustering, longKey) == 0 && (flags[i] & FLAG_SUPERSEDED) == 0)
                    return i;
            }
        }
        return -1;
    }

    /**
     * Folds the unsorted tail into the sorted prefix. Never called by in-order ingest. Builds new
     * arrays rather than permuting in place: a reader that captured the old arrays keeps walking an
     * unchanging view.
     */
    private void consolidate()
    {
        if (sortedCount == size)
            return;

        int tailLength = size - sortedCount;
        Integer[] tail = new Integer[tailLength];
        for (int i = 0; i < tailLength; i++)
            tail[i] = sortedCount + i;
        // The sort is stable and the tie below prefers the prefix, so an equal-clustering run stays
        // in append order — oldest first, newest (the live or last-superseded one) last.
        Arrays.sort(tail, this::compareSlots);

        int[] permutation = new int[size];
        int prefix = 0;
        int t = 0;
        int out = 0;
        while (prefix < sortedCount && t < tailLength)
            permutation[out++] = compareSlots(prefix, tail[t]) <= 0 ? prefix++ : tail[t++];
        while (prefix < sortedCount)
            permutation[out++] = prefix++;
        while (t < tailLength)
            permutation[out++] = tail[t++];

        if (longClusterings)
        {
            long[] next = new long[capacity];
            for (int i = 0; i < size; i++)
                next[i] = clusteringKeys[permutation[i]];
            clusteringKeys = next;
        }
        else
        {
            Object[] next = new Object[capacity];
            for (int i = 0; i < size; i++)
                next[i] = clusteringObjects[permutation[i]];
            clusteringObjects = next;
        }
        long[] nextTimestamps = new long[capacity];
        long[] nextLdts = new long[capacity];
        int[] nextTtls = new int[capacity];
        byte[] nextFlags = new byte[capacity];
        for (int i = 0; i < size; i++)
        {
            nextTimestamps[i] = timestamps[permutation[i]];
            nextLdts[i] = localDeletionTimes[permutation[i]];
            nextTtls[i] = ttls[permutation[i]];
            nextFlags[i] = flags[permutation[i]];
        }
        timestamps = nextTimestamps;
        localDeletionTimes = nextLdts;
        ttls = nextTtls;
        flags = nextFlags;
        for (ColumnStore store : stores)
            store.applyPermutation(permutation, size, capacity);

        sortedCount = size;
        // Every former tail slot now lives in the sorted prefix under a new index; the map's slot
        // numbers are stale and the prefix is the binary search's job, so the index empties here.
        if (tailIndex != null)
            tailIndex.clear();
    }

    // ------------------------------------------------------------------------------------- reads

    /** Rebuilds one slot as a real {@code BTreeRow}. Caller must hold the lock. */
    private Row materializeRow(int slot)
    {
        return currentWalk().assembleRow(slot);
    }

    private Clustering<?> clusteringAt(int slot)
    {
        return longClusterings
               ? Clustering.make(ByteBufferUtil.bytes(clusteringKeys[slot]))
               : (Clustering<?>) clusteringObjects[slot];
    }

    /** The arrays' current state as a walkable capture. Caller must hold the lock. */
    private Walk currentWalk()
    {
        ColumnWalk[] captured = new ColumnWalk[stores.size()];
        for (int i = 0; i < stores.size(); i++)
            captured[i] = stores.get(i).data;
        return new Walk(this, size, clusteringKeys, clusteringObjects,
                        timestamps, localDeletionTimes, ttls, flags, captured, overflow, deletionInfo,
                        staticRow, cumulativeStats);
    }

    /**
     * The overflow map as it is <em>now</em>, not as it was captured: the streaming double-check
     * must find a promotion that happened after the read opened — including the very first
     * promotion of the partition, which is what creates the map.
     */
    ConcurrentSkipListMap<Clustering<?>, Row> liveOverflow()
    {
        return overflow;
    }

    /**
     * What the streaming read path opens on: folds the unsorted tail (the one moment a read takes
     * the lock) and captures the array references and the row count. Everything captured is frozen —
     * later writes append past the captured count or replace the arrays wholesale — except the flag
     * bytes, whose only possible change is the one-shot supersede the reader double-checks for.
     */
    Walk captureWalk()
    {
        writeLock.lock();
        try
        {
            return captureWalkLocked();
        }
        finally
        {
            writeLock.unlock();
        }
    }

    /**
     * The full rebuild: every live slot plus the overflow, merged in comparator order. The fail-open
     * fallback of both streaming paths (read and flush) and the answer for the shapes streaming does
     * not serve — built on demand, consumed, discarded; never retained anywhere. Caller must hold
     * the lock.
     *
     * <p>The {@code EncodingStats} collection below is a second walk of every row just built. It is
     * what a rebuilt {@code Partition} owes its callers, and it is also why the flush path no longer
     * comes through here: the sstable writer takes its {@code SerializationHeader} from the
     * memtable-level {@code flushSet.encodingStats()} and {@code SortedTableWriter.append} never
     * asks the partition or the iterator for stats, so on a flush this pass bought nothing.
     */
    private Snapshot buildSnapshot()
    {
        Walk walk = captureWalkLocked();
        TableMetadata tableMetadata = metadata.get();
        ClusteringComparator comparator = tableMetadata.comparator;

        try (BTree.FastBuilder<Row> builder = BTree.fastBuilder())
        {
            Iterator<Row> overflowRows = overflow == null
                                         ? Collections.emptyIterator()
                                         : overflow.values().iterator();
            Row pendingOverflow = overflowRows.hasNext() ? overflowRows.next() : null;

            for (int i = 0; i < walk.size; i++)
            {
                if (walk.superseded(i))
                    continue;
                Row row = walk.assembleRow(i);
                while (pendingOverflow != null && comparator.compare(pendingOverflow.clustering(), row.clustering()) < 0)
                {
                    builder.add(pendingOverflow);
                    pendingOverflow = overflowRows.hasNext() ? overflowRows.next() : null;
                }
                builder.add(row);
            }
            while (pendingOverflow != null)
            {
                builder.add(pendingOverflow);
                pendingOverflow = overflowRows.hasNext() ? overflowRows.next() : null;
            }

            Object[] tree = builder.build();
            EncodingStats stats = EncodingStats.Collector.collect(staticRow, BTree.iterator(tree), deletionInfo);
            return new Snapshot(tableMetadata, key, columns, staticRow, tree, deletionInfo, stats);
        }
    }

    /** {@link #captureWalk()} for callers already holding the lock. */
    private Walk captureWalkLocked()
    {
        consolidate();
        return currentWalk();
    }

    /**
     * A freshly built, immediately discarded object view for the paths the streaming iterator does
     * not serve (point {@code getRow}, names filters, the read path's no-argument iterator) and for
     * the fail-open fallback of every streaming path, flush included. When the partition is demoted,
     * the frozen columnar state and the object-tier sibling are presented as one merged partition.
     */
    private Partition readView()
    {
        Partition base;
        writeLock.lock();
        try
        {
            base = buildSnapshot();
        }
        finally
        {
            writeLock.unlock();
        }
        TimeSeriesMemtable.ObjectShardPartition demoted = demotedTo;
        return demoted == null
               ? base
               : new TimeSeriesMemtable.MergedWindowPartition(metadata.get(), key, Arrays.<Partition>asList(base, demoted));
    }

    @Override
    public Partition flushView()
    {
        // Flush streams off the arrays; it no longer rebuilds them. It is the one caller that reads
        // every row of every partition exactly once, in order, and then drops it, so the rebuild was
        // all cost and no reuse: a BTreeRow and a cell per row — ~4.9 GiB allocated and ~19s of CPU
        // to flush 2.9M rows — plus buildSnapshot's second whole-tree pass to collect EncodingStats
        // that nothing on this path reads (the writer's SerializationHeader comes from the
        // memtable-level flushSet.encodingStats(), and SortedTableWriter.append never calls stats()
        // on the partition or the iterator).
        //
        // Nothing is retained, still — the invariant of f70a0a0610 ("Remove the snapshot retention
        // that OOMed production"), and satisfied a fortiori: the view below caches nothing at all,
        // and each iterator captures its walk at open and leaves nothing behind at close.
        //
        // The guards mirror streamOrRebuild's, and for the same reasons: a demoted partition keeps
        // the exact merged rebuild it has always had (its rows are split between the frozen arrays
        // and the object-tier sibling, which only MergedWindowPartition presents as one), and so
        // does a clustering-less table. Everything else the streaming open cannot reason about is
        // caught later, by the fail-open inside streamOrRebuild: a flush must never fail because of
        // an optimisation.
        if (demotedTo != null || metadata.get().comparator.size() == 0)
            return readView();
        return new StreamingFlushView();
    }

    // ------------------------------------------------------------------- Partition implementation

    @Override
    public TableMetadata metadata()
    {
        return metadata.get();
    }

    @Override
    public DecoratedKey partitionKey()
    {
        return allocator.ensureOnHeap().applyToPartitionKey(key);
    }

    @Override
    public DeletionTime partitionLevelDeletion()
    {
        DeletionTime own;
        writeLock.lock();
        try
        {
            own = deletionInfo.getPartitionDeletion();
        }
        finally
        {
            writeLock.unlock();
        }
        TimeSeriesMemtable.ObjectShardPartition demoted = demotedTo;
        if (demoted != null)
        {
            DeletionTime theirs = demoted.partitionLevelDeletion();
            if (theirs.supersedes(own))
                return theirs;
        }
        return own;
    }

    @Override
    public RegularAndStaticColumns columns()
    {
        RegularAndStaticColumns own;
        writeLock.lock();
        try
        {
            own = columns;
        }
        finally
        {
            writeLock.unlock();
        }
        TimeSeriesMemtable.ObjectShardPartition demoted = demotedTo;
        return demoted == null ? own : own.mergeTo(demoted.columns());
    }

    @Override
    public EncodingStats stats()
    {
        EncodingStats own = cumulativeStats;
        TimeSeriesMemtable.ObjectShardPartition demoted = demotedTo;
        return demoted == null ? own : own.mergeWith(demoted.stats());
    }

    @Override
    public boolean isEmpty()
    {
        writeLock.lock();
        try
        {
            if (!deletionInfo.isLive() || !staticRow.isEmpty() || size > supersededCount
                || (overflow != null && !overflow.isEmpty()))
                return false;
        }
        finally
        {
            writeLock.unlock();
        }
        TimeSeriesMemtable.ObjectShardPartition demoted = demotedTo;
        return demoted == null || demoted.isEmpty();
    }

    @Override
    public boolean hasRows()
    {
        writeLock.lock();
        try
        {
            if (size > supersededCount || (overflow != null && !overflow.isEmpty()))
                return true;
        }
        finally
        {
            writeLock.unlock();
        }
        TimeSeriesMemtable.ObjectShardPartition demoted = demotedTo;
        return demoted != null && demoted.hasRows();
    }

    @Override
    public Row getRow(Clustering<?> clustering)
    {
        return allocator.ensureOnHeap().applyToRow(readView().getRow(clustering));
    }

    @Override
    public UnfilteredRowIterator unfilteredIterator()
    {
        return onHeap(readView().unfilteredIterator());
    }

    @Override
    public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, Slices slices, boolean reversed)
    {
        return onHeap(streamOrRebuild(selection, slices, reversed));
    }

    /**
     * The hot read path: stream straight off the arrays — O(log n) to find the slice bounds, one
     * {@code BTreeRow} assembled per pulled row, nothing retained when the iterator closes. Fails
     * open on the same principle as shard pruning ({@link TimeSeriesMemtable#pruneFailedOpen}): any
     * state or shape the streaming open cannot reason about — a demoted partition, a clustering-less
     * table, or any unexpected exception — falls back to the full rebuild, because a missed
     * optimisation is a slow read and an exception here is an outage.
     */
    private UnfilteredRowIterator streamOrRebuild(ColumnFilter selection, Slices slices, boolean reversed)
    {
        try
        {
            if (demotedTo == null && metadata.get().comparator.size() > 0)
                return TimeSeriesStreamingIterator.open(captureWalk(), selection, slices, reversed);
        }
        catch (RuntimeException e)
        {
            TimeSeriesMemtable.streamFailedOpen(e);
        }
        return readView().unfilteredIterator(selection, slices, reversed);
    }

    @Override
    public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, NavigableSet<Clustering<?>> clusteringsInQueryOrder, boolean reversed)
    {
        return onHeap(readView().unfilteredIterator(selection, clusteringsInQueryOrder, reversed));
    }

    /**
     * A read-path iterator with its rows, its static row <b>and its partition key</b> on heap.
     *
     * <p>{@code allocator.ensureOnHeap().applyToPartition} alone is not enough here. It clones rows,
     * but the key a transformed iterator reports comes from its input ({@code BaseRows.partitionKey()}
     * answers {@code input.partitionKey()}; {@code EnsureOnHeap.applyToPartitionKey} only rewrites a
     * field nobody reads back). Upstream {@code AtomicBTreePartition} never notices because it builds
     * its iterators from its own cloning {@code partitionKey()} accessor. The iterators this class
     * builds carry {@link #key}, the memtable's stored key, which under {@code offheap_objects} is a
     * {@code NativeDecoratedKey} whose length and bytes live in regions freed when the memtable is
     * reclaimed. A consumer that keeps the key past the read's OpOrder group then reads freed memory.
     * CASSANDRA-21354's in-memory local read response does exactly that; its later digest SIGSEGV'd
     * node 41 on 2026-09-28.
     *
     * <p>The flush path must not come through here: {@link #flushView()} hands the writer the
     * memtable's own key and rows on purpose (see {@link StreamingFlushView}).
     */
    private UnfilteredRowIterator onHeap(UnfilteredRowIterator iterator)
    {
        EnsureOnHeap ensureOnHeap = allocator.ensureOnHeap();
        DecoratedKey onHeapKey = ensureOnHeap.applyToPartitionKey(key);
        UnfilteredRowIterator keyed = onHeapKey == key ? iterator : new OnHeapKeyIterator(iterator, onHeapKey);
        return ensureOnHeap.applyToPartition(keyed);
    }

    /** Reports an on-heap copy of the partition key in place of the wrapped iterator's own. */
    private static final class OnHeapKeyIterator implements WrappingUnfilteredRowIterator
    {
        private final UnfilteredRowIterator wrapped;
        private final DecoratedKey partitionKey;

        OnHeapKeyIterator(UnfilteredRowIterator wrapped, DecoratedKey partitionKey)
        {
            this.wrapped = wrapped;
            this.partitionKey = partitionKey;
        }

        @Override
        public UnfilteredRowIterator wrapped()
        {
            return wrapped;
        }

        @Override
        public DecoratedKey partitionKey()
        {
            return partitionKey;
        }
    }

    /**
     * Whether a sliced read of this shard partition can return anything, answered from the
     * <b>clustering</b> values actually held — the arrays' maintained min/max plus the overflow
     * map's ends — and never from the shard's write-time window, because clustering time and write
     * time are independent. Fails open: any partition or range deletion, any static row, an empty
     * partition, a clustering-less table, and any state or slices shape the probe cannot handle all
     * answer {@code true} — pruning is an optimisation, and throwing here is a read outage.
     * Deliberately materialises nothing and never calls {@code Slice.make} on min/max: they are
     * memtable-owned clusterings, and fabricating bounds from them needs the value accessor's object
     * factory, which the native (offheap_objects) accessor refuses with
     * {@code UnsupportedOperationException} — the 2026-08-02 paged-read outage, hit on every page
     * after the first because only page one carries {@code Slices.ALL}.
     */
    @Override
    public boolean mayContainRowsIn(Slices slices)
    {
        writeLock.lock();
        try
        {
            TimeSeriesMemtable.ObjectShardPartition demoted = demotedTo;
            if (demoted != null && demoted.mayContainRowsIn(slices))
                return true;

            if (!deletionInfo.isLive() || !staticRow.isEmpty())
                return true;

            ClusteringComparator comparator = metadata.get().comparator;
            if (comparator.size() == 0)
                return true;

            Clustering<?> min = null;
            Clustering<?> max = null;
            if (size > 0)
            {
                min = longClusterings ? Clustering.make(ByteBufferUtil.bytes(minClusteringKey))
                                      : (Clustering<?>) minClusteringObject;
                max = longClusterings ? Clustering.make(ByteBufferUtil.bytes(maxClusteringKey))
                                      : (Clustering<?>) maxClusteringObject;
            }
            if (overflow != null && !overflow.isEmpty())
            {
                // Superseded slots keep their clustering in the arrays, so the array bounds already
                // cover them; this widens for the rows the arrays never fit.
                Clustering<?> first = overflow.firstKey();
                Clustering<?> last = overflow.lastKey();
                min = min == null || comparator.compare(first, min) < 0 ? first : min;
                max = max == null || comparator.compare(last, max) > 0 ? last : max;
            }
            return min == null || TimeSeriesMemtable.intersectsBounds(slices, comparator, min, max);
        }
        catch (RuntimeException e)
        {
            return TimeSeriesMemtable.pruneFailedOpen(e);
        }
        finally
        {
            writeLock.unlock();
        }
    }

    // ------------------------------------------------------------------------------ observability

    // Each of these reads writer-owned state and so takes the same lock the writer does — the whole
    // point of there being exactly one lock. They are not instrumented: they are test and diagnostic
    // calls, and counting them as "writes" would pollute the contention ratio above.

    /** Rows currently live in the primitive arrays (the fast path): slots not yet superseded. */
    @VisibleForTesting
    public int fastPathRowCount()
    {
        writeLock.lock();
        try
        {
            return size - supersededCount;
        }
        finally
        {
            writeLock.unlock();
        }
    }

    /** Appends that proved the clustering new by a bounds check alone, skipping {@link #findSlot}. */
    @VisibleForTesting
    public long certainNewAppends()
    {
        writeLock.lock();
        try
        {
            return certainNewAppends;
        }
        finally
        {
            writeLock.unlock();
        }
    }

    /** Rows in the object-tier overflow map — promoted rows plus rows the arrays never fit. */
    @VisibleForTesting
    public int overflowRowCount()
    {
        writeLock.lock();
        try
        {
            return overflow == null ? 0 : overflow.size();
        }
        finally
        {
            writeLock.unlock();
        }
    }

    /** Slots dead-weighted by an append-only re-write or a promotion; feeds the degradation valve. */
    @VisibleForTesting
    public int supersededSlotCount()
    {
        writeLock.lock();
        try
        {
            return supersededCount;
        }
        finally
        {
            writeLock.unlock();
        }
    }

    /** Whether the degradation valve moved this partition's future writes to the object tier. */
    @VisibleForTesting
    public boolean isDemoted()
    {
        return demotedTo != null;
    }

    /** Whether the clustering column is held as a bare {@code long} per row. */
    @VisibleForTesting
    public boolean usesLongClusterings()
    {
        return longClusterings;
    }

    /**
     * The key instance this partition actually holds — <b>not</b> {@link #partitionKey()}, which puts
     * it through {@code allocator.ensureOnHeap()} and therefore returns a copy under a native or
     * off-heap-objects allocator. The one-instance-per-key invariant
     * ({@code TimeSeriesMemtable.keys} holds the single cloned key every shard shares) can only be
     * asserted against the stored reference, so a test that used the accessor would pass vacuously on
     * heap buffers and be unwritable off heap.
     */
    @VisibleForTesting
    public DecoratedKey storedKey()
    {
        return key;
    }

    // ------------------------------------------------------------------------------ inner classes

    private static final class RowShape
    {
        final long timestamp;
        final int ttl;
        final long localDeletionTime;
        final boolean hasLiveness;
        /**
         * What {@code row.dataSize()} would have answered, summed by {@code classify} out of the
         * walk it was already doing. A {@code long}, though {@code Row.dataSize()} is an {@code int}
         * checked-cast from a long accumulation: the caller adds it to a {@code long} delta, so
         * widening here drops an overflow check that no row on a write path can reach.
         */
        final long dataSize;

        RowShape(long timestamp, int ttl, long localDeletionTime, boolean hasLiveness, long dataSize)
        {
            this.timestamp = timestamp;
            this.ttl = ttl;
            this.localDeletionTime = localDeletionTime;
            this.hasLiveness = hasLiveness;
            this.dataSize = dataSize;
        }
    }

    /**
     * One column's values. Fixed-width types (double, bigint, timestamp / float, int, date /
     * boolean) keep the serialized bit pattern in a primitive array; everything else keeps the
     * memtable-cloned {@code Cell} in an object array — total fallback, so no type can fail.
     *
     * <p>The arrays themselves live in a {@link ColumnWalk}, replaced wholesale on growth and
     * consolidation so a reader's captured reference is never mutated under it.
     */
    private static final class ColumnStore
    {
        final ColumnMetadata column;
        final int width;
        ColumnWalk data;

        ColumnStore(ColumnMetadata column, int capacity)
        {
            this.column = column;
            this.width = fixedWidthOf(column.type);
            this.data = new ColumnWalk(column, width, capacity);
        }

        long heapSize()
        {
            return data.present.length * (long) (width == 0 ? 9 : width + 1) + 32;
        }

        void grow(int newCapacity)
        {
            data = data.copyOf(newCapacity);
        }

        void applyPermutation(int[] permutation, int count, int capacity)
        {
            data = data.permuted(permutation, count, capacity);
        }

        void set(int slot, Cell<?> cell, Put put)
        {
            switch (width)
            {
                case 8:
                {
                    ByteBuffer value = cell.buffer();
                    data.longs[slot] = value.getLong(value.position());
                    break;
                }
                case 4:
                {
                    ByteBuffer value = cell.buffer();
                    data.ints[slot] = value.getInt(value.position());
                    break;
                }
                case 1:
                {
                    ByteBuffer value = cell.buffer();
                    data.bytes[slot] = value.get(value.position());
                    break;
                }
                default:
                {
                    Object previous = data.cells[slot];
                    if (previous != cell)
                    {
                        Cell<?> cloned = put.cloner.clone(cell);
                        put.heapDelta += cloned.unsharedHeapSizeExcludingData()
                                         - (previous == null ? 0 : ((Cell<?>) previous).unsharedHeapSizeExcludingData());
                        data.cells[slot] = cloned;
                    }
                    break;
                }
            }
            data.present[slot] = 1;
        }
    }

    /**
     * One column's value arrays, immutable from a reader's point of view: writes touch only slots
     * beyond any captured row count, and structural changes swap in a new instance.
     */
    static final class ColumnWalk
    {
        final ColumnMetadata column;
        final int width;
        final byte[] present;
        final long[] longs;
        final int[] ints;
        final byte[] bytes;
        final Object[] cells;

        ColumnWalk(ColumnMetadata column, int width, int capacity)
        {
            this.column = column;
            this.width = width;
            this.present = new byte[capacity];
            this.longs = width == 8 ? new long[capacity] : null;
            this.ints = width == 4 ? new int[capacity] : null;
            this.bytes = width == 1 ? new byte[capacity] : null;
            this.cells = width == 0 ? new Object[capacity] : null;
        }

        private ColumnWalk(ColumnMetadata column, int width, byte[] present, long[] longs, int[] ints, byte[] bytes, Object[] cells)
        {
            this.column = column;
            this.width = width;
            this.present = present;
            this.longs = longs;
            this.ints = ints;
            this.bytes = bytes;
            this.cells = cells;
        }

        ColumnWalk copyOf(int newCapacity)
        {
            return new ColumnWalk(column, width,
                                  Arrays.copyOf(present, newCapacity),
                                  longs == null ? null : Arrays.copyOf(longs, newCapacity),
                                  ints == null ? null : Arrays.copyOf(ints, newCapacity),
                                  bytes == null ? null : Arrays.copyOf(bytes, newCapacity),
                                  cells == null ? null : Arrays.copyOf(cells, newCapacity));
        }

        ColumnWalk permuted(int[] permutation, int count, int capacity)
        {
            ColumnWalk next = new ColumnWalk(column, width, capacity);
            for (int i = 0; i < count; i++)
            {
                next.present[i] = present[permutation[i]];
                if (longs != null)
                    next.longs[i] = longs[permutation[i]];
                if (ints != null)
                    next.ints[i] = ints[permutation[i]];
                if (bytes != null)
                    next.bytes[i] = bytes[permutation[i]];
                if (cells != null)
                    next.cells[i] = cells[permutation[i]];
            }
            return next;
        }

        Cell<?> cellAt(int slot, long timestamp, int ttl, long localDeletionTime)
        {
            // ArrayCell over a bare byte[] rather than BufferCell over a ByteBuffer: the value is
            // rebuilt from the primitive arrays either way, and the ByteBuffer wrapper was ~50
            // bytes of pure overhead per assembled cell on the read and flush paths.
            switch (width)
            {
                case 8:
                    return new ArrayCell(column, timestamp, ttl, localDeletionTime,
                                         ByteArrayUtil.bytes(longs[slot]), null);
                case 4:
                    return new ArrayCell(column, timestamp, ttl, localDeletionTime,
                                         ByteArrayUtil.bytes(ints[slot]), null);
                case 1:
                    return new ArrayCell(column, timestamp, ttl, localDeletionTime,
                                         new byte[]{ bytes[slot] }, null);
                default:
                    return (Cell<?>) cells[slot];
            }
        }
    }

    /**
     * Everything a reader needs to walk the partition without the lock, captured in one place under
     * it. The captured arrays are never mutated at any slot below {@link #size}: appends land beyond
     * it and structural changes replace the arrays. The single exception is {@link #flags}, whose
     * one-shot supersede bit may flip under the reader — which is exactly what the streaming
     * iterator's double-check reads.
     */
    static final class Walk
    {
        final TableMetadata metadata;
        final ClusteringComparator comparator;
        final DecoratedKey key;
        final int size;
        final boolean longClusterings;
        final long[] clusteringKeys;
        final Object[] clusteringObjects;
        final long[] timestamps;
        final long[] localDeletionTimes;
        final int[] ttls;
        final byte[] flags;
        final ColumnWalk[] stores;
        /** The overflow map as captured at open — what the merge cursor iterates. May be null. */
        final ConcurrentSkipListMap<Clustering<?>, Row> overflow;
        final DeletionInfo deletionInfo;
        final Row staticRow;
        final EncodingStats stats;
        private final TimeSeriesColumnarPartition owner;

        Walk(TimeSeriesColumnarPartition owner, int size,
             long[] clusteringKeys, Object[] clusteringObjects, long[] timestamps,
             long[] localDeletionTimes, int[] ttls, byte[] flags, ColumnWalk[] stores,
             ConcurrentSkipListMap<Clustering<?>, Row> overflow, DeletionInfo deletionInfo,
             Row staticRow, EncodingStats stats)
        {
            this.owner = owner;
            this.metadata = owner.metadata.get();
            this.comparator = metadata.comparator;
            this.key = owner.key;
            this.size = size;
            this.longClusterings = owner.longClusterings;
            this.clusteringKeys = clusteringKeys;
            this.clusteringObjects = clusteringObjects;
            this.timestamps = timestamps;
            this.localDeletionTimes = localDeletionTimes;
            this.ttls = ttls;
            this.flags = flags;
            this.stores = stores;
            this.overflow = overflow;
            this.deletionInfo = deletionInfo;
            this.staticRow = staticRow;
            this.stats = stats;
        }

        Clustering<?> clusteringAt(int slot)
        {
            return longClusterings
                   ? Clustering.make(ByteBufferUtil.bytes(clusteringKeys[slot]))
                   : (Clustering<?>) clusteringObjects[slot];
        }

        boolean equalSlots(int a, int b)
        {
            return longClusterings
                   ? clusteringKeys[a] == clusteringKeys[b]
                   : comparator.compare((Clustering<?>) clusteringObjects[a], (Clustering<?>) clusteringObjects[b]) == 0;
        }

        boolean superseded(int slot)
        {
            return (flags[slot] & FLAG_SUPERSEDED) != 0;
        }

        /**
         * A point lookup against the <b>live</b> overflow map, not the captured one: this is the
         * double-check's read, and the promotion it exists to catch may be the partition's first —
         * the one that creates the map after this walk captured {@code null}. No duplicate can
         * result: an entry found here but missed by the capture-time cursor was inserted behind
         * that cursor's fetch position, which the skip-list iterator will never revisit.
         */
        Row overflowGet(Clustering<?> clustering)
        {
            ConcurrentSkipListMap<Clustering<?>, Row> map = owner.liveOverflow();
            return map == null ? null : map.get(clustering);
        }

        /** One slot rebuilt as a real {@code BTreeRow} — the per-pull allocation of the streaming read. */
        Row assembleRow(int slot)
        {
            rowsAssembled.increment();
            long ts = timestamps[slot];
            int ttl = ttls[slot];
            long ldt = localDeletionTimes[slot];

            Object[] scratch = null;
            int n = 0;
            for (int s = 0; s < stores.length; s++)
            {
                ColumnWalk store = stores[s];
                if (store.present[slot] == 0)
                    continue;
                if (scratch == null)
                    scratch = new Object[stores.length];
                scratch[n++] = store.cellAt(slot, ts, ttl, ldt);
            }

            LivenessInfo liveness = (flags[slot] & FLAG_HAS_LIVENESS) != 0
                                    ? LivenessInfo.withExpirationTime(ts, ttl, ldt)
                                    : LivenessInfo.EMPTY;

            // Stores are kept in column order, so the scratch prefix is already BTree input order.
            // The BulkIterator is pooled and must be closed, or its thread-local slot leaks.
            Object[] tree;
            if (n == 0)
                tree = BTree.empty();
            else if (n == 1)
                tree = BTree.singleton(scratch[0]);
            else
                try (BulkIterator<Object> bulk = BulkIterator.of(scratch, 0))
                {
                    tree = BTree.build(bulk, n, UpdateFunction.noOp());
                }

            // O(1) by construction: classify() admits a slot only with Deletion.LIVE, no cell
            // tombstone, and one shared (ts, ttl, ldt) across the liveness and every cell — so the
            // 4-arg create's per-cell accumulate over the tree would always fold to exactly this.
            long minDeletionTime = ttl == LivenessInfo.NO_TTL
                                   ? Cell.MAX_DELETION_TIME
                                   : Math.min(Cell.MAX_DELETION_TIME, ldt);
            return BTreeRow.create(clusteringAt(slot), liveness, Row.Deletion.LIVE, tree, minDeletionTime);
        }
    }

    /**
     * Accumulates one {@code put}'s accounting and stands in as the reconciliation callback so cell
     * merges report {@code colUpdateTimeDelta} exactly as {@code BTreePartitionUpdater} does.
     */
    private static final class Put implements ColumnData.PostReconciliationFunction
    {
        final Cloner cloner;
        final UpdateTransaction indexer;
        long heapDelta;
        long dataDelta;
        long colUpdateTimeDelta = Long.MAX_VALUE;

        Put(Cloner cloner, UpdateTransaction indexer)
        {
            this.cloner = cloner;
            this.indexer = indexer;
        }

        @Override
        public ColumnData insert(ColumnData insert)
        {
            return insert;
        }

        @Override
        public Cell<?> merge(Cell<?> previous, Cell<?> insert)
        {
            if (insert != previous)
            {
                long timeDelta = Math.abs(insert.timestamp() - previous.timestamp());
                if (timeDelta < colUpdateTimeDelta)
                    colUpdateTimeDelta = timeDelta;
            }
            return insert;
        }

        @Override
        public void delete(ColumnData existing)
        {
        }

        @Override
        public void onAllocatedOnHeap(long delta)
        {
        }
    }

    /**
     * The streamed view, handed to the flush path by {@link TimeSeriesColumnarPartition#flushView()}.
     * Every row-bearing call goes through {@link TimeSeriesColumnarPartition#streamOrRebuild}, so the
     * arrays are walked and one row assembled per row actually written out, instead of the whole
     * partition being rebuilt into objects first and walked afterwards. It holds no row state of its
     * own — the walk is captured per iterator, at open, and dropped at close — so the flush path's
     * "never retain a materialization" rule is kept by construction rather than by discipline.
     *
     * <p><b>Why this and not the outer partition itself.</b> The outer {@code Partition} overrides
     * are the read path's: they put iterators, rows and the partition key through
     * {@code allocator.ensureOnHeap()}. Flush never did — it consumed {@link Snapshot} directly,
     * whose key and rows are the memtable's own buffers — so copying here would change what the
     * sstable writer is handed and add back a slice of the very allocation this class exists to
     * remove. Each method below therefore reproduces what {@code Snapshot} answered, not what the
     * read path answers.
     *
     * <p>The shapes streaming does not serve ({@code getRow}, a names filter) fall through to the
     * rebuild, unwrapped, for the same reason.
     */
    private final class StreamingFlushView implements Partition
    {
        @Override
        public TableMetadata metadata()
        {
            return TimeSeriesColumnarPartition.this.metadata();
        }

        @Override
        public DecoratedKey partitionKey()
        {
            // The memtable's own key object, exactly as Snapshot handed it to the writer: see the
            // note above on ensureOnHeap.
            return key;
        }

        @Override
        public DeletionTime partitionLevelDeletion()
        {
            return TimeSeriesColumnarPartition.this.partitionLevelDeletion();
        }

        @Override
        public RegularAndStaticColumns columns()
        {
            return TimeSeriesColumnarPartition.this.columns();
        }

        @Override
        public EncodingStats stats()
        {
            // The maintained cumulative stats, not the exact per-row collection the rebuild made:
            // collecting exactly costs a walk of every row, and no one on the flush path reads this
            // (see buildSnapshot). It is the same value the streaming read path already publishes on
            // every iterator it opens.
            return TimeSeriesColumnarPartition.this.stats();
        }

        @Override
        public boolean isEmpty()
        {
            return TimeSeriesColumnarPartition.this.isEmpty();
        }

        @Override
        public boolean hasRows()
        {
            return TimeSeriesColumnarPartition.this.hasRows();
        }

        @Override
        public Row getRow(Clustering<?> clustering)
        {
            return readView().getRow(clustering);
        }

        @Override
        public UnfilteredRowIterator unfilteredIterator()
        {
            // ColumnFilter.all(columns()) — the RegularAndStaticColumns overload, Slices.ALL,
            // forward: the exact arguments AbstractBTreePartition.unfilteredIterator() passes, so
            // the iterator's columns() and the unfiltereds it yields are what the rebuild yielded.
            // This is the call the flush path makes (Flushing.writeSortedContents, and the tiering
            // hook's ColdWindowChunkFlush.encodePartition).
            return streamOrRebuild(ColumnFilter.all(columns()), Slices.ALL, false);
        }

        @Override
        public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, Slices slices, boolean reversed)
        {
            return streamOrRebuild(selection, slices, reversed);
        }

        @Override
        public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection,
                                                        NavigableSet<Clustering<?>> clusteringsInQueryOrder,
                                                        boolean reversed)
        {
            return readView().unfilteredIterator(selection, clusteringsInQueryOrder, reversed);
        }
    }

    /**
     * The rebuilt view. {@code canHaveShadowedData} must be true: like any memtable partition,
     * the arrays keep rows that a later partition or range deletion shadows, and the iterator has to
     * filter them exactly as {@code AtomicBTreePartition} does.
     */
    private static final class Snapshot extends ImmutableBTreePartition
    {
        Snapshot(TableMetadata metadata,
                 DecoratedKey key,
                 RegularAndStaticColumns columns,
                 Row staticRow,
                 Object[] tree,
                 DeletionInfo deletionInfo,
                 EncodingStats stats)
        {
            super(metadata, key, columns, staticRow, tree, deletionInfo, stats);
        }

        @Override
        protected boolean canHaveShadowedData()
        {
            return true;
        }
    }
}
