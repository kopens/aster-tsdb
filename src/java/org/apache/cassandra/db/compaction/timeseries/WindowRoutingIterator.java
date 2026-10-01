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

package org.apache.cassandra.db.compaction.timeseries;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.LongConsumer;
import java.util.function.LongUnaryOperator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * Routes the contents of one partition into per-time-window buckets, keyed by window start in
 * milliseconds. This is the core primitive behind TSCS T3's window-boundary splits (design spec
 * section 4): every output bucket, written to its own sstable, is fully contained in one time window
 * on the <em>write-timestamp</em> axis — the same axis T1/T2 classify sstables by.
 *
 * <h2>Routing granularity: individually-timestamped elements, not whole rows</h2>
 *
 * A row is atomic; its timestamps are not. Routing a whole {@link Row} by the maximum of its
 * timestamps cannot uphold the containment invariant, because sstable min-timestamp metadata is the
 * <em>minimum</em> over every cell/liveness/deletion it contains ({@code MetadataCollector.update}),
 * while the freeze classifier demands {@code windowStartFor(min) == windowStartFor(max)}. An
 * ordinary {@code INSERT} at T_old followed by an {@code UPDATE} of a different column at T_new
 * merges into one row whose timestamps straddle a window boundary; whichever single window such a
 * row is sent to, the output sstable still straddles, classifies FREEZING forever and is re-selected
 * for a split rewrite on every background round.
 *
 * So every element that carries its own write timestamp is routed on its own:
 * <ul>
 *   <li>the primary-key liveness info, by its timestamp;</li>
 *   <li>the row deletion, by its {@code markedForDeleteAt};</li>
 *   <li>every simple cell, by its timestamp;</li>
 *   <li>every complex (collection) deletion, by its {@code markedForDeleteAt}, and every cell
 *       inside a complex column by its own timestamp;</li>
 *   <li>the partition-level deletion, by its {@code markedForDeleteAt};</li>
 *   <li>every static cell, by its own timestamp (the static row is split exactly like a regular one);</li>
 *   <li>a {@link RangeTombstoneBoundMarker} by its deletion timestamp — an open/close pair shares one
 *       deletion time and therefore travels together;</li>
 *   <li>a {@link RangeTombstoneBoundaryMarker} whose close- and open-deletions fall in different
 *       windows is decomposed into its corresponding close and open bound markers, each routed
 *       separately.</li>
 * </ul>
 *
 * An output sstable therefore only ever contains timestamps from inside its own window, so its min
 * and max are both contained and the window classifies FROZEN.
 *
 * <p>This is semantically sound and is not a new idea: Cassandra already stores one row's cells
 * across many sstables and reconciles them at read time, so a per-window cell subset is an ordinary
 * sstable. Two consequences are deliberate:
 * <ul>
 *   <li>a row emitted into window B may have <b>no</b> primary-key liveness, because the liveness
 *       went to window A. That is exactly what an {@code UPDATE}-created row looks like, and no
 *       liveness is synthesised to "fix" it — doing so would change delete semantics;</li>
 *   <li>a row with nothing at all in a given window is not emitted into that window.</li>
 * </ul>
 *
 * <p>Whole-window retention drops stay safe because a deletion always lands in a window at least as
 * new as everything it shadows, and drops proceed oldest-first: by the time a deletion's window is
 * dropped, every window it could still shadow is already gone. That holds for what this iterator
 * routes; it does not hold for a window-spanning sstable (a UCS-delegate output, or a parked
 * split-refreeze), which is filed under the window of its <em>max</em> timestamp -- rows in it can be
 * older than a deletion's window and outlive it (see TimeSeriesCompactionController).
 *
 * <p>Each bucket preserves the original clustering order (it is a subsequence of a sorted stream).
 */
public final class WindowRoutingIterator
{
    private static final Logger logger = LoggerFactory.getLogger(WindowRoutingIterator.class);

    /** Window key used for "no window" (no partition deletion / no static content). */
    private static final long NO_WINDOW = Long.MIN_VALUE;

    /**
     * Per-partition routing budget. Splitting is inherently a buffering operation: an
     * {@link org.apache.cassandra.io.sstable.format.SSTableWriter} consumes a partition's iterator in
     * one pass and accepts each partition key exactly once, so the whole partition has to be
     * distributed before any window's slice can be appended. At flush that is bounded by a memtable
     * partition, but {@code SplitRefreezeCompactionTask} applies the same routing to arbitrarily large
     * compaction partitions.
     *
     * <p>Rather than materialise a million-row time-series partition on heap, {@link #slices} gives up
     * on splitting a partition once it exceeds this budget: the untouched source prefix and the unread
     * remainder are handed back as a single lazy slice, which keeps memory bounded and the data correct
     * at the cost of one window-spanning sstable. That is the flush path's behaviour.
     *
     * <p>The spanning sstable classifies FREEZING, so {@code SplitRefreezeCompactionTask} picks it up.
     * It can read its input again, so it routes an over-budget partition with {@link #rereadSlices}
     * (via {@link #slicesWithinBudget}) instead of buffering it, and its outputs are window-contained.
     * Before that, the split overflowed the same way the flush had, never made progress, and the
     * strategy's no-progress guard parked the window.
     *
     * <p>Failing instead was rejected: this routing is on the memtable flush path, where an exception
     * fails the whole flush and blocks writes.
     *
     * <p><b>What this actually measures.</b> The counter is a sum of {@link Row#dataSize()}, i.e. the
     * <em>serialized</em> size of the buffered rows, and a flat 64 bytes for a range-tombstone marker.
     * Retained heap is a multiple of that: every buffered row also costs a {@code BTreeRow} plus its
     * BTree and {@code Cell} objects (object headers, references and padding), the routing keeps two
     * reference lists over the prefix (the stream-order buffer used by the overflow path and the
     * per-window buckets), and a row whose timestamps straddle a boundary is retained both whole and
     * as its per-window pieces. So this is a coarse throttle that stops unbounded growth, not a hard
     * heap ceiling: expect real retention of a few times the configured number while a partition is
     * being routed. Size it accordingly rather than against a heap headroom figure.
     */
    @VisibleForTesting
    public static volatile long maxBufferedBytesPerPartition = 64L * 1024 * 1024;

    private WindowRoutingIterator()
    {
    }

    /**
     * Routes one partition's {@link Unfiltered}s into per-window buckets, splitting rows at element
     * granularity (see the class javadoc). Materialises the whole partition; {@link #slices} is the
     * bounded entry point used on the flush/compaction paths.
     *
     * @param partition            the partition's unfiltereds, in clustering order (forward iteration)
     * @param windowStartOfMillis  maps an epoch-millisecond timestamp to its window start
     * @param tableResolution      the table's timestamp resolution (cell timestamps → milliseconds)
     * @return window start (ms) → that window's unfiltereds, in original clustering order
     */
    public static NavigableMap<Long, List<Unfiltered>> route(UnfilteredRowIterator partition,
                                                             LongUnaryOperator windowStartOfMillis,
                                                             TimeUnit tableResolution)
    {
        return routeBounded(partition, windowStartOfMillis, tableResolution, Long.MAX_VALUE).buckets;
    }

    /** The outcome of routing a partition body: per-window buckets, plus whether the budget ran out. */
    private static final class Routed
    {
        final NavigableMap<Long, List<Unfiltered>> buckets = new TreeMap<>();
        /**
         * The buffered prefix in <em>source</em> order — the original {@link Unfiltered}s, untouched: no
         * row split into pieces, no range-tombstone boundary decomposed. Only the overflow path reads
         * it; see {@link #slices} for why that is the only ordering that can be written back out.
         */
        final List<Unfiltered> prefix = new ArrayList<>();
        /** Non-null once the budget was exhausted: the unread remainder of the source partition. */
        Iterator<Unfiltered> remainder;
        /** The newest window observed before giving up — where the unsplit remainder is parked. */
        long overflowWindow = NO_WINDOW;

        boolean overflowed()
        {
            return remainder != null;
        }
    }

    private static Routed routeBounded(UnfilteredRowIterator partition,
                                       LongUnaryOperator windowStartOfMillis,
                                       TimeUnit tableResolution,
                                       long budgetBytes)
    {
        Routed routed = new Routed();
        long buffered = 0;
        while (partition.hasNext())
        {
            Unfiltered unfiltered = partition.next();
            if (buffered > budgetBytes)
            {
                // Give up splitting this partition: hand the caller the buffered prefix plus the
                // unread tail as one slice. See maxBufferedBytesPerPartition for the rationale.
                routed.remainder = Iterators.concat(Iterators.singletonIterator(unfiltered), partition);
                routed.overflowWindow = routed.buckets.isEmpty() ? 0L : routed.buckets.lastKey();
                return routed;
            }
            buffered += sizeOf(unfiltered);
            routed.prefix.add(unfiltered);
            forEachPiece(unfiltered, windowStartOfMillis, tableResolution, (window, piece) -> bucket(routed.buckets, window).add(piece));
        }
        return routed;
    }

    /**
     * Routes one {@link Unfiltered} at element granularity: hands {@code sink} one piece per window the
     * element's own timestamps name (see the class javadoc). A row yields at most one piece per window;
     * a range-tombstone boundary whose two deletions fall in different windows yields its close half
     * and its open half separately. This is the single definition both {@link #route} and
     * {@link #rereadSlices} apply, so the two cannot disagree about where anything goes.
     */
    private static void forEachPiece(Unfiltered unfiltered,
                                     LongUnaryOperator windowStartOfMillis,
                                     TimeUnit tableResolution,
                                     BiConsumer<Long, Unfiltered> sink)
    {
        if (unfiltered instanceof RangeTombstoneBoundaryMarker)
        {
            RangeTombstoneBoundaryMarker boundary = (RangeTombstoneBoundaryMarker) unfiltered;
            long closeWindow = windowOf(boundary.closeDeletionTime(false).markedForDeleteAt(), windowStartOfMillis, tableResolution);
            long openWindow = windowOf(boundary.openDeletionTime(false).markedForDeleteAt(), windowStartOfMillis, tableResolution);
            if (closeWindow == openWindow)
            {
                sink.accept(closeWindow, boundary);
            }
            else
            {
                // The closing and opening deletions live in different windows: split the boundary
                // so each window's sstable carries a self-contained marker.
                sink.accept(closeWindow, boundary.createCorrespondingCloseMarker(false));
                sink.accept(openWindow, boundary.createCorrespondingOpenMarker(false));
            }
        }
        else if (unfiltered instanceof RangeTombstoneBoundMarker)
        {
            RangeTombstoneBoundMarker marker = (RangeTombstoneBoundMarker) unfiltered;
            sink.accept(windowOf(marker.deletionTime().markedForDeleteAt(), windowStartOfMillis, tableResolution), marker);
        }
        else
        {
            for (Map.Entry<Long, Row> piece : splitRow((Row) unfiltered, windowStartOfMillis, tableResolution).entrySet())
                sink.accept(piece.getKey(), piece.getValue());
        }
    }

    /**
     * Splits one row into per-window pieces, one per window named by any of the row's own timestamps.
     * A window with nothing in it gets no piece; no piece is ever an empty row (which the sstable
     * writers' {@code Rows.collectStats} rightly refuses).
     *
     * <p>Pieces are built with a {@link BTreeRow#sortedBuilder()}: iterating a row yields its column
     * data in comparator order, and complex cells in cell-path order after that column's complex
     * deletion, so every per-window subset is handed to its builder already sorted — the same
     * discipline {@code UnfilteredSerializer} uses when reading rows back off disk.
     *
     * <p>A row that carries <em>no</em> timestamp at all is dropped, on purpose. Every element a row can
     * hold — primary-key liveness, row deletion, complex deletion, simple or complex cell — carries a
     * write timestamp, so "no timestamp" is exactly "no content": {@link Row#isEmpty()}. There is no
     * window such a row could be routed to and nothing to write if there were; the sstable writers'
     * {@code Rows.collectStats} rightly refuses an empty row. An earlier revision threw here instead,
     * which was defensible while routing was per-row (an empty row meant the caller had gone wrong) but
     * is not now: on the memtable-flush path an exception fails the whole flush and blocks writes, and
     * the condition is checked and skipped rather than merely falling through untested.
     */
    @VisibleForTesting
    static NavigableMap<Long, Row> splitRow(Row row, LongUnaryOperator windowStartOfMillis, TimeUnit tableResolution)
    {
        if (row.isEmpty())
            return Collections.emptyNavigableMap();

        // Overwhelmingly the common case: every timestamp in the row names the same window, so the row
        // is already contained and is passed through untouched - no rebuild, no allocation, and callers
        // keep the original instance.
        long single = singleWindowOf(row, windowStartOfMillis, tableResolution);
        if (single != NOT_SINGLE_WINDOW)
        {
            NavigableMap<Long, Row> whole = new TreeMap<>();
            whole.put(single, row);
            return whole;
        }

        NavigableMap<Long, Row.Builder> builders = new TreeMap<>();
        Clustering<?> clustering = row.clustering();

        LivenessInfo liveness = row.primaryKeyLivenessInfo();
        if (!liveness.isEmpty())
            builderFor(builders, clustering, windowOf(liveness.timestamp(), windowStartOfMillis, tableResolution))
                .addPrimaryKeyLivenessInfo(liveness);

        Row.Deletion deletion = row.deletion();
        if (!deletion.isLive())
            builderFor(builders, clustering, windowOf(deletion.time().markedForDeleteAt(), windowStartOfMillis, tableResolution))
                .addRowDeletion(deletion);

        for (ColumnData cd : row)
        {
            if (cd.column().isSimple())
            {
                Cell<?> cell = (Cell<?>) cd;
                builderFor(builders, clustering, windowOf(cell.timestamp(), windowStartOfMillis, tableResolution)).addCell(cell);
            }
            else
            {
                ComplexColumnData complex = (ComplexColumnData) cd;
                DeletionTime complexDeletion = complex.complexDeletion();
                if (!complexDeletion.isLive())
                    builderFor(builders, clustering, windowOf(complexDeletion.markedForDeleteAt(), windowStartOfMillis, tableResolution))
                        .addComplexDeletion(complex.column(), complexDeletion);
                for (Cell<?> cell : complex)
                    builderFor(builders, clustering, windowOf(cell.timestamp(), windowStartOfMillis, tableResolution)).addCell(cell);
            }
        }

        NavigableMap<Long, Row> pieces = new TreeMap<>();
        for (Map.Entry<Long, Row.Builder> entry : builders.entrySet())
        {
            Row piece = entry.getValue().build();
            // A piece can still come out empty if every cell offered to it was shadowed by a row
            // deletion routed to the same window; such a window simply gets nothing.
            if (!piece.isEmpty())
                pieces.put(entry.getKey(), piece);
        }
        return pieces;
    }

    /** Sentinel for {@link #singleWindowOf}: this row's timestamps do not all name one window. */
    private static final long NOT_SINGLE_WINDOW = Long.MIN_VALUE;

    /**
     * @return the one window every timestamp in {@code row} belongs to, or {@link #NOT_SINGLE_WINDOW}
     *         if they differ. Callers screen out empty rows first ({@link #splitRow}), so a non-empty
     *         row always names at least one window.
     */
    private static long singleWindowOf(Row row, LongUnaryOperator windowStartOfMillis, TimeUnit tableResolution)
    {
        long window = NOT_SINGLE_WINDOW;

        LivenessInfo liveness = row.primaryKeyLivenessInfo();
        if (!liveness.isEmpty())
            window = windowOf(liveness.timestamp(), windowStartOfMillis, tableResolution);

        Row.Deletion deletion = row.deletion();
        if (!deletion.isLive())
        {
            long w = windowOf(deletion.time().markedForDeleteAt(), windowStartOfMillis, tableResolution);
            if (window != NOT_SINGLE_WINDOW && w != window)
                return NOT_SINGLE_WINDOW;
            window = w;
        }

        for (ColumnData cd : row)
        {
            if (cd.column().isSimple())
            {
                long w = windowOf(((Cell<?>) cd).timestamp(), windowStartOfMillis, tableResolution);
                if (window != NOT_SINGLE_WINDOW && w != window)
                    return NOT_SINGLE_WINDOW;
                window = w;
            }
            else
            {
                ComplexColumnData complex = (ComplexColumnData) cd;
                if (!complex.complexDeletion().isLive())
                {
                    long w = windowOf(complex.complexDeletion().markedForDeleteAt(), windowStartOfMillis, tableResolution);
                    if (window != NOT_SINGLE_WINDOW && w != window)
                        return NOT_SINGLE_WINDOW;
                    window = w;
                }
                for (Cell<?> cell : complex)
                {
                    long w = windowOf(cell.timestamp(), windowStartOfMillis, tableResolution);
                    if (window != NOT_SINGLE_WINDOW && w != window)
                        return NOT_SINGLE_WINDOW;
                    window = w;
                }
            }
        }
        return window;
    }

    private static Row.Builder builderFor(NavigableMap<Long, Row.Builder> builders, Clustering<?> clustering, long windowStart)
    {
        Row.Builder builder = builders.get(windowStart);
        if (builder == null)
        {
            builder = BTreeRow.sortedBuilder();
            builder.newRow(clustering);
            builders.put(windowStart, builder);
        }
        return builder;
    }

    private static List<Unfiltered> bucket(NavigableMap<Long, List<Unfiltered>> buckets, long windowStart)
    {
        return buckets.computeIfAbsent(windowStart, k -> new ArrayList<>());
    }

    /**
     * Routes a partition into per-window {@link UnfilteredRowIterator} slices ready to be appended to
     * per-window writers. The partition-level deletion goes to the window of its own
     * {@code markedForDeleteAt}, and the static row is split per static cell exactly like a regular
     * row — so no slice ever carries a timestamp from outside its own window.
     *
     * <p>Non-header slices get {@link DeletionTime#LIVE} and {@link Rows#EMPTY_STATIC_ROW}, which
     * {@code MetadataCollector} ignores; that is what keeps a live partition deletion from polluting
     * every window's min-timestamp metadata.
     *
     * @return window start (ms) → that window's slice; empty map for a truly empty partition
     */
    public static NavigableMap<Long, UnfilteredRowIterator> slices(UnfilteredRowIterator partition,
                                                                   LongUnaryOperator windowStartOfMillis,
                                                                   TimeUnit tableResolution)
    {
        DeletionTime partitionDeletion = partition.partitionLevelDeletion();
        Row staticRow = partition.staticRow();
        Routed routed = routeBounded(partition, windowStartOfMillis, tableResolution, maxBufferedBytesPerPartition);

        if (routed.overflowed())
        {
            // Degraded, memory-bounded path: one slice carrying the whole partition, header included.
            //
            // The rate-limiter key names the table. NoSpamLogger keeps one interval per key, so a
            // constant key would let whichever table overflowed first that minute suppress every
            // other table's warning - and the tables that vanish are indistinguishable from tables
            // that never overflowed. That is the one question this message exists to answer: an
            // operator seeing parked windows needs to know which tables are hitting the buffer.
            // Observed on a production node, where four tables logged and six parked silently.
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN,
                             "window-routing-buffer:" + partition.metadata().keyspace + '.' + partition.metadata().name,
                             1, TimeUnit.MINUTES,
                             "Partition {} of {}.{} exceeded the {}-byte window-routing buffer; writing it " +
                             "unsplit into window {}. The resulting sstable spans windows until a " +
                             "split-refreeze re-reads it window by window; a partition this large also " +
                             "hurts repair and streaming, so consider a time bucket in the partition key.",
                             partition.partitionKey(), partition.metadata().keyspace, partition.metadata().name,
                             maxBufferedBytesPerPartition, routed.overflowWindow);

            // The routing work done so far is DISCARDED and the untouched source prefix is written
            // instead. Two reasons, both fatal to any attempt to reuse the buckets here:
            //
            //  1. routed.buckets holds split PIECES. A row whose cells straddle a boundary contributes
            //     one piece per window, all sharing the row's Clustering, and ClusteringComparator ranks
            //     those equal - so concatenating the buckets and sorting cannot separate them, and
            //     SortedTablePartitionWriter.addUnfiltered does not validate monotonicity. The partition
            //     would be written with the same clustering twice, and every later split-refreeze would
            //     re-read and re-propagate it. That is the very defect element-granularity routing exists
            //     to fix, reintroduced on the degraded path.
            //  2. A RangeTombstoneBoundaryMarker whose deletions fall in different windows is decomposed
            //     into a close and an open marker. INCL_END_BOUND and EXCL_START_BOUND compare equal
            //     (ClusteringPrefix.Kind.comparison == 3), so once the buckets are concatenated in
            //     window-key order a stable sort cannot restore their relative order: when the open
            //     deletion is older than the close deletion the open marker precedes the close marker and
            //     one of the two range tombstones silently loses its coverage, resurrecting deleted rows.
            //
            // The source prefix has neither problem by construction: clusterings are strictly increasing,
            // boundary markers are still whole, and the unread remainder appends behind it in order.
            Row overflowStatic = staticRow.isEmpty() ? Rows.EMPTY_STATIC_ROW : staticRow;
            NavigableMap<Long, UnfilteredRowIterator> slices = new TreeMap<>();
            slices.put(routed.overflowWindow,
                       new WindowSlice(partition, partitionDeletion, overflowStatic,
                                       Iterators.concat(routed.prefix.iterator(), routed.remainder)));
            return slices;
        }

        return windowSlices(partition, partitionDeletion, staticRow, routed, windowStartOfMillis, tableResolution);
    }

    private static NavigableMap<Long, UnfilteredRowIterator> windowSlices(UnfilteredRowIterator partition,
                                                                          DeletionTime partitionDeletion,
                                                                          Row staticRow,
                                                                          Routed routed,
                                                                          LongUnaryOperator windowStartOfMillis,
                                                                          TimeUnit tableResolution)
    {
        long deletionWindow = partitionDeletion.isLive()
                              ? NO_WINDOW
                              : windowOf(partitionDeletion.markedForDeleteAt(), windowStartOfMillis, tableResolution);
        NavigableMap<Long, Row> statics = staticRow.isEmpty()
                                          ? Collections.emptyNavigableMap()
                                          : splitRow(staticRow, windowStartOfMillis, tableResolution);

        TreeSet<Long> windows = new TreeSet<>(routed.buckets.keySet());
        windows.addAll(statics.keySet());
        if (deletionWindow != NO_WINDOW)
            windows.add(deletionWindow);

        NavigableMap<Long, UnfilteredRowIterator> slices = new TreeMap<>();
        for (long window : windows)
        {
            List<Unfiltered> content = routed.buckets.getOrDefault(window, List.of());
            Row windowStatic = statics.getOrDefault(window, Rows.EMPTY_STATIC_ROW);
            DeletionTime windowDeletion = window == deletionWindow ? partitionDeletion : DeletionTime.LIVE;
            slices.put(window, new WindowSlice(partition, windowDeletion, windowStatic, content.iterator()));
        }
        return slices;
    }

    /**
     * {@link #slices} without the degraded path: the per-window slices if the partition routes within
     * {@link #maxBufferedBytesPerPartition}, or {@code null} if it does not. On {@code null} the
     * partition has been partly consumed and is no use to the caller except to close; a caller that can
     * read the partition again routes it with {@link #rereadSlices} instead.
     */
    public static NavigableMap<Long, UnfilteredRowIterator> slicesWithinBudget(UnfilteredRowIterator partition,
                                                                               LongUnaryOperator windowStartOfMillis,
                                                                               TimeUnit tableResolution)
    {
        DeletionTime partitionDeletion = partition.partitionLevelDeletion();
        Row staticRow = partition.staticRow();
        Routed routed = routeBounded(partition, windowStartOfMillis, tableResolution, maxBufferedBytesPerPartition);
        return routed.overflowed() ? null : windowSlices(partition, partitionDeletion, staticRow, routed, windowStartOfMillis, tableResolution);
    }

    /** A partition that can be read again from the start, restricted to clustering {@link Slices}. */
    @FunctionalInterface
    public interface Rereadable
    {
        /** A fresh forward iterator over the partition's content within {@code slices}; the caller closes it. */
        UnfilteredRowIterator open(Slices slices);
    }

    /** Receives one window's slice; it must consume the slice before returning, which is then closed. */
    @FunctionalInterface
    public interface SliceSink
    {
        void accept(long window, UnfilteredRowIterator slice);
    }

    /**
     * Routes a partition of any size into per-window slices in bounded memory, by reading it again
     * instead of buffering it. Pass 1 reads the partition once and records, for every window, the
     * clustering span its elements occupy; pass 2 re-opens the partition once per window, restricted
     * to that span, and hands {@code sink} only that window's elements (via {@link #forEachPiece}, the
     * same routing {@link #slices} applies). What each window receives is exactly what {@link #slices}
     * would have given it - {@code WindowRoutingIteratorTest#rereadingEachWindowMatchesInMemoryRouting}.
     * <p>
     * Memory is one pair of bounds per window. Reads cost the partition once plus the sum of the
     * windows' spans; for time-series data, where write time follows clustering time, the spans barely
     * overlap and that is about two reads in total. Windows are emitted in ascending order, one
     * partition at a time, so each writer still sees partitions in token order.
     * <p>
     * Why a span edge cannot cut a tombstone: every marker of a range tombstone carries the same
     * deletion time, so the tombstone's open and close markers both belong to the window that time
     * names, and both lie inside that window's span. Clipping at a span edge therefore only ever
     * truncates tombstones of <em>other</em> windows, whose markers the filter drops anyway, or
     * re-emits a window's own marker at the very bound it already sits on.
     *
     * @param bytesRead told the {@link Row#dataSize()} (or a flat 64 per marker) of everything read,
     *                  in both passes, so the caller can charge the compaction throughput limiter
     */
    public static void rereadSlices(Rereadable source,
                                    LongUnaryOperator windowStartOfMillis,
                                    TimeUnit tableResolution,
                                    LongConsumer bytesRead,
                                    SliceSink sink)
    {
        // window -> {start, end} of its body elements; null for a window only the header reaches.
        NavigableMap<Long, ClusteringBound<?>[]> spans = new TreeMap<>();
        ClusteringComparator comparator;
        try (UnfilteredRowIterator partition = source.open(Slices.ALL))
        {
            comparator = partition.metadata().comparator;
            DeletionTime partitionDeletion = partition.partitionLevelDeletion();
            if (!partitionDeletion.isLive())
                spans.putIfAbsent(windowOf(partitionDeletion.markedForDeleteAt(), windowStartOfMillis, tableResolution), null);
            Row staticRow = partition.staticRow();
            if (!staticRow.isEmpty())
                for (long window : splitRow(staticRow, windowStartOfMillis, tableResolution).keySet())
                    spans.putIfAbsent(window, null);

            while (partition.hasNext())
            {
                Unfiltered unfiltered = partition.next();
                bytesRead.accept(sizeOf(unfiltered));
                ClusteringPrefix<?> position = unfiltered.clustering();
                forEachPiece(unfiltered, windowStartOfMillis, tableResolution,
                             (window, piece) -> widen(spans, window, position, comparator));
            }
        }

        for (Map.Entry<Long, ClusteringBound<?>[]> entry : spans.entrySet())
        {
            long window = entry.getKey();
            ClusteringBound<?>[] span = entry.getValue();
            // A header-only window still needs the header; its (lazily read) body is never iterated.
            Slices slices = span == null ? Slices.ALL : Slices.with(comparator, Slice.make(span[0], span[1]));
            try (UnfilteredRowIterator reread = source.open(slices))
            {
                DeletionTime partitionDeletion = reread.partitionLevelDeletion();
                DeletionTime windowDeletion = !partitionDeletion.isLive()
                                              && windowOf(partitionDeletion.markedForDeleteAt(), windowStartOfMillis, tableResolution) == window
                                              ? partitionDeletion : DeletionTime.LIVE;
                Row staticRow = reread.staticRow();
                Row windowStatic = staticRow.isEmpty()
                                   ? Rows.EMPTY_STATIC_ROW
                                   : splitRow(staticRow, windowStartOfMillis, tableResolution).getOrDefault(window, Rows.EMPTY_STATIC_ROW);
                Iterator<Unfiltered> content = span == null
                                               ? Collections.emptyIterator()
                                               : new WindowFilter(reread, window, windowStartOfMillis, tableResolution, bytesRead);
                sink.accept(window, new WindowSlice(reread, windowDeletion, windowStatic, content));
            }
        }
    }

    private static void widen(NavigableMap<Long, ClusteringBound<?>[]> spans, long window, ClusteringPrefix<?> position, ClusteringComparator comparator)
    {
        ClusteringBound<?>[] span = spans.get(window);
        if (span == null)
        {
            spans.put(window, new ClusteringBound<?>[]{ startOf(position), endOf(position) });
            return;
        }
        // The partition is read in clustering order, so a window's first element fixes its start and
        // only the end can move.
        if (comparator.compare(position, span[1]) > 0)
            span[1] = endOf(position);
    }

    // retainable(): the position may point into a reused read buffer; a span outlives the element.
    private static ClusteringBound<?> startOf(ClusteringPrefix<?> position)
    {
        return ClusteringBound.inclusiveStartOf(position.retainable());
    }

    private static ClusteringBound<?> endOf(ClusteringPrefix<?> position)
    {
        return ClusteringBound.inclusiveEndOf(position.retainable());
    }

    /** One window's elements of a (re-read) partition body, produced lazily. */
    private static final class WindowFilter extends AbstractIterator<Unfiltered>
    {
        private final UnfilteredRowIterator source;
        private final long window;
        private final LongUnaryOperator windowStartOfMillis;
        private final TimeUnit tableResolution;
        private final LongConsumer bytesRead;
        private Unfiltered found;

        WindowFilter(UnfilteredRowIterator source, long window, LongUnaryOperator windowStartOfMillis,
                     TimeUnit tableResolution, LongConsumer bytesRead)
        {
            this.source = source;
            this.window = window;
            this.windowStartOfMillis = windowStartOfMillis;
            this.tableResolution = tableResolution;
            this.bytesRead = bytesRead;
        }

        @Override
        protected Unfiltered computeNext()
        {
            while (source.hasNext())
            {
                Unfiltered unfiltered = source.next();
                bytesRead.accept(sizeOf(unfiltered));
                found = null;
                forEachPiece(unfiltered, windowStartOfMillis, tableResolution, (w, piece) -> {
                    if (w == window)
                        found = piece;
                });
                if (found != null)
                    return found;
            }
            return endOfData();
        }
    }

    /** One window's view of a partition: original key/columns/stats + that window's content. */
    private static final class WindowSlice extends AbstractUnfilteredRowIterator
    {
        private final Iterator<Unfiltered> content;

        WindowSlice(UnfilteredRowIterator source, DeletionTime partitionDeletion, Row staticRow, Iterator<Unfiltered> content)
        {
            super(source.metadata(),
                  source.partitionKey(),
                  partitionDeletion,
                  source.columns(),
                  staticRow,
                  false,
                  source.stats());
            this.content = content;
        }

        @Override
        protected Unfiltered computeNext()
        {
            return content.hasNext() ? content.next() : endOfData();
        }
    }

    private static int sizeOf(Unfiltered unfiltered)
    {
        // Range tombstone markers carry only a bound and one or two deletion times; a flat estimate is
        // enough for a buffer budget whose only job is to stop unbounded growth.
        return unfiltered instanceof Row ? ((Row) unfiltered).dataSize() : 64;
    }

    private static long windowOf(long rawTimestamp, LongUnaryOperator windowStartOfMillis, TimeUnit tableResolution)
    {
        return windowStartOfMillis.applyAsLong(TimeUnit.MILLISECONDS.convert(rawTimestamp, tableResolution));
    }
}
