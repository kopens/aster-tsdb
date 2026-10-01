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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongUnaryOperator;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.btree.BTree;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the D2 routing rules of {@link WindowRoutingIterator}: every Unfiltered of one
 * partition is routed to the time window of its max cell write-timestamp (rows) or range-tombstone
 * deletion timestamp (markers), preserving clustering order within each output window.
 */
public class WindowRoutingIteratorTest
{
    private static final long HOUR_MS = 3_600_000L;
    /** An arbitrary hour-aligned base well in the past (2020-01-01T00:00Z). */
    private static final long BASE = 1_577_836_800_000L;
    private static final LongUnaryOperator WINDOW = ms -> ms - Math.floorMod(ms, HOUR_MS);

    private static TableMetadata metadata;
    private static ColumnMetadata value;
    private static ColumnMetadata value2;
    private static ColumnMetadata unit;
    private static ColumnMetadata unit2;
    private static DecoratedKey key;

    @BeforeClass
    public static void setUpClazz()
    {
        DatabaseDescriptor.daemonInitialization();
        metadata = TableMetadata.builder("t3ks", "t3tbl")
                                .partitioner(Murmur3Partitioner.instance)
                                .addPartitionKeyColumn("tag_id", UTF8Type.instance)
                                .addClusteringColumn("timestamp", TimestampType.instance)
                                .addRegularColumn("value", DoubleType.instance)
                                .addRegularColumn("value2", DoubleType.instance)
                                .addStaticColumn("unit", UTF8Type.instance)
                                .addStaticColumn("unit2", UTF8Type.instance)
                                .build();
        value = metadata.getColumn(ByteBufferUtil.bytes("value"));
        value2 = metadata.getColumn(ByteBufferUtil.bytes("value2"));
        unit = metadata.getColumn(ByteBufferUtil.bytes("unit"));
        unit2 = metadata.getColumn(ByteBufferUtil.bytes("unit2"));
        key = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes("tag-1"));
    }

    private static Clustering<?> ck(long eventMs)
    {
        return Clustering.make(TimestampType.instance.decompose(new Date(eventMs)));
    }

    private static long micros(long ms)
    {
        return ms * 1000;
    }

    /** Single-cell row whose routing timestamp is the value cell's write timestamp. */
    private static Row row(long eventMs, long writeTsMs)
    {
        return BTreeRow.singleCellRow(ck(eventMs), BufferCell.live(value, micros(writeTsMs), DoubleType.instance.decompose(1.0)));
    }

    private static Row twoCellRow(long eventMs, long writeTsMsA, long writeTsMsB)
    {
        BufferCell a = BufferCell.live(value, micros(writeTsMsA), DoubleType.instance.decompose(1.0));
        BufferCell b = BufferCell.live(value2, micros(writeTsMsB), DoubleType.instance.decompose(2.0));
        return BTreeRow.create(ck(eventMs), org.apache.cassandra.db.LivenessInfo.EMPTY, Row.Deletion.LIVE,
                               BTree.build(List.of(a, b)));
    }

    private static UnfilteredRowIterator partition(List<Unfiltered> content)
    {
        return new Util.UnfilteredSource(metadata, key, null, content.iterator());
    }

    private static NavigableMap<Long, List<Unfiltered>> route(List<Unfiltered> content)
    {
        return WindowRoutingIterator.route(partition(content), WINDOW, TimeUnit.MICROSECONDS);
    }

    @Test
    public void singleWindowRowsStayTogether()
    {
        List<Unfiltered> rows = List.of(row(BASE + 1, BASE + 1), row(BASE + 2, BASE + 2), row(BASE + 3, BASE + 3));
        NavigableMap<Long, List<Unfiltered>> routed = route(rows);
        assertEquals(1, routed.size());
        List<Unfiltered> only = routed.get(BASE);
        assertEquals(3, only.size());
        for (int i = 0; i < 3; i++)
            assertSame(rows.get(i), only.get(i));
    }

    @Test
    public void rowsSplitAcrossTwoWindows()
    {
        Row w1a = row(BASE + 1, BASE + 1);
        Row w2 = row(BASE + 2, BASE + HOUR_MS + 5);   // late event written in the next window
        Row w1b = row(BASE + 3, BASE + 10);
        NavigableMap<Long, List<Unfiltered>> routed = route(List.of(w1a, w2, w1b));
        assertEquals(2, routed.size());
        assertEquals(List.of(w1a, w1b), routed.get(BASE));
        assertEquals(List.of(w2), routed.get(BASE + HOUR_MS));
    }

    /**
     * The C1 defect, at routing granularity: a row whose two cells were written in different windows
     * used to travel whole to the window of its max timestamp, leaving the older cell's timestamp in a
     * newer window's sstable - which then had min and max in different windows and could never freeze.
     * Each cell must now go to the window its own timestamp names.
     */
    @Test
    public void multiCellRowSplitsPerCellWindow()
    {
        Row r = twoCellRow(BASE + 1, BASE + 1, BASE + HOUR_MS + 1);
        NavigableMap<Long, List<Unfiltered>> routed = route(List.of(r));
        assertEquals(2, routed.size());

        Row first = (Row) routed.get(BASE).get(0);
        assertEquals(1, first.columnCount());
        assertEquals(micros(BASE + 1), first.getCell(value).timestamp());

        Row second = (Row) routed.get(BASE + HOUR_MS).get(0);
        assertEquals(1, second.columnCount());
        assertEquals(micros(BASE + HOUR_MS + 1), second.getCell(value2).timestamp());

        // Both pieces keep the row's clustering, so read-time merge reassembles the original row.
        assertEquals(r.clustering(), first.clustering());
        assertEquals(r.clustering(), second.clustering());
    }

    /** No piece may carry a timestamp from outside its own window - that is the whole containment invariant. */
    @Test
    public void everyPieceIsContainedInItsWindow()
    {
        Row r = twoCellRow(BASE + 1, BASE + 1, BASE + HOUR_MS + 1);
        NavigableMap<Long, List<Unfiltered>> routed = route(List.of(r));
        for (java.util.Map.Entry<Long, List<Unfiltered>> entry : routed.entrySet())
            for (Unfiltered u : entry.getValue())
                for (Cell<?> cell : ((Row) u).cells())
                    assertEquals("cell " + cell + " landed outside its window",
                                 (long) entry.getKey(), WINDOW.applyAsLong(cell.timestamp() / 1000));
    }

    @Test
    public void rowDeletionOnlyRoutesByDeletionTime()
    {
        long delMs = BASE + HOUR_MS + 42;
        Row tombstone = BTreeRow.emptyDeletedRow(ck(BASE + 1), Row.Deletion.regular(DeletionTime.build(micros(delMs), 1000)));
        NavigableMap<Long, List<Unfiltered>> routed = route(List.of(tombstone));
        assertEquals(1, routed.size());
        assertEquals(List.of(tombstone), routed.get(BASE + HOUR_MS));
    }

    @Test
    public void openClosePairTravelTogether()
    {
        long delMs = BASE + 7;
        DeletionTime del = DeletionTime.build(micros(delMs), 1000);
        Unfiltered open = RangeTombstoneBoundMarker.inclusiveOpen(false, ck(BASE + 1), del);
        Row mid = row(BASE + 2, BASE + 2);
        Unfiltered close = RangeTombstoneBoundMarker.inclusiveClose(false, ck(BASE + 3), del);
        NavigableMap<Long, List<Unfiltered>> routed = route(List.of(open, mid, close));
        assertEquals(1, routed.size());
        assertEquals(List.of(open, mid, close), routed.get(BASE));
    }

    @Test
    public void boundaryMarkerSplitsWhenDeletionsDiverge()
    {
        long closeDelMs = BASE + 5;                    // window 1
        long openDelMs = BASE + HOUR_MS + 5;           // window 2
        RangeTombstoneBoundaryMarker boundary =
            RangeTombstoneBoundaryMarker.exclusiveCloseInclusiveOpen(false,
                                                                     ck(BASE + 30),
                                                                     DeletionTime.build(micros(closeDelMs), 1000),
                                                                     DeletionTime.build(micros(openDelMs), 1000));
        NavigableMap<Long, List<Unfiltered>> routed = route(List.of(boundary));
        assertEquals(2, routed.size());

        List<Unfiltered> w1 = routed.get(BASE);
        assertEquals(1, w1.size());
        RangeTombstoneBoundMarker closeMarker = (RangeTombstoneBoundMarker) w1.get(0);
        assertTrue(closeMarker.isClose(false));
        assertEquals(micros(closeDelMs), closeMarker.deletionTime().markedForDeleteAt());

        List<Unfiltered> w2 = routed.get(BASE + HOUR_MS);
        assertEquals(1, w2.size());
        RangeTombstoneBoundMarker openMarker = (RangeTombstoneBoundMarker) w2.get(0);
        assertTrue(openMarker.isOpen(false));
        assertEquals(micros(openDelMs), openMarker.deletionTime().markedForDeleteAt());
    }

    @Test
    public void boundaryMarkerStaysWholeWhenDeletionsShareWindow()
    {
        RangeTombstoneBoundaryMarker boundary =
            RangeTombstoneBoundaryMarker.exclusiveCloseInclusiveOpen(false,
                                                                     ck(BASE + 30),
                                                                     DeletionTime.build(micros(BASE + 5), 1000),
                                                                     DeletionTime.build(micros(BASE + 9), 1000));
        NavigableMap<Long, List<Unfiltered>> routed = route(List.of(boundary));
        assertEquals(1, routed.size());
        assertEquals(List.of((Unfiltered) boundary), routed.get(BASE));
    }

    @Test
    public void farFutureRowRoutesToItsOwnWindow()
    {
        long future = BASE + 365L * 24 * HOUR_MS;
        Row r = row(BASE + 1, future + 3);
        NavigableMap<Long, List<Unfiltered>> routed = route(List.of(r));
        assertEquals(1, routed.size());
        assertEquals(List.of(r), routed.get(WINDOW.applyAsLong(future + 3)));
    }

    @Test
    public void exactBoundaryTimestampRoutesToItsWindow()
    {
        Row r = row(BASE + 1, BASE + HOUR_MS);         // written exactly at the boundary
        NavigableMap<Long, List<Unfiltered>> routed = route(List.of(r));
        assertEquals(1, routed.size());
        assertEquals(List.of(r), routed.get(BASE + HOUR_MS));
    }

    /**
     * Primary-key liveness is just another timestamped element: it goes to its own window, and the older
     * cell stays in its own. The piece left in the cell's window legitimately has NO liveness - exactly
     * what an UPDATE-created row looks like - and none is synthesised, which would change delete semantics.
     */
    @Test
    public void livenessInfoRoutesSeparatelyFromOlderCells()
    {
        BufferCell old = BufferCell.live(value, micros(BASE + 1), DoubleType.instance.decompose(1.0));
        Row r = BTreeRow.create(ck(BASE + 1),
                                org.apache.cassandra.db.LivenessInfo.create(micros(BASE + HOUR_MS + 8)),
                                Row.Deletion.LIVE,
                                BTree.build(List.of(old)));
        NavigableMap<Long, List<Unfiltered>> routed = route(List.of(r));
        assertEquals(2, routed.size());

        Row cellPiece = (Row) routed.get(BASE).get(0);
        assertTrue("no liveness must be synthesised into the cell's window", cellPiece.primaryKeyLivenessInfo().isEmpty());
        assertEquals(micros(BASE + 1), cellPiece.getCell(value).timestamp());

        Row livenessPiece = (Row) routed.get(BASE + HOUR_MS).get(0);
        assertEquals(micros(BASE + HOUR_MS + 8), livenessPiece.primaryKeyLivenessInfo().timestamp());
        assertEquals(0, livenessPiece.columnCount());
    }

    /** A row deletion carries its own timestamp too, and must not drag newer cells into its window. */
    @Test
    public void rowDeletionRoutesSeparatelyFromNewerCells()
    {
        BufferCell newer = BufferCell.live(value, micros(BASE + HOUR_MS + 10), DoubleType.instance.decompose(1.0));
        Row r = BTreeRow.create(ck(BASE + 1),
                                org.apache.cassandra.db.LivenessInfo.EMPTY,
                                Row.Deletion.regular(DeletionTime.build(micros(BASE + 5), 1000)),
                                BTree.build(List.of(newer)));
        NavigableMap<Long, List<Unfiltered>> routed = route(List.of(r));
        assertEquals(2, routed.size());

        Row deletionPiece = (Row) routed.get(BASE).get(0);
        assertEquals(micros(BASE + 5), deletionPiece.deletion().time().markedForDeleteAt());
        assertEquals(0, deletionPiece.columnCount());

        Row cellPiece = (Row) routed.get(BASE + HOUR_MS).get(0);
        assertTrue(cellPiece.deletion().isLive());
        assertEquals(micros(BASE + HOUR_MS + 10), cellPiece.getCell(value).timestamp());
    }

    /**
     * A row with nothing at all in a window must not be emitted into that window, and no piece that IS
     * emitted may be an empty row.
     * <p>
     * Deliberately a row that takes {@code splitRow}'s SPLIT path - its two cells were written two hours
     * apart. Asserting against a single-window row, as this test used to, exercises only the fast path,
     * which returns the caller's row untouched and structurally cannot emit an empty piece or an extra
     * window: the guarantee named here was never tested.
     */
    @Test
    public void windowsWithNothingGetNoRow()
    {
        Row r = twoCellRow(BASE + 1, BASE + 1, BASE + 2 * HOUR_MS + 1);
        NavigableMap<Long, List<Unfiltered>> routed = route(List.of(r));

        // The hour in between is named by no timestamp of this row, so it gets no bucket at all.
        assertEquals(List.of(BASE, BASE + 2 * HOUR_MS), List.copyOf(routed.keySet()));
        for (List<Unfiltered> bucket : routed.values())
            for (Unfiltered u : bucket)
                assertFalse("an empty piece was emitted into a window", ((Row) u).isEmpty());
    }

    /**
     * A row carrying no timestamp at all is dropped, deliberately and explicitly. Every element a row
     * can hold is timestamped, so "no timestamp" is exactly "no content", there is no window to route it
     * to, and the sstable writers refuse an empty row anyway. The previous revision reached the same
     * outcome by falling through a split path that happened to build no pieces; this pins it.
     */
    @Test
    public void emptyRowIsDroppedRatherThanRouted()
    {
        Row empty = BTreeRow.create(ck(BASE + 1), org.apache.cassandra.db.LivenessInfo.EMPTY,
                                    Row.Deletion.LIVE, BTree.empty());
        assertTrue(empty.isEmpty());
        assertTrue(WindowRoutingIterator.splitRow(empty, WINDOW, TimeUnit.MICROSECONDS).isEmpty());
        assertTrue(route(List.of(empty)).isEmpty());
    }

    // ---------------------------------------------------------------------------------------------
    // slices(): the partition header. Until now this method - the whole point of the D3 revision - had
    // no test at all, and every partition built here used a null static row.
    // ---------------------------------------------------------------------------------------------

    /** A partition source that can carry a partition-level deletion and a static row. */
    private static final class Source extends org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator
    {
        private final java.util.Iterator<Unfiltered> content;

        Source(DeletionTime partitionDeletion, Row staticRow, List<Unfiltered> content)
        {
            this(WindowRoutingIteratorTest.metadata, partitionDeletion, staticRow, content);
        }

        Source(TableMetadata md, DeletionTime partitionDeletion, Row staticRow, List<Unfiltered> content)
        {
            // Qualified: AbstractUnfilteredRowIterator has its own protected `metadata` field, which
            // would otherwise shadow the outer class's static one inside this super() call.
            super(md, WindowRoutingIteratorTest.key, partitionDeletion,
                  md.regularAndStaticColumns(),
                  staticRow == null ? org.apache.cassandra.db.rows.Rows.EMPTY_STATIC_ROW : staticRow,
                  false, org.apache.cassandra.db.rows.EncodingStats.NO_STATS);
            this.content = content.iterator();
        }

        @Override
        protected Unfiltered computeNext()
        {
            return content.hasNext() ? content.next() : endOfData();
        }
    }

    private static Row staticRow(long unitWriteMs, long unit2WriteMs)
    {
        BufferCell a = BufferCell.live(unit, micros(unitWriteMs), UTF8Type.instance.decompose("C"));
        BufferCell b = BufferCell.live(unit2, micros(unit2WriteMs), UTF8Type.instance.decompose("F"));
        return BTreeRow.create(Clustering.STATIC_CLUSTERING, org.apache.cassandra.db.LivenessInfo.EMPTY,
                               Row.Deletion.LIVE, BTree.build(List.of(a, b)));
    }

    private static NavigableMap<Long, UnfilteredRowIterator> slices(DeletionTime partitionDeletion, Row staticRow, List<Unfiltered> content)
    {
        return WindowRoutingIterator.slices(new Source(partitionDeletion, staticRow, content), WINDOW, TimeUnit.MICROSECONDS);
    }

    /**
     * The overflow warning must name every table that overflows, not just the first one per minute.
     * <p>
     * {@link NoSpamLogger} keeps one rate-limiter per key string, so a constant key makes whichever
     * table overflowed first that minute silence all the others -- and a silenced table is
     * indistinguishable from a table that never overflowed. That is the only question the message
     * exists to answer: an operator looking at parked windows needs to know which tables are hitting
     * the buffer. On a production node this cost real diagnosis time -- four tables appeared in the
     * log while ten had parked windows, which reads as two different root causes and is not.
     * <p>
     * Both partitions are routed back-to-back, far inside the one-minute interval, so a per-table key
     * is the only thing that lets the second one through.
     */
    @Test
    public void overflowWarningNamesEveryOverflowingTableWithinOneInterval()
    {
        // Both names are unique to this test. The rate-limiter is process-wide and never resets
        // inside a run, so reusing the shared `metadata` would make the assertion depend on whether
        // an earlier test in this class had already spent that key's minute.
        TableMetadata first = overflowTable("t3tbl_spam_a");
        TableMetadata other = overflowTable("t3tbl_spam_b");

        long saved = WindowRoutingIterator.maxBufferedBytesPerPartition;
        ch.qos.logback.classic.Logger routingLogger =
            (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(WindowRoutingIterator.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        routingLogger.addAppender(appender);
        try
        {
            WindowRoutingIterator.maxBufferedBytesPerPartition = 1;   // overflow on the first row
            List<Unfiltered> spanning = List.of(row(BASE + 1, BASE + 1), row(BASE + 2, BASE + HOUR_MS + 2));

            WindowRoutingIterator.slices(new Source(first, DeletionTime.LIVE, null, spanning),
                                         WINDOW, TimeUnit.MICROSECONDS);
            WindowRoutingIterator.slices(new Source(other, DeletionTime.LIVE, null, spanning),
                                         WINDOW, TimeUnit.MICROSECONDS);

            String logged = appender.list.stream()
                                         .filter(e -> e.getLevel() == Level.WARN)
                                         .map(ILoggingEvent::getFormattedMessage)
                                         .collect(java.util.stream.Collectors.joining("\n"));
            assertTrue("the first overflowing table must be named; got:\n" + logged,
                       logged.contains("t3ks.t3tbl_spam_a "));
            assertTrue("the second table overflowed in the same interval and must not be suppressed;"
                       + " got:\n" + logged,
                       logged.contains("t3ks.t3tbl_spam_b "));
        }
        finally
        {
            routingLogger.detachAppender(appender);
            WindowRoutingIterator.maxBufferedBytesPerPartition = saved;
        }
    }

    private static TableMetadata overflowTable(String name)
    {
        return TableMetadata.builder("t3ks", name)
                            .partitioner(Murmur3Partitioner.instance)
                            .addPartitionKeyColumn("tag_id", UTF8Type.instance)
                            .addClusteringColumn("timestamp", TimestampType.instance)
                            .addRegularColumn("value", DoubleType.instance)
                            .build();
    }

    /** Static cells are individually timestamped, so they split exactly like regular cells. */
    @Test
    public void staticCellsRouteToTheirOwnWindows()
    {
        NavigableMap<Long, UnfilteredRowIterator> sliced =
            slices(DeletionTime.LIVE, staticRow(BASE + 3, BASE + HOUR_MS + 3), List.of());
        assertEquals(2, sliced.size());

        Row firstStatic = sliced.get(BASE).staticRow();
        assertEquals(1, firstStatic.columnCount());
        assertEquals(micros(BASE + 3), firstStatic.getCell(unit).timestamp());

        Row secondStatic = sliced.get(BASE + HOUR_MS).staticRow();
        assertEquals(1, secondStatic.columnCount());
        assertEquals(micros(BASE + HOUR_MS + 3), secondStatic.getCell(unit2).timestamp());
    }

    /**
     * The partition deletion goes to the window of its OWN timestamp, not to max(deletion, staticRow) -
     * pairing it with a newer static row used to stamp an old deletion timestamp into a newer window's
     * sstable metadata, which is the second path into the C1 loop.
     */
    @Test
    public void partitionDeletionRoutesByItsOwnTimestampOnly()
    {
        DeletionTime deletion = DeletionTime.build(micros(BASE + 5), 1000);
        NavigableMap<Long, UnfilteredRowIterator> sliced =
            slices(deletion, staticRow(BASE + HOUR_MS + 7, BASE + HOUR_MS + 8), List.of());
        assertEquals(2, sliced.size());

        assertEquals(deletion, sliced.get(BASE).partitionLevelDeletion());
        assertTrue("the static row's window must not inherit the old deletion timestamp",
                   sliced.get(BASE + HOUR_MS).partitionLevelDeletion().isLive());
    }

    /**
     * Exactly one slice carries the partition deletion and exactly one carries the static row; the rest
     * get LIVE/EMPTY, which MetadataCollector ignores.
     * <p>
     * Three windows, one piece of the partition in each: the deletion in window 1, both static cells in
     * window 2, one regular row in window 3. Passing a {@code null} static row - as this test used to -
     * made the static half vacuous: {@code Source} substitutes {@code EMPTY_STATIC_ROW}, which is
     * exactly what the assertion looks for, so it held no matter what routing did with a real one.
     */
    @Test
    public void nonHeaderSlicesCarryNoHeader()
    {
        DeletionTime deletion = DeletionTime.build(micros(BASE + 5), 1000);
        NavigableMap<Long, UnfilteredRowIterator> sliced =
            slices(deletion, staticRow(BASE + HOUR_MS + 3, BASE + HOUR_MS + 4),
                   List.of(row(BASE + 1, BASE + 2 * HOUR_MS + 1)));
        assertEquals(3, sliced.size());

        assertEquals(deletion, sliced.get(BASE).partitionLevelDeletion());
        assertTrue("the deletion's window must not also inherit the static row",
                   sliced.get(BASE).staticRow().isEmpty());

        assertTrue(sliced.get(BASE + HOUR_MS).partitionLevelDeletion().isLive());
        assertEquals(2, sliced.get(BASE + HOUR_MS).staticRow().columnCount());

        assertTrue(sliced.get(BASE + 2 * HOUR_MS).partitionLevelDeletion().isLive());
        assertTrue(sliced.get(BASE + 2 * HOUR_MS).staticRow().isEmpty());
    }

    /** A partition with nothing in it at all yields no slices. */
    @Test
    public void emptyPartitionYieldsNoSlices()
    {
        assertTrue(slices(DeletionTime.LIVE, null, List.of()).isEmpty());
    }

    // ---------------------------------------------------------------------------------------------
    // The degraded overflow path: a partition too large to window-route is written UNSPLIT. The prefix
    // that has already been buffered must go out exactly as it came in - not as re-sorted split pieces.
    // ---------------------------------------------------------------------------------------------

    private static List<Unfiltered> drain(UnfilteredRowIterator slice)
    {
        List<Unfiltered> out = new ArrayList<>();
        while (slice.hasNext())
            out.add(slice.next());
        return out;
    }

    /**
     * C1: a partition over the routing budget must not be written with the same clustering twice.
     * <p>
     * The old overflow path concatenated the per-window buckets and re-sorted them. Those buckets hold
     * split <em>pieces</em>, and a row whose cells straddle a window boundary contributes one piece per
     * window, all carrying the row's own {@code Clustering} - which {@code ClusteringComparator} ranks
     * equal, so the sort kept both and {@code SortedTablePartitionWriter.addUnfiltered} (which does not
     * validate monotonicity) wrote them both. That is the exact INSERT-then-UPDATE shape this release
     * exists to fix, reintroduced on the degraded path, and every later split-refreeze re-propagated it.
     */
    @Test
    public void overflowingPartitionIsWrittenUnsplitAndNeverDuplicatesAClustering()
    {
        long saved = WindowRoutingIterator.maxBufferedBytesPerPartition;
        try
        {
            // INSERT ... USING TIMESTAMP old, then UPDATE ... SET val0 USING TIMESTAMP new, merged into
            // one row - placed FIRST so it lands in the buffered prefix, the only place the old
            // concatenate-and-sort could duplicate it.
            Row straddling = twoCellRow(BASE + 1, BASE + 1, BASE + HOUR_MS + 1);
            Row second = row(BASE + 2, BASE + 2);
            Row third = row(BASE + 3, BASE + HOUR_MS + 3);

            WindowRoutingIterator.maxBufferedBytesPerPartition = 1;   // overflow right after the first row
            NavigableMap<Long, UnfilteredRowIterator> sliced =
                slices(DeletionTime.LIVE, null, List.of(straddling, second, third));

            assertEquals("an overflowing partition yields exactly one, unsplit slice", 1, sliced.size());
            List<Unfiltered> emitted = drain(sliced.firstEntry().getValue());

            // The source Unfiltereds, untouched and in source order - so no clustering can repeat.
            assertEquals(3, emitted.size());
            assertSame(straddling, emitted.get(0));
            assertSame(second, emitted.get(1));
            assertSame(third, emitted.get(2));
            assertStrictlyIncreasing(emitted);
        }
        finally
        {
            WindowRoutingIterator.maxBufferedBytesPerPartition = saved;
        }
    }

    /**
     * I4: the overflow path must not reorder a range-tombstone boundary's two halves.
     * <p>
     * {@code INCL_END_BOUND} and {@code EXCL_START_BOUND} share {@code comparison == 3}, so they compare
     * equal; the old path decomposed a boundary whose deletions fell in different windows and then
     * concatenated the buckets in window-key order, and when the OPEN deletion was the older of the two
     * its half came out first. A stable sort cannot repair that, and the result -
     * {@code open(a,D1), open(x,D2), close(x,D1), close(b,D2)} - silently drops D2's coverage of
     * {@code [x,b]}, resurrecting deleted rows. Written unsplit, the boundary is never decomposed at all.
     */
    @Test
    public void overflowingPartitionKeepsRangeTombstoneBoundariesIntact()
    {
        long saved = WindowRoutingIterator.maxBufferedBytesPerPartition;
        try
        {
            DeletionTime newer = DeletionTime.build(micros(BASE + HOUR_MS + 5), 1000);   // window 2
            DeletionTime older = DeletionTime.build(micros(BASE + 5), 1000);             // window 1
            Unfiltered open = RangeTombstoneBoundMarker.inclusiveOpen(false, ck(BASE + 10), newer);
            // closes the newer deletion and opens the older one: closeWindow > openWindow, the inversion.
            RangeTombstoneBoundaryMarker boundary =
                RangeTombstoneBoundaryMarker.exclusiveCloseInclusiveOpen(false, ck(BASE + 20), newer, older);
            Unfiltered close = RangeTombstoneBoundMarker.inclusiveClose(false, ck(BASE + 30), older);

            // sizeOf() charges a marker a flat 64 bytes, so 100 buffers two and overflows on the third.
            WindowRoutingIterator.maxBufferedBytesPerPartition = 100;
            NavigableMap<Long, UnfilteredRowIterator> sliced =
                slices(DeletionTime.LIVE, null, List.of(open, boundary, close));

            assertEquals(1, sliced.size());
            List<Unfiltered> emitted = drain(sliced.firstEntry().getValue());
            assertEquals(3, emitted.size());
            assertSame(open, emitted.get(0));
            assertSame("the boundary must still be whole, not decomposed and reordered", boundary, emitted.get(1));
            assertSame(close, emitted.get(2));
        }
        finally
        {
            WindowRoutingIterator.maxBufferedBytesPerPartition = saved;
        }
    }

    /** The overflow slice keeps the partition header, since it is the only slice there is. */
    @Test
    public void overflowingPartitionKeepsItsHeader()
    {
        long saved = WindowRoutingIterator.maxBufferedBytesPerPartition;
        try
        {
            DeletionTime deletion = DeletionTime.build(micros(BASE + 5), 1000);
            Row statics = staticRow(BASE + 3, BASE + HOUR_MS + 3);
            WindowRoutingIterator.maxBufferedBytesPerPartition = 1;
            NavigableMap<Long, UnfilteredRowIterator> sliced =
                slices(deletion, statics, List.of(row(BASE + 1, BASE + 1), row(BASE + 2, BASE + HOUR_MS + 2)));

            assertEquals(1, sliced.size());
            UnfilteredRowIterator only = sliced.firstEntry().getValue();
            assertEquals(deletion, only.partitionLevelDeletion());
            assertEquals("the static row goes out whole, not split per cell", 2, only.staticRow().columnCount());
        }
        finally
        {
            WindowRoutingIterator.maxBufferedBytesPerPartition = saved;
        }
    }

    /**
     * The bounded-memory route split-refreeze takes for a partition over the routing budget: read the
     * partition again once per window, restricted to that window's clustering span, and keep only that
     * window's elements. Each window's output must be exactly what in-memory routing produces for it
     * (same header, same unfiltereds in the same order); anything else loses or resurrects data.
     * <p>
     * Random partitions with rows that straddle windows, overlapping range tombstones whose deletions
     * fall in different windows (so the source carries boundary markers and the per-window spans cut
     * through other windows' tombstones), a split static row and a partition deletion. The source is a
     * real {@link PartitionUpdate}, so slicing and range-tombstone clipping at the span edges are the
     * storage engine's own, not a test double's.
     */
    @Test
    public void rereadingEachWindowMatchesInMemoryRouting()
    {
        for (long s = 1; s <= 300; s++)
        {
            long seed = s;
            Random random = new Random(seed);
            PartitionUpdate update = randomPartition(random);

            NavigableMap<Long, UnfilteredRowIterator> expected =
                WindowRoutingIterator.slices(update.unfilteredIterator(), WINDOW, TimeUnit.MICROSECONDS);

            NavigableMap<Long, List<Unfiltered>> actualContent = new TreeMap<>();
            NavigableMap<Long, DeletionTime> actualDeletion = new TreeMap<>();
            NavigableMap<Long, Row> actualStatic = new TreeMap<>();
            WindowRoutingIterator.rereadSlices(slices -> update.unfilteredIterator(ColumnFilter.all(metadata), slices, false),
                                               WINDOW, TimeUnit.MICROSECONDS, bytes -> {},
                                               (window, slice) -> {
                                                   assertFalse("window " + window + " emitted twice (seed " + seed + ')',
                                                               actualContent.containsKey(window));
                                                   actualDeletion.put(window, slice.partitionLevelDeletion());
                                                   actualStatic.put(window, slice.staticRow());
                                                   actualContent.put(window, drain(slice));
                                               });

            assertEquals("windows (seed " + seed + ')', expected.keySet(), actualContent.keySet());
            for (Map.Entry<Long, UnfilteredRowIterator> entry : expected.entrySet())
            {
                long window = entry.getKey();
                UnfilteredRowIterator want = entry.getValue();
                String where = "window " + window + " (seed " + seed + ')';
                assertEquals("partition deletion, " + where, want.partitionLevelDeletion(), actualDeletion.get(window));
                assertEquals("static row, " + where, want.staticRow(), actualStatic.get(window));
                assertEquals("content, " + where, drain(want), actualContent.get(window));
            }
        }
    }

    /** Twenty rows ten seconds apart, write timestamps over four windows, plus tombstones and a header. */
    private static PartitionUpdate randomPartition(Random random)
    {
        PartitionUpdate.Builder builder = new PartitionUpdate.Builder(metadata, key, metadata.regularAndStaticColumns(), 32);
        for (int i = 0; i < 20; i++)
        {
            long event = BASE + i * 10_000L;
            if (random.nextInt(4) == 0)
                continue;                                               // gaps, so spans do not tile
            if (random.nextInt(3) == 0)
                builder.add(twoCellRow(event, randomWrite(random), randomWrite(random)));
            else
                builder.add(row(event, randomWrite(random)));
        }
        int tombstones = random.nextInt(4);
        for (int t = 0; t < tombstones; t++)
        {
            long a = BASE + random.nextInt(20) * 10_000L + random.nextInt(3) * 5_000L;
            long b = a + random.nextInt(8) * 10_000L;
            DeletionTime deletion = DeletionTime.build(micros(randomWrite(random)), 1000);
            builder.add(new RangeTombstone(Slice.make(ck(a), ck(b)), deletion));
        }
        if (random.nextInt(3) == 0)
            builder.add(staticRow(randomWrite(random), randomWrite(random)));
        if (random.nextInt(5) == 0)
            builder.addPartitionDeletion(DeletionTime.build(micros(BASE - 1), 1000));   // shadows nothing live
        return builder.build();
    }

    private static long randomWrite(Random random)
    {
        return BASE + random.nextInt(4) * HOUR_MS + 1 + random.nextInt(1000);
    }

    private static void assertStrictlyIncreasing(List<Unfiltered> unfiltereds)
    {
        for (int i = 1; i < unfiltereds.size(); i++)
            assertTrue("clustering must strictly increase at " + i + ": " + unfiltereds,
                       metadata.comparator.compare(unfiltereds.get(i - 1).clustering(),
                                                   unfiltereds.get(i).clustering()) < 0);
    }

    @Test
    public void clusteringOrderPreservedWithinEachWindow()
    {
        List<Unfiltered> content = new ArrayList<>();
        for (int i = 0; i < 10; i++)
            content.add(row(BASE + i, (i % 2 == 0 ? BASE : BASE + HOUR_MS) + i));
        NavigableMap<Long, List<Unfiltered>> routed = route(content);
        assertEquals(2, routed.size());
        for (List<Unfiltered> bucket : routed.values())
        {
            ByteBuffer prev = null;
            for (Unfiltered u : bucket)
            {
                ByteBuffer cur = ((Row) u).clustering().bufferAt(0);
                if (prev != null)
                    assertTrue(TimestampType.instance.compare(prev, cur) < 0);
                prev = cur;
            }
        }
    }
}
