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

package org.apache.cassandra.db.timeseries;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.DroppedColumn;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.BulkIterator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;

/**
 * Encodes the rows of one partition of a time-series table (one timestamp clustering column) into a chunk-format-v4
 * payload such that decoding gives back rows {@link Row#equals equal} to the input -- values, the primary-key
 * liveness, every cell's timestamp, TTL and local deletion time, and cell tombstones. This is what lets a columnar
 * frozen sstable ({@code tscol}, docs/superpowers/specs/2026-11-04-columnar-frozen-sstable-design.md section 4.2)
 * stand in for a row-format one: replicas compare digests, merge with other sstables and purge on compaction by
 * exactly these fields, so anything less than an exact round trip would make a columnar replica disagree with a
 * row-format one.
 * <p>
 * The v4 byte layout is untouched. The metadata travels as extra columns of the same chunk whose names start with
 * NUL, which no CQL identifier can contain, so they can never collide with a user column. On ordinary data they cost
 * almost nothing: every cell written with its row's timestamp and no TTL gives an all-zero writetime-delta column
 * (v4 CONSTANT) and all-null TTL, deletion-time and tombstone columns (v4 ALL_NULL).
 * <p>
 * What the format cannot carry makes {@link #encode} return {@code null} and the caller keeps the partition in row
 * form: row deletions, complex (non-frozen) columns and their deletions, counters, duplicate clusterings, and more
 * than {@link ColumnarChunkCodec#MAX_ROWS} rows.
 */
public final class RowBlockCodec
{
    /** The primary-key liveness timestamp, or null when the row has none (a row made by UPDATE alone). */
    public static final String ROW_TS = "\0row.ts";
    /** The primary-key liveness TTL and local expiration time, present only for expiring liveness. */
    public static final String ROW_TTL = "\0row.ttl";
    public static final String ROW_LDT = "\0row.ldt";
    /** The timestamp cell writetimes are stored relative to, present only when the row has no liveness. */
    public static final String ROW_BASE = "\0row.base";

    private static final String WT = "\0wt.";
    private static final String TTL = "\0ttl.";
    private static final String LDT = "\0ldt.";
    private static final String DEL = "\0del.";
    /**
     * Present (and all null, so O(1)) when every cell of the column in this block is live and shares its row's
     * timestamp and expiry -- what an INSERT writes, table default TTL included. Such a column has nothing to say but
     * its values, so it stores no writetimes (a non-null value is a cell), and
     * {@link #decode(TableMetadata, ByteBuffer, ColumnFilter, boolean)} can pass over it entirely when it is not queried.
     */
    private static final String PLAIN = "\0plain.";

    private RowBlockCodec()
    {
    }

    /**
     * @return whether {@code metadata}'s shape can be encoded at all: exactly one clustering column, of type
     *         timestamp, and only single-cell, non-counter regular columns. Individual rows can still be refused
     *         by {@link #encode}.
     */
    public static boolean supports(TableMetadata metadata)
    {
        if (metadata.clusteringColumns().size() != 1
            || !metadata.clusteringColumns().get(0).type.unwrap().equals(TimestampType.instance))
            return false;
        for (ColumnMetadata column : metadata.regularColumns())
            if (column.isComplex() || column.type.isCounter())
                return false;
        return true;
    }

    /**
     * @param rows one partition's non-static rows in the table's clustering order; at least one
     * @return the payload, or {@code null} if any row holds something this format cannot carry
     */
    public static ByteBuffer encode(TableMetadata metadata, List<Row> rows)
    {
        if (!supports(metadata) || rows.isEmpty() || rows.size() > ColumnarChunkCodec.MAX_ROWS)
            return null;

        List<Row> ascending = rows;
        if (isDescending(metadata))
        {
            ascending = new ArrayList<>(rows);
            Collections.reverse(ascending);
        }

        int n = ascending.size();
        List<ColumnMetadata> columns = new ArrayList<>();
        metadata.regularColumns().forEach(columns::add);
        int c = columns.size();

        long[] timestamps = new long[n];
        ByteBuffer[] rowTs = new ByteBuffer[n], rowTtl = new ByteBuffer[n], rowLdt = new ByteBuffer[n], rowBase = new ByteBuffer[n];
        ByteBuffer[][] values = new ByteBuffer[c][n], wt = new ByteBuffer[c][n], ttl = new ByteBuffer[c][n],
                       ldt = new ByteBuffer[c][n], del = new ByteBuffer[c][n];
        boolean[] plain = new boolean[c];
        Arrays.fill(plain, true);

        for (int r = 0; r < n; r++)
        {
            Row row = ascending.get(r);
            if (!row.deletion().isLive() || row.hasComplexDeletion())
                return null;
            timestamps[r] = TimestampType.instance.compose(row.clustering().bufferAt(0)).getTime();
            if (r > 0 && timestamps[r] <= timestamps[r - 1])
                return null;

            LivenessInfo liveness = row.primaryKeyLivenessInfo();
            long base;
            if (!liveness.isEmpty())
            {
                base = liveness.timestamp();
                rowTs[r] = LongType.instance.decompose(base);
                if (liveness.isExpiring())
                {
                    rowTtl[r] = Int32Type.instance.decompose(liveness.ttl());
                    rowLdt[r] = LongType.instance.decompose(liveness.localExpirationTime());
                }
            }
            else
            {
                base = Long.MAX_VALUE;
                for (ColumnData data : row)
                    base = Math.min(base, ((Cell<?>) data).timestamp());
                rowBase[r] = LongType.instance.decompose(base);
            }

            for (ColumnData data : row)
            {
                if (data.column().isComplex() || data.column().type.isCounter())
                    return null;
                Cell<?> cell = (Cell<?>) data;
                int i = columns.indexOf(cell.column());
                wt[i][r] = LongType.instance.decompose(cell.timestamp() - base);
                plain[i] &= sharesRowLiveness(cell, liveness, base);
                if (cell.isTombstone())
                {
                    del[i][r] = BooleanType.instance.decompose(true);
                    ldt[i][r] = LongType.instance.decompose(cell.localDeletionTime());
                    continue;
                }
                values[i][r] = cell.buffer();
                if (cell.isExpiring())
                {
                    ttl[i][r] = Int32Type.instance.decompose(cell.ttl());
                    ldt[i][r] = LongType.instance.decompose(cell.localDeletionTime());
                }
            }
        }

        SortedMap<String, ChunkV4Codec.ColumnInput> inputs = new TreeMap<>();
        put(inputs, ROW_TS, LongType.instance, rowTs);
        putUnlessAllNull(inputs, ROW_TTL, Int32Type.instance, rowTtl);
        putUnlessAllNull(inputs, ROW_LDT, LongType.instance, rowLdt);
        putUnlessAllNull(inputs, ROW_BASE, LongType.instance, rowBase);
        for (int i = 0; i < c; i++)
        {
            ColumnMetadata column = columns.get(i);
            String name = column.name.toString();
            inputs.put(name, new ChunkV4Codec.ColumnInput(ChunkColumnTypes.typeCodeFor(column.type),
                                                          ChunkColumnTypes.statOrderFor(column.type), values[i]));
            if (plain[i])
            {
                put(inputs, PLAIN + name, BooleanType.instance, new ByteBuffer[n]);
                continue;
            }
            put(inputs, WT + name, LongType.instance, wt[i]);
            // All null is the usual case; a column left out costs no directory entry and no per-cell lookup.
            putUnlessAllNull(inputs, TTL + name, Int32Type.instance, ttl[i]);
            putUnlessAllNull(inputs, LDT + name, LongType.instance, ldt[i]);
            putUnlessAllNull(inputs, DEL + name, BooleanType.instance, del[i]);
        }
        return ColumnarChunkCodec.encode(timestamps, n, inputs);
    }

    /**
     * @param columns the read's column filter, or {@code null} for every column. Applied exactly as the row-format
     *                deserializer applies it ({@code DeserializationHelper.includes}/{@code canSkipValue}): a column
     *                the filter does not fetch is left out; a fetched but unqueried column keeps only the cells at or
     *                above the row's liveness timestamp, with their values skipped. Anything else would hand a replica
     *                reading this format a different row -- and a different digest -- than one reading row format.
     * @return the rows {@link #encode} was given, in the table's clustering order, filtered as above
     */
    public static List<Row> decode(TableMetadata metadata, ByteBuffer payload, ColumnFilter columns)
    {
        return decode(metadata, payload, columns, false);
    }

    /**
     * @param skipRedundantCells leave out the value-less cells of unqueried columns where they prove nothing: a plain
     *                           column's cell has its row marker's timestamp and expiry, so anything that shadows or
     *                           expires the one does the same to the other and the row's liveness does not depend on
     *                           it. The answer is the same, and most of a narrow scan's work is gone, but the rows
     *                           differ from row format's, and so would their digest: only for a table with one replica.
     */
    public static List<Row> decode(TableMetadata metadata, ByteBuffer payload, ColumnFilter columns, boolean skipRedundantCells)
    {
        // What the block holds decides what is worth stepping through, so look before opening the real cursor.
        Set<String> probeNames = new HashSet<>(List.of(ROW_BASE));
        for (ColumnMetadata column : metadata.regularColumns())
            probeNames.add(PLAIN + column.name);
        ColumnarCursor probe = ColumnarChunkCodec.cursor(payload, probeNames);
        boolean everyRowHasMarker = probe.columnSlot(ROW_BASE) == ColumnarCursor.ABSENT_COLUMN;

        List<ColumnMetadata> emitted = new ArrayList<>();
        Set<String> projection = new HashSet<>(List.of(ROW_TS, ROW_TTL, ROW_LDT, ROW_BASE));
        for (ColumnMetadata column : metadata.regularColumns())
        {
            if (columns != null && !columns.fetches(column))
                continue;
            String name = column.name.toString();
            boolean queried = columns == null || columns.fetchedColumnIsQueried(column);
            boolean plain = probe.columnSlot(PLAIN + name) != ColumnarCursor.ABSENT_COLUMN;
            if (plain && !queried && skipRedundantCells && everyRowHasMarker)
                continue;
            emitted.add(column);
            if (queried || plain)
                projection.add(name);                                     // a plain column's values say where its cells are
            if (!plain)
                Collections.addAll(projection, WT + name, TTL + name, LDT + name, DEL + name);
        }
        int c = emitted.size();

        ColumnarCursor cursor = ColumnarChunkCodec.cursor(payload, projection);
        // Every name resolved once per block, not once per row and column (ColumnarCursor#columnSlot).
        int rowTsSlot = cursor.columnSlot(ROW_TS), rowTtlSlot = cursor.columnSlot(ROW_TTL);
        int rowLdtSlot = cursor.columnSlot(ROW_LDT), rowBaseSlot = cursor.columnSlot(ROW_BASE);
        int[] valueSlot = new int[c], wtSlot = new int[c], ttlSlot = new int[c], ldtSlot = new int[c], delSlot = new int[c];
        boolean[] queried = new boolean[c], plain = new boolean[c];
        long[] dropHorizon = new long[c];
        for (int i = 0; i < c; i++)
        {
            ColumnMetadata column = emitted.get(i);
            // Cells from before a column was dropped stay in the file until the next rewrite. Without this a column
            // re-added under the same name would read them back, which row format's deserializer never lets happen.
            DroppedColumn dropped = metadata.droppedColumns.get(column.name.bytes);
            dropHorizon[i] = dropped == null ? DeserializationHelper.NO_DROP_HORIZON : dropped.droppedTime;
            String name = column.name.toString();
            queried[i] = columns == null || columns.fetchedColumnIsQueried(column);
            plain[i] = probe.columnSlot(PLAIN + name) != ColumnarCursor.ABSENT_COLUMN;
            valueSlot[i] = cursor.columnSlot(name);
            wtSlot[i] = cursor.columnSlot(WT + name);
            ttlSlot[i] = cursor.columnSlot(TTL + name);
            ldtSlot[i] = cursor.columnSlot(LDT + name);
            delSlot[i] = cursor.columnSlot(DEL + name);
        }

        List<Row> rows = new ArrayList<>(ColumnarChunkCodec.rowCount(payload));
        Object[] cells = new Object[c];
        while (cursor.advance())
        {
            Clustering<?> clustering = Clustering.make(TimestampType.instance.fromTimeInMillis(cursor.timestamp()));
            long base;
            long livenessTimestamp = LivenessInfo.NO_TIMESTAMP;
            LivenessInfo liveness = LivenessInfo.EMPTY;
            int rowTtl = Cell.NO_TTL;
            long rowLdt = Cell.NO_DELETION_TIME;
            ByteBuffer ts = cursor.getBytes(rowTsSlot);
            if (ts != null)
            {
                base = longAt(ts);
                livenessTimestamp = base;
                ByteBuffer ttl = cursor.getBytes(rowTtlSlot);
                if (ttl == null)
                {
                    liveness = LivenessInfo.create(base);
                }
                else
                {
                    rowTtl = intAt(ttl);
                    rowLdt = longAt(cursor.getBytes(rowLdtSlot));
                    liveness = LivenessInfo.withExpirationTime(base, rowTtl, rowLdt);
                }
            }
            else
            {
                base = longAt(cursor.getBytes(rowBaseSlot));
            }

            int count = 0;
            boolean neverExpires = rowTtl == Cell.NO_TTL;
            for (int i = 0; i < c; i++)
            {
                ColumnMetadata column = emitted.get(i);
                if (plain[i])
                {
                    ByteBuffer value = cursor.getBytes(valueSlot[i]);
                    if (value != null && !DeserializationHelper.isDroppedAtHorizon(base, dropHorizon[i]))
                        cells[count++] = new BufferCell(column, base, rowTtl, rowLdt, queried[i] ? value : ByteBufferUtil.EMPTY_BYTE_BUFFER, null);
                    continue;
                }
                ByteBuffer delta = cursor.getBytes(wtSlot[i]);
                if (delta == null)
                    continue;                                             // no cell for this column on this row
                long timestamp = base + longAt(delta);
                if (DeserializationHelper.isDroppedAtHorizon(timestamp, dropHorizon[i]))
                    continue;
                if (!queried[i] && timestamp < livenessTimestamp)
                    continue;                                             // the row marker already proves the row live
                if (cursor.getBytes(delSlot[i]) != null)
                {
                    neverExpires = false;
                    cells[count++] = new BufferCell(column, timestamp, Cell.NO_TTL, longAt(cursor.getBytes(ldtSlot[i])),
                                                    ByteBufferUtil.EMPTY_BYTE_BUFFER, null);
                    continue;
                }
                ByteBuffer value = queried[i] ? cursor.getBytes(valueSlot[i]) : ByteBufferUtil.EMPTY_BYTE_BUFFER;
                ByteBuffer cellTtl = cursor.getBytes(ttlSlot[i]);
                if (cellTtl == null)
                {
                    cells[count++] = BufferCell.live(column, timestamp, value);
                    continue;
                }
                neverExpires = false;
                cells[count++] = new BufferCell(column, timestamp, intAt(cellTtl), longAt(cursor.getBytes(ldtSlot[i])), value, null);
            }
            if (count == 0 && liveness.isEmpty())
                continue;                                                 // all it had was dropped or not fetched
            rows.add(neverExpires ? BTreeRow.create(clustering, liveness, Row.Deletion.LIVE, tree(cells, count), Cell.MAX_DELETION_TIME)
                                  : BTreeRow.create(clustering, liveness, Row.Deletion.LIVE, tree(cells, count)));
        }
        if (isDescending(metadata))
            Collections.reverse(rows);
        return rows;
    }

    /** {@code cells} are in BTree order and one per column, so the row's BTree can be built directly. */
    private static Object[] tree(Object[] cells, int count)
    {
        if (count == 0)
            return BTree.empty();
        if (count == 1)
            return BTree.singleton(cells[0]);
        // The BulkIterator is pooled and must be closed, or its thread-local slot leaks.
        try (BulkIterator<Object> bulk = BulkIterator.of(cells, 0))
        {
            return BTree.build(bulk, count, UpdateFunction.noOp());
        }
    }

    private static boolean sharesRowLiveness(Cell<?> cell, LivenessInfo liveness, long base)
    {
        return !cell.isTombstone()
               && cell.timestamp() == base
               && cell.ttl() == (liveness.isExpiring() ? liveness.ttl() : Cell.NO_TTL)
               && (!cell.isExpiring() || cell.localDeletionTime() == liveness.localExpirationTime());
    }

    private static boolean allNull(ByteBuffer[] values)
    {
        for (ByteBuffer value : values)
            if (value != null)
                return false;
        return true;
    }

    private static void putUnlessAllNull(SortedMap<String, ChunkV4Codec.ColumnInput> inputs, String name,
                                         org.apache.cassandra.db.marshal.AbstractType<?> type, ByteBuffer[] values)
    {
        if (!allNull(values))
            put(inputs, name, type, values);
    }

    private static long longAt(ByteBuffer serialized)
    {
        return serialized.getLong(serialized.position());
    }

    private static int intAt(ByteBuffer serialized)
    {
        return serialized.getInt(serialized.position());
    }

    private static boolean isDescending(TableMetadata metadata)
    {
        return metadata.clusteringColumns().get(0).type.isReversed();
    }

    private static void put(SortedMap<String, ChunkV4Codec.ColumnInput> inputs, String name,
                            org.apache.cassandra.db.marshal.AbstractType<?> type, ByteBuffer[] values)
    {
        inputs.put(name, new ChunkV4Codec.ColumnInput(ChunkColumnTypes.typeCodeFor(type), ChunkColumnTypes.statOrderFor(type), values));
    }
}
