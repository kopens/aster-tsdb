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
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.timeseries.tiering.ChunkColumnTypes;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

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
        put(inputs, ROW_TTL, Int32Type.instance, rowTtl);
        put(inputs, ROW_LDT, LongType.instance, rowLdt);
        put(inputs, ROW_BASE, LongType.instance, rowBase);
        for (int i = 0; i < c; i++)
        {
            ColumnMetadata column = columns.get(i);
            String name = column.name.toString();
            inputs.put(name, new ChunkV4Codec.ColumnInput(ChunkColumnTypes.typeCodeFor(column.type),
                                                          ChunkColumnTypes.statOrderFor(column.type), values[i]));
            put(inputs, WT + name, LongType.instance, wt[i]);
            put(inputs, TTL + name, Int32Type.instance, ttl[i]);
            put(inputs, LDT + name, LongType.instance, ldt[i]);
            put(inputs, DEL + name, BooleanType.instance, del[i]);
        }
        return ColumnarChunkCodec.encode(timestamps, n, inputs);
    }

    /**
     * @param columns the columns to materialise, or {@code null} for all; rows keep their liveness either way
     * @return the rows {@link #encode} was given, in the table's clustering order, restricted to {@code columns}
     */
    public static List<Row> decode(TableMetadata metadata, ByteBuffer payload, ColumnFilter columns)
    {
        List<ColumnMetadata> emitted = new ArrayList<>();
        for (ColumnMetadata column : metadata.regularColumns())
            if (columns == null || columns.fetchedColumns().contains(column))
                emitted.add(column);

        Set<String> projection = new HashSet<>(List.of(ROW_TS, ROW_TTL, ROW_LDT, ROW_BASE));
        for (ColumnMetadata column : emitted)
        {
            String name = column.name.toString();
            Collections.addAll(projection, name, WT + name, TTL + name, LDT + name, DEL + name);
        }

        ColumnarCursor cursor = ColumnarChunkCodec.cursor(payload, projection);
        List<Row> rows = new ArrayList<>();
        Row.Builder builder = BTreeRow.sortedBuilder();
        while (cursor.advance())
        {
            builder.newRow(Clustering.make(TimestampType.instance.fromTimeInMillis(cursor.timestamp())));
            long base;
            ByteBuffer ts = cursor.getBytes(ROW_TS);
            if (ts != null)
            {
                base = LongType.instance.compose(ts);
                ByteBuffer rowTtl = cursor.getBytes(ROW_TTL);
                builder.addPrimaryKeyLivenessInfo(rowTtl == null
                                                  ? LivenessInfo.create(base)
                                                  : LivenessInfo.withExpirationTime(base, Int32Type.instance.compose(rowTtl),
                                                                                    LongType.instance.compose(cursor.getBytes(ROW_LDT))));
            }
            else
            {
                base = LongType.instance.compose(cursor.getBytes(ROW_BASE));
            }

            for (ColumnMetadata column : emitted)
            {
                String name = column.name.toString();
                ByteBuffer delta = cursor.getBytes(WT + name);
                if (delta == null)
                    continue;                                             // no cell for this column on this row
                long timestamp = base + LongType.instance.compose(delta);
                if (cursor.getBytes(DEL + name) != null)
                {
                    builder.addCell(new BufferCell(column, timestamp, Cell.NO_TTL, LongType.instance.compose(cursor.getBytes(LDT + name)),
                                                   ByteBufferUtil.EMPTY_BYTE_BUFFER, null));
                    continue;
                }
                ByteBuffer value = cursor.getBytes(name);
                ByteBuffer cellTtl = cursor.getBytes(TTL + name);
                builder.addCell(cellTtl == null
                                ? BufferCell.live(column, timestamp, value)
                                : new BufferCell(column, timestamp, Int32Type.instance.compose(cellTtl),
                                                 LongType.instance.compose(cursor.getBytes(LDT + name)), value, null));
            }
            rows.add(builder.build());
        }
        if (isDescending(metadata))
            Collections.reverse(rows);
        return rows;
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
