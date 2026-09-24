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
package org.apache.cassandra.db.timeseries.tiering;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.annotation.Nullable;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.timeseries.ChunkV4Codec;
import org.apache.cassandra.db.timeseries.ColumnarChunkCodec;
import org.apache.cassandra.db.timeseries.ColumnarCursor;
import org.apache.cassandra.db.timeseries.StatOrder;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

/**
 * The one definition of how a (tag, window) becomes a chunk row: merge the window's samples into
 * whatever chunk already exists, encode, and decide whether and at which timestamp to write it.
 *
 * <p>Both writers of the chunk table go through here -- the re-encoder
 * ({@link TieredStorageService#runOnce}) and the cold-window flush ({@link ColdWindowChunkFlush}).
 * They used to carry their own copies of this logic, and they <em>must</em> agree: the two can
 * encode the same window concurrently, and what makes that converge instead of tearing the chunk is
 * that both produce byte-identical payloads for identical content and write them at the same
 * deterministic timestamp. Two copies kept that true only by convention.
 *
 * <p>Deliberately free of I/O: the callers read the existing chunk row, hand it in, and issue the
 * INSERT themselves, because <em>when</em> they do so differs and matters -- the re-encoder must
 * widen {@link ChunkCoverage} before writing, the flush must decline windows the re-encoder owns.
 */
final class ChunkWindowEncoder
{
    private final String[] valueRawNames;
    private final int[] valueTypeCodes;
    private final StatOrder[] valueStatOrders;

    ChunkWindowEncoder(TableMetadata base)
    {
        // The regular columns in their canonical order (ColumnMetadata's comparator -- stable and
        // deterministic, Columns.java), which is also the order ColumnarChunkCodec writes its directory
        // in, so a payload is byte-stable across runs, nodes and JVMs. That stability is what lets a
        // re-run recognise it has nothing new to write (see Encoded#unchanged).
        List<ColumnMetadata> valueColumns = new ArrayList<>();
        for (ColumnMetadata column : base.regularColumns())
            valueColumns.add(column);
        int valueCount = valueColumns.size();
        valueRawNames = new String[valueCount];
        valueTypeCodes = new int[valueCount];
        // Paired with the type code, never derived independently of it: the order a column may
        // declare depends on which code carries its bytes (v4 §4), and ChunkColumnTypes.statOrderFor
        // makes that pairing in one place. A type whose comparator the code cannot express (time:
        // unsigned comparator, signed INT64 extrema) declares NONE and forgoes pruning.
        valueStatOrders = new StatOrder[valueCount];
        for (int c = 0; c < valueCount; c++)
        {
            valueRawNames[c] = valueColumns.get(c).name.toString();
            valueTypeCodes[c] = ChunkColumnTypes.typeCodeFor(valueColumns.get(c).type);
            valueStatOrders[c] = ChunkColumnTypes.statOrderFor(valueColumns.get(c).type);
        }
    }

    /** The existing-chunk read both writers issue; binds {@code (<tag>, window_start)}. */
    static String existingChunkQuery(String chunkRef, String tagPredicate)
    {
        return String.format("SELECT payload, max_row_writetime, WRITETIME(payload) AS chunk_wt " +
                             "FROM %s WHERE %s AND window_start = ?", chunkRef, tagPredicate);
    }

    /** The chunk INSERT both writers issue; binds {@link Encoded#insertValues}. */
    static String insertChunkQuery(String chunkRef, String tagCqlList, String tagBindMarkers)
    {
        return String.format("INSERT INTO %s (%s, window_start, codec, samples, max_row_writetime, payload) " +
                             "VALUES (%s, ?, ?, ?, ?, ?) USING TIMESTAMP ?", chunkRef, tagCqlList, tagBindMarkers);
    }

    int valueCount()
    {
        return valueRawNames.length;
    }

    /** @return the raw name of regular column {@code c}; sample slots are indexed in this order. */
    String valueRawName(int c)
    {
        return valueRawNames[c];
    }

    /**
     * Starts a window from the chunk row already stored for it, or from nothing.
     *
     * @param existingChunk the chunk table row for this (tag, window) -- {@code payload},
     *                      {@code max_row_writetime} and {@code chunk_wt} are read -- or {@code null}
     */
    Window open(@Nullable UntypedResultSet.Row existingChunk)
    {
        return new Window(existingChunk);
    }

    final class Window
    {
        // ts -> one slot per regular column, in valueRawNames order; a null slot is a null cell and
        // stays null all the way into the chunk (presence is encoded per column).
        private final TreeMap<Long, ByteBuffer[]> merged = new TreeMap<>();
        @Nullable private final ByteBuffer existingPayload;
        private final long existingMaxWt;
        private final long existingChunkWt;
        private long maxWt = Long.MIN_VALUE;
        // Whether maxWt is a real writetime rather than the Long.MIN_VALUE sentinel.
        private boolean haveWritetime;

        private Window(@Nullable UntypedResultSet.Row existingChunk)
        {
            if (existingChunk == null)
            {
                existingPayload = null;
                existingMaxWt = Long.MIN_VALUE;
                existingChunkWt = Long.MIN_VALUE;
                return;
            }
            existingPayload = existingChunk.getBytes("payload");
            ColumnarCursor cursor = ColumnarChunkCodec.cursor(existingPayload, null);
            while (cursor.advance())
            {
                ByteBuffer[] values = new ByteBuffer[valueRawNames.length];
                for (int c = 0; c < valueRawNames.length; c++)
                    // null for a column this chunk does not carry (ADDed to the table after the chunk
                    // was written); a column the chunk carries but the table has since DROPped is
                    // simply never asked for, so it drops out here.
                    values[c] = cursor.getBytes(valueRawNames[c]);
                merged.put(cursor.timestamp(), values);
            }
            existingMaxWt = existingChunk.getLong("max_row_writetime");
            existingChunkWt = existingChunk.getLong("chunk_wt");
            maxWt = existingMaxWt;
            haveWritetime = true;
        }

        boolean hasExistingChunk()
        {
            return existingPayload != null;
        }

        /** @return the stored chunk's {@code max_row_writetime}; only meaningful if {@link #hasExistingChunk}. */
        long existingMaxWritetime()
        {
            return existingMaxWt;
        }

        /**
         * Merges one sample in PER COLUMN, not as a row-level replace: a non-null slot of
         * {@code fresh} wins over whatever the chunk held, a null slot leaves the chunk's value in
         * place. A sample that reappears at a timestamp the chunk already holds is a partial update of
         * that stored row ({@code UPDATE t SET quality = ?} writes ONE cell) -- exactly Cassandra's own
         * per-cell last-write-wins. Replacing the whole row would blank every column the update did
         * not mention.
         *
         * @param fresh one slot per regular column, in {@link #valueRawName} order; an empty but
         *              present value (e.g. text '') is non-null and counts as present
         */
        void mergeSample(long timestamp, ByteBuffer[] fresh)
        {
            ByteBuffer[] values = merged.get(timestamp);
            if (values == null)
            {
                values = new ByteBuffer[valueRawNames.length];
                merged.put(timestamp, values);
            }
            for (int c = 0; c < valueRawNames.length; c++)
            {
                if (fresh[c] != null)
                    values[c] = fresh[c];
            }
        }

        /** Feeds one cell writetime into the window's maximum. */
        void noteWritetime(long writetime)
        {
            maxWt = Math.max(maxWt, writetime);
            haveWritetime = true;
        }

        /**
         * @return whether the window has a real writetime -- from a cell or inherited from the stored
         * chunk. Without one there is no timestamp that is provably not older than every row it
         * covers, and both writers leave such a window entirely untouched.
         */
        boolean haveWritetime()
        {
            return haveWritetime;
        }

        long maxWritetime()
        {
            return maxWt;
        }

        int sampleCount()
        {
            return merged.size();
        }

        /** Encodes the merged samples; the caller has already checked {@link #sampleCount} against its limit. */
        Encoded encode()
        {
            int count = merged.size();
            long[] timestamps = new long[count];
            ByteBuffer[][] columnValues = new ByteBuffer[valueRawNames.length][count];
            int idx = 0;
            for (Map.Entry<Long, ByteBuffer[]> sample : merged.entrySet())
            {
                timestamps[idx] = sample.getKey();
                ByteBuffer[] values = sample.getValue();
                for (int c = 0; c < valueRawNames.length; c++)
                    columnValues[c][idx] = values[c];
                idx++;
            }
            SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
            for (int c = 0; c < valueRawNames.length; c++)
                columns.put(valueRawNames[c],
                            new ChunkV4Codec.ColumnInput(valueTypeCodes[c], valueStatOrders[c], columnValues[c]));

            ByteBuffer payload = ColumnarChunkCodec.encode(timestamps, count, columns);

            // The encoding is deterministic, so identical content encodes to identical bytes: if the
            // stored chunk already IS what was just built, from the same maximum writetime, re-writing
            // it -- a replayed flush, a re-run cycle, a replica repeating another's encode -- would only
            // bump its own write timestamp.
            boolean unchanged = existingPayload != null && maxWt == existingMaxWt && payload.equals(existingPayload);

            // Strictly after both the rows encoded AND the chunk row being replaced. maxWt+1 alone could
            // land exactly on the existing chunk's own write timestamp after a crash-then-backfill, and
            // same-timestamp writes to different columns of one row resolve per column, tearing the
            // chunk. This deterministic rule is also what makes concurrent encodes of the same content
            // converge.
            long insertTs = Math.max(maxWt + 1, existingChunkWt + 1);
            return new Encoded(payload, count, maxWt, insertTs, unchanged);
        }
    }

    static final class Encoded
    {
        final ByteBuffer payload;
        final int count;
        final long maxWritetime;
        final long insertTimestamp;
        /** The stored chunk already holds exactly this; the caller should not re-write it. */
        final boolean unchanged;

        private Encoded(ByteBuffer payload, int count, long maxWritetime, long insertTimestamp, boolean unchanged)
        {
            this.payload = payload;
            this.count = count;
            this.maxWritetime = maxWritetime;
            this.insertTimestamp = insertTimestamp;
            this.unchanged = unchanged;
        }

        /**
         * @return the bind values for the chunk INSERT both writers issue --
         * {@code (<tag>, window_start, codec, sample_count, max_row_writetime, payload) USING TIMESTAMP ?}
         */
        List<ByteBuffer> insertValues(List<ByteBuffer> tag, long windowStart)
        {
            List<ByteBuffer> values = new ArrayList<>(tag.size() + 6);
            values.addAll(tag);
            values.add(TimestampType.instance.fromTimeInMillis(windowStart));
            // Read the version byte back out of the payload rather than naming a codec constant: the
            // `codec` column must describe what was actually written, so it stays honest if the encode
            // path ever changes underneath this call.
            values.add(ByteType.instance.decompose(payload.get(payload.position())));
            values.add(Int32Type.instance.decompose(count));
            values.add(LongType.instance.decompose(maxWritetime));
            values.add(payload);
            values.add(LongType.instance.decompose(insertTimestamp));
            return values;
        }
    }
}
