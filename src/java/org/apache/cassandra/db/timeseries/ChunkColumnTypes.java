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

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;

/**
 * Maps a table column's {@link AbstractType} onto the chunk-format-v4 type code its values are
 * encoded with, and onto the {@link StatOrder} its pruning statistics may declare.
 * <p>
 * The <b>only</b> thing a type code selects is which compression the column's <em>serialized</em>
 * bytes go through -- it never changes what those bytes are (v4 §4). Every code round-trips the
 * exact {@code ByteBuffer} Cassandra stored, so nothing here needs to know how a type is composed
 * or decomposed, and a column decoded out of a chunk is byte-identical to the cell that went in:
 * <ul>
 *   <li>{@code double} -> {@link ChunkV4Directory#TYPE_DOUBLE} (8-byte IEEE-754 payload, encoded
 *       per block by ALP or ALP-RD -- lossless for every bit pattern, including {@code -0.0}, NaN
 *       payloads, the infinities and subnormals)</li>
 *   <li>{@code boolean} -> {@link ChunkV4Directory#TYPE_BOOLEAN} (1 byte, one packed bit)</li>
 *   <li>{@code int} -> {@link ChunkV4Directory#TYPE_INT32} (exactly 4 bytes big-endian,
 *       signed)</li>
 *   <li>{@code date} -> {@link ChunkV4Directory#TYPE_DATE32} -- new in v4, no longer folded into
 *       {@code INT32}: a {@code date} is an <em>unsigned</em> day count (its serializer shifts by
 *       2^31 so unsigned byte order is chronological), and giving it its own code is what lets its
 *       block extrema be computed and declared under {@link StatOrder#UNSIGNED_INT} instead of
 *       being either wrong or dropped</li>
 *   <li>{@code bigint}, {@code timestamp} and {@code time} -> {@link ChunkV4Directory#TYPE_INT64}
 *       (all three are exactly 8 bytes big-endian: raw value, epoch millis, and nanoseconds since
 *       midnight respectively -- no normalisation is applied or needed)</li>
 *   <li>{@code text}/{@code varchar}/{@code ascii} -> {@link ChunkV4Directory#TYPE_TEXT}
 *       (per-section dictionary when it pays for itself, else length-prefixed raw)</li>
 *   <li><b>everything else</b> -> {@link ChunkV4Directory#TYPE_OPAQUE}: the value's serialized
 *       bytes verbatim. This fallback is what makes "any time-series table can be tiered" true --
 *       {@code blob}, {@code uuid}, {@code timeuuid}, {@code decimal}, {@code varint},
 *       {@code inet}, {@code smallint}, {@code tinyint}, {@code float}, {@code duration}, frozen
 *       collections, frozen UDTs and tuples all land here.</li>
 * </ul>
 * Widths matter: a type is only mapped onto a fixed-width code when its serialized form is exactly
 * that width. {@code smallint} (2 bytes), {@code tinyint} (1 byte) and {@code float} (4 bytes) are
 * deliberately opaque rather than squeezed into {@code INT32}/{@code BOOLEAN}, because those codes
 * re-serialize at their own width (or, for {@code BOOLEAN}, collapse the value to a 0/1 bit) and
 * would silently corrupt the round trip.
 * <p>
 * <b>{@link #statOrderFor} is the mapping's other half and must travel with the type code.</b> It
 * routes through {@link ChunkV4Codec#statOrderFor}, which keeps the type's own comparison order
 * only when it matches the canonical order the type code's block extrema are computed under, and
 * degrades to {@link StatOrder#NONE} otherwise -- silent, and sound by construction: a column
 * without a declared order merely loses pruning, while a column declaring an order its statistics
 * are not in loses <em>rows</em>. The known live degradation is {@code time}: eight bytes, so
 * {@code INT64}, but {@link TimeType} compares unsigned while {@code INT64} extrema are signed, so
 * it declares {@code NONE} and carries no chunk statistics (v4 §4, and the risk §12 ranks fourth).
 * <p>
 * Counter columns and non-frozen (multi-cell) columns never reach here: {@link RowBlockCodec} keeps them out of its
 * blocks.
 * <p>
 * The mapping is per column <em>type</em>, but a chunk records a type code per column per chunk,
 * and {@link ChunkV4Codec#encode} downgrades a column to {@code TYPE_OPAQUE} (forcing
 * {@code statOrder} to {@code NONE} with it) for that one chunk if any of its present values is not
 * exactly the fixed width the code re-serializes at. That covers the values Cassandra accepts but
 * the natural code cannot represent -- {@code blobAsInt(0x)} is legal and deserializes as a 0-byte
 * {@code int} -- without a schema-level special case.
 * <p>
 * One documented non-byte-exactness: {@code boolean} values are stored as one bit, so a cell whose
 * serialized byte is neither {@code 0x00} nor {@code 0x01} (e.g. {@code blobAsBoolean(0x02)}, which
 * Cassandra reads as {@code true}) is downgraded with its whole column to {@code TYPE_OPAQUE} for
 * that chunk rather than being normalised (v4 tightened this over v3, which rewrote the byte).
 */
public final class ChunkColumnTypes
{
    private ChunkColumnTypes()
    {
    }

    /** @return the v4 type code to encode {@code type}'s serialized values with; never fails. */
    public static int typeCodeFor(AbstractType<?> type)
    {
        AbstractType<?> unwrapped = type.unwrap();
        if (unwrapped instanceof DoubleType)
            return ChunkV4Directory.TYPE_DOUBLE;
        if (unwrapped instanceof BooleanType)
            return ChunkV4Directory.TYPE_BOOLEAN;
        if (unwrapped instanceof Int32Type)
            return ChunkV4Directory.TYPE_INT32;
        if (unwrapped instanceof SimpleDateType)
            return ChunkV4Directory.TYPE_DATE32;
        if (unwrapped instanceof LongType || unwrapped instanceof TimestampType || unwrapped instanceof TimeType)
            return ChunkV4Directory.TYPE_INT64;
        if (unwrapped instanceof UTF8Type || unwrapped instanceof AsciiType)
            return ChunkV4Directory.TYPE_TEXT;
        return ChunkV4Directory.TYPE_OPAQUE;
    }

    /**
     * The {@link StatOrder} a column of {@code type} declares when encoded under
     * {@link #typeCodeFor}'s code: the type's own order when the code's block extrema are computed
     * in that order, and {@link StatOrder#NONE} otherwise (see the class javadoc for why the silent
     * degradation is the sound direction). Never fails.
     */
    public static StatOrder statOrderFor(AbstractType<?> type)
    {
        return ChunkV4Codec.statOrderFor(typeCodeFor(type), type.unwrap());
    }
}
