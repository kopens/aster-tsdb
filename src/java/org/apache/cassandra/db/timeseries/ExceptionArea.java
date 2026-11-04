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
import java.nio.ByteOrder;

/**
 * The chunk-format-v4 §5 shared exception area: a sorted list of {@code (position, verbatim value)}
 * pairs that sits after a packed lane and rescues the handful of values that would otherwise have
 * forced the whole lane wider.
 *
 * <p>Layout, exactly §5: {@code excCount u16 | pad u16 | pad u32} then {@code excCount} entries of
 * {@code position u16, value W bytes}, positions strictly ascending, the whole area padded to a
 * multiple of 8 (§6). {@code W} is 8 for {@code DOUBLE}/{@code INT64}, 4 for {@code INT32}/
 * {@code DATE32}, and 2 for the ALP-RD left parts a later phase will store here.
 *
 * <p><b>Making this a shared facility rather than an ALP-internal detail is the decision that pays
 * for itself twice</b> (§5). It gives integer columns outlier tolerance: one value of 2^31 in an
 * otherwise 10-bit block costs a 10-byte entry instead of widening 1,024 values from 10 bits to 31,
 * which is 8 bytes against 2,688. And it gives the timestamp axis gap tolerance -- a single missed
 * sample no longer widens every delta in the block -- which is what let §0 delete the
 * delta-of-delta bitstream outright and make timestamps an ordinary block-shaped {@code INT64}-like
 * column. A per-codec exception mechanism would have bought the first of those and none of the
 * second.
 *
 * <p><b>The lane value at an exception position must be zero, and the decoder verifies it.</b> This
 * is §5 rule 6's "specified filler" and the reason is byte determinism, not tidiness: the lane slot
 * is dead -- whatever it holds is discarded and replaced by the exception's verbatim value -- so
 * without a specified value an encoder is free to leave the truncated residual, or a stale scratch
 * byte, or zero, and three encoders produce three different chunks from one input
 * (§12). Zero is cheap to
 * write, cheap to assert, and the check costs one array read per exception.
 *
 * <p><b>A position is an index into the lane this area accompanies</b>, not a row offset and not a
 * value index in some other domain. What a lane slot means is the encoding's business:
 * {@code FOR_BITPACK}'s slot {@code i} feeds present value {@code i}, {@code DELTA_FOR_BITPACK}'s
 * slot {@code j} feeds present value {@code j + 1}. This class only requires that positions are
 * distinct, ascending, and inside the lane the caller declares -- which is exactly what makes the
 * area decodable in one forward pass with no seek and no sort.
 *
 * <p><b>Strictly ascending is one check doing two jobs.</b> §5 requires ascending order (so a
 * decoder can merge the area into the lane in one pass) and distinct positions (so one lane slot
 * cannot be given two values). {@code position <= previous} rejects both, and it also removes the
 * second byte form a set of exceptions would otherwise have -- any permutation of the same pairs
 * would decode identically and re-encode differently.
 *
 * <p>Values are stored and returned as the low {@code 8*W} bits of the verbatim value, zero-extended
 * into a {@code long} -- the same "raw bits, not numbers" convention {@link ChunkV4BlockTable.Entry}
 * uses for its extrema, and for the same reason: whether those bits mean a signed {@code int}, an
 * unsigned day count or the bit pattern of a {@code double} is the column's business, and a layer
 * that sign-extended on the way in would write one value as two different byte sequences.
 *
 * <p>Corruption is {@link IllegalArgumentException} (§11). Nothing here allocates: the position and
 * value arrays are caller-supplied and caller-sized, {@code excCount} is bounded against the lane
 * length the caller derived from the header before it is used for anything, and the entry region is
 * bounds-checked against the buffer before the first entry is read. A corrupt {@code excCount} can
 * make this throw; it cannot make it reserve memory.
 */
public final class ExceptionArea
{
    /** {@code excCount u16 | pad u16 | pad u32} (§5). */
    public static final int HEADER_BYTES = 8;

    /** {@code position u16} ahead of each verbatim value. */
    public static final int POSITION_BYTES = 2;

    /** {@code excCount} is a {@code u16}, so an area holds at most this many entries. */
    public static final int MAX_EXCEPTIONS = 0xFFFF;

    /** {@code position} is a {@code u16}; §7 caps a block at 32,768 rows, so this never binds. */
    public static final int MAX_POSITION = 0xFFFF;

    /**
     * {@code W} for the ALP-RD left parts (§5). Named rather than written as a bare 2 at the seam
     * because the later ALP phase is the only caller that will use it, and a constant is how that
     * phase finds out this area already supports its width.
     */
    public static final int VALUE_BYTES_ALP_RD_LEFT = 2;

    private ExceptionArea()
    {
    }

    // -----------------------------------------------------------------------------------------
    // sizing
    // -----------------------------------------------------------------------------------------

    /**
     * §5's {@code W} for a column type code. Only the fixed-width numeric types can carry an
     * exception area: {@code BOOLEAN} has no residual to overflow, and {@code TEXT}/{@code OPAQUE}
     * are not packed into a lane at all, so asking for their {@code W} is a caller bug rather than a
     * width to invent.
     */
    public static int valueBytes(int typeCode)
    {
        switch (typeCode)
        {
            case ChunkV4Directory.TYPE_DOUBLE:
            case ChunkV4Directory.TYPE_INT64:
                return 8;
            case ChunkV4Directory.TYPE_INT32:
            case ChunkV4Directory.TYPE_DATE32:
                return 4;
            default:
                throw new IllegalArgumentException("type code " + typeCode + " has no exception-area value width; " +
                                                   "only DOUBLE, INT64, INT32 and DATE32 pack into a lane (v4 §5)");
        }
    }

    /**
     * Exact bytes the area occupies: {@link #HEADER_BYTES} plus {@code roundUp8(count * (2 + W))}.
     * Zero for {@code count == 0}, because §5 gives the area no representation at all when the
     * block carries no exceptions -- {@code blockFlags} bit 4 is clear and no bytes are written.
     *
     * <p><b>This is the same number {@link BitPacking#chooseWidth} scores an exception area at</b>:
     * {@link BitPacking#EXCEPTION_AREA_OVERHEAD} is {@link #HEADER_BYTES} by reference, so
     * {@code widthCost}'s exception term is exactly this function. It has not always been: §5's
     * prose gave the cost formula a fixed term of 16 while giving the layout an eight-byte header,
     * and until the two were reconciled the chooser over-charged every width carrying exceptions by
     * eight bytes -- conservative and reproducible, but able to pick a width one step wider than the
     * true minimum. The layout is what describes bytes on disk, so the layout won and the doc's
     * formula was corrected. Every size this class and {@link BlockEncodings} report for a *body* is
     * the exact emitted length, and the argmin now scores exactly those sizes.
     */
    public static int encodedLength(int count, int valueBytes)
    {
        checkValueBytes(valueBytes);
        if (count < 0 || count > MAX_EXCEPTIONS)
            throw new IllegalArgumentException("exception count " + count + " outside 0.." + MAX_EXCEPTIONS);
        if (count == 0)
            return 0;
        return HEADER_BYTES + (int) BitPacking.roundUpToMultipleOf8((long) count * (POSITION_BYTES + valueBytes));
    }

    // -----------------------------------------------------------------------------------------
    // encode
    // -----------------------------------------------------------------------------------------

    /** Encodes into a freshly allocated array of exactly {@link #encodedLength} bytes. */
    public static byte[] encode(int[] positions, long[] values, int count, int valueBytes)
    {
        byte[] out = new byte[encodedLength(count, valueBytes)];
        encode(positions, values, count, valueBytes, ByteBuffer.wrap(out));
        return out;
    }

    /**
     * Writes the area at {@code dst}'s position, advancing it by {@link #encodedLength} and
     * returning that count. Positions must be strictly ascending and every value must already be
     * narrowed to {@code W} bytes.
     *
     * <p>{@code count == 0} is rejected rather than written as an empty area: §5 gives "no
     * exceptions" exactly one representation -- no area and a clear {@code blockFlags} bit 4 -- and
     * an eight-byte area with {@code excCount 0} would be a second byte form of the same state.
     *
     * <p>Every byte of the range is written, the header pads and the trailing pad included, because
     * §5 rule 5 exists precisely for encoders that write into a reused scratch buffer: leaked stale
     * bytes make identical input produce different bytes and leave every round-trip test green.
     */
    public static int encode(int[] positions, long[] values, int count, int valueBytes, ByteBuffer dst)
    {
        int length = encodedLength(count, valueBytes);
        if (count == 0)
            throw new IllegalArgumentException("a block with no exceptions carries no exception area; " +
                                               "clear blockFlags bit 4 instead (v4 §5)");
        requireBigEndian(dst);
        if (positions.length < count || values.length < count)
            throw new IllegalArgumentException("exception arrays hold " + positions.length + '/' + values.length +
                                               " < count " + count);
        if (dst.remaining() < length)
            throw new IllegalArgumentException("destination holds " + dst.remaining() + " < exception area " + length);

        int base = dst.position();
        dst.putShort((short) count);
        dst.putShort((short) 0);
        dst.putInt(0);
        int previous = -1;
        for (int i = 0; i < count; i++)
        {
            int position = positions[i];
            if (position < 0 || position > MAX_POSITION)
                throw new IllegalArgumentException("exception position " + position + " outside 0.." + MAX_POSITION);
            // One comparison for both of §5's requirements: ascending order and distinctness.
            if (position <= previous)
                throw new IllegalArgumentException("exception positions must strictly ascend, got " + previous +
                                                   " then " + position + " at entry " + i + " (v4 §5)");
            previous = position;
            checkValueFits(values[i], valueBytes, i);
            dst.putShort((short) position);
            putValue(dst, values[i], valueBytes);
        }
        while (dst.position() - base < length)
            dst.put((byte) 0);
        assert dst.position() - base == length : "wrote " + (dst.position() - base) + ", sized " + length;
        return length;
    }

    // -----------------------------------------------------------------------------------------
    // decode
    // -----------------------------------------------------------------------------------------

    /**
     * Reads an area from {@code src}'s position into the caller-supplied arrays, advancing the
     * position by {@link #encodedLength} and returning the number of exceptions read.
     *
     * @param positionLimit lane slots the area may address, i.e. the packed lane's value count. It
     *                      comes from the header and the block table, never from these bytes, which
     *                      is what bounds {@code excCount} before it is trusted for anything.
     */
    public static int decode(ByteBuffer src, int valueBytes, int positionLimit, int[] positions, long[] values)
    {
        checkValueBytes(valueBytes);
        requireBigEndian(src);
        if (positionLimit < 0 || positionLimit > MAX_POSITION + 1)
            throw new IllegalArgumentException("positionLimit " + positionLimit + " outside 0.." + (MAX_POSITION + 1));
        if (src.remaining() < HEADER_BYTES)
            throw new IllegalArgumentException("Corrupt v4 chunk: truncated exception-area header, have " +
                                               src.remaining() + " bytes");

        int base = src.position();
        int count = src.getShort() & 0xFFFF;
        int pad16 = src.getShort() & 0xFFFF;
        long pad32 = src.getInt() & 0xFFFFFFFFL;
        if (pad16 != 0 || pad32 != 0)
            throw new IllegalArgumentException("Corrupt v4 chunk: non-zero exception-area header padding " + pad16 +
                                               '/' + pad32 + " (v4 §5 rule 5)");
        if (count == 0)
            throw new IllegalArgumentException("Corrupt v4 chunk: exception area with excCount 0; no exceptions is " +
                                               "spelled by a clear blockFlags bit 4, not by an empty area (v4 §5)");
        // Bounded against caller-derived metadata before it sizes or indexes anything: a lane cannot
        // hold more exceptions than it has slots.
        if (count > positionLimit)
            throw new IllegalArgumentException("Corrupt v4 chunk: excCount " + count + " exceeds the lane's " +
                                               positionLimit + " slots");
        if (positions.length < count || values.length < count)
            throw new IllegalArgumentException("exception arrays hold " + positions.length + '/' + values.length +
                                               " < excCount " + count);
        int length = encodedLength(count, valueBytes);
        if (src.remaining() < length - HEADER_BYTES)
            throw new IllegalArgumentException("Corrupt v4 chunk: truncated exception area, need " +
                                               (length - HEADER_BYTES) + " more bytes, have " + src.remaining());

        int previous = -1;
        for (int i = 0; i < count; i++)
        {
            int position = src.getShort() & 0xFFFF;
            if (position <= previous)
                throw new IllegalArgumentException("Corrupt v4 chunk: exception positions must strictly ascend, got " +
                                                   previous + " then " + position + " at entry " + i);
            if (position >= positionLimit)
                throw new IllegalArgumentException("Corrupt v4 chunk: exception position " + position +
                                                   " outside the lane's " + positionLimit + " slots");
            previous = position;
            positions[i] = position;
            values[i] = getValue(src, valueBytes);
        }
        for (int i = src.position() - base; i < length; i++)
            if (src.get() != 0)
                throw new IllegalArgumentException("Corrupt v4 chunk: non-zero exception-area padding at byte " + i +
                                                   " (v4 §5 rule 5)");
        assert src.position() - base == length : "read " + (src.position() - base) + ", sized " + length;
        return count;
    }

    /**
     * §5 rule 6's specified filler, checked: every lane slot an exception covers must have unpacked
     * to zero.
     *
     * <p>Call this on the raw unpacked lane, before any frame-of-reference addition or prefix sum
     * folds the block's minimum into it -- afterwards the slot holds {@code min} rather than 0 and
     * the check silently stops testing anything.
     */
    public static void checkLaneIsZeroAtExceptions(long[] lane, int laneLength, int[] positions, int count)
    {
        if (laneLength < 0 || laneLength > lane.length)
            throw new IllegalArgumentException("laneLength " + laneLength + " outside 0.." + lane.length);
        for (int i = 0; i < count; i++)
        {
            int position = positions[i];
            if (position < 0 || position >= laneLength)
                throw new IllegalArgumentException("Corrupt v4 chunk: exception position " + position +
                                                   " outside the lane's " + laneLength + " slots");
            if (lane[position] != 0)
                throw new IllegalArgumentException("Corrupt v4 chunk: lane slot " + position + " under an exception " +
                                                   "holds " + lane[position] + ", not the specified filler 0; the " +
                                                   "slot is dead and an unspecified value gives one block two byte " +
                                                   "forms (v4 §5 rule 6)");
        }
    }

    // -----------------------------------------------------------------------------------------
    // internals
    // -----------------------------------------------------------------------------------------

    /**
     * Writes the low {@code 8*valueBytes} bits, most significant byte first. A width-agnostic loop
     * rather than a {@code putLong}/{@code putInt}/{@code putShort} switch because §5 already needs
     * three widths and §9 permits a fourth to arrive with a new encoding code; one loop cannot
     * acquire a per-width bug.
     */
    private static void putValue(ByteBuffer dst, long value, int valueBytes)
    {
        for (int shift = (valueBytes - 1) * 8; shift >= 0; shift -= 8)
            dst.put((byte) (value >>> shift));
    }

    private static long getValue(ByteBuffer src, int valueBytes)
    {
        long value = 0;
        for (int i = 0; i < valueBytes; i++)
            value = (value << 8) | (src.get() & 0xFFL);
        return value;
    }

    private static void checkValueFits(long value, int valueBytes, int index)
    {
        // Zero-extended, like the block table's extrema: a caller that sign-extended would hand the
        // same value two byte forms, and for DATE32 -- unsigned days -- a different value entirely.
        if (valueBytes < 8 && (value >>> (valueBytes * 8)) != 0)
            throw new IllegalArgumentException("exception value 0x" + Long.toHexString(value) + " at entry " + index +
                                               " does not fit " + valueBytes + " zero-extended bytes");
    }

    private static void checkValueBytes(int valueBytes)
    {
        // 2, 4 and 8 are the three §5 assigns. Anything else is a caller bug, not a width to honour:
        // an odd width would still round-trip and would silently make encodedLength disagree with
        // the size the width chooser scored.
        if (valueBytes != 2 && valueBytes != 4 && valueBytes != 8)
            throw new IllegalArgumentException("exception value width " + valueBytes +
                                               " is not one of 2, 4, 8 (v4 §5)");
    }

    private static void requireBigEndian(ByteBuffer buffer)
    {
        if (buffer.order() != ByteOrder.BIG_ENDIAN)
            throw new IllegalArgumentException("v4 exception areas are big-endian, buffer order is " + buffer.order());
    }
}
