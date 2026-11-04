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
import java.util.Arrays;

/**
 * The chunk-format-v4 §5 block encodings: the exact size of every candidate for one block, the
 * deterministic choice between them, and the encode/decode of the ones that do not need ALP.
 *
 * <p>A block <em>payload</em> is what follows the block's presence bytes inside its body; §4.5 puts
 * the two together and {@link ChunkV4Section} is where that composition lives. The payloads:
 *
 * <pre>
 *   0x01 EMPTY            0 B                       -- only when the block has no present value
 *   0x02 CONSTANT         0 B when statWidth &gt; 0     -- the value is the block table's min
 *                         varint len + bytes, pad 8 -- when statWidth == 0 there is no min to hold it
 *   0x03 PRESENCE_ONLY    0 B                       -- the value is the column's constant (directory)
 *   0x10 FOR_BITPACK      width u8 | pad 7 | lane(n, width) | [exception area]
 *   0x11 DELTA_FOR_BITPACK firstValue i64 | deltaBase i64 | width u8 | pad 7 |
 *                          lane(n-1, width) | [exception area]
 *   0x20 ALP / 0x21 ALP_RD                          -- routed to the {@link DoubleBlockCodec} seam
 *   0x30 BITPACK1         lane(n, 1)                -- boolean
 *   0x40 DICT             lane(n, ceil(log2(dictCount)))
 *   0x41 RAW              n x (canonical varint len + bytes), pad 8
 * </pre>
 *
 * <p><b>{@code n} is the present count, never the row count.</b> Every lane length here is
 * {@code ceil(n*width/64)*8} where {@code n} is the popcount of the block's presence words (§5, §6).
 * A block that is 1,024 rows and 3 present values has a three-value lane; using the row count would
 * not merely waste bytes, it would put every value at the wrong lane index and return other rows'
 * data without throwing.
 *
 * <p><b>The code points are {@link ChunkV4BlockTable}'s, not restated here.</b> Two definitions of
 * an encoding code is how a format acquires two layouts for one code, and the block table is where
 * the byte is written.
 *
 * <p><b>The choice among encodings is an argmin over an exact closed-form size, ties broken by the
 * lowest code point.</b> That is §5 rule 3, a determinism rule and not a preference: nothing here
 * trial-encodes, times, samples or consults a threshold. A replica that broke a tie the other way
 * would emit a byte-different chunk from the same input (§12). The candidates are evaluated in ascending code order and the incumbent is only displaced
 * by a strictly smaller size, so "first to reach the minimum keeps it" is exactly "lowest code
 * wins"; never relax that comparison to {@code <=}.
 *
 * <p>Two consequences of the tie rule are worth naming because they look like omissions:
 * {@code PRESENCE_ONLY} is unreachable on a column with a block-table min, since {@code CONSTANT} is
 * also 0 bytes there and has the lower code; and {@code DICT} beats {@code RAW} whenever they cost
 * the same. Both are stable, both are pinned by tests.
 *
 * <p><b>Values arrive in the column's arithmetic domain</b>: sign-extended for {@code INT32},
 * zero-extended for {@code DATE32} (unsigned days), the raw {@code doubleToRawLongBits} pattern for
 * {@code DOUBLE}, 0 or 1 for {@code BOOLEAN}. That is what makes {@code v - min} a non-negative
 * frame-of-reference residual for the types {@code FOR_BITPACK} accepts. It is deliberately
 * <em>not</em> the "raw bits, zero-extended" convention the block table uses for its extrema, so
 * {@link #toStatBits} and {@link #fromStatBits} convert at that boundary and nowhere else.
 *
 * <p>{@code FOR_BITPACK} takes its minimum from the block table entry rather than repeating it in
 * the body -- which is why its header is one width byte -- and that is only sound while the column's
 * {@link StatOrder} is the same order the subtraction assumes. It is therefore offered for
 * {@code INT32}, {@code INT64} and {@code DATE32} and never for {@code DOUBLE}: a double column's
 * extrema are ordered by {@code Double.compare} (§4), and the raw bits of a negative double ascend
 * as the value descends, so {@code rawBits(v) - rawBits(min)} is not a residual at all. Doubles are
 * ALP's or they are {@code RAW}.
 *
 * <p><b>The ALP seam.</b> {@code 0x20} and {@code 0x21} are declared and routed here and framed
 * there: {@link DoubleBlockCodec} is the interface, {@link AlpBlockCodec} the implementation over
 * the existing {@code AlpCodec}. What a codec must supply is on that interface, and one requirement
 * is worth repeating here because it is not local to it: the codec must be a build-time constant,
 * identical on every node. A codec that is present on some replicas and absent on others is a
 * byte-determinism break of exactly the shape §12 calls the format's largest risk -- which is why
 * {@link AlpBlockCodec#INSTANCE} is the only way to obtain one and there is no switch that removes
 * it.
 *
 * <p>Corruption is {@link IllegalArgumentException} (§11), including an ALP code arriving on a
 * caller that passed no codec: a v4 payload's encoding byte is only meaningful to a build that can
 * decode it, so inside one it is a flipped bit. The scalar decode path allocates nothing per value:
 * lanes unpack into a caller-supplied {@code long[]}, width choice reuses a caller-supplied
 * histogram, and the exception buffers live in a caller-supplied {@link Scratch}.
 */
public final class BlockEncodings
{
    private BlockEncodings()
    {
    }

    // -----------------------------------------------------------------------------------------
    // the ALP seam
    // -----------------------------------------------------------------------------------------

    /**
     * What makes {@code 0x20 ALP} and {@code 0x21 ALP_RD} live. {@link AlpBlockCodec} is the
     * implementation; this is the contract it is held to, kept here because it is a property of the
     * seam rather than of one codec.
     *
     * <p>The contract, in the order it bites:
     *
     * <ul>
     * <li>{@link #payloadLength} must be an <b>exact closed form</b> computed from a parameter
     *     search, not a trial encode. §5 rule 3 forbids encode-and-compare, and the argmin here
     *     calls it before any bytes exist -- the section lays out every following block's
     *     {@code bodyOffset} from the number it returns.</li>
     * <li>The parameter search must be reproducible: {@code strictfp}, a table of exact powers of
     *     ten rather than {@code Math.pow}, and a <b>specified</b> tie-break over {@code (e, f)}
     *     candidates. §12 names the unspecified tie-break as a concrete risk.</li>
     * <li>Sampling is permitted only if the sampling rule is specified and input-determined -- the
     *     reference implementation's speed comes from sampling, and §12 explicitly prefers a pinned
     *     sampling rule to a relaxed determinism rule.</li>
     * <li>Every payload must be a multiple of 8 bytes and every pad byte 0x00, verified on decode
     *     (§5 rule 5, §6).</li>
     * <li>Exceptions go in the shared {@link ExceptionArea}, {@code W} 8 for ALP and
     *     {@link ExceptionArea#VALUE_BYTES_ALP_RD_LEFT} for ALP-RD's left parts.</li>
     * <li>NaN payloads, -0.0, subnormals and the infinities must survive bit-exactly; a port that
     *     normalises NaN passes a round-trip test on ordinary data and corrupts a real column.</li>
     * <li>The codec is a build-time constant. One that is present on some replicas and absent on
     *     others makes identical input produce different bytes, which §12 calls the format's largest
     *     risk.</li>
     * </ul>
     *
     * <p><b>An ALP block leaves {@code blockFlags} bit 4 clear even when it carries an exception
     * area</b>, and {@link #decodeFixed} rejects one that has it set. The bit is how
     * {@code FOR_BITPACK} and {@code DELTA_FOR_BITPACK} say "an area follows the lane", and those
     * two publish their exception count into {@link Choice} so the section's layout can set it. This
     * interface has no such channel -- {@link #payloadLength} returns a length and nothing else --
     * and {@link #decode} is handed the derived body length rather than the flag, so a codec could
     * neither set the bit nor read it.
     *
     * <p>A codec that carries exceptions must therefore say so <em>inside its payload</em>, and must
     * check that against the length it was handed; {@link AlpBlockCodec#FLAG_HAS_EXCEPTIONS} is how.
     * Length arithmetic alone is not enough and the difference is not academic: a body length
     * corrupted downwards by exactly an area's size makes the block parse as a shorter,
     * self-consistent one whose exception rows come back as whatever their filler lane slot decodes
     * to -- wrong values, no throw. Bit 4 is what stops the same thing happening to
     * {@code FOR_BITPACK}.
     */
    public interface DoubleBlockCodec
    {
        /**
         * Exact payload bytes {@code encoding} would occupy for these raw double bit patterns, or a
         * negative value when the encoding does not apply to this block.
         */
        long payloadLength(int encoding, long[] rawBits, int count);

        /** Writes the payload at {@code dst}'s position; must return {@link #payloadLength}. */
        int encode(int encoding, long[] rawBits, int count, ByteBuffer dst);

        /**
         * Decodes {@code count} raw bit patterns into {@code dst}, consuming exactly
         * {@code payloadLength} bytes and rejecting anything else as corruption.
         */
        void decode(int encoding, ByteBuffer payload, int payloadLength, int count, long[] dst);
    }

    // -----------------------------------------------------------------------------------------
    // the choice, and the scratch it runs in
    // -----------------------------------------------------------------------------------------

    /**
     * The outcome of {@link #chooseFixed}/{@link #chooseVariable}: an encoding code, the parameters
     * that encoding needs, and the exact payload length the argmin scored.
     *
     * <p>Caller-supplied and reused across blocks, in the style of
     * {@link BitPacking#chooseWidth(long[], int, int, int[])}'s histogram. The length matters as
     * much as the code: the section lays out {@code bodyOffset}s from these numbers before a single
     * payload byte is written, so an encoder whose output length disagreed with the number scored
     * here would corrupt every following block's offset. The encode methods assert the agreement.
     */
    public static final class Choice
    {
        /** One of {@link ChunkV4BlockTable}'s {@code ENC_*} codes. */
        public int encoding;
        /** Lane width for {@code FOR_BITPACK}, {@code DELTA_FOR_BITPACK} and {@code DICT}. */
        public int width;
        /** Exceptions the chosen encoding will emit; 0 means {@code blockFlags} bit 4 stays clear. */
        public int exceptionCount;
        /** {@code DELTA_FOR_BITPACK}: the first present value, stored verbatim in the header. */
        public long firstValue;
        /** {@code DELTA_FOR_BITPACK}: the frame of reference the deltas are stored against. */
        public long deltaBase;
        /** Exact payload bytes, always a multiple of 8 (§6). */
        public int payloadLength;

        public boolean hasExceptions()
        {
            return exceptionCount > 0;
        }

        private void set(int encoding, int payloadLength)
        {
            this.encoding = encoding;
            this.payloadLength = payloadLength;
            this.width = 0;
            this.exceptionCount = 0;
            this.firstValue = 0;
            this.deltaBase = 0;
        }
    }

    /**
     * Per-block working memory, sized once from the block size so that a block loop allocates
     * nothing. Same contract as the histogram {@link BitPacking#chooseWidth} takes: the caller owns
     * it, the codec never grows it, and nothing in it is sized from payload content.
     */
    public static final class Scratch
    {
        final long[] residuals;
        final int[] histogram;
        final int[] exceptionPositions;
        final long[] exceptionValues;

        public Scratch(int maxValuesPerBlock)
        {
            if (maxValuesPerBlock < 1 || maxValuesPerBlock > BlockPresence.MAX_BLOCK_ROWS)
                throw new IllegalArgumentException("maxValuesPerBlock " + maxValuesPerBlock + " outside 1.." +
                                                   BlockPresence.MAX_BLOCK_ROWS);
            this.residuals = new long[maxValuesPerBlock];
            this.histogram = new int[BitPacking.WIDTH_HISTOGRAM_SIZE];
            this.exceptionPositions = new int[maxValuesPerBlock];
            this.exceptionValues = new long[maxValuesPerBlock];
        }

        private void require(int count)
        {
            if (residuals.length < count)
                throw new IllegalArgumentException("scratch holds " + residuals.length + " < count " + count);
        }
    }

    // -----------------------------------------------------------------------------------------
    // the value domain
    // -----------------------------------------------------------------------------------------

    /** True for the types whose values this class carries in a {@code long}. */
    public static boolean isFixedWidth(int typeCode)
    {
        switch (typeCode)
        {
            case ChunkV4Directory.TYPE_DOUBLE:
            case ChunkV4Directory.TYPE_BOOLEAN:
            case ChunkV4Directory.TYPE_INT32:
            case ChunkV4Directory.TYPE_INT64:
            case ChunkV4Directory.TYPE_DATE32:
                return true;
            default:
                ChunkV4Directory.statWidth(typeCode);   // rejects 0x00 and unallocated codes
                return false;
        }
    }

    /** Serialized width of a fixed-width value: 8, 4, or 1 for {@code BOOLEAN}. */
    public static int fixedWidthBytes(int typeCode)
    {
        switch (typeCode)
        {
            case ChunkV4Directory.TYPE_DOUBLE:
            case ChunkV4Directory.TYPE_INT64:
                return 8;
            case ChunkV4Directory.TYPE_INT32:
            case ChunkV4Directory.TYPE_DATE32:
                return 4;
            case ChunkV4Directory.TYPE_BOOLEAN:
                return 1;
            default:
                throw new IllegalArgumentException("type code " + typeCode + " is not fixed width");
        }
    }

    /**
     * Rejects a value outside its type's domain. Not defensive noise: an {@code INT32} value that is
     * not sign-extended, or a {@code BOOLEAN} that is neither 0 nor 1, would be narrowed on the way
     * into a {@code RAW} body or an exception entry and widened back to something else -- a silently
     * wrong value that round-trips consistently within one build.
     */
    public static void checkValueDomain(long value, int typeCode)
    {
        switch (typeCode)
        {
            case ChunkV4Directory.TYPE_INT32:
                if (value != (int) value)
                    throw new IllegalArgumentException("INT32 value " + value + " is not sign-extended into a long");
                break;
            case ChunkV4Directory.TYPE_DATE32:
                if ((value >>> 32) != 0)
                    throw new IllegalArgumentException("DATE32 value 0x" + Long.toHexString(value) +
                                                       " is not a zero-extended unsigned day count");
                break;
            case ChunkV4Directory.TYPE_BOOLEAN:
                // Cassandra's serializer writes 0 or 1 and its comparator normalises anything else
                // to true, so a second truthy byte would be a second byte form of one value.
                if (value != 0 && value != 1)
                    throw new IllegalArgumentException("BOOLEAN value " + value + " is neither 0 nor 1");
                break;
            case ChunkV4Directory.TYPE_DOUBLE:
            case ChunkV4Directory.TYPE_INT64:
                break;
            default:
                throw new IllegalArgumentException("type code " + typeCode + " is not fixed width");
        }
    }

    /**
     * The order this type's values sort under, matching the column's {@link StatOrder} exactly --
     * {@code SIGNED_INT} for {@code INT32}/{@code INT64}, {@code UNSIGNED_INT} for {@code DATE32}
     * (whose serializer shifts by 2^31 so unsigned order is chronological), {@code IEEE754_TOTAL}
     * for {@code DOUBLE}.
     *
     * <p>The agreement is not decorative. The block table's min is both the pruning statistic and
     * {@code FOR_BITPACK}'s frame of reference, so a min computed under one order and subtracted
     * under another produces negative residuals -- a 64-bit lane on data that wanted ten bits, or,
     * for {@code DATE32}, extrema that are not bounds. A test pins this function against
     * {@link StatOrder} over serialized bytes rather than trusting the pairing to stay true.
     */
    public static int compareValues(long left, long right, int typeCode)
    {
        switch (typeCode)
        {
            case ChunkV4Directory.TYPE_DOUBLE:
                return Double.compare(Double.longBitsToDouble(left), Double.longBitsToDouble(right));
            case ChunkV4Directory.TYPE_DATE32:
                return Long.compareUnsigned(left, right);
            default:
                return Long.compare(left, right);
        }
    }

    /** Big-endian serialization of a fixed-width value, {@link #fixedWidthBytes} long. */
    public static byte[] toFixedBytes(long value, int typeCode)
    {
        checkValueDomain(value, typeCode);
        int width = fixedWidthBytes(typeCode);
        byte[] out = new byte[width];
        for (int i = 0; i < width; i++)
            out[i] = (byte) (value >>> (8 * (width - 1 - i)));
        return out;
    }

    /** Inverse of {@link #toFixedBytes}, widened back into the type's arithmetic domain. */
    public static long fromFixedBytes(byte[] bytes, int typeCode)
    {
        int width = fixedWidthBytes(typeCode);
        if (bytes == null || bytes.length != width)
            throw new IllegalArgumentException("Corrupt v4 chunk: a " + width + "-byte type carries a " +
                                               (bytes == null ? "null" : bytes.length + "-byte") + " value");
        long raw = 0;
        for (byte b : bytes)
            raw = (raw << 8) | (b & 0xFFL);
        long value = widen(raw, typeCode);
        checkValueDomain(value, typeCode);
        return value;
    }

    /**
     * A value in this type's arithmetic domain, as the block table stores it: raw fixed-width bits,
     * zero-extended (see {@link ChunkV4BlockTable.Entry}). Zero for a {@code statWidth 0} type,
     * which has no extrema field at all.
     */
    public static long toStatBits(long value, int typeCode)
    {
        int statWidth = ChunkV4Directory.statWidth(typeCode);
        if (statWidth == 8)
            return value;
        if (statWidth == 4)
        {
            checkValueDomain(value, typeCode);
            return value & 0xFFFFFFFFL;
        }
        if (value != 0)
            throw new IllegalArgumentException("type code " + typeCode + " has no block extrema; unused stat fields " +
                                               "are zero (v4 §5 rule 6)");
        return 0;
    }

    /** Inverse of {@link #toStatBits}: the block table's raw bits back in the arithmetic domain. */
    public static long fromStatBits(long bits, int typeCode)
    {
        int statWidth = ChunkV4Directory.statWidth(typeCode);
        if (statWidth == 8)
            return bits;
        if (statWidth == 4)
        {
            if ((bits >>> 32) != 0)
                throw new IllegalArgumentException("Corrupt v4 chunk: statWidth 4 extremum 0x" +
                                                   Long.toHexString(bits) + " is not zero-extended");
            return widen(bits, typeCode);
        }
        if (bits != 0)
            throw new IllegalArgumentException("Corrupt v4 chunk: statWidth 0 block carries extremum " + bits);
        return 0;
    }

    private static long widen(long raw, int typeCode)
    {
        // INT32 is the only type whose stored bits are narrower than its domain and signed; DATE32
        // is unsigned by construction and the eight-byte types are already whole.
        return typeCode == ChunkV4Directory.TYPE_INT32 ? (int) raw : raw;
    }

    /** {@code ceil(log2(dictCount))}: the bits a dictionary code occupies (§5). Zero for one entry. */
    public static int dictionaryCodeBits(int dictCount)
    {
        if (dictCount < 1 || dictCount > ChunkV4Section.MAX_DICTIONARY_ENTRIES)
            throw new IllegalArgumentException("dictCount " + dictCount + " outside 1.." +
                                               ChunkV4Section.MAX_DICTIONARY_ENTRIES);
        return dictCount == 1 ? 0 : 32 - Integer.numberOfLeadingZeros(dictCount - 1);
    }

    // -----------------------------------------------------------------------------------------
    // sizing
    // -----------------------------------------------------------------------------------------

    /** {@code varint len + bytes}, padded to 8 (§6): the {@code CONSTANT} body on a statWidth 0 type. */
    static int constantBodyLength(int valueLength)
    {
        return (int) BitPacking.roundUpToMultipleOf8(BlockPresence.varIntLength(valueLength) + (long) valueLength);
    }

    /** {@code n x (varint len + bytes)}, padded to 8 (§6). */
    private static int rawLength(long unpadded)
    {
        long padded = BitPacking.roundUpToMultipleOf8(unpadded);
        if (padded > Integer.MAX_VALUE)
            throw new IllegalArgumentException("RAW block of " + padded + " bytes overflows int");
        return (int) padded;
    }

    // -----------------------------------------------------------------------------------------
    // choice: fixed width
    // -----------------------------------------------------------------------------------------

    /**
     * The §5 rule 3 argmin over every encoding applicable to this fixed-width block.
     *
     * @param values         the block's present values, densely packed, in the arithmetic domain
     * @param count          {@code n}, the present count -- never the row count
     * @param min            the block's minimum under {@link #compareValues}, i.e. what the block
     *                       table entry will carry; unused when {@code count == 0}
     * @param columnConstant the column-level constant from the directory, or null; its presence is
     *                       what makes {@code PRESENCE_ONLY} available
     * @param alp            the ALP seam, or null while it is unimplemented
     */
    public static void chooseFixed(int typeCode,
                                   long[] values,
                                   int count,
                                   long min,
                                   byte[] columnConstant,
                                   DoubleBlockCodec alp,
                                   Scratch scratch,
                                   Choice out)
    {
        checkFixedInputs(typeCode, values, count, scratch);
        out.set(0, -1);

        // 0x01 EMPTY. An empty block has no other candidate: every encoding below needs a value.
        if (count == 0)
        {
            out.set(ChunkV4BlockTable.ENC_EMPTY, 0);
            return;
        }

        for (int i = 0; i < count; i++)
            checkValueDomain(values[i], typeCode);
        // FOR_BITPACK subtracts this without storing it, so a min that is not the minimum under the
        // column's declared order does not merely waste lane width -- it produces residuals that
        // wrap, and the block table's pruning statistic is wrong in the direction that loses rows.
        if (!isMinimum(values, count, min, typeCode))
            throw new IllegalArgumentException("declared block minimum " + min + " is not the minimum of the block's " +
                                               count + " values under the type's order");

        int statWidth = ChunkV4Directory.statWidth(typeCode);
        boolean uniform = allEqual(values, count);
        int best = Integer.MAX_VALUE;

        // 0x02 CONSTANT. Free when the block table already holds the value; otherwise the value has
        // to be in the body, which is the only reason a statWidth 0 type pays anything for it.
        if (uniform)
        {
            int length = statWidth > 0 ? 0 : constantBodyLength(fixedWidthBytes(typeCode));
            if (length < best)
            {
                best = length;
                out.set(ChunkV4BlockTable.ENC_CONSTANT, length);
            }
        }

        // 0x03 PRESENCE_ONLY. Loses the tie to CONSTANT whenever the column has a block-table min,
        // so in practice it is the BOOLEAN/TEXT/OPAQUE spelling of "this block adds no value bytes".
        if (uniform && columnConstant != null && fromFixedBytes(columnConstant, typeCode) == values[0])
        {
            if (0 < best)
            {
                best = 0;
                out.set(ChunkV4BlockTable.ENC_PRESENCE_ONLY, 0);
            }
        }

        if (packsIntoALane(typeCode))
        {
            int valueBytes = ExceptionArea.valueBytes(typeCode);

            // 0x10 FOR_BITPACK: v - min, the min coming from the block table entry.
            forResiduals(values, count, min, scratch.residuals);
            int forWidth = BitPacking.chooseWidth(scratch.residuals, count, valueBytes, scratch.histogram);
            int forExceptions = BitPacking.exceptionCount(scratch.residuals, count, forWidth);
            int forLength = FOR_HEADER_BYTES + BitPacking.packedLength(count, forWidth)
                            + ExceptionArea.encodedLength(forExceptions, valueBytes);
            if (forLength < best)
            {
                best = forLength;
                out.set(ChunkV4BlockTable.ENC_FOR_BITPACK, forLength);
                out.width = forWidth;
                out.exceptionCount = forExceptions;
            }

            // 0x11 DELTA_FOR_BITPACK: (v[i] - v[i-1]) - deltaBase for i in 1..n-1.
            long deltaBase = deltaResiduals(values, count, scratch.residuals);
            int deltaCount = count - 1;
            int deltaWidth = BitPacking.chooseWidth(scratch.residuals, deltaCount, valueBytes, scratch.histogram);
            int deltaExceptions = BitPacking.exceptionCount(scratch.residuals, deltaCount, deltaWidth);
            int deltaLength = DELTA_HEADER_BYTES + BitPacking.packedLength(deltaCount, deltaWidth)
                              + ExceptionArea.encodedLength(deltaExceptions, valueBytes);
            if (deltaLength < best)
            {
                best = deltaLength;
                out.set(ChunkV4BlockTable.ENC_DELTA_FOR_BITPACK, deltaLength);
                out.width = deltaWidth;
                out.exceptionCount = deltaExceptions;
                out.firstValue = values[0];
                out.deltaBase = deltaBase;
            }
        }

        // 0x20 ALP / 0x21 ALP_RD, when the seam is filled. Both are asked for an exact closed form;
        // neither is trial-encoded (§5 rule 3).
        if (typeCode == ChunkV4Directory.TYPE_DOUBLE && alp != null)
        {
            for (int encoding : ALP_CANDIDATES)
            {
                long length = alp.payloadLength(encoding, values, count);
                if (length < 0)
                    continue;
                if ((length & 7) != 0 || length > Integer.MAX_VALUE)
                    throw new IllegalArgumentException("ALP codec reported payload length " + length +
                                                       ", which is not an 8-aligned int (v4 §6)");
                if (length < best)
                {
                    best = (int) length;
                    out.set(encoding, (int) length);
                }
            }
        }

        // 0x30 BITPACK1.
        if (typeCode == ChunkV4Directory.TYPE_BOOLEAN)
        {
            int length = BitPacking.packedLength(count, 1);
            if (length < best)
            {
                best = length;
                out.set(ChunkV4BlockTable.ENC_BITPACK1, length);
                out.width = 1;
            }
        }

        // 0x41 RAW: the universal fallback, and the reason no block is ever unencodable. With the
        // ALP seam filled it should never win for a double block; without it, it is what keeps a
        // double column encodable at all.
        int width = fixedWidthBytes(typeCode);
        int rawLength = rawLength((long) count * (BlockPresence.varIntLength(width) + width));
        if (rawLength < best)
        {
            best = rawLength;
            out.set(ChunkV4BlockTable.ENC_RAW, rawLength);
        }

        if (out.payloadLength < 0)
            throw new IllegalArgumentException("no encoding applies to a " + count + "-value block of type " + typeCode);
    }

    // -----------------------------------------------------------------------------------------
    // choice: variable width
    // -----------------------------------------------------------------------------------------

    /**
     * The §5 rule 3 argmin for a {@code TEXT}/{@code OPAQUE} block.
     *
     * @param dictionary the section's dictionary in ascending unsigned byte order, or null when the
     *                   section carries none; whether to build one at all is a section-level exact
     *                   size comparison and lives in {@link ChunkV4Section}
     */
    public static void chooseVariable(int typeCode,
                                      byte[][] values,
                                      int count,
                                      byte[] columnConstant,
                                      byte[][] dictionary,
                                      Choice out)
    {
        checkVariableInputs(typeCode, values, count);
        out.set(0, -1);

        if (count == 0)
        {
            out.set(ChunkV4BlockTable.ENC_EMPTY, 0);
            return;
        }

        boolean uniform = allEqual(values, count);
        int best = Integer.MAX_VALUE;

        if (uniform)
        {
            int length = constantBodyLength(values[0].length);
            if (length < best)
            {
                best = length;
                out.set(ChunkV4BlockTable.ENC_CONSTANT, length);
            }
        }
        if (uniform && columnConstant != null && Arrays.equals(values[0], columnConstant))
        {
            if (0 < best)
            {
                best = 0;
                out.set(ChunkV4BlockTable.ENC_PRESENCE_ONLY, 0);
            }
        }
        if (dictionary != null && dictionary.length > 0)
        {
            int codeBits = dictionaryCodeBits(dictionary.length);
            int length = BitPacking.packedLength(count, codeBits);
            if (length < best)
            {
                best = length;
                out.set(ChunkV4BlockTable.ENC_DICT, length);
                out.width = codeBits;
            }
        }

        long unpadded = 0;
        for (int i = 0; i < count; i++)
            unpadded += BlockPresence.varIntLength(values[i].length) + (long) values[i].length;
        int rawLength = rawLength(unpadded);
        if (rawLength < best)
            out.set(ChunkV4BlockTable.ENC_RAW, rawLength);

        if (out.payloadLength < 0)
            throw new IllegalArgumentException("no encoding applies to a " + count + "-value block of type " + typeCode);
    }

    // -----------------------------------------------------------------------------------------
    // encode
    // -----------------------------------------------------------------------------------------

    /** {@code width u8 | pad 7}: one byte of information, 8-aligned so the lane starts on a word. */
    static final int FOR_HEADER_BYTES = 8;
    /** {@code firstValue i64 | deltaBase i64 | width u8 | pad 7}. */
    static final int DELTA_HEADER_BYTES = 24;

    /**
     * Writes the payload {@code choice} describes at {@code dst}'s position, advancing it by and
     * returning {@link Choice#payloadLength}.
     *
     * <p>The residuals are recomputed here rather than carried over from {@link #chooseFixed},
     * through the same two private functions, so that the width the argmin scored and the width the
     * lane is packed at cannot drift apart.
     */
    public static int encodeFixed(Choice choice,
                                  int typeCode,
                                  long[] values,
                                  int count,
                                  long min,
                                  DoubleBlockCodec alp,
                                  Scratch scratch,
                                  ByteBuffer dst)
    {
        checkFixedInputs(typeCode, values, count, scratch);
        requireBigEndian(dst);
        if (dst.remaining() < choice.payloadLength)
            throw new IllegalArgumentException("destination holds " + dst.remaining() + " < payload " +
                                               choice.payloadLength);
        int base = dst.position();
        switch (choice.encoding)
        {
            case ChunkV4BlockTable.ENC_EMPTY:
            case ChunkV4BlockTable.ENC_PRESENCE_ONLY:
                break;
            case ChunkV4BlockTable.ENC_CONSTANT:
                // Nothing to write when the block table carries the value; §5 rule 6's "unused
                // fields are zero" is satisfied by there being no field.
                if (ChunkV4Directory.statWidth(typeCode) == 0)
                    writeLengthPrefixedPadded(dst, toFixedBytes(values[0], typeCode), choice.payloadLength);
                break;
            case ChunkV4BlockTable.ENC_FOR_BITPACK:
            {
                forResiduals(values, count, min, scratch.residuals);
                int exceptions = extractExceptions(scratch, values, 0, count, choice.width, typeCode);
                writeWidthHeader(dst, choice.width);
                BitPacking.pack(scratch.residuals, count, choice.width, dst);
                writeExceptions(dst, exceptions, choice, typeCode, scratch);
                break;
            }
            case ChunkV4BlockTable.ENC_DELTA_FOR_BITPACK:
            {
                long deltaBase = deltaResiduals(values, count, scratch.residuals);
                int exceptions = extractExceptions(scratch, values, 1, count - 1, choice.width, typeCode);
                dst.putLong(values[0]);
                dst.putLong(deltaBase);
                writeWidthHeader(dst, choice.width);
                BitPacking.pack(scratch.residuals, count - 1, choice.width, dst);
                writeExceptions(dst, exceptions, choice, typeCode, scratch);
                break;
            }
            case ChunkV4BlockTable.ENC_ALP:
            case ChunkV4BlockTable.ENC_ALP_RD:
                requireAlp(alp, choice.encoding).encode(choice.encoding, values, count, dst);
                break;
            case ChunkV4BlockTable.ENC_BITPACK1:
                BitPacking.pack(values, count, 1, dst);
                break;
            case ChunkV4BlockTable.ENC_RAW:
            {
                for (int i = 0; i < count; i++)
                {
                    byte[] bytes = toFixedBytes(values[i], typeCode);
                    BlockPresence.writeVarInt(dst, bytes.length);
                    dst.put(bytes);
                }
                pad(dst, base, choice.payloadLength);
                break;
            }
            default:
                throw new IllegalArgumentException("encoding 0x" + Integer.toHexString(choice.encoding) +
                                                   " does not apply to a fixed-width block");
        }
        int written = dst.position() - base;
        if (written != choice.payloadLength)
            throw new IllegalArgumentException("encoding 0x" + Integer.toHexString(choice.encoding) + " wrote " +
                                               written + " bytes, the argmin scored " + choice.payloadLength);
        return written;
    }

    /** {@link #encodeFixed} for a {@code TEXT}/{@code OPAQUE} block. */
    public static int encodeVariable(Choice choice,
                                     int typeCode,
                                     byte[][] values,
                                     int count,
                                     byte[][] dictionary,
                                     Scratch scratch,
                                     ByteBuffer dst)
    {
        checkVariableInputs(typeCode, values, count);
        scratch.require(Math.max(count, 1));
        requireBigEndian(dst);
        if (dst.remaining() < choice.payloadLength)
            throw new IllegalArgumentException("destination holds " + dst.remaining() + " < payload " +
                                               choice.payloadLength);
        int base = dst.position();
        switch (choice.encoding)
        {
            case ChunkV4BlockTable.ENC_EMPTY:
            case ChunkV4BlockTable.ENC_PRESENCE_ONLY:
                break;
            case ChunkV4BlockTable.ENC_CONSTANT:
                writeLengthPrefixedPadded(dst, values[0], choice.payloadLength);
                break;
            case ChunkV4BlockTable.ENC_DICT:
            {
                if (dictionary == null)
                    throw new IllegalArgumentException("DICT block without a section dictionary");
                for (int i = 0; i < count; i++)
                {
                    int code = ChunkV4Section.dictionaryCodeOf(dictionary, values[i]);
                    if (code < 0)
                        throw new IllegalArgumentException("value at index " + i + " is not in the section dictionary");
                    scratch.residuals[i] = code;
                }
                BitPacking.pack(scratch.residuals, count, choice.width, dst);
                break;
            }
            case ChunkV4BlockTable.ENC_RAW:
                for (int i = 0; i < count; i++)
                {
                    BlockPresence.writeVarInt(dst, values[i].length);
                    dst.put(values[i]);
                }
                pad(dst, base, choice.payloadLength);
                break;
            default:
                throw new IllegalArgumentException("encoding 0x" + Integer.toHexString(choice.encoding) +
                                                   " does not apply to a variable-width block");
        }
        int written = dst.position() - base;
        if (written != choice.payloadLength)
            throw new IllegalArgumentException("encoding 0x" + Integer.toHexString(choice.encoding) + " wrote " +
                                               written + " bytes, the argmin scored " + choice.payloadLength);
        return written;
    }

    // -----------------------------------------------------------------------------------------
    // decode
    // -----------------------------------------------------------------------------------------

    /**
     * Decodes {@code count} values into {@code dst[0..count)}, consuming exactly
     * {@code payloadLength} bytes of {@code payload}.
     *
     * <p>The length is checked against what the encoding accounts for rather than trusted, which is
     * what turns a wrong {@code blockFlags} bit 4 -- an exception area claimed but absent, or
     * present but unclaimed -- into an exception instead of a misread lane.
     *
     * @param hasExceptions the block table's {@code blockFlags} bit 4
     * @param min           the block table's min, already in the arithmetic domain
     */
    public static void decodeFixed(int encoding,
                                   boolean hasExceptions,
                                   ByteBuffer payload,
                                   int payloadLength,
                                   int typeCode,
                                   int count,
                                   long min,
                                   byte[] columnConstant,
                                   DoubleBlockCodec alp,
                                   Scratch scratch,
                                   long[] dst)
    {
        if (!isFixedWidth(typeCode))
            throw new IllegalArgumentException("type code " + typeCode + " is not fixed width");
        requireBigEndian(payload);
        if (count < 0 || dst.length < count)
            throw new IllegalArgumentException("destination holds " + dst.length + " < count " + count);
        if (payloadLength < 0 || payload.remaining() < payloadLength)
            throw new IllegalArgumentException("Corrupt v4 chunk: block payload claims " + payloadLength +
                                               " bytes, body holds " + payload.remaining());
        scratch.require(Math.max(count, 1));
        int base = payload.position();

        switch (encoding)
        {
            case ChunkV4BlockTable.ENC_EMPTY:
                if (count != 0)
                    throw new IllegalArgumentException("Corrupt v4 chunk: EMPTY block with " + count +
                                                       " present values");
                break;
            case ChunkV4BlockTable.ENC_CONSTANT:
            {
                requireNonEmpty(count, encoding);
                long value;
                if (ChunkV4Directory.statWidth(typeCode) == 0)
                    value = fromFixedBytes(readLengthPrefixedPadded(payload, payloadLength, fixedWidthBytes(typeCode)),
                                           typeCode);
                else
                    value = min;
                checkValueDomain(value, typeCode);
                Arrays.fill(dst, 0, count, value);
                break;
            }
            case ChunkV4BlockTable.ENC_PRESENCE_ONLY:
            {
                requireNonEmpty(count, encoding);
                if (columnConstant == null)
                    throw new IllegalArgumentException("Corrupt v4 chunk: PRESENCE_ONLY block on a column with no " +
                                                       "constant in the directory");
                Arrays.fill(dst, 0, count, fromFixedBytes(columnConstant, typeCode));
                break;
            }
            case ChunkV4BlockTable.ENC_FOR_BITPACK:
            {
                requireNonEmpty(count, encoding);
                requireLanePacked(typeCode, encoding);
                int width = readWidthHeader(payload);
                BitPacking.unpack(payload, count, width, dst);
                int exceptions = readExceptions(payload, hasExceptions, typeCode, count, scratch);
                ExceptionArea.checkLaneIsZeroAtExceptions(dst, count, scratch.exceptionPositions, exceptions);
                for (int i = 0; i < count; i++)
                    dst[i] += min;
                for (int e = 0; e < exceptions; e++)
                {
                    long value = widen(scratch.exceptionValues[e], typeCode);
                    checkValueDomain(value, typeCode);
                    dst[scratch.exceptionPositions[e]] = value;
                }
                checkDecodedDomain(dst, count, typeCode);
                break;
            }
            case ChunkV4BlockTable.ENC_DELTA_FOR_BITPACK:
            {
                requireNonEmpty(count, encoding);
                requireLanePacked(typeCode, encoding);
                // Checked before the reads, not left to BufferUnderflowException: §11 requires every
                // corrupt byte to surface as IllegalArgumentException, and a truncated body must not
                // escape the codec as a JDK buffer error.
                if (payload.remaining() < DELTA_HEADER_BYTES)
                    throw new IllegalArgumentException("Corrupt v4 chunk: truncated DELTA_FOR_BITPACK header, have " +
                                                       payload.remaining() + " bytes");
                long firstValue = payload.getLong();
                long deltaBase = payload.getLong();
                checkValueDomain(firstValue, typeCode);
                int width = readWidthHeader(payload);
                int deltaCount = count - 1;
                BitPacking.unpack(payload, deltaCount, width, scratch.residuals);
                int exceptions = readExceptions(payload, hasExceptions, typeCode, deltaCount, scratch);
                ExceptionArea.checkLaneIsZeroAtExceptions(scratch.residuals, deltaCount,
                                                          scratch.exceptionPositions, exceptions);
                // The overrides have to land inside the prefix sum, not after it: value j+1 is the
                // base of value j+2, so an exception applied afterwards would leave every later
                // value in the block offset by the gap it was meant to absorb.
                dst[0] = firstValue;
                int next = 0;
                for (int j = 0; j < deltaCount; j++)
                {
                    if (next < exceptions && scratch.exceptionPositions[next] == j)
                    {
                        long value = widen(scratch.exceptionValues[next], typeCode);
                        checkValueDomain(value, typeCode);
                        dst[j + 1] = value;
                        next++;
                    }
                    else
                    {
                        dst[j + 1] = dst[j] + deltaBase + scratch.residuals[j];
                    }
                }
                checkDecodedDomain(dst, count, typeCode);
                break;
            }
            case ChunkV4BlockTable.ENC_ALP:
            case ChunkV4BlockTable.ENC_ALP_RD:
            {
                requireNonEmpty(count, encoding);
                if (typeCode != ChunkV4Directory.TYPE_DOUBLE)
                    throw new IllegalArgumentException("Corrupt v4 chunk: ALP encoding on a non-double column");
                // An ALP block's exception area is located by arithmetic on payloadLength, not by
                // this flag -- see the DoubleBlockCodec javadoc. Bit 4 is therefore always clear on
                // one, and a set bit is a flipped bit rather than a variant spelling.
                if (hasExceptions)
                    throw new IllegalArgumentException("Corrupt v4 chunk: blockFlags bit 4 set on encoding 0x" +
                                                       Integer.toHexString(encoding) + "; an ALP block locates its " +
                                                       "exception area by payload length and leaves the bit clear");
                requireAlp(alp, encoding).decode(encoding, payload, payloadLength, count, dst);
                break;
            }
            case ChunkV4BlockTable.ENC_BITPACK1:
            {
                requireNonEmpty(count, encoding);
                if (typeCode != ChunkV4Directory.TYPE_BOOLEAN)
                    throw new IllegalArgumentException("Corrupt v4 chunk: BITPACK1 on a non-boolean column");
                BitPacking.unpack(payload, count, 1, dst);
                break;
            }
            case ChunkV4BlockTable.ENC_RAW:
            {
                requireNonEmpty(count, encoding);
                int width = fixedWidthBytes(typeCode);
                byte[] value = new byte[width];
                for (int i = 0; i < count; i++)
                {
                    long length = BlockPresence.readVarInt(payload);
                    if (length != width)
                        throw new IllegalArgumentException("Corrupt v4 chunk: RAW value " + i + " is " + length +
                                                           " bytes, a " + width + "-byte type");
                    if (payload.remaining() < width)
                        throw new IllegalArgumentException("Corrupt v4 chunk: truncated RAW value " + i);
                    payload.get(value);
                    dst[i] = fromFixedBytes(value, typeCode);
                }
                checkPadding(payload, base, payloadLength);
                break;
            }
            default:
                throw new IllegalArgumentException("Corrupt v4 chunk: unknown block encoding 0x" +
                                                   Integer.toHexString(encoding));
        }
        checkConsumed(payload, base, payloadLength, encoding);
    }

    /**
     * {@link #decodeFixed} for a {@code TEXT}/{@code OPAQUE} block.
     *
     * <p>A {@code DICT} block hands out references to the section's dictionary entries rather than
     * copies -- the dictionary outlives every block that cites it, so a copy per value would be pure
     * garbage. The arrays must not be mutated by the caller. {@code RAW} and {@code CONSTANT} have
     * no such backing store and do allocate.
     */
    public static void decodeVariable(int encoding,
                                      ByteBuffer payload,
                                      int payloadLength,
                                      int typeCode,
                                      int count,
                                      byte[] columnConstant,
                                      byte[][] dictionary,
                                      Scratch scratch,
                                      byte[][] dst)
    {
        if (isFixedWidth(typeCode))
            throw new IllegalArgumentException("type code " + typeCode + " is not variable width");
        scratch.require(Math.max(count, 1));
        requireBigEndian(payload);
        if (count < 0 || dst.length < count)
            throw new IllegalArgumentException("destination holds " + dst.length + " < count " + count);
        if (payloadLength < 0 || payload.remaining() < payloadLength)
            throw new IllegalArgumentException("Corrupt v4 chunk: block payload claims " + payloadLength +
                                               " bytes, body holds " + payload.remaining());
        int base = payload.position();

        switch (encoding)
        {
            case ChunkV4BlockTable.ENC_EMPTY:
                if (count != 0)
                    throw new IllegalArgumentException("Corrupt v4 chunk: EMPTY block with " + count +
                                                       " present values");
                break;
            case ChunkV4BlockTable.ENC_CONSTANT:
            {
                requireNonEmpty(count, encoding);
                byte[] value = readLengthPrefixedPadded(payload, payloadLength, -1);
                Arrays.fill(dst, 0, count, value);
                break;
            }
            case ChunkV4BlockTable.ENC_PRESENCE_ONLY:
                requireNonEmpty(count, encoding);
                if (columnConstant == null)
                    throw new IllegalArgumentException("Corrupt v4 chunk: PRESENCE_ONLY block on a column with no " +
                                                       "constant in the directory");
                Arrays.fill(dst, 0, count, columnConstant);
                break;
            case ChunkV4BlockTable.ENC_DICT:
            {
                requireNonEmpty(count, encoding);
                if (dictionary == null || dictionary.length == 0)
                    throw new IllegalArgumentException("Corrupt v4 chunk: DICT block in a section with no dictionary");
                int codeBits = dictionaryCodeBits(dictionary.length);
                long[] codes = scratch.residuals;
                BitPacking.unpack(payload, count, codeBits, codes);
                for (int i = 0; i < count; i++)
                {
                    // A code is representable that the dictionary does not define whenever dictCount
                    // is not a power of two; unchecked it would be an array index out of bounds
                    // escaping the codec as the wrong exception type.
                    if (codes[i] >= dictionary.length)
                        throw new IllegalArgumentException("Corrupt v4 chunk: dictionary code " + codes[i] +
                                                           " at value " + i + ", dictionary holds " +
                                                           dictionary.length);
                    dst[i] = dictionary[(int) codes[i]];
                }
                break;
            }
            case ChunkV4BlockTable.ENC_RAW:
            {
                requireNonEmpty(count, encoding);
                for (int i = 0; i < count; i++)
                {
                    long length = BlockPresence.readVarInt(payload);
                    // Bounded against the bytes that are actually there, before the array exists.
                    if (length > payload.remaining())
                        throw new IllegalArgumentException("Corrupt v4 chunk: RAW value " + i + " claims " + length +
                                                           " bytes, body holds " + payload.remaining());
                    byte[] value = new byte[(int) length];
                    payload.get(value);
                    dst[i] = value;
                }
                checkPadding(payload, base, payloadLength);
                break;
            }
            default:
                throw new IllegalArgumentException("Corrupt v4 chunk: unknown block encoding 0x" +
                                                   Integer.toHexString(encoding));
        }
        checkConsumed(payload, base, payloadLength, encoding);
    }

    // -----------------------------------------------------------------------------------------
    // internals
    // -----------------------------------------------------------------------------------------

    /** Ascending code order, so the argmin's strict {@code <} still means "lowest code wins". */
    private static final int[] ALP_CANDIDATES = { ChunkV4BlockTable.ENC_ALP, ChunkV4BlockTable.ENC_ALP_RD };

    /** {@code FOR_BITPACK}/{@code DELTA_FOR_BITPACK} apply to the integer-domain types only. */
    private static boolean packsIntoALane(int typeCode)
    {
        return typeCode == ChunkV4Directory.TYPE_INT32
               || typeCode == ChunkV4Directory.TYPE_INT64
               || typeCode == ChunkV4Directory.TYPE_DATE32;
    }

    private static void forResiduals(long[] values, int count, long min, long[] dst)
    {
        // Two's complement subtraction, deliberately unguarded: when the block spans more than 2^63
        // the difference wraps to the correct unsigned magnitude, which is exactly what BitPacking
        // treats a lane value as, and the round trip is exact because the addition wraps back.
        for (int i = 0; i < count; i++)
            dst[i] = values[i] - min;
    }

    private static long deltaResiduals(long[] values, int count, long[] dst)
    {
        long base = 0;
        for (int i = 1; i < count; i++)
        {
            long delta = values[i] - values[i - 1];
            if (i == 1 || delta < base)
                base = delta;
        }
        for (int i = 1; i < count; i++)
            dst[i - 1] = (values[i] - values[i - 1]) - base;
        return base;
    }

    /**
     * Rewrites every residual that does not fit {@code width} to the §5 rule 6 filler 0 and records
     * the verbatim value at that position, returning the exception count.
     *
     * <p>The exception's value is the <em>value</em>, not the residual: that is what gives
     * {@code DELTA_FOR_BITPACK} its gap tolerance, because a value fixed verbatim also re-bases
     * every delta after it, and it is what keeps {@code W} equal to the type's own width instead of
     * the width a difference of two extremes would need.
     */
    private static int extractExceptions(Scratch scratch, long[] values, int valueOffset, int laneCount,
                                         int width, int typeCode)
    {
        int valueBytes = ExceptionArea.valueBytes(typeCode);
        long mask = valueBytes == 8 ? -1L : (1L << (valueBytes * 8)) - 1;
        int exceptions = 0;
        for (int j = 0; j < laneCount; j++)
        {
            if (width < BitPacking.MAX_WIDTH && (scratch.residuals[j] >>> width) != 0)
            {
                scratch.exceptionPositions[exceptions] = j;
                scratch.exceptionValues[exceptions] = values[valueOffset + j] & mask;
                exceptions++;
                scratch.residuals[j] = 0;
            }
        }
        return exceptions;
    }

    /**
     * Writes the area, after checking that the exception count the lane actually produced is the one
     * the argmin scored. They can only disagree if the residuals were recomputed differently between
     * choice and encode, which would put the payload at a length no {@code bodyOffset} expects.
     */
    private static void writeExceptions(ByteBuffer dst, int exceptions, Choice choice, int typeCode, Scratch scratch)
    {
        if (exceptions != choice.exceptionCount)
            throw new IllegalArgumentException("extracted " + exceptions + " exceptions, the argmin scored " +
                                               choice.exceptionCount);
        if (exceptions > 0)
            ExceptionArea.encode(scratch.exceptionPositions, scratch.exceptionValues, exceptions,
                                 ExceptionArea.valueBytes(typeCode), dst);
    }

    private static int readExceptions(ByteBuffer payload, boolean hasExceptions, int typeCode, int laneCount,
                                      Scratch scratch)
    {
        if (!hasExceptions)
            return 0;
        return ExceptionArea.decode(payload, ExceptionArea.valueBytes(typeCode), laneCount,
                                    scratch.exceptionPositions, scratch.exceptionValues);
    }

    private static void writeWidthHeader(ByteBuffer dst, int width)
    {
        if (width < 0 || width > BitPacking.MAX_WIDTH)
            throw new IllegalArgumentException("lane width " + width + " outside 0.." + BitPacking.MAX_WIDTH);
        dst.put((byte) width);
        // Written, never skipped: §5 rule 5 exists because these seven bytes are exactly where a
        // reused scratch buffer leaks and where no round-trip test looks.
        for (int i = 1; i < FOR_HEADER_BYTES; i++)
            dst.put((byte) 0);
    }

    private static int readWidthHeader(ByteBuffer src)
    {
        if (src.remaining() < FOR_HEADER_BYTES)
            throw new IllegalArgumentException("Corrupt v4 chunk: truncated lane width header");
        int width = src.get() & 0xFF;
        if (width > BitPacking.MAX_WIDTH)
            throw new IllegalArgumentException("Corrupt v4 chunk: lane width " + width + " exceeds " +
                                               BitPacking.MAX_WIDTH);
        for (int i = 1; i < FOR_HEADER_BYTES; i++)
            if (src.get() != 0)
                throw new IllegalArgumentException("Corrupt v4 chunk: non-zero lane-header padding at byte " + i +
                                                   " (v4 §5 rule 5)");
        return width;
    }

    private static void writeLengthPrefixedPadded(ByteBuffer dst, byte[] value, int payloadLength)
    {
        int base = dst.position();
        BlockPresence.writeVarInt(dst, value.length);
        dst.put(value);
        pad(dst, base, payloadLength);
    }

    private static byte[] readLengthPrefixedPadded(ByteBuffer src, int payloadLength, int expectedLength)
    {
        int base = src.position();
        long length = BlockPresence.readVarInt(src);
        if (expectedLength >= 0 && length != expectedLength)
            throw new IllegalArgumentException("Corrupt v4 chunk: CONSTANT value is " + length + " bytes, type wants " +
                                               expectedLength);
        // Bounded against the payload before the array exists: a corrupt length throws, it does not
        // reserve memory.
        if (length > payloadLength - (src.position() - base))
            throw new IllegalArgumentException("Corrupt v4 chunk: CONSTANT value claims " + length +
                                               " bytes, payload holds " + payloadLength);
        byte[] value = new byte[(int) length];
        src.get(value);
        checkPadding(src, base, payloadLength);
        return value;
    }

    private static void pad(ByteBuffer dst, int base, int payloadLength)
    {
        while (dst.position() - base < payloadLength)
            dst.put((byte) 0);
    }

    private static void checkPadding(ByteBuffer src, int base, int payloadLength)
    {
        for (int i = src.position() - base; i < payloadLength; i++)
            if (src.get() != 0)
                throw new IllegalArgumentException("Corrupt v4 chunk: non-zero block-body padding at byte " + i +
                                                   " (v4 §5 rule 5)");
    }

    private static void checkConsumed(ByteBuffer payload, int base, int payloadLength, int encoding)
    {
        int consumed = payload.position() - base;
        if (consumed != payloadLength)
            throw new IllegalArgumentException("Corrupt v4 chunk: encoding 0x" + Integer.toHexString(encoding) +
                                               " accounts for " + consumed + " bytes, the block table derives " +
                                               payloadLength);
    }

    private static DoubleBlockCodec requireAlp(DoubleBlockCodec alp, int encoding)
    {
        // Not UnsupportedOperationException: a v4 encoder and a v4 reader are the same build (§9),
        // so a reader that was handed no codec cannot be looking at bytes one wrote -- inside a v4
        // payload the byte is a flipped bit, and §11 says corruption is skipped, not propagated.
        if (alp == null)
            throw new IllegalArgumentException("Corrupt v4 chunk: block encoding 0x" + Integer.toHexString(encoding) +
                                               " but no ALP codec was passed, so no v4 encoder this reader shares a " +
                                               "build with can have written one");
        return alp;
    }

    private static void requireLanePacked(int typeCode, int encoding)
    {
        if (!packsIntoALane(typeCode))
            throw new IllegalArgumentException("Corrupt v4 chunk: encoding 0x" + Integer.toHexString(encoding) +
                                               " on type code " + typeCode + ", whose order is not the order its " +
                                               "frame of reference would subtract in");
    }

    private static void requireNonEmpty(int count, int encoding)
    {
        if (count == 0)
            throw new IllegalArgumentException("Corrupt v4 chunk: encoding 0x" + Integer.toHexString(encoding) +
                                               " on a block with no present values; EMPTY is the only encoding for " +
                                               "one");
    }

    /**
     * A decoded lane must land back in the type's domain. Only the narrow types can fail: a corrupt
     * width, min or residual on an {@code INT32} column decodes to a long outside int range, and
     * without this the wrong value would flow into Cassandra's comparison paths as a plausible
     * number rather than as corruption. Skipped for the eight-byte types, whose domain is the whole
     * long, so this costs nothing on the common path.
     */
    private static void checkDecodedDomain(long[] dst, int count, int typeCode)
    {
        if (ChunkV4Directory.statWidth(typeCode) != 4)
            return;
        for (int i = 0; i < count; i++)
            checkValueDomain(dst[i], typeCode);
    }

    private static boolean isMinimum(long[] values, int count, long min, int typeCode)
    {
        for (int i = 0; i < count; i++)
            if (compareValues(values[i], min, typeCode) < 0)
                return false;
        return true;
    }

    private static boolean allEqual(long[] values, int count)
    {
        for (int i = 1; i < count; i++)
            if (values[i] != values[0])
                return false;
        return true;
    }

    private static boolean allEqual(byte[][] values, int count)
    {
        for (int i = 1; i < count; i++)
            if (!Arrays.equals(values[i], values[0]))
                return false;
        return true;
    }

    private static void checkFixedInputs(int typeCode, long[] values, int count, Scratch scratch)
    {
        if (!isFixedWidth(typeCode))
            throw new IllegalArgumentException("type code " + typeCode + " is not fixed width");
        if (count < 0 || values.length < count)
            throw new IllegalArgumentException("values array holds " + values.length + " < count " + count);
        if (count > BlockPresence.MAX_BLOCK_ROWS)
            throw new IllegalArgumentException("block holds " + count + " values, §7 caps a block at " +
                                               BlockPresence.MAX_BLOCK_ROWS);
        scratch.require(Math.max(count, 1));
    }

    private static void checkVariableInputs(int typeCode, byte[][] values, int count)
    {
        if (isFixedWidth(typeCode))
            throw new IllegalArgumentException("type code " + typeCode + " is not variable width");
        if (count < 0 || values.length < count)
            throw new IllegalArgumentException("values array holds " + values.length + " < count " + count);
        if (count > BlockPresence.MAX_BLOCK_ROWS)
            throw new IllegalArgumentException("block holds " + count + " values, §7 caps a block at " +
                                               BlockPresence.MAX_BLOCK_ROWS);
        for (int i = 0; i < count; i++)
            if (values[i] == null)
                throw new IllegalArgumentException("null value at index " + i + "; absent rows are presence, not " +
                                                   "values");
    }

    private static void requireBigEndian(ByteBuffer buffer)
    {
        if (buffer.order() != ByteOrder.BIG_ENDIAN)
            throw new IllegalArgumentException("v4 block bodies are big-endian, buffer order is " + buffer.order());
    }
}
