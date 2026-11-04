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
 * The chunk-format-v4 §5 double encodings, {@code 0x20 ALP} and {@code 0x21 ALP_RD}: the
 * {@link BlockEncodings.DoubleBlockCodec} seam, implemented over {@link AlpCodec}.
 *
 * <pre>
 *   0x20 ALP     exponent u8 | factor u8 | width u8 | flags u8 | pad u32
 *                reference i64
 *                lane(n, width)                 -- (i - reference) per value, LSB-first (§6)
 *                [exception area, W = 8]        -- verbatim 64-bit patterns
 *
 *   0x21 ALP_RD  leftBits u8 | dictSize u8 | flags u8 | pad u8 | pad u32
 *                dictionary: dictSize x u16, padded to 8
 *                lane(n, codeBits)              -- codeBits = ceil(log2(dictSize)), 0 bytes at 0 bits
 *                lane(n, 64 - leftBits)         -- the verbatim low parts
 *                [exception area, W = 2]        -- left parts that missed the dictionary
 * </pre>
 *
 * <h2>What was already true, and what this class had to add</h2>
 *
 * <p>{@link AlpCodec} already owned everything that is hard to get right about ALP and nothing that
 * is specific to a container. It guarantees, and this class inherits without restating: the
 * {@code (e, f)} search with its fixed integer stride and its specified (score asc, {@code e} asc,
 * {@code f} asc) tie-break; the exact power-of-ten tables, which are decimal literals and never
 * {@code Math.pow}; the acceptance test being {@code doubleToRawLongBits(decode(encode(v))) == raw}
 * rather than {@code ==} on doubles, which is what refuses {@code -0.0} and every NaN into the
 * exception path instead of canonicalising them; the ALP-RD dictionary chosen by an array scan under
 * (frequency desc, value asc) and never by hash iteration; and {@code strictfp} arithmetic whose
 * only floating-point operations are multiplication and {@link Math#rint}.
 *
 * <p>What it did <b>not</b> have is anything about the v4 block: it was written against the v3
 * container, and its own framing is a sub-format byte, LEB128 varints, gap-coded exception positions
 * and an MSB-first {@code BitWriter} bitstream with no alignment.
 *
 * <h2>The framing decision: reuse the planning, re-frame the bytes</h2>
 *
 * <p><b>{@link AlpCodec}'s framing is not reusable inside a v4 block and is not kept whole.</b> Four
 * of its five framing choices are things v4 removed on purpose:
 *
 * <ul>
 * <li>Its bitstream was {@code BitWriter} (deleted with v3), which was <b>MSB-first</b>. §6 makes v4 lanes LSB-first and
 *     {@link BitPacking}'s javadoc states that the two conventions must never meet -- they agree at
 *     width 8 and silently disagree everywhere else.</li>
 * <li>Its exception list is a private gap-coded varint list. §5 gives v4 one exception mechanism,
 *     the shared {@link ExceptionArea}, and the whole argument for making it shared is that a
 *     per-codec one buys nothing for the integer columns and the timestamp axis.</li>
 * <li>Its sub-format byte re-states, inside the body, what {@code blockEncoding} already says in the
 *     block table. Two spellings of one fact is a second byte form.</li>
 * <li>It pads to nothing. §6 requires every block body to be a multiple of 8 with verified zero
 *     padding, which is the format's cheapest tripwire for a reused scratch buffer.</li>
 * </ul>
 *
 * <p>So the bytes are v4's. <b>The planning is {@link AlpCodec}'s, by call and not by copy</b>, which
 * is the half of the decision that matters: {@link AlpCodec#exponentCandidates} is the only
 * {@code (e, f)} search in the build and {@link AlpCodec#selectRdDictionary} is the only ALP-RD
 * dictionary in the build. A second copy of either is how two replicas start disagreeing about which
 * exponent pair or which eight left parts a block uses -- a byte difference from the
 * same input (§12). What this class adds around them is the exact
 * v4 cost of a candidate, which is a different function from the v3 cost and has to be, because the
 * bytes are different.
 *
 * <h2>Exact sizes, never a trial encode</h2>
 *
 * <p>{@link #payloadLength} is a closed form over a completed plan: header, plus
 * {@link BitPacking#packedLength}, plus {@link ExceptionArea#encodedLength}. Nothing is encoded to
 * find out how long it is (§5 rule 3), which matters because {@link BlockEncodings#chooseFixed}
 * calls it before any byte exists and lays out the following blocks' {@code bodyOffset}s from the
 * number it returns. {@link #encode} re-plans through the same pure function and asserts it wrote
 * exactly that many bytes.
 *
 * <p><b>The variant choice is not made here.</b> {@link BlockEncodings#chooseFixed} scores
 * {@code 0x20} then {@code 0x21} and displaces the incumbent only on a strictly smaller size, so an
 * exact tie goes to {@code 0x20 ALP} -- the same rule v3's {@code AlpCodec.choose} applied, arrived
 * at by the same lowest-code-wins argmin every other v4 encoding is chosen by.
 *
 * <h2>Two details that look like waste and are not</h2>
 *
 * <p><b>The lane has a slot for every value, including exceptions</b>, holding §5 rule 6's specified
 * filler {@code 0} there. {@link AlpCodec} skipped exception slots in its bitstream. v4 cannot: an
 * {@link ExceptionArea} position is by definition an index into the lane it accompanies, and
 * {@link ExceptionArea#checkLaneIsZeroAtExceptions} -- the check that gives a dead slot exactly one
 * byte form -- has nothing to check if the slot is absent. The cost is {@code exceptions * width}
 * bits, which is what {@code FOR_BITPACK} pays for the same guarantee.
 *
 * <p><b>The ALP-RD dictionary is written in ascending value order, not in code order.</b>
 * {@link AlpCodec#selectRdDictionary} returns it most-frequent-first because that is v3's code
 * assignment; here the entries are sorted and the code is the index in that order. It costs nothing
 * -- every code is {@code codeBits} wide whatever the order -- and it buys §5 rule 2's ascending,
 * duplicate-free dictionary, so the decoder rejects a duplicated entry (which would give one left
 * part two codes, and one block two byte forms) with the same comparison that checks the order.
 *
 * <h2>Installation is a build-time constant</h2>
 *
 * <p>{@link #INSTANCE} is the only way to obtain this codec and there is no switch, no property and
 * no {@code cassandra.yaml} key that turns it off. That is deliberate and it is the single largest
 * risk in the format (§12): a codec present on some replicas and absent on others makes identical
 * input produce different bytes, and the failure does not look like a codec bug. A node either runs
 * a build that has {@code 0x20}/{@code 0x21} or runs a build that does not, and §9's version byte is
 * what distinguishes those.
 *
 * <p>Corruption is {@link IllegalArgumentException} (§11): every field read out of the payload is
 * range-checked before it is used, every pad byte is verified zero, the sub-lengths are required to
 * add up to the {@code payloadLength} the block table derived, and nothing allocates against a
 * number taken from the bytes.
 */
// strictfp is redundant from Java 17 and javac says so; §5 rule 8 asks for it on the ALP path
// anyway, so the note is suppressed rather than the rule dropped. The arithmetic itself is in
// AlpCodec, which carries the same modifier for the same reason.
@SuppressWarnings("strictfp")
public final strictfp class AlpBlockCodec implements BlockEncodings.DoubleBlockCodec
{
    /**
     * The codec, as a build-time constant. Not configurable, not injectable, not optional -- see the
     * class javadoc: an encoder that some replicas have and others do not is a byte-determinism
     * break of exactly the shape §12 names as the format's largest risk.
     */
    public static final AlpBlockCodec INSTANCE = new AlpBlockCodec();

    /** {@code exponent u8 | factor u8 | width u8 | flags u8 | pad u32} then {@code reference i64}. */
    static final int ALP_HEADER_BYTES = 16;

    /** {@code leftBits u8 | dictSize u8 | flags u8 | pad u8 | pad u32}; the dictionary follows. */
    static final int RD_HEADER_BYTES = 8;

    /** §5: {@code W} for decimal ALP's exceptions, which are verbatim 64-bit patterns. */
    static final int ALP_EXCEPTION_VALUE_BYTES = 8;

    /**
     * {@code flags} bit 0: an {@link ExceptionArea} follows the lanes.
     *
     * <p><b>This is the in-payload stand-in for {@code blockFlags} bit 4</b>, and it is not
     * decoration. Without it the area's presence would have to be inferred from
     * {@code payloadLength}, and a body length corrupted downwards by exactly the area's size would
     * then parse as a shorter, self-consistent, <em>silently wrong</em> block -- the exception rows
     * coming back as whatever their filler {@code 0} lane slot decodes to. With it, the flag and the
     * length have to agree, so either corruption is caught by the other. The other lane encodings
     * get the same guarantee from {@code blockFlags} bit 4, which the seam cannot hand a codec.
     */
    static final int FLAG_HAS_EXCEPTIONS = 0x01;

    /** {@code flags} bits 1-7 are reserved and must be zero (§5 rule 5). */
    static final int FLAGS_RESERVED_MASK = 0xFE;

    /**
     * Per-thread working memory. Nothing in it reaches the bytes: the lanes are fully written before
     * they are packed, the histogram is zeroed by {@link AlpCodec#selectRdDictionary} over the prefix
     * it uses, and the exception arrays are only read up to the count that was just written. It is a
     * {@link ThreadLocal} rather than a field because the codec is a shared constant, and rather than
     * a per-call allocation because {@link AlpCodec#RD_MAX_LEFT_BITS} makes the histogram 64K
     * entries -- a quarter of a megabyte of garbage per planning pass, three passes per double block.
     */
    private static final ThreadLocal<Scratch> SCRATCH = ThreadLocal.withInitial(Scratch::new);

    private AlpBlockCodec()
    {
    }

    // -----------------------------------------------------------------------------------------
    // the seam
    // -----------------------------------------------------------------------------------------

    @Override
    public long payloadLength(int encoding, long[] rawBits, int count)
    {
        checkInputs(rawBits, count);
        // An empty block is EMPTY's, not ALP's: every field below describes at least one value, and
        // §5 gives a block with no present value exactly one encoding.
        if (count == 0)
            return -1;
        switch (encoding)
        {
            case ChunkV4BlockTable.ENC_ALP:
                return planAlp(rawBits, count).payloadLength;
            case ChunkV4BlockTable.ENC_ALP_RD:
                return planRd(rawBits, count).payloadLength;
            default:
                return -1;
        }
    }

    @Override
    public int encode(int encoding, long[] rawBits, int count, ByteBuffer dst)
    {
        checkInputs(rawBits, count);
        requireBigEndian(dst);
        if (count == 0)
            throw new IllegalArgumentException("a block with no present values is EMPTY, not ALP (v4 §5)");
        switch (encoding)
        {
            case ChunkV4BlockTable.ENC_ALP:
                return encodeAlp(planAlp(rawBits, count), rawBits, count, dst);
            case ChunkV4BlockTable.ENC_ALP_RD:
                return encodeRd(planRd(rawBits, count), rawBits, count, dst);
            default:
                throw new IllegalArgumentException("encoding 0x" + Integer.toHexString(encoding) +
                                                   " is not one of this codec's (0x20 ALP, 0x21 ALP_RD)");
        }
    }

    @Override
    public void decode(int encoding, ByteBuffer payload, int payloadLength, int count, long[] dst)
    {
        requireBigEndian(payload);
        if (count < 1 || count > BlockPresence.MAX_BLOCK_ROWS)
            throw new IllegalArgumentException("Corrupt v4 chunk: ALP block with " + count + " present values");
        if (dst.length < count)
            throw new IllegalArgumentException("destination holds " + dst.length + " < count " + count);
        if (payloadLength < 0 || payload.remaining() < payloadLength)
            throw new IllegalArgumentException("Corrupt v4 chunk: ALP payload claims " + payloadLength +
                                               " bytes, body holds " + payload.remaining());
        switch (encoding)
        {
            case ChunkV4BlockTable.ENC_ALP:
                decodeAlp(payload, payloadLength, count, dst);
                break;
            case ChunkV4BlockTable.ENC_ALP_RD:
                decodeRd(payload, payloadLength, count, dst);
                break;
            default:
                throw new IllegalArgumentException("Corrupt v4 chunk: encoding 0x" + Integer.toHexString(encoding) +
                                                   " routed to the ALP codec");
        }
    }

    // -----------------------------------------------------------------------------------------
    // planning -- decimal ALP
    // -----------------------------------------------------------------------------------------

    /** One block's decimal-ALP decision and the exact number of bytes it will occupy. */
    static final class AlpPlan
    {
        final int exponent;
        final int factor;
        final int width;
        final long reference;
        final int exceptionCount;
        final int payloadLength;

        AlpPlan(int exponent, int factor, int width, long reference, int exceptionCount, int payloadLength)
        {
            this.exponent = exponent;
            this.factor = factor;
            this.width = width;
            this.reference = reference;
            this.exceptionCount = exceptionCount;
            this.payloadLength = payloadLength;
        }
    }

    /**
     * The {@code (e, f)} pair this block will use, and the bytes it costs.
     *
     * <p>Stage 1 is {@link AlpCodec#exponentCandidates}, shared with v3: a fixed-stride sample scores
     * every pair and the best {@link AlpCodec#CANDIDATE_LIMIT} come back in (score asc, {@code e}
     * asc, {@code f} asc) order. Stage 2 measures each of them exactly under the <em>v4</em> cost
     * function -- which is a different function from v3's, because a v4 block pays for an 8-aligned
     * lane over every value and for a shared exception area, where a v3 section paid for a
     * byte-aligned bitstream over the encodable values and a varint exception list.
     *
     * <p>The rank is (bytes asc, {@code e} asc, {@code f} asc): the candidates already arrive in
     * {@code (e, f)} order within a score, and a strict {@code <} keeps the first of any tie, so the
     * tie-break is specified rather than incidental. §12 names an unspecified one as a risk.
     */
    static AlpPlan planAlp(long[] rawBits, int count)
    {
        int[] candidateE = new int[AlpCodec.CANDIDATE_LIMIT];
        int[] candidateF = new int[AlpCodec.CANDIDATE_LIMIT];
        int candidates = AlpCodec.exponentCandidates(rawBits, count, candidateE, candidateF);
        if (candidates < 1)
            throw new IllegalStateException("the (e, f) search returned no candidate for a " + count +
                                            "-value block");

        AlpPlan best = null;
        for (int c = 0; c < candidates; c++)
        {
            AlpPlan plan = measureAlp(rawBits, count, candidateE[c], candidateF[c]);
            if (best == null || plan.payloadLength < best.payloadLength)
                best = plan;
        }
        return best;
    }

    /** Exact whole-block v4 cost of encoding under {@code (e, f)}. */
    private static AlpPlan measureAlp(long[] rawBits, int count, int e, int f)
    {
        long minimum = Long.MAX_VALUE;
        long maximum = Long.MIN_VALUE;
        int exceptions = 0;
        for (int i = 0; i < count; i++)
        {
            long encoded = AlpCodec.tryEncode(rawBits[i], e, f);
            if (encoded == AlpCodec.NOT_ENCODABLE)
            {
                exceptions++;
            }
            else
            {
                minimum = Math.min(minimum, encoded);
                maximum = Math.max(maximum, encoded);
            }
        }
        // A block with nothing encodable has no frame of reference to record. Zero rather than
        // Long.MAX_VALUE: §5 rule 6's unused fields are zero, and a leftover sentinel in the header
        // would be a byte-nondeterministic field no round-trip test reads.
        boolean none = exceptions == count;
        long reference = none ? 0L : minimum;
        // Both bounds are inside +/-2^53 (AlpCodec.ENCODED_LIMIT), so the span is at most 2^54 and
        // the width is at most AlpCodec.MAX_BIT_WIDTH. The subtraction cannot overflow.
        int width = none ? 0 : AlpCodec.bitWidthOf(maximum - minimum);
        int payloadLength = ALP_HEADER_BYTES
                            + BitPacking.packedLength(count, width)
                            + ExceptionArea.encodedLength(exceptions, ALP_EXCEPTION_VALUE_BYTES);
        return new AlpPlan(e, f, width, reference, exceptions, payloadLength);
    }

    // -----------------------------------------------------------------------------------------
    // planning -- ALP-RD
    // -----------------------------------------------------------------------------------------

    /** One block's ALP-RD decision and the exact number of bytes it will occupy. */
    static final class RdPlan
    {
        final int leftBits;
        /** Ascending unsigned order (§5 rule 2); the code of an entry is its index here. */
        final int[] dictionary;
        final int codeBits;
        final int exceptionCount;
        final int payloadLength;

        RdPlan(int leftBits, int[] dictionary, int codeBits, int exceptionCount, int payloadLength)
        {
            this.leftBits = leftBits;
            this.dictionary = dictionary;
            this.codeBits = codeBits;
            this.exceptionCount = exceptionCount;
            this.payloadLength = payloadLength;
        }

        int rightBits()
        {
            return Long.SIZE - leftBits;
        }
    }

    /**
     * The {@code leftBits} cut this block will use, and the bytes it costs. Widths ascend and the
     * incumbent is displaced only on a strictly smaller size, so a tie goes to the narrowest cut --
     * the same rule v3's {@code AlpCodec.planRd} applied, for the same determinism reason.
     *
     * <p>{@code leftBits >= 1} keeps the right lane at most 63 bits wide, so its mask never shifts by
     * 64; {@code leftBits <= 16} keeps a left part inside the two bytes
     * {@link ExceptionArea#VALUE_BYTES_ALP_RD_LEFT} allows for it.
     */
    static RdPlan planRd(long[] rawBits, int count)
    {
        Scratch scratch = SCRATCH.get();
        RdPlan best = null;
        for (int leftBits = 1; leftBits <= AlpCodec.RD_MAX_LEFT_BITS; leftBits++)
        {
            RdPlan plan = measureRd(rawBits, count, leftBits, scratch);
            if (best == null || plan.payloadLength < best.payloadLength)
                best = plan;
        }
        return best;
    }

    private static RdPlan measureRd(long[] rawBits, int count, int leftBits, Scratch scratch)
    {
        int rightBits = Long.SIZE - leftBits;
        int size = AlpCodec.selectRdDictionary(rawBits, count, leftBits, scratch.histogram, scratch.dictionary);
        int[] dictionary = Arrays.copyOf(scratch.dictionary, size);
        // Ascending value order rather than the frequency order the selection returns: the set is
        // the same either way, the codes are the same width either way, and ascending is what lets
        // the decoder reject a duplicate with the same comparison that checks the order (§5 rule 2).
        Arrays.sort(dictionary);
        int codeBits = AlpCodec.codeBitsFor(size);

        int exceptions = 0;
        for (int i = 0; i < count; i++)
            if (codeOf(dictionary, (int) (rawBits[i] >>> rightBits)) < 0)
                exceptions++;

        int payloadLength = RD_HEADER_BYTES
                            + dictionaryBytes(size)
                            + BitPacking.packedLength(count, codeBits)
                            + BitPacking.packedLength(count, rightBits)
                            + ExceptionArea.encodedLength(exceptions, ExceptionArea.VALUE_BYTES_ALP_RD_LEFT);
        return new RdPlan(leftBits, dictionary, codeBits, exceptions, payloadLength);
    }

    /** {@code dictSize} big-endian {@code u16}s, padded to the §6 eight-byte boundary. */
    private static int dictionaryBytes(int dictionarySize)
    {
        return (int) BitPacking.roundUpToMultipleOf8(2L * dictionarySize);
    }

    /**
     * The code for {@code left}, i.e. its index in the ascending dictionary, or -1. A linear scan
     * over at most {@link AlpCodec#RD_MAX_DICTIONARY} entries: eight comparisons beat a hash lookup
     * at this size, and §5 rule 7 keeps hash structures out of anything that reaches the bytes.
     */
    private static int codeOf(int[] dictionary, int left)
    {
        for (int code = 0; code < dictionary.length; code++)
            if (dictionary[code] == left)
                return code;
        return -1;
    }

    // -----------------------------------------------------------------------------------------
    // encode
    // -----------------------------------------------------------------------------------------

    private static int encodeAlp(AlpPlan plan, long[] rawBits, int count, ByteBuffer dst)
    {
        Scratch scratch = SCRATCH.get();
        scratch.require(count);
        long[] lane = scratch.lane;
        int exceptions = 0;
        for (int i = 0; i < count; i++)
        {
            long encoded = AlpCodec.tryEncode(rawBits[i], plan.exponent, plan.factor);
            if (encoded == AlpCodec.NOT_ENCODABLE)
            {
                // §5 rule 6's specified filler. The slot is dead -- the exception's verbatim pattern
                // replaces it -- so without a specified value three encoders write three chunks.
                lane[i] = 0;
                scratch.positions[exceptions] = i;
                // The raw 64-bit pattern, never a double that has been through a numeric round trip:
                // this is the path -0.0, every NaN payload and the infinities take, and the whole
                // point of it is that nothing here looks at them as numbers.
                scratch.values[exceptions] = rawBits[i];
                exceptions++;
            }
            else
            {
                lane[i] = encoded - plan.reference;
            }
        }
        // Recomputed rather than carried, then checked: the plan's count and the lane's count can
        // only differ if the two passes disagreed, which would put the body at a length no
        // bodyOffset expects.
        if (exceptions != plan.exceptionCount)
            throw new IllegalStateException("extracted " + exceptions + " ALP exceptions, the plan scored " +
                                            plan.exceptionCount);

        int base = dst.position();
        if (dst.remaining() < plan.payloadLength)
            throw new IllegalArgumentException("destination holds " + dst.remaining() + " < ALP payload " +
                                               plan.payloadLength);
        dst.put((byte) plan.exponent);
        dst.put((byte) plan.factor);
        dst.put((byte) plan.width);
        dst.put((byte) (exceptions > 0 ? FLAG_HAS_EXCEPTIONS : 0));
        // §5 rule 5: written, not skipped. These four bytes are exactly where a reused scratch
        // buffer leaks and exactly where no round-trip test looks.
        dst.putInt(0);
        dst.putLong(plan.reference);
        BitPacking.pack(lane, count, plan.width, dst);
        if (exceptions > 0)
            ExceptionArea.encode(scratch.positions, scratch.values, exceptions, ALP_EXCEPTION_VALUE_BYTES, dst);
        return checkWritten(dst, base, plan.payloadLength, ChunkV4BlockTable.ENC_ALP);
    }

    private static int encodeRd(RdPlan plan, long[] rawBits, int count, ByteBuffer dst)
    {
        Scratch scratch = SCRATCH.get();
        scratch.require(count);
        int rightBits = plan.rightBits();
        long rightMask = (1L << rightBits) - 1;     // rightBits <= 63, so this cannot shift by 64
        long[] codes = scratch.lane;
        long[] rights = scratch.second;
        int exceptions = 0;
        for (int i = 0; i < count; i++)
        {
            int left = (int) (rawBits[i] >>> rightBits);
            int code = codeOf(plan.dictionary, left);
            // The specified filler again: an exception's code slot is 0 and the decoder ignores it.
            codes[i] = code < 0 ? 0 : code;
            rights[i] = rawBits[i] & rightMask;
            if (code < 0)
            {
                scratch.positions[exceptions] = i;
                scratch.values[exceptions] = left;
                exceptions++;
            }
        }
        if (exceptions != plan.exceptionCount)
            throw new IllegalStateException("extracted " + exceptions + " ALP-RD exceptions, the plan scored " +
                                            plan.exceptionCount);

        int base = dst.position();
        if (dst.remaining() < plan.payloadLength)
            throw new IllegalArgumentException("destination holds " + dst.remaining() + " < ALP-RD payload " +
                                               plan.payloadLength);
        dst.put((byte) plan.leftBits);
        dst.put((byte) plan.dictionary.length);
        dst.put((byte) (exceptions > 0 ? FLAG_HAS_EXCEPTIONS : 0));
        dst.put((byte) 0);
        dst.putInt(0);
        int dictionaryFrom = dst.position();
        for (int i = 0; i < plan.dictionary.length; i++)
        {
            if (i > 0 && plan.dictionary[i] <= plan.dictionary[i - 1])
                throw new IllegalStateException("ALP-RD dictionary is not strictly ascending at " + i);
            dst.putShort((short) plan.dictionary[i]);
        }
        int dictionaryLength = dictionaryBytes(plan.dictionary.length);
        while (dst.position() - dictionaryFrom < dictionaryLength)
            dst.put((byte) 0);
        BitPacking.pack(codes, count, plan.codeBits, dst);
        BitPacking.pack(rights, count, rightBits, dst);
        if (exceptions > 0)
            ExceptionArea.encode(scratch.positions, scratch.values, exceptions,
                                 ExceptionArea.VALUE_BYTES_ALP_RD_LEFT, dst);
        return checkWritten(dst, base, plan.payloadLength, ChunkV4BlockTable.ENC_ALP_RD);
    }

    private static int checkWritten(ByteBuffer dst, int base, int payloadLength, int encoding)
    {
        int written = dst.position() - base;
        if (written != payloadLength)
            throw new IllegalStateException("encoding 0x" + Integer.toHexString(encoding) + " wrote " + written +
                                            " bytes, the plan scored " + payloadLength);
        return written;
    }

    // -----------------------------------------------------------------------------------------
    // decode
    // -----------------------------------------------------------------------------------------

    private static void decodeAlp(ByteBuffer payload, int payloadLength, int count, long[] dst)
    {
        int base = payload.position();
        if (payloadLength < ALP_HEADER_BYTES)
            throw new IllegalArgumentException("Corrupt v4 chunk: ALP payload of " + payloadLength +
                                               " bytes cannot hold its " + ALP_HEADER_BYTES + "-byte header");
        int exponent = payload.get() & 0xFF;
        int factor = payload.get() & 0xFF;
        int width = payload.get() & 0xFF;
        int flags = payload.get() & 0xFF;
        long pad32 = payload.getInt() & 0xFFFFFFFFL;
        if (pad32 != 0)
            throw new IllegalArgumentException("Corrupt v4 chunk: non-zero ALP header padding " + pad32 +
                                               " (v4 §5 rule 5)");
        checkFlags(flags);
        if (exponent > AlpCodec.MAX_EXPONENT || factor > exponent)
            throw new IllegalArgumentException("Corrupt v4 chunk: ALP exponent pair (" + exponent + ", " + factor +
                                               ") outside 0 <= f <= e <= " + AlpCodec.MAX_EXPONENT);
        // A property of the format, not of this implementation: a frame of reference over integers
        // bounded by +/-2^53 spans at most 2^54.
        if (width > AlpCodec.MAX_BIT_WIDTH)
            throw new IllegalArgumentException("Corrupt v4 chunk: ALP lane width " + width + " exceeds " +
                                               AlpCodec.MAX_BIT_WIDTH);
        long reference = payload.getLong();
        if (reference < -AlpCodec.ENCODED_LIMIT || reference > AlpCodec.ENCODED_LIMIT)
            throw new IllegalArgumentException("Corrupt v4 chunk: ALP frame of reference " + reference +
                                               " outside +/-2^53");

        int laneLength = BitPacking.packedLength(count, width);
        int areaLength = areaLength(payloadLength, ALP_HEADER_BYTES + laneLength, flags, ChunkV4BlockTable.ENC_ALP);
        BitPacking.unpack(payload, count, width, dst);

        Scratch scratch = SCRATCH.get();
        scratch.require(count);
        int exceptions = readExceptions(payload, areaLength, ALP_EXCEPTION_VALUE_BYTES, count, scratch);
        // On the raw lane, before the frame of reference is folded in: afterwards the slot holds
        // `reference` rather than 0 and the check silently stops testing anything.
        ExceptionArea.checkLaneIsZeroAtExceptions(dst, count, scratch.positions, exceptions);

        int next = 0;
        for (int i = 0; i < count; i++)
        {
            if (next < exceptions && scratch.positions[next] == i)
            {
                dst[i] = scratch.values[next++];
                continue;
            }
            long encoded = reference + dst[i];
            // A legitimate scaled integer is always inside +/-2^53; outside it the decode would
            // fabricate a plausible finite double out of a corrupt lane instead of throwing.
            if (encoded < -AlpCodec.ENCODED_LIMIT || encoded > AlpCodec.ENCODED_LIMIT)
                throw new IllegalArgumentException("Corrupt v4 chunk: ALP scaled integer " + encoded + " at value " +
                                                   i + " outside +/-2^53");
            dst[i] = Double.doubleToRawLongBits(AlpCodec.decodeOne(encoded, exponent, factor));
        }
        checkConsumed(payload, base, payloadLength, ChunkV4BlockTable.ENC_ALP);
    }

    private static void decodeRd(ByteBuffer payload, int payloadLength, int count, long[] dst)
    {
        int base = payload.position();
        if (payloadLength < RD_HEADER_BYTES)
            throw new IllegalArgumentException("Corrupt v4 chunk: ALP-RD payload of " + payloadLength +
                                               " bytes cannot hold its " + RD_HEADER_BYTES + "-byte header");
        int leftBits = payload.get() & 0xFF;
        int dictionarySize = payload.get() & 0xFF;
        int flags = payload.get() & 0xFF;
        int pad8 = payload.get() & 0xFF;
        long pad32 = payload.getInt() & 0xFFFFFFFFL;
        if (pad8 != 0 || pad32 != 0)
            throw new IllegalArgumentException("Corrupt v4 chunk: non-zero ALP-RD header padding " + pad8 + '/' +
                                               pad32 + " (v4 §5 rule 5)");
        checkFlags(flags);
        if (leftBits < 1 || leftBits > AlpCodec.RD_MAX_LEFT_BITS)
            throw new IllegalArgumentException("Corrupt v4 chunk: ALP-RD left bit width " + leftBits + " outside [1, " +
                                               AlpCodec.RD_MAX_LEFT_BITS + ']');
        if (dictionarySize < 1 || dictionarySize > AlpCodec.RD_MAX_DICTIONARY)
            throw new IllegalArgumentException("Corrupt v4 chunk: ALP-RD dictionary size " + dictionarySize +
                                               " outside [1, " + AlpCodec.RD_MAX_DICTIONARY + ']');
        int rightBits = Long.SIZE - leftBits;
        int codeBits = AlpCodec.codeBitsFor(dictionarySize);
        int dictionaryLength = dictionaryBytes(dictionarySize);

        int laneLength = BitPacking.packedLength(count, codeBits) + BitPacking.packedLength(count, rightBits);
        int fixed = RD_HEADER_BYTES + dictionaryLength + laneLength;
        int areaLength = areaLength(payloadLength, fixed, flags, ChunkV4BlockTable.ENC_ALP_RD);

        // Sized from dictionarySize, which was bounded against the format's own cap before this line
        // and never against a length taken from the payload.
        int[] dictionary = new int[dictionarySize];
        int dictionaryFrom = payload.position();
        int leftLimit = 1 << leftBits;
        for (int i = 0; i < dictionarySize; i++)
        {
            dictionary[i] = payload.getShort() & 0xFFFF;
            // Strictly ascending is one comparison doing two jobs (§5 rule 2): out-of-order entries,
            // and a duplicate -- which would give one left part two codes and one block two byte
            // forms.
            if (i > 0 && dictionary[i] <= dictionary[i - 1])
                throw new IllegalArgumentException("Corrupt v4 chunk: ALP-RD dictionary entries out of ascending " +
                                                   "order at " + i + " (v4 §5 rule 2)");
            if (dictionary[i] >= leftLimit)
                throw new IllegalArgumentException("Corrupt v4 chunk: ALP-RD dictionary entry 0x" +
                                                   Integer.toHexString(dictionary[i]) + " does not fit " + leftBits +
                                                   " bits");
        }
        for (int at = payload.position() - dictionaryFrom; at < dictionaryLength; at++)
            if (payload.get() != 0)
                throw new IllegalArgumentException("Corrupt v4 chunk: non-zero ALP-RD dictionary padding at byte " +
                                                   at + " (v4 §5 rule 5)");

        Scratch scratch = SCRATCH.get();
        scratch.require(count);
        long[] codes = scratch.lane;
        BitPacking.unpack(payload, count, codeBits, codes);
        BitPacking.unpack(payload, count, rightBits, dst);
        int exceptions = readExceptions(payload, areaLength, ExceptionArea.VALUE_BYTES_ALP_RD_LEFT, count, scratch);
        ExceptionArea.checkLaneIsZeroAtExceptions(codes, count, scratch.positions, exceptions);

        int next = 0;
        for (int i = 0; i < count; i++)
        {
            long left;
            if (next < exceptions && scratch.positions[next] == i)
            {
                left = scratch.values[next++];
                if (left >= leftLimit)
                    throw new IllegalArgumentException("Corrupt v4 chunk: ALP-RD exception left part 0x" +
                                                       Long.toHexString(left) + " does not fit " + leftBits + " bits");
            }
            else
            {
                // codeBits rounds up to a power of two, so a dictionary of, say, five entries leaves
                // codes 5..7 expressible: a corrupt lane could name one and must not index past.
                if (codes[i] >= dictionarySize)
                    throw new IllegalArgumentException("Corrupt v4 chunk: ALP-RD code " + codes[i] + " at value " + i +
                                                       ", dictionary holds " + dictionarySize);
                left = dictionary[(int) codes[i]];
            }
            dst[i] = (left << rightBits) | dst[i];
        }
        checkConsumed(payload, base, payloadLength, ChunkV4BlockTable.ENC_ALP_RD);
    }

    private static void checkFlags(int flags)
    {
        if ((flags & FLAGS_RESERVED_MASK) != 0)
            throw new IllegalArgumentException("Corrupt v4 chunk: reserved ALP flag bits set in 0x" +
                                               Integer.toHexString(flags) + " (v4 §5 rule 5)");
    }

    /**
     * The bytes left for the exception area after everything the header fixes, checked against the
     * header's own {@link #FLAG_HAS_EXCEPTIONS}.
     *
     * <p><b>Both signals are required to agree, and that is the point.</b> {@code payloadLength} is
     * derived by the block table from two {@code bodyOffset}s; the flag is inside the body. A
     * corrupt offset that shortened the body by exactly an area's length would, on the length alone,
     * yield a shorter self-consistent block whose exception rows came back as whatever their filler
     * {@code 0} slot decodes to -- wrong values, no throw. A flipped flag bit would, on the flag
     * alone, mis-read the area. Requiring the two to agree costs one comparison and removes both.
     */
    private static int areaLength(int payloadLength, int fixed, int flags, int encoding)
    {
        int remaining = payloadLength - fixed;
        boolean claimed = (flags & FLAG_HAS_EXCEPTIONS) != 0;
        if (remaining < 0 || (claimed ? remaining <= 0 : remaining != 0))
            throw new IllegalArgumentException("Corrupt v4 chunk: encoding 0x" + Integer.toHexString(encoding) +
                                               " fixes " + fixed + " bytes and " +
                                               (claimed ? "claims an exception area" : "claims no exception area") +
                                               ", but the block table derives " + payloadLength);
        return remaining;
    }

    /** Reads the block's exception area, or none at all; the length was agreed by {@link #areaLength}. */
    private static int readExceptions(ByteBuffer payload, int areaLength, int valueBytes, int count, Scratch scratch)
    {
        if (areaLength == 0)
            return 0;
        int exceptions = ExceptionArea.decode(payload, valueBytes, count, scratch.positions, scratch.values);
        if (ExceptionArea.encodedLength(exceptions, valueBytes) != areaLength)
            throw new IllegalArgumentException("Corrupt v4 chunk: " + exceptions + " exceptions occupy " +
                                               ExceptionArea.encodedLength(exceptions, valueBytes) +
                                               " bytes, the payload leaves " + areaLength);
        return exceptions;
    }

    private static void checkConsumed(ByteBuffer payload, int base, int payloadLength, int encoding)
    {
        int consumed = payload.position() - base;
        if (consumed != payloadLength)
            throw new IllegalArgumentException("Corrupt v4 chunk: encoding 0x" + Integer.toHexString(encoding) +
                                               " accounts for " + consumed + " bytes, the block table derives " +
                                               payloadLength);
    }

    // -----------------------------------------------------------------------------------------
    // internals
    // -----------------------------------------------------------------------------------------

    private static final class Scratch
    {
        final int[] histogram = new int[1 << AlpCodec.RD_MAX_LEFT_BITS];
        final int[] dictionary = new int[AlpCodec.RD_MAX_DICTIONARY];
        long[] lane = new long[0];
        long[] second = new long[0];
        int[] positions = new int[0];
        long[] values = new long[0];

        void require(int count)
        {
            if (lane.length >= count)
                return;
            // Grown to exactly what is asked for rather than doubled: a block size is a chunk-wide
            // constant, so this runs once per thread per block size and never in a loop.
            lane = new long[count];
            second = new long[count];
            positions = new int[count];
            values = new long[count];
        }
    }

    private static void checkInputs(long[] rawBits, int count)
    {
        if (count < 0 || rawBits.length < count)
            throw new IllegalArgumentException("values array holds " + rawBits.length + " < count " + count);
        if (count > BlockPresence.MAX_BLOCK_ROWS)
            throw new IllegalArgumentException("block holds " + count + " values, §7 caps a block at " +
                                               BlockPresence.MAX_BLOCK_ROWS);
    }

    private static void requireBigEndian(ByteBuffer buffer)
    {
        if (buffer.order() != ByteOrder.BIG_ENDIAN)
            throw new IllegalArgumentException("v4 block bodies are big-endian, buffer order is " + buffer.order());
    }
}
