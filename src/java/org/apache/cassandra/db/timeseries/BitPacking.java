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
 * Scalar LSB-first bit packing: the lane primitive every version-4 block encoding sits on
 * (chunk-format-v4 §6), together with the §5 width chooser that decides how wide a lane must be.
 *
 * <p><b>The bit order is normative and has two plausible readings, so it is stated once, here, and
 * nowhere else.</b> §6: a packed lane is a sequence of 64-bit <em>big-endian</em> words; bit index
 * {@code b} lives in word {@code b >>> 6} at the position {@code b & 63} counted <em>from the least
 * significant bit</em>; value {@code i} of width {@code w} occupies bits {@code [i*w, (i+1)*w)},
 * low-order bits of the value first, straddling the word boundary whenever {@code w} does not
 * divide 64. The §6 worked example, {@code [1, 2, 3]} at width 2, is therefore
 * {@code 1 | (2 << 2) | (3 << 4)} = {@code 0x39}, serialised as {@code 00 .. 00 39}, and
 * {@code BitPackingTest.goldenVectorFromSpecSection6} pins it.
 *
 * <p><b>The hex literal printed beside that formula in §6 says {@code 0x31}, and it is a
 * single-nibble typo.</b> {@code 0x31} is what you get if the second value is dropped: under the
 * very rule the same sentence states, {@code 0x31} decodes as {@code [1, 0, 3]}. No bit-order
 * convention -- MSB-first within the value, descending value order, either endianness -- produces
 * {@code 0x31} from {@code [1, 2, 3]}; it is not an alternative reading, it is data loss. The
 * closed form in the same line and the normative sentence above it both say {@code 0x39}, so the
 * code follows them, and the test decodes {@code 0x31} back to {@code [1, 0, 3]} so that a future
 * reader who trusts the doc's literal hits a red test instead of shipping a second bit order.
 *
 * <p>This is deliberately the opposite convention from {@code BitWriter}/{@code BitReader}, the
 * MSB-first pair that belonged to the version-2/3 chunk paths v4 replaced outright (§0; both
 * classes were deleted with those formats). The two conventions must never meet: an MSB-first
 * packer and an LSB-first unpacker agree exactly at width 8 and silently disagree at every other
 * width, which is the shape of bug that survives a smoke test.
 *
 * <p><b>No tail case.</b> The lane holds {@code ceil(count*width/64)} words, so the highest bit any
 * value touches is {@code count*width - 1}, which is inside the last word by construction. The
 * per-value loops below therefore carry the straddle branch and nothing else -- there is no
 * "remaining values" epilogue and no separate path for {@code count % 64 != 0}. The single
 * post-loop statement in {@link #pack} is not a tail case but the store of the last, partially
 * filled word; the accumulator's untouched high bits are zero, which is precisely what §5 rule 5
 * and rule 6 demand of the bits past the final value.
 *
 * <p><b>Width 0 is legal and means every value is zero</b> (§5: {@code cost(0)} has no lane bytes).
 * It needs no special case on either side: the pack loop never advances its bit offset so it never
 * stores a word, and the unpack loop masks everything to zero without loading one.
 *
 * <p>Values are treated as <em>unsigned</em> 64-bit magnitudes. Callers frame-of-reference their
 * blocks first (§5), so values arrive non-negative; a value that nonetheless has bit 63 set is
 * simply a width-64 value here rather than a silent corruption.
 *
 * <p>Every rejection is an {@link IllegalArgumentException} -- an out-of-range width, a value too
 * wide for its declared width (which means the encoder's width chooser and its exception extraction
 * disagreed), or a source buffer shorter than the lane a corrupt block table claims.
 */
public final class BitPacking
{
    /** Widths run 0..64 inclusive; 64 is a verbatim lane, 0 is an absent one. */
    public static final int MAX_WIDTH = 64;

    /** {@code 64 - numberOfLeadingZeros(v)} yields 0..64, so the §5 histogram has 65 buckets. */
    public static final int WIDTH_HISTOGRAM_SIZE = 65;

    /**
     * Fixed overhead §5 charges to a block that carries any exception at all, on top of the
     * {@code roundUp8(excCount * (2 + W))} body: the exception area's own header,
     * {@code excCount u16 | pad u16 | pad u32}.
     *
     * <p><b>It is {@link ExceptionArea#HEADER_BYTES} by reference rather than by repetition</b>,
     * because the two numbers must be one number. §5's prose stated the cost term as 16 while
     * stating the layout as an eight-byte header; the layout is what describes bytes on disk, so it
     * won, and the doc's formula was corrected with the commit that changed this constant. Writing
     * the reference instead of the literal is what stops that contradiction from being reachable a
     * second time: a later phase that re-lays the area out changes one field and the width chooser
     * follows it.
     *
     * <p>This number is not decorative: it is one of the two terms of the argmin, so it must equal
     * what the encoder actually emits. When it was 16 the chooser over-charged every width carrying
     * exceptions by exactly eight bytes -- conservative and reproducible, but able to pick a width
     * one step wider than the true minimum, which is real bytes on every chunk in the cluster.
     */
    public static final int EXCEPTION_AREA_OVERHEAD = ExceptionArea.HEADER_BYTES;

    private BitPacking()
    {
    }

    // -----------------------------------------------------------------------------------------
    // sizing
    // -----------------------------------------------------------------------------------------

    /** Words a lane of {@code count} values at {@code width} bits occupies: {@code ceil(count*width/64)}. */
    public static int packedWordCount(int count, int width)
    {
        checkCount(count);
        checkWidth(width);
        long words = (((long) count * width) + 63) >>> 6;
        // A lane is addressed with int offsets everywhere downstream (block bodies, ByteBuffer
        // positions), so a length that cannot be expressed as an int is rejected here rather than
        // wrapping negative in a caller's bounds check.
        if (words * 8 > Integer.MAX_VALUE)
            throw new IllegalArgumentException("packed lane overflows int: count " + count + " width " + width);
        return (int) words;
    }

    /** Lane length in bytes: {@code ceil(count*width/64)*8}, always a multiple of 8 per §6. */
    public static int packedLength(int count, int width)
    {
        return packedWordCount(count, width) * 8;
    }

    /**
     * Rounds up to the §6 eight-byte boundary. Lives here rather than being copied per call site
     * because §6 applies the same padding rule to section ends, block-table ends and block-body
     * ends, and two copies of a padding rule is how a format grows two paddings.
     */
    static long roundUpToMultipleOf8(long bytes)
    {
        if (bytes < 0)
            throw new IllegalArgumentException("negative byte count: " + bytes);
        return (bytes + 7) & ~7L;
    }

    // -----------------------------------------------------------------------------------------
    // pack
    // -----------------------------------------------------------------------------------------

    /** Packs into a freshly allocated array of exactly {@link #packedLength} bytes. */
    public static byte[] pack(long[] values, int count, int width)
    {
        byte[] out = new byte[packedLength(count, width)];
        pack(values, count, width, ByteBuffer.wrap(out));
        return out;
    }

    /**
     * Packs {@code count} values at {@code width} bits into {@code dst} starting at its position,
     * advancing the position by {@link #packedLength}. Returns the bytes written.
     *
     * <p>Every byte of that range is written, including the zero-filled remainder of the final
     * word: the destination may be a reused scratch buffer, and §5 rule 5 exists because an encoder
     * that leaks stale bytes into padding produces byte-different output for identical input
     * and leaves every round-trip test green.
     */
    public static int pack(long[] values, int count, int width, ByteBuffer dst)
    {
        int words = packedWordCount(count, width);
        int length = words * 8;
        if (values.length < count)
            throw new IllegalArgumentException("values array holds " + values.length + " < count " + count);
        requireBigEndian(dst);
        if (dst.remaining() < length)
            throw new IllegalArgumentException("destination holds " + dst.remaining() + " < lane length " + length);

        int base = dst.position();
        long acc = 0;
        int wordIndex = 0;
        int bitOffset = 0;
        for (int i = 0; i < count; i++)
        {
            long v = values[i];
            // A value wider than the lane is not something to truncate: it means the width chooser
            // and the exception extraction disagreed, and truncating would write a wrong value that
            // round-trips consistently and is invisible until someone reads the data.
            if (width < MAX_WIDTH && (v >>> width) != 0)
                throw new IllegalArgumentException("value " + Long.toUnsignedString(v) + " at index " + i +
                                                   " does not fit in " + width + " bits");
            acc |= v << bitOffset;
            bitOffset += width;
            if (bitOffset >= 64)
            {
                dst.putLong(base + (wordIndex << 3), acc);
                wordIndex++;
                bitOffset -= 64;
                // bitOffset is now the number of high bits of v that did not fit, so the carry is
                // v >>> (width - bitOffset). The guard matters: at bitOffset == 0 the expression
                // would be a shift by 64, which Java evaluates modulo 64 and would re-deposit v.
                acc = bitOffset == 0 ? 0 : (v >>> (width - bitOffset));
            }
        }
        if (bitOffset != 0)
        {
            dst.putLong(base + (wordIndex << 3), acc);
            wordIndex++;
        }
        assert wordIndex == words : "wrote " + wordIndex + " words, sized " + words;
        dst.position(base + length);
        return length;
    }

    // -----------------------------------------------------------------------------------------
    // unpack
    // -----------------------------------------------------------------------------------------

    /** Unpacks from {@code src[srcOffset..]} into {@code dst[0..count)}. */
    public static void unpack(byte[] src, int srcOffset, int count, int width, long[] dst)
    {
        int length = packedLength(count, width);
        if (srcOffset < 0 || srcOffset > src.length || src.length - srcOffset < length)
            throw new IllegalArgumentException("truncated packed lane: need " + length + " bytes at offset " +
                                               srcOffset + ", array holds " + src.length);
        unpack(ByteBuffer.wrap(src, srcOffset, length), count, width, dst);
    }

    /**
     * Unpacks {@code count} values at {@code width} bits from {@code src} starting at its position
     * into the caller-supplied {@code dst}, advancing the position by {@link #packedLength}.
     * Returns the bytes consumed.
     *
     * <p>The destination is caller-supplied and caller-sized: nothing here allocates against a
     * length read out of the payload, so a corrupt block table can make this throw but cannot make
     * it reserve memory.
     */
    public static int unpack(ByteBuffer src, int count, int width, long[] dst)
    {
        int words = packedWordCount(count, width);
        int length = words * 8;
        if (dst.length < count)
            throw new IllegalArgumentException("destination array holds " + dst.length + " < count " + count);
        requireBigEndian(src);
        if (src.remaining() < length)
            throw new IllegalArgumentException("truncated packed lane: need " + length + " bytes, have " +
                                               src.remaining());

        int base = src.position();
        // width 64 cannot use (1L << width) - 1: the shift is modulo 64 and would yield 0. width 0
        // correctly yields 0, which is what makes the absent lane fall out of the general loop.
        long mask = width == MAX_WIDTH ? -1L : (1L << width) - 1;
        int wordIndex = 0;
        int bitOffset = 0;
        long cur = words > 0 ? src.getLong(base) : 0L;
        for (int i = 0; i < count; i++)
        {
            long v = cur >>> bitOffset;
            bitOffset += width;
            if (bitOffset >= 64)
            {
                bitOffset -= 64;
                wordIndex++;
                // The guard is a bounds check, not a data path: a value only consumes the next word
                // when bitOffset != 0 below, and such a value is provably inside the sized lane.
                cur = wordIndex < words ? src.getLong(base + (wordIndex << 3)) : 0L;
                if (bitOffset != 0)
                    v |= cur << (width - bitOffset);
            }
            dst[i] = v & mask;
        }
        src.position(base + length);
        return length;
    }

    // -----------------------------------------------------------------------------------------
    // width chooser (§5)
    // -----------------------------------------------------------------------------------------

    /**
     * §5 width cost: {@code ceil(n*w/64)*8} lane bytes plus, if the block carries any exception at
     * all, {@link #EXCEPTION_AREA_OVERHEAD} plus {@code roundUp8(excCount * (2 + W))} for the
     * shared exception area, where {@code W} is the verbatim value width in bytes.
     *
     * <p>The exception term is exactly {@link ExceptionArea#encodedLength(int, int)}, and
     * {@code ExceptionAreaTest} asserts that equality rather than leaving it to inspection: the
     * argmin is only meaningful while the size it scores is the size that gets written.
     */
    public static long widthCost(int count, int width, int exceptionCount, int exceptionValueBytes)
    {
        checkCount(count);
        checkWidth(width);
        checkExceptionValueBytes(exceptionValueBytes);
        if (exceptionCount < 0 || exceptionCount > count)
            throw new IllegalArgumentException("exceptionCount " + exceptionCount + " outside 0.." + count);

        long lanes = ((((long) count * width) + 63) >>> 6) * 8;
        if (exceptionCount == 0)
            return lanes;
        return lanes + EXCEPTION_AREA_OVERHEAD
               + roundUpToMultipleOf8((long) exceptionCount * (2 + exceptionValueBytes));
    }

    /** Values that do not fit in {@code width} bits, i.e. the exception count at that width. */
    public static int exceptionCount(long[] values, int count, int width)
    {
        checkCount(count);
        checkWidth(width);
        if (values.length < count)
            throw new IllegalArgumentException("values array holds " + values.length + " < count " + count);
        if (width == MAX_WIDTH)
            return 0;
        int exceptions = 0;
        for (int i = 0; i < count; i++)
            if ((values[i] >>> width) != 0)
                exceptions++;
        return exceptions;
    }

    /** {@link #chooseWidth(long[], int, int, int[])} with a throwaway histogram. */
    public static int chooseWidth(long[] values, int count, int exceptionValueBytes)
    {
        return chooseWidth(values, count, exceptionValueBytes, new int[WIDTH_HISTOGRAM_SIZE]);
    }

    /**
     * The §5 width chooser: the width in 0..64 minimising
     * {@link #widthCost(int, int, int, int)}, <b>ties broken by the smallest width</b>.
     *
     * <p>The tie-break is a determinism rule, not a preference (§5 rule 3). Two widths can cost
     * exactly the same number of bytes; whichever one the encoder picks must be a function of the
     * input alone, because a replica that picks the other produces a byte-different chunk.
     *
     * <p>All 65 widths are scored from one pass: a 65-bucket histogram of
     * {@code 64 - numberOfLeadingZeros(v)} (the unsigned bit length; 0 for zero, 64 for a value
     * with bit 63 set), turned in place into a suffix sum so that {@code histogram[w + 1]} is
     * exactly the exception count at width {@code w}. O(n + 65), and the caller may hand in a
     * reused {@code histogram} of at least {@link #WIDTH_HISTOGRAM_SIZE} entries so that a block
     * loop allocates nothing at all.
     */
    public static int chooseWidth(long[] values, int count, int exceptionValueBytes, int[] histogram)
    {
        checkCount(count);
        checkExceptionValueBytes(exceptionValueBytes);
        if (values.length < count)
            throw new IllegalArgumentException("values array holds " + values.length + " < count " + count);
        if (histogram.length < WIDTH_HISTOGRAM_SIZE)
            throw new IllegalArgumentException("histogram holds " + histogram.length + " < " + WIDTH_HISTOGRAM_SIZE);

        Arrays.fill(histogram, 0, WIDTH_HISTOGRAM_SIZE, 0);
        for (int i = 0; i < count; i++)
            histogram[64 - Long.numberOfLeadingZeros(values[i])]++;

        // In-place suffix sum: histogram[b] becomes the number of values needing at least b bits,
        // so the exception count at width w -- values needing more than w bits -- is histogram[w+1].
        for (int b = 63; b >= 0; b--)
            histogram[b] += histogram[b + 1];

        int bestWidth = 0;
        long bestCost = Long.MAX_VALUE;
        // Ascending with a strict '<' is the tie-break: the first width to reach the minimum keeps
        // it. Never relax this to '<=' -- that silently flips the rule to largest-w.
        for (int w = 0; w <= MAX_WIDTH; w++)
        {
            int exceptions = w == MAX_WIDTH ? 0 : histogram[w + 1];
            long cost = widthCost(count, w, exceptions, exceptionValueBytes);
            if (cost < bestCost)
            {
                bestCost = cost;
                bestWidth = w;
            }
        }
        return bestWidth;
    }

    // -----------------------------------------------------------------------------------------
    // argument checks
    // -----------------------------------------------------------------------------------------

    private static void checkCount(int count)
    {
        if (count < 0)
            throw new IllegalArgumentException("negative count: " + count);
    }

    private static void checkWidth(int width)
    {
        if (width < 0 || width > MAX_WIDTH)
            throw new IllegalArgumentException("width " + width + " outside 0.." + MAX_WIDTH);
    }

    private static void checkExceptionValueBytes(int exceptionValueBytes)
    {
        // The exception area stores a verbatim value next to a u16 position; v4 has no type wider
        // than a long, so anything outside 1..8 is a caller bug rather than a size to trust.
        if (exceptionValueBytes < 1 || exceptionValueBytes > 8)
            throw new IllegalArgumentException("exceptionValueBytes " + exceptionValueBytes + " outside 1..8");
    }

    private static void requireBigEndian(ByteBuffer buffer)
    {
        // Checked rather than fixed with duplicate().order(BIG_ENDIAN) because duplicating on a
        // per-block path allocates, and because a little-endian buffer reaching here means a caller
        // has lost track of §6's "every number is big-endian" -- worth failing loudly, once.
        if (buffer.order() != ByteOrder.BIG_ENDIAN)
            throw new IllegalArgumentException("v4 packed lanes are big-endian words, buffer order is " +
                                               buffer.order());
    }
}
