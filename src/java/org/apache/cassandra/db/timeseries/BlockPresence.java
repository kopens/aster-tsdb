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
 * The four per-block presence modes of chunk-format-v4 §4 -- {@code ALL_PRESENT}, {@code ALL_NULL},
 * {@code BITMAP}, {@code RLE} -- and the block-local {@code rank} of §2.
 *
 * <p><b>The load-bearing decision is that all four modes decode into the same {@code long[]} bit-word
 * array, once per block.</b> §4 keeps RLE because a sparse column really is smaller as runs (the
 * cross-over is around 84 transitions per 1024 rows), and keeps BITMAP because runs cannot be
 * vectorised or randomly accessed. Expanding both into words at block-open time buys the
 * compression without paying for it on every read: the value-decode loop, the {@code rank} that
 * turns a row offset into a value index, and any future {@code BitPackingVector} kernel
 * ({@code VectorMask.fromLong} takes one word per 64 lanes, §6) all see words and never a varint.
 * A decoder that walked the runs directly would reintroduce exactly the serial dependency v4 spent
 * its budget removing.
 *
 * <p><b>Storage-mode choice is an exact-size argmin, never a heuristic</b> (§5 rule 3). Both zero
 * byte modes win outright when they apply; otherwise the smaller of the two byte counts wins and an
 * exact tie resolves to the smaller mode code, i.e. {@code BITMAP}. Nothing here trial-encodes,
 * samples, or consults a threshold: a replica that resolved a tie differently would produce a
 * byte-different chunk from the same input.
 *
 * <p><b>Mode codes.</b> §4 lists the four modes in the low two bits of {@code blockFlags} but does
 * not number them; this class takes the listing order, which also puts {@code ALL_PRESENT} at 0 so
 * that the overwhelmingly common block -- fully present, no exceptions -- has all-zero flags. Unlike
 * the type codes of §4 there is no zero trap here, because a presence mode is not dispatched on by
 * name and an all-zero {@code blockFlags} byte is a legitimate, expected value.
 *
 * <p><b>Padding and canonical varints are verified, not assumed</b> (§5 rules 4 and 5). The RLE
 * layout is exactly {@code u16 runCount | u8 firstRunPresent | u8 pad | varint runLengths[]} padded
 * to 8; the pad byte, the trailing padding, the {@code firstRunPresent} byte being strictly 0 or 1,
 * the minimal length of every varint, runs of at least one row and runs summing to exactly
 * {@code blockRows} are all checked on decode. The bitmap's bits at positions {@code >= blockRows}
 * must be zero (§5 rule 6) and that too is verified: a non-zero trailing bit breaks {@code rank} --
 * silently, by returning another row's value rather than throwing -- and breaks byte determinism,
 * because the same presence pattern would then have more than one encoding.
 *
 * <p>Corruption surfaces as {@link IllegalArgumentException}, and every allocation here is sized
 * from {@code blockRows}, which the caller derives from the header, never from a length read out of
 * the payload. A corrupt block therefore cannot make this class reserve memory.
 */
public final class BlockPresence
{
    /** Every row present; zero bytes (§4). Code 0 so the common block's {@code blockFlags} are all zero. */
    public static final int MODE_ALL_PRESENT = 0;
    /** No row present; zero bytes (§4). */
    public static final int MODE_ALL_NULL = 1;
    /** {@code ceil(blockRows/64)*8} bytes of big-endian, LSB-first-within-word bit words (§4, §6). */
    public static final int MODE_BITMAP = 2;
    /** {@code u16 runCount | u8 firstRunPresent | u8 pad | varint runLengths[]}, padded to 8 (§4). */
    public static final int MODE_RLE = 3;

    /** The mode occupies the low two bits of {@code blockFlags} (§4). */
    public static final int MODE_MASK = 0x03;

    /** §7 bounds the block size at 64..32,768 rows, so a block never holds more than this. */
    public static final int MAX_BLOCK_ROWS = 1 << 15;

    /** {@code u16 runCount | u8 firstRunPresent | u8 pad} ahead of the run varints. */
    static final int RLE_HEADER_BYTES = 4;

    private BlockPresence()
    {
    }

    // -----------------------------------------------------------------------------------------
    // bit words
    // -----------------------------------------------------------------------------------------

    /** Words a block's presence occupies: {@code ceil(blockRows/64)}. */
    public static int wordCount(int blockRows)
    {
        checkBlockRows(blockRows);
        return (blockRows + 63) >>> 6;
    }

    /**
     * §2 block-local rank: the number of present rows strictly below {@code offset}, which is the
     * index of row {@code offset}'s value inside the block's value lane. At most 16
     * {@link Long#bitCount} intrinsics for a 1024-row block, and no allocation.
     *
     * <p>An off-by-one here does not throw; it returns another row's value. §12 ranks that as the
     * second-largest risk in the format, which is why the test for it is a property over random
     * null patterns rather than a handful of examples.
     *
     * <p><b>Normative, for every caller that walks rows in order: a sequential scan must not call
     * this per row.</b> This is O(offset/64) by construction -- the right shape for a random-access
     * seek, which is what it exists for, and the wrong shape for a scan. A cursor that called it
     * once per row would turn a block walk into O(n * n/64): 8,192 {@code bitCount} intrinsics to
     * read a 1024-row block instead of 1,024 increments, and 16x worse again at the §7 maximum block
     * size. Instead, carry a running value index alongside the row offset and increment it by the
     * row's own presence bit -- {@code valueIndex += (words[row >>> 6] >>> (row & 63)) & 1} -- which
     * is exactly {@code rank(words, row + 1) - rank(words, row)} and needs no {@code bitCount} at
     * all. Call {@code rank()} only when the scan position moves discontinuously: opening a block,
     * a seek, or resuming a paged read. The v4 block cursor is not written yet; this rule is cheap
     * to honour while writing it and expensive to retrofit afterwards, because by then the per-row
     * call reads as correct code and its cost is invisible in a profile that blames
     * {@code Long.bitCount}.
     */
    public static int rank(long[] words, int offset)
    {
        if (offset < 0 || offset > (long) words.length * 64)
            throw new IllegalArgumentException("rank offset " + offset + " outside 0.." + (long) words.length * 64);
        int full = offset >>> 6;
        int count = 0;
        for (int i = 0; i < full; i++)
            count += Long.bitCount(words[i]);
        int partial = offset & 63;
        // The guard is a bounds check as much as an optimisation: at a word-aligned offset the
        // partial word may be one past the end of the array.
        if (partial != 0)
            count += Long.bitCount(words[full] & ((1L << partial) - 1));
        return count;
    }

    /** Present rows in the block, i.e. the length of its value lane. */
    public static int cardinality(long[] words, int blockRows)
    {
        checkWordsFit(words, blockRows);
        return rank(words, blockRows);
    }

    /** Packs a presence flag per row into §6 bit words: row {@code i} is bit {@code i & 63} of word {@code i >>> 6}. */
    public static long[] toWords(boolean[] present, int blockRows)
    {
        checkBlockRows(blockRows);
        if (present.length < blockRows)
            throw new IllegalArgumentException("presence array holds " + present.length + " < blockRows " + blockRows);
        long[] words = new long[wordCount(blockRows)];
        for (int i = 0; i < blockRows; i++)
            if (present[i])
                words[i >>> 6] |= 1L << (i & 63);
        return words;
    }

    /** Inverse of {@link #toWords}, into a caller-supplied array. */
    public static void toBooleans(long[] words, int blockRows, boolean[] dst)
    {
        checkWordsFit(words, blockRows);
        if (dst.length < blockRows)
            throw new IllegalArgumentException("destination holds " + dst.length + " < blockRows " + blockRows);
        for (int i = 0; i < blockRows; i++)
            dst[i] = bit(words, i);
    }

    // -----------------------------------------------------------------------------------------
    // mode choice and sizing
    // -----------------------------------------------------------------------------------------

    /** {@link #chooseMode(long[], int)} over a presence flag per row. */
    public static int chooseMode(boolean[] present, int blockRows)
    {
        return chooseMode(toWords(present, blockRows), blockRows);
    }

    /**
     * The §4 mode for this presence pattern: the exact-size argmin, ties to the smaller mode code.
     *
     * <p>{@code ALL_PRESENT} and {@code ALL_NULL} cost zero bytes, so when they apply they are the
     * minimum by definition and are tested first. Otherwise the choice is between the bitmap's
     * fixed {@code ceil(blockRows/64)*8} and the RLE's {@code roundUp8(4 + sum of run varints)},
     * both computed exactly -- there is no encode-and-compare, whose cost would be paid on every
     * block and whose result could still depend on a reused buffer.
     */
    public static int chooseMode(long[] words, int blockRows)
    {
        checkWordsFit(words, blockRows);
        checkTrailingBitsZero(words, blockRows);
        int present = rank(words, blockRows);
        if (present == blockRows)
            return MODE_ALL_PRESENT;
        if (present == 0)
            return MODE_ALL_NULL;
        int bitmap = wordCount(blockRows) * 8;
        int rle = (int) BitPacking.roundUpToMultipleOf8(rleUnpaddedLength(words, blockRows));
        // Strictly-smaller: an exact tie keeps BITMAP, the smaller mode code (§5 rule 3), which is
        // also the mode that decodes without touching a varint.
        return rle < bitmap ? MODE_RLE : MODE_BITMAP;
    }

    /** Exact encoded size in bytes of {@code words} under {@code mode}. */
    public static int encodedLength(int mode, long[] words, int blockRows)
    {
        checkWordsFit(words, blockRows);
        checkTrailingBitsZero(words, blockRows);
        checkModeApplies(mode, words, blockRows);
        switch (mode)
        {
            case MODE_ALL_PRESENT:
            case MODE_ALL_NULL:
                return 0;
            case MODE_BITMAP:
                return wordCount(blockRows) * 8;
            case MODE_RLE:
                return (int) BitPacking.roundUpToMultipleOf8(rleUnpaddedLength(words, blockRows));
            default:
                throw new IllegalArgumentException("unknown presence mode " + mode);
        }
    }

    // -----------------------------------------------------------------------------------------
    // encode
    // -----------------------------------------------------------------------------------------

    /** Encodes into a freshly allocated array of exactly {@link #encodedLength} bytes. */
    public static byte[] encode(int mode, long[] words, int blockRows)
    {
        byte[] out = new byte[encodedLength(mode, words, blockRows)];
        encode(mode, words, blockRows, ByteBuffer.wrap(out));
        return out;
    }

    /**
     * Encodes into {@code dst} at its position, advancing it by {@link #encodedLength} and
     * returning the bytes written. Every byte in that range is written, padding included: §5 rule 5
     * exists because a reused scratch buffer that leaks stale bytes into padding makes identical
     * input produce different bytes while every round-trip test stays green.
     */
    public static int encode(int mode, long[] words, int blockRows, ByteBuffer dst)
    {
        int length = encodedLength(mode, words, blockRows);
        requireBigEndian(dst);
        if (dst.remaining() < length)
            throw new IllegalArgumentException("destination holds " + dst.remaining() + " < presence length " + length);

        int base = dst.position();
        switch (mode)
        {
            case MODE_ALL_PRESENT:
            case MODE_ALL_NULL:
                break;
            case MODE_BITMAP:
            {
                int count = wordCount(blockRows);
                for (int w = 0; w < count; w++)
                    dst.putLong(base + (w << 3), words[w]);
                dst.position(base + length);
                break;
            }
            case MODE_RLE:
            {
                dst.putShort((short) runCount(words, blockRows));
                dst.put((byte) (bit(words, 0) ? 1 : 0));
                dst.put((byte) 0);
                int runStart = 0;
                for (int i = 1; i <= blockRows; i++)
                {
                    if (i == blockRows || bit(words, i) != bit(words, i - 1))
                    {
                        writeVarInt(dst, i - runStart);
                        runStart = i;
                    }
                }
                while (dst.position() - base < length)
                    dst.put((byte) 0);
                break;
            }
            default:
                throw new IllegalArgumentException("unknown presence mode " + mode);
        }
        assert dst.position() - base == length : "wrote " + (dst.position() - base) + ", sized " + length;
        return length;
    }

    // -----------------------------------------------------------------------------------------
    // decode
    // -----------------------------------------------------------------------------------------

    /** {@link #decode(int, ByteBuffer, int, long[])} into a fresh array of {@link #wordCount} words. */
    public static long[] decode(int mode, ByteBuffer src, int blockRows)
    {
        long[] words = new long[wordCount(blockRows)];
        decode(mode, src, blockRows, words);
        return words;
    }

    /**
     * Expands any of the four modes into {@code dst[0..wordCount(blockRows))}, advancing
     * {@code src} past the bytes the mode occupies and returning that count.
     *
     * <p>{@code dst} is caller-supplied and sized from {@code blockRows}, which comes from the
     * header, so no allocation here is driven by payload content; a corrupt block can make this
     * throw but cannot make it reserve memory.
     */
    public static int decode(int mode, ByteBuffer src, int blockRows, long[] dst)
    {
        checkBlockRows(blockRows);
        int count = wordCount(blockRows);
        if (dst.length < count)
            throw new IllegalArgumentException("destination holds " + dst.length + " < " + count + " presence words");
        requireBigEndian(src);
        switch (mode)
        {
            case MODE_ALL_PRESENT:
            {
                Arrays.fill(dst, 0, count, -1L);
                // §5 rule 6: the bits past the last row are zero in every mode, so that one
                // presence pattern has exactly one word array and rank cannot count a phantom row.
                int tail = blockRows & 63;
                if (tail != 0)
                    dst[count - 1] = -1L >>> (64 - tail);
                return 0;
            }
            case MODE_ALL_NULL:
                Arrays.fill(dst, 0, count, 0L);
                return 0;
            case MODE_BITMAP:
                return decodeBitmap(src, blockRows, dst);
            case MODE_RLE:
                return decodeRle(src, blockRows, dst);
            default:
                throw new IllegalArgumentException("unknown presence mode " + mode);
        }
    }

    private static int decodeBitmap(ByteBuffer src, int blockRows, long[] dst)
    {
        int count = wordCount(blockRows);
        int length = count * 8;
        if (src.remaining() < length)
            throw new IllegalArgumentException("truncated presence bitmap: need " + length + " bytes, have " +
                                               src.remaining());
        int base = src.position();
        for (int w = 0; w < count; w++)
            dst[w] = src.getLong(base + (w << 3));
        checkTrailingBitsZero(dst, blockRows);
        src.position(base + length);
        return length;
    }

    private static int decodeRle(ByteBuffer src, int blockRows, long[] dst)
    {
        int base = src.position();
        if (src.remaining() < RLE_HEADER_BYTES)
            throw new IllegalArgumentException("truncated RLE presence header: have " + src.remaining() + " bytes");
        int runs = src.getShort() & 0xFFFF;
        int firstRunPresent = src.get() & 0xFF;
        int pad = src.get() & 0xFF;
        // Strictly 0 or 1: any other truthy byte would decode to the same bitmap through a
        // different byte sequence, which is the ambiguity §5 rule 3 forbids.
        if (firstRunPresent > 1)
            throw new IllegalArgumentException("RLE firstRunPresent must be 0 or 1, got " + firstRunPresent);
        if (pad != 0)
            throw new IllegalArgumentException("non-zero RLE header padding: " + pad + " (v4 rule 5)");
        // Each run covers at least one row and costs at least one varint byte, so these two bounds
        // are exact upper limits on the loop below -- a corrupt runCount cannot spin.
        if (runs < 1 || runs > blockRows)
            throw new IllegalArgumentException("RLE runCount " + runs + " outside 1.." + blockRows);
        if (runs > src.remaining())
            throw new IllegalArgumentException("RLE runCount " + runs + " exceeds " + src.remaining() +
                                               " remaining bytes");

        int count = wordCount(blockRows);
        Arrays.fill(dst, 0, count, 0L);
        boolean present = firstRunPresent == 1;
        int row = 0;
        for (int r = 0; r < runs; r++)
        {
            long run = readVarInt(src);
            // A zero-length run is rejected rather than tolerated: it would let two encodings
            // describe one pattern, and merging adjacent same-polarity runs is what makes the run
            // list canonical.
            if (run < 1 || run > blockRows - row)
                throw new IllegalArgumentException("RLE run " + run + " at row " + row + " overruns blockRows " +
                                                   blockRows);
            if (present)
                setBits(dst, row, row + (int) run);
            row += (int) run;
            present = !present;
        }
        if (row != blockRows)
            throw new IllegalArgumentException("RLE runs sum to " + row + ", block holds " + blockRows + " rows");

        int consumed = src.position() - base;
        int length = (int) BitPacking.roundUpToMultipleOf8(consumed);
        if (src.remaining() < length - consumed)
            throw new IllegalArgumentException("truncated RLE presence padding: need " + (length - consumed) +
                                               " bytes, have " + src.remaining());
        for (int i = consumed; i < length; i++)
            if (src.get() != 0)
                throw new IllegalArgumentException("non-zero RLE presence padding at byte " + i + " (v4 rule 5)");
        return length;
    }

    // -----------------------------------------------------------------------------------------
    // canonical varints (§5 rule 4)
    // -----------------------------------------------------------------------------------------
    //
    // v4 keeps varints only inside block bodies, and only in their canonical minimal-length form.
    // These deliberately did not reuse v3's varint reader (ColumnarChunkCodec.readVarLong, deleted
    // with the v3 implementation): that one accepted a non-minimal encoding, which was harmless in
    // v3 (nothing re-encoded a chunk byte-for-byte) and fatal here, because a payload that decodes
    // to the same runs but re-encodes to different bytes gives one chunk two byte forms.
    // Package-private so a later phase can lift them into a shared helper when the
    // directory and dictionary paths need the same rule.

    /** Bytes the canonical encoding of {@code value} occupies. */
    static int varIntLength(long value)
    {
        if (value < 0)
            throw new IllegalArgumentException("negative varint: " + value);
        int length = 1;
        while ((value >>>= 7) != 0)
            length++;
        return length;
    }

    /** Writes {@code value} as a minimal-length LEB128 varint. Minimality is by construction here. */
    static void writeVarInt(ByteBuffer dst, long value)
    {
        if (value < 0)
            throw new IllegalArgumentException("negative varint: " + value);
        while (true)
        {
            int low7 = (int) (value & 0x7F);
            value >>>= 7;
            if (value == 0)
            {
                dst.put((byte) low7);
                return;
            }
            dst.put((byte) (low7 | 0x80));
        }
    }

    /** Reads a canonical varint, rejecting truncation, overflow and any non-minimal encoding. */
    static long readVarInt(ByteBuffer src)
    {
        long result = 0;
        int shift = 0;
        while (true)
        {
            if (!src.hasRemaining())
                throw new IllegalArgumentException("truncated varint");
            int b = src.get() & 0xFF;
            // At shift 63 only bit 0 of the group can survive; anything above it would be dropped
            // silently, turning an out-of-range value into a plausible small one.
            if (shift == 63 && (b & 0x7F) > 1)
                throw new IllegalArgumentException("varint overflows 64 bits");
            result |= ((long) (b & 0x7F)) << shift;
            if ((b & 0x80) == 0)
            {
                // A terminating zero group after a continuation contributes nothing: the value had
                // a shorter encoding, so accepting this one would give it two byte forms.
                if (shift > 0 && b == 0)
                    throw new IllegalArgumentException("non-canonical varint: trailing zero group");
                return result;
            }
            shift += 7;
            if (shift >= 64)
                throw new IllegalArgumentException("varint exceeds 64 bits");
        }
    }

    // -----------------------------------------------------------------------------------------
    // internals
    // -----------------------------------------------------------------------------------------

    private static boolean bit(long[] words, int index)
    {
        return ((words[index >>> 6] >>> (index & 63)) & 1L) != 0;
    }

    /**
     * Sets bits {@code [from, to)} a word at a time: a masked OR for the partial head word,
     * {@link Arrays#fill} for the whole-word interior, a masked OR for the partial tail. Cost is
     * O(words touched), not O(rows).
     *
     * <p>This is the whole point of the class javadoc's argument, and it was originally written the
     * other way round. The claim above is that expanding RLE into bit words at block-open time is
     * what removes the serial per-row dependency the format spent its budget on -- but a per-row
     * {@code words[i >>> 6] |= 1L << (i & 63)} loop *is* that serial dependency, just moved. It
     * repeatedly read-modify-writes the same word: a 1024-row block that is one long present run
     * cost 1024 dependent iterations to set 16 words. The expansion has to be word-at-a-time or it
     * reintroduces exactly what it was meant to remove.
     *
     * <p>The two partial words are where this can go wrong, and nowhere else. {@code -1L << (from &
     * 63)} keeps bits {@code from..63}; {@code -1L >>> (-to & 63)} keeps bits {@code 0..(to-1)&63}
     * and is deliberately written against {@code -to} so that a word-aligned {@code to} yields a
     * shift of 0 and a full mask rather than an empty one. A run confined to one word takes the
     * intersection of both masks. An off-by-one at either edge does not throw -- it shifts one
     * column's values by one row -- so the test for it is a differential property against a naive
     * per-row reference, over runs placed at every alignment.
     */
    private static void setBits(long[] words, int from, int to)
    {
        if (from >= to)
            return;
        int firstWord = from >>> 6;
        int lastWord = (to - 1) >>> 6;
        long head = -1L << (from & 63);
        long tail = -1L >>> (-to & 63);
        if (firstWord == lastWord)
        {
            words[firstWord] |= head & tail;
            return;
        }
        words[firstWord] |= head;
        // Empty when the run spans exactly two words; Arrays.fill tolerates from == to.
        Arrays.fill(words, firstWord + 1, lastWord, -1L);
        words[lastWord] |= tail;
    }

    /** Runs in the presence pattern; at least 1 for a non-empty block, at most {@code blockRows}. */
    private static int runCount(long[] words, int blockRows)
    {
        int runs = 1;
        for (int i = 1; i < blockRows; i++)
            if (bit(words, i) != bit(words, i - 1))
                runs++;
        return runs;
    }

    /** RLE size before the §6 pad to 8: the fixed header plus one canonical varint per run. */
    private static int rleUnpaddedLength(long[] words, int blockRows)
    {
        int bytes = RLE_HEADER_BYTES;
        int runStart = 0;
        for (int i = 1; i <= blockRows; i++)
        {
            if (i == blockRows || bit(words, i) != bit(words, i - 1))
            {
                bytes += varIntLength(i - runStart);
                runStart = i;
            }
        }
        return bytes;
    }

    private static void checkModeApplies(int mode, long[] words, int blockRows)
    {
        // ALL_PRESENT and ALL_NULL carry no bytes, so encoding a partial pattern under one of them
        // would not be a size mistake, it would be data loss that round-trips to a wrong answer.
        if (mode == MODE_ALL_PRESENT && rank(words, blockRows) != blockRows)
            throw new IllegalArgumentException("ALL_PRESENT with " + rank(words, blockRows) + '/' + blockRows +
                                               " rows present");
        if (mode == MODE_ALL_NULL && rank(words, blockRows) != 0)
            throw new IllegalArgumentException("ALL_NULL with " + rank(words, blockRows) + " rows present");
    }

    private static void checkTrailingBitsZero(long[] words, int blockRows)
    {
        int tail = blockRows & 63;
        if (tail == 0)
            return;
        long valid = -1L >>> (64 - tail);
        long last = words[((blockRows + 63) >>> 6) - 1];
        if ((last & ~valid) != 0)
            throw new IllegalArgumentException("presence bits at or above blockRows " + blockRows +
                                               " are set (v4 rule 6); they break rank and byte determinism");
    }

    private static void checkBlockRows(int blockRows)
    {
        // A block with no rows does not exist: blockRows(k) = min(blockSize, rowCount - k*blockSize)
        // is at least 1 for every block the header declares (§2), so 0 here means a corrupt or
        // miscomputed block index rather than an empty block to tolerate.
        if (blockRows < 1 || blockRows > MAX_BLOCK_ROWS)
            throw new IllegalArgumentException("blockRows " + blockRows + " outside 1.." + MAX_BLOCK_ROWS);
    }

    private static void checkWordsFit(long[] words, int blockRows)
    {
        int count = wordCount(blockRows);
        if (words.length < count)
            throw new IllegalArgumentException("presence array holds " + words.length + " < " + count + " words");
    }

    private static void requireBigEndian(ByteBuffer buffer)
    {
        if (buffer.order() != ByteOrder.BIG_ENDIAN)
            throw new IllegalArgumentException("v4 presence words are big-endian, buffer order is " +
                                               buffer.order());
    }
}
