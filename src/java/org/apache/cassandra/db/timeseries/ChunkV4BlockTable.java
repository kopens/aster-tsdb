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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * One column section's block table (chunk-format-v4 §5): a fixed-width entry per block, uniform
 * across the section, carrying the block's extrema, where its body starts, how it is encoded, and
 * its presence mode.
 *
 * <p>Entry width follows the column's {@code statWidth} (§5, and {@link ChunkV4Directory#statWidth}
 * derives it from the type code):
 *
 * <pre>
 *   statWidth 8  (DOUBLE, INT64, timestamp axis)  24 B  min(8) max(8) bodyOffset(4) enc(1) flags(1) pad(2)
 *   statWidth 4  (INT32, DATE32)                  16 B  min(4) max(4) bodyOffset(4) enc(1) flags(1) pad(2)
 *   statWidth 0  (BOOLEAN, TEXT, OPAQUE)           8 B                bodyOffset(4) enc(1) flags(1) pad(2)
 * </pre>
 *
 * <p><b>Body length is derived, never stored:</b> block {@code k} occupies
 * {@code entry[k+1].bodyOffset - entry[k].bodyOffset}, and the last block occupies
 * {@code sectionLen - entry[last].bodyOffset}. That is not a byte-saving trick, it is what makes
 * every body bounds-checkable. A stored length can disagree with the next block's offset, and a
 * decoder that trusted it would read past its block into the next one and return the wrong values
 * without throwing. Derived lengths cannot disagree with anything, provided the offsets are
 * verified monotonic and inside the section -- which is the whole of this class's validation
 * burden, and is done in one place, once, on read and on write alike.
 *
 * <p><b>Offsets are section-relative.</b> The derivation above subtracts from {@code sectionLen},
 * which is a section-relative quantity, so {@code bodyOffset} must be measured from the section's
 * first byte and not from the payload's. The table itself lives inside the section (after the
 * preamble §2 allows for text and opaque dictionaries), so {@code tableOffset} is passed in and the
 * first body offset is checked against the end of the table -- a body that started inside the table
 * that described it would be self-overlapping in a way no bounds check on {@code sectionLen} alone
 * would catch.
 *
 * <p><b>Everything here is a multiple of 8 and stays that way for free</b> (§6): all three entry
 * sizes are, so the table's length is; every {@code bodyOffset} and {@code sectionLen} is checked
 * to be, so every derived body length is. That is what lets an unpack kernel read a body as whole
 * {@code getLong}s with no epilogue.
 *
 * <p><b>Corruption</b> (§11, §5): an unknown {@code blockEncoding} -- including {@code 0x00}, which
 * no code is assigned -- is a flipped bit and not a future format, because a future format would
 * carry a different version byte. §9 does allow new encoding codes without a version bump; that is
 * a statement about how the format may grow, not a licence for this build to guess at a code it
 * cannot decode. Reserved {@code blockFlags} bits 5-7 must be zero, the two pad bytes must be zero
 * and are verified (§5 rule 5), and every stat field a block does not use must be zero (§5 rule 6),
 * which is checked for {@code statWidth 0} entries, for all-null blocks, and for the boolean flags
 * on a column that has numeric extrema. The reward for the pad check is out of proportion to its
 * cost: a zeroed or half-written region cannot parse at all, because encoding {@code 0x00} is
 * invalid, so an all-zero entry is rejected on its first byte.
 *
 * <p><b>What this class deliberately does not check:</b> that {@code min <= max}. The extrema are
 * raw fixed-width bits and their order is the column's {@link StatOrder}, which lives in the
 * directory entry, not here -- the same separation that makes pruning sound (§4). The section
 * encoder holds both and must do it there.
 *
 * <p>Allocation is bounded by {@code blockCount}, which the caller takes from the directory, where
 * it is already checked against the §2 derivation from the header. Nothing here sizes an array from
 * a length read out of the payload.
 */
public final class ChunkV4BlockTable
{
    public static final int ENTRY_SIZE_STAT8 = 24;
    public static final int ENTRY_SIZE_STAT4 = 16;
    public static final int ENTRY_SIZE_STAT0 = 8;

    // §5 block encodings. 0x00 is unassigned and stays that way: it makes a zeroed entry
    // unparseable, which is the cheapest possible detector for a half-written table.
    public static final int ENC_EMPTY = 0x01;
    public static final int ENC_CONSTANT = 0x02;
    public static final int ENC_PRESENCE_ONLY = 0x03;
    public static final int ENC_FOR_BITPACK = 0x10;
    public static final int ENC_DELTA_FOR_BITPACK = 0x11;
    public static final int ENC_ALP = 0x20;
    public static final int ENC_ALP_RD = 0x21;
    public static final int ENC_BITPACK1 = 0x30;
    public static final int ENC_DICT = 0x40;
    public static final int ENC_RAW = 0x41;

    /**
     * §5: the presence mode occupies {@code blockFlags} bits 0-1. Taken from {@link BlockPresence}
     * rather than restated -- two definitions of a mode code is how a format acquires two presence
     * layouts, and the modes and this mask must move together or not at all.
     */
    public static final int FLAGS_PRESENCE_MASK = BlockPresence.MODE_MASK;

    /** Bit 2: the block contains at least one {@code false}. Boolean columns only (statWidth 0). */
    public static final int FLAG_HAS_FALSE = 0x04;
    /** Bit 3: the block contains at least one {@code true}. Boolean columns only (statWidth 0). */
    public static final int FLAG_HAS_TRUE = 0x08;
    /** Bit 4: the block carries a §5 shared exception area after its lane. */
    public static final int FLAG_HAS_EXCEPTIONS = 0x10;
    /** Bits 5-7 are reserved and must be zero. */
    public static final int FLAGS_RESERVED_MASK = 0xE0;

    private final int statWidth;
    private final int sectionLen;
    private final List<Entry> entries;

    private ChunkV4BlockTable(int statWidth, int sectionLen, List<Entry> entries)
    {
        this.statWidth = statWidth;
        this.sectionLen = sectionLen;
        this.entries = Collections.unmodifiableList(entries);
    }

    /** §5: 24, 16 or 8 bytes. Any other {@code statWidth} is a caller bug, not a format variant. */
    public static int entrySize(int statWidth)
    {
        switch (statWidth)
        {
            case 8: return ENTRY_SIZE_STAT8;
            case 4: return ENTRY_SIZE_STAT4;
            case 0: return ENTRY_SIZE_STAT0;
            default:
                throw new IllegalArgumentException("statWidth " + statWidth + " is not one of 8, 4, 0 (v4 §5)");
        }
    }

    /**
     * Bytes the table occupies. Always a multiple of 8, because all three entry sizes are -- §6's
     * "the block table's end is 8-padded" needs no padding bytes to hold.
     */
    public static int tableLength(int blockCount, int statWidth)
    {
        if (blockCount < 0)
            throw new IllegalArgumentException("negative blockCount " + blockCount);
        long length = (long) blockCount * entrySize(statWidth);
        if (length > Integer.MAX_VALUE)
            throw new IllegalArgumentException("block table of " + length + " bytes overflows int");
        return (int) length;
    }

    // -----------------------------------------------------------------------------------------
    // entry
    // -----------------------------------------------------------------------------------------

    /**
     * One block's table entry (§5). Immutable, and an entry that could not be read cannot be built.
     *
     * <p><b>{@link #min} and {@link #max} are raw bits, zero-extended</b>, not numbers: a
     * {@code statWidth 4} entry holds its four bytes in the low half of the long and the high half
     * is required to be zero. Whether those bits mean a signed int, an unsigned day count or the
     * top half of a double is the column's {@link StatOrder}, which is not this class's to know --
     * §4 keeps the order beside the stat exactly so no layer has to guess. A caller that
     * sign-extended before storing would produce a different byte for the same value, which is both
     * a determinism break and, for {@code DATE32}, a wrong answer.
     */
    public static final class Entry
    {
        public final int statWidth;
        public final long min;
        public final long max;
        public final int bodyOffset;
        public final int blockEncoding;
        public final int blockFlags;

        public Entry(int statWidth, long min, long max, int bodyOffset, int blockEncoding, int blockFlags)
        {
            entrySize(statWidth);       // rejects any width but 8, 4, 0
            checkEncoding(blockEncoding);
            if ((blockFlags & FLAGS_RESERVED_MASK) != 0)
                throw new IllegalArgumentException("reserved blockFlags bits set in 0x" +
                                                   Integer.toHexString(blockFlags) + " (v4 §5)");
            if (bodyOffset < 0)
                throw new IllegalArgumentException("negative bodyOffset " + bodyOffset);
            if ((bodyOffset & 7) != 0)
                throw new IllegalArgumentException("bodyOffset " + bodyOffset + " is not a multiple of 8 (v4 §6)");
            if (statWidth == 0 && (min != 0 || max != 0))
                throw new IllegalArgumentException("statWidth 0 block carries extrema " + min + '/' + max +
                                                   "; unused stat fields are zero (v4 §5 rule 6)");
            if (statWidth == 4 && ((min >>> 32) != 0 || (max >>> 32) != 0))
                throw new IllegalArgumentException("statWidth 4 extrema must be zero-extended, got 0x" +
                                                   Long.toHexString(min) + "/0x" + Long.toHexString(max));
            // A block with no present rows has no extrema to record, so leaving whatever the
            // previous block's min happened to be is both meaningless and byte-nondeterministic --
            // the classic reused-scratch-buffer failure, in a field a round-trip test never reads.
            if ((blockFlags & FLAGS_PRESENCE_MASK) == BlockPresence.MODE_ALL_NULL && (min != 0 || max != 0))
                throw new IllegalArgumentException("all-null block carries extrema " + min + '/' + max +
                                                   "; unused stat fields are zero (v4 §5 rule 6)");
            // HAS_FALSE/HAS_TRUE are a boolean column's substitute for extrema, and BOOLEAN is a
            // statWidth 0 type. On a column that has numeric extrema they describe nothing.
            if (statWidth != 0 && (blockFlags & (FLAG_HAS_FALSE | FLAG_HAS_TRUE)) != 0)
                throw new IllegalArgumentException("boolean presence flags set on a statWidth " + statWidth +
                                                   " block: 0x" + Integer.toHexString(blockFlags));

            this.statWidth = statWidth;
            this.min = min;
            this.max = max;
            this.bodyOffset = bodyOffset;
            this.blockEncoding = blockEncoding;
            this.blockFlags = blockFlags;
        }

        /** §4 presence mode, one of {@link BlockPresence}'s four {@code MODE_*} codes. */
        public int presenceMode()
        {
            return blockFlags & FLAGS_PRESENCE_MASK;
        }

        public boolean hasExceptions()
        {
            return (blockFlags & FLAG_HAS_EXCEPTIONS) != 0;
        }

        public boolean hasFalse()
        {
            return (blockFlags & FLAG_HAS_FALSE) != 0;
        }

        public boolean hasTrue()
        {
            return (blockFlags & FLAG_HAS_TRUE) != 0;
        }

        private void write(ByteBuffer dst)
        {
            if (statWidth == 8)
            {
                dst.putLong(min);
                dst.putLong(max);
            }
            else if (statWidth == 4)
            {
                dst.putInt((int) min);
                dst.putInt((int) max);
            }
            dst.putInt(bodyOffset);
            dst.put((byte) blockEncoding);
            dst.put((byte) blockFlags);
            // §5 rule 5: written, not skipped. An encoder that left these two bytes to whatever the
            // scratch buffer held would emit different bytes for identical input and pass every
            // round-trip test in the suite.
            dst.putShort((short) 0);
        }
    }

    // -----------------------------------------------------------------------------------------
    // accessors
    // -----------------------------------------------------------------------------------------

    public int blockCount()
    {
        return entries.size();
    }

    public int statWidth()
    {
        return statWidth;
    }

    public Entry entry(int block)
    {
        return entries.get(block);
    }

    public List<Entry> entries()
    {
        return entries;
    }

    /** Section-relative offset of block {@code k}'s body. */
    public int bodyOffset(int block)
    {
        return entries.get(block).bodyOffset;
    }

    /**
     * §5: {@code entry[k+1].bodyOffset - entry[k].bodyOffset}, and {@code sectionLen - bodyOffset}
     * for the last block. Never negative and always a multiple of 8, because the offsets were
     * verified monotonic, 8-aligned and bounded by an 8-aligned {@code sectionLen} when the table
     * was parsed.
     */
    public int bodyLength(int block)
    {
        int end = block + 1 < entries.size() ? entries.get(block + 1).bodyOffset : sectionLen;
        return end - entries.get(block).bodyOffset;
    }

    // -----------------------------------------------------------------------------------------
    // write
    // -----------------------------------------------------------------------------------------

    /**
     * Writes the table at {@code dst}'s position, advancing it by
     * {@link #tableLength(int, int)} and returning that count.
     *
     * @param tableOffset section-relative offset the table itself sits at, i.e. the length of the
     *                    §2 section preamble; bodies must start after the table ends
     * @param sectionLen  the section's total length, which is the last block's body end
     */
    public static int write(List<Entry> entries, int statWidth, int tableOffset, int sectionLen, ByteBuffer dst)
    {
        int length = tableLength(entries.size(), statWidth);
        requireBigEndian(dst);
        if (dst.remaining() < length)
            throw new IllegalArgumentException("destination holds " + dst.remaining() + " < block table " + length);
        checkLayout(entries, statWidth, tableOffset, sectionLen);

        int base = dst.position();
        for (Entry entry : entries)
            entry.write(dst);
        assert dst.position() - base == length : "wrote " + (dst.position() - base) + ", sized " + length;
        return length;
    }

    // -----------------------------------------------------------------------------------------
    // read
    // -----------------------------------------------------------------------------------------

    /**
     * Parses {@code blockCount} entries from {@code src}'s position, advancing it past the table.
     *
     * <p>{@code blockCount} and {@code statWidth} come from the directory entry, where both were
     * already checked -- the count against the §2 derivation from the header, the width against the
     * type code -- so the array sized here is sized from validated metadata and not from the bytes
     * being parsed.
     *
     * @param tableOffset section-relative offset this table sits at
     * @param sectionLen  the section's total length
     */
    public static ChunkV4BlockTable read(ByteBuffer src, int blockCount, int statWidth, int tableOffset, int sectionLen)
    {
        int length = tableLength(blockCount, statWidth);
        requireBigEndian(src);
        if (src.remaining() < length)
            throw new IllegalArgumentException("Corrupt v4 chunk: truncated block table, need " + length +
                                               " bytes, have " + src.remaining());

        int size = entrySize(statWidth);
        int base = src.position();
        List<Entry> entries = new ArrayList<>(blockCount);
        for (int k = 0; k < blockCount; k++)
        {
            int at = base + k * size;
            long min = 0;
            long max = 0;
            int cursor = at;
            if (statWidth == 8)
            {
                min = src.getLong(cursor);
                max = src.getLong(cursor + 8);
                cursor += 16;
            }
            else if (statWidth == 4)
            {
                // Zero-extended, not sign-extended: these are the four bytes as written, and what
                // they mean is the column's statOrder (see Entry's javadoc).
                min = src.getInt(cursor) & 0xFFFFFFFFL;
                max = src.getInt(cursor + 4) & 0xFFFFFFFFL;
                cursor += 8;
            }
            int bodyOffset = src.getInt(cursor);
            int blockEncoding = src.get(cursor + 4) & 0xFF;
            int blockFlags = src.get(cursor + 5) & 0xFF;
            int pad = src.getShort(cursor + 6) & 0xFFFF;
            if (pad != 0)
                throw new IllegalArgumentException("Corrupt v4 chunk: non-zero block-entry padding " + pad +
                                                   " at block " + k + " (v4 §5 rule 5)");
            entries.add(new Entry(statWidth, min, max, bodyOffset, blockEncoding, blockFlags));
        }
        checkLayout(entries, statWidth, tableOffset, sectionLen);
        src.position(base + length);
        return new ChunkV4BlockTable(statWidth, sectionLen, entries);
    }

    // -----------------------------------------------------------------------------------------
    // layout invariants -- the reason a derived body length is safe
    // -----------------------------------------------------------------------------------------

    /**
     * The three properties that make every body bounds-checkable: bodies begin after the table that
     * describes them, offsets never decrease, and no offset passes the section's end.
     *
     * <p>Monotonicity is non-strict on purpose. A block encoded {@code EMPTY} has no body at all,
     * so its offset equals the next block's, and requiring a strict increase would forbid the
     * cheapest block in the format.
     */
    private static void checkLayout(List<Entry> entries, int statWidth, int tableOffset, int sectionLen)
    {
        if (sectionLen < 0 || (sectionLen & 7) != 0)
            throw new IllegalArgumentException("sectionLen " + sectionLen + " is negative or not a multiple of 8");
        if (tableOffset < 0 || (tableOffset & 7) != 0)
            throw new IllegalArgumentException("tableOffset " + tableOffset + " is negative or not a multiple of 8");
        int length = tableLength(entries.size(), statWidth);
        long bodiesFrom = (long) tableOffset + length;
        if (bodiesFrom > sectionLen)
            throw new IllegalArgumentException("block table ends at " + bodiesFrom + ", past sectionLen " + sectionLen);

        long previous = bodiesFrom;
        for (int k = 0; k < entries.size(); k++)
        {
            Entry entry = entries.get(k);
            if (entry.statWidth != statWidth)
                throw new IllegalArgumentException("block " + k + " has statWidth " + entry.statWidth +
                                                   ", table is " + statWidth);
            if (entry.bodyOffset < previous)
                throw new IllegalArgumentException("Corrupt v4 chunk: bodyOffset " + entry.bodyOffset + " at block " +
                                                   k + " is below " + previous +
                                                   (k == 0 ? " where the bodies start" : ", the previous block"));
            if (entry.bodyOffset > sectionLen)
                throw new IllegalArgumentException("Corrupt v4 chunk: bodyOffset " + entry.bodyOffset + " at block " +
                                                   k + " passes sectionLen " + sectionLen);
            previous = entry.bodyOffset;
        }
    }

    private static void checkEncoding(int blockEncoding)
    {
        switch (blockEncoding)
        {
            case ENC_EMPTY:
            case ENC_CONSTANT:
            case ENC_PRESENCE_ONLY:
            case ENC_FOR_BITPACK:
            case ENC_DELTA_FOR_BITPACK:
            case ENC_ALP:
            case ENC_ALP_RD:
            case ENC_BITPACK1:
            case ENC_DICT:
            case ENC_RAW:
                return;
            default:
                throw new IllegalArgumentException("Corrupt v4 chunk: unknown blockEncoding 0x" +
                                                   Integer.toHexString(blockEncoding) +
                                                   "; inside a v4 payload an unallocated code is a flipped bit, " +
                                                   "not a future format (§11)");
        }
    }

    private static void requireBigEndian(ByteBuffer buffer)
    {
        if (buffer.order() != ByteOrder.BIG_ENDIAN)
            throw new IllegalArgumentException("v4 block-table fields are big-endian, buffer order is " +
                                               buffer.order());
    }
}
