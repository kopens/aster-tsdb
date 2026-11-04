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
 * The forty-byte chunk-format-v4 header (§3), and the single place the §2 row-to-block arithmetic
 * lives.
 *
 * <p><b>Offsets 0, 1, 5 and 13 are byte-for-byte the version-3 header's</b>: {@code version} u8,
 * {@code rowCount} u32, {@code firstTimestamp} i64, {@code lastTimestamp} i64. That is not
 * nostalgia, it is the whole of §3's stated requirement -- {@link #rowCount(ByteBuffer)},
 * {@link #firstTimestamp(ByteBuffer)} and {@link #lastTimestamp(ByteBuffer)} stay constant-time
 * peeks that read one field out of the first 21 bytes and <b>never parse the directory, a block
 * table or a section</b>. A reader asks a chunk for its time bounds to decide whether to open it
 * at all, so a peek that had to decode anything would make "skip this chunk" cost as much as
 * reading it. Keep those four offsets where they are; a field inserted ahead of them is not a
 * layout change, it is a performance regression in the one path that runs on chunks nobody wants.
 *
 * <p>The rest of the header is the v4 delta (§0): a u32 {@code directoryLen} in place of v3's u16
 * ({@code dirSize} could not address the 4 GiB directory §7 permits -- an unreachable contradiction
 * v4 removes), an explicit {@code directoryOffset} so the constant is stated rather than assumed,
 * the timestamp axis promoted to a section with its own offset and length, and {@code blockSizeLog2}
 * so a reader never has to guess the block geometry it is about to index with.
 *
 * <p>§3 records what was deliberately left out and why, and those decisions are load-bearing here:
 * no magic number (the version byte already dispatches), no CRC32C (SSTable compression already
 * checksums, so this class detects structural corruption -- an impossible length, a misaligned
 * offset -- and does not pretend to detect a flipped bit inside a timestamp), no total length
 * ({@code payload.remaining()} is authoritative and is what every bound below is checked against),
 * and no chunk flags byte.
 *
 * <p><b>Validation is total on {@link #read} and on construction.</b> The constructor checks
 * everything expressible without the payload, {@link #read} adds the payload bounds, and the two
 * together mean the encoder cannot build a header the decoder would reject. Nothing in this class
 * allocates from a length read out of the payload -- the header is fixed-size and its fields are
 * only ever compared against {@code payload.remaining()} -- so a corrupt length here throws and
 * cannot reserve memory.
 */
public final class ChunkV4Header
{
    /** §9: the version byte means "the entire layout below is v4". Not a feature flag, not a minor. */
    public static final byte VERSION = 4;

    /** §3 tabulates ten fields ending at offset 39. */
    public static final int HEADER_SIZE = 40;

    /**
     * §7 keeps v3's per-chunk row limit unchanged, so it is referenced rather than restated: the
     * bound exists because a decode materialises row-indexed arrays, and two copies of it would
     * drift the moment one is tuned.
     */
    public static final int MAX_ROWS = ColumnarChunkCodec.MAX_ROWS;

    /** §7: block size 64..32,768 rows, i.e. {@code blockSizeLog2} in 6..15. */
    public static final int MIN_BLOCK_SIZE_LOG2 = 6;
    public static final int MAX_BLOCK_SIZE_LOG2 = 15;

    /**
     * §2: v4.0 writes 1024. The reasons are all arithmetic -- exactly {@code 16w} words at width
     * {@code w} with no tail, divisible by every vector length, presence in exactly two cache
     * lines, and a 24-byte block entry amortised to 0.023 B/row -- so this is a constant the
     * encoder uses, not a tunable.
     */
    public static final int DEFAULT_BLOCK_SIZE_LOG2 = 10;

    /** §7: 65,535 columns, which is also all a u16 can express. */
    public static final int MAX_COLUMNS = 0xFFFF;

    // §3's table, as constants, so the writer, the reader and the peeks cannot drift apart and the
    // "these four are v3's" claim above is checkable by a test rather than by eye.
    public static final int OFFSET_VERSION = 0;
    public static final int OFFSET_ROW_COUNT = 1;
    public static final int OFFSET_FIRST_TIMESTAMP = 5;
    public static final int OFFSET_LAST_TIMESTAMP = 13;
    public static final int OFFSET_BLOCK_SIZE_LOG2 = 21;
    public static final int OFFSET_COLUMN_COUNT = 22;
    public static final int OFFSET_DIRECTORY_OFFSET = 24;
    public static final int OFFSET_DIRECTORY_LEN = 28;
    public static final int OFFSET_TS_SECTION_OFFSET = 32;
    public static final int OFFSET_TS_SECTION_LEN = 36;

    public final int rowCount;
    public final long firstTimestamp;
    public final long lastTimestamp;
    public final int blockSizeLog2;
    public final int columnCount;
    public final int directoryLen;
    public final int tsSectionOffset;
    public final int tsSectionLen;

    /**
     * @param directoryLen    bytes of directory, including its §6 pad to 8
     * @param tsSectionOffset payload-relative offset of the timestamp section, 8-aligned
     * @param tsSectionLen    bytes of timestamp section, including its §6 pad to 8
     */
    public ChunkV4Header(int rowCount,
                         long firstTimestamp,
                         long lastTimestamp,
                         int blockSizeLog2,
                         int columnCount,
                         int directoryLen,
                         int tsSectionOffset,
                         int tsSectionLen)
    {
        checkRowCount(rowCount);
        checkBlockSizeLog2(blockSizeLog2);
        if (columnCount < 0 || columnCount > MAX_COLUMNS)
            throw new IllegalArgumentException("columnCount " + columnCount + " outside 0.." + MAX_COLUMNS);
        // The chunk's declared bounds are the timestamps of its first and last row, so they cannot
        // be inverted, and a one-row chunk's two bounds are one timestamp. Finer disagreement -- a
        // bit flipped inside a timestamp -- is out of scope by §3's decision not to carry a CRC.
        if (firstTimestamp > lastTimestamp)
            throw new IllegalArgumentException("firstTimestamp " + firstTimestamp + " after lastTimestamp " +
                                               lastTimestamp);
        if (rowCount == 1 && firstTimestamp != lastTimestamp)
            throw new IllegalArgumentException("single-row chunk with distinct bounds " + firstTimestamp + " != " +
                                               lastTimestamp);
        checkAligned("directoryLen", directoryLen);
        checkAligned("tsSectionOffset", tsSectionOffset);
        checkAligned("tsSectionLen", tsSectionLen);
        // The directory starts immediately after the header and the timestamp section cannot begin
        // inside it. Where the section ends relative to the column sections is not fixed by §3 --
        // every section carries its own offset precisely so the layout need not be an implied
        // prefix sum -- so nothing stronger is asserted here.
        if (tsSectionOffset < HEADER_SIZE + (long) directoryLen)
            throw new IllegalArgumentException("tsSectionOffset " + tsSectionOffset + " overlaps the directory, " +
                                               "which ends at " + (HEADER_SIZE + (long) directoryLen));

        this.rowCount = rowCount;
        this.firstTimestamp = firstTimestamp;
        this.lastTimestamp = lastTimestamp;
        this.blockSizeLog2 = blockSizeLog2;
        this.columnCount = columnCount;
        this.directoryLen = directoryLen;
        this.tsSectionOffset = tsSectionOffset;
        this.tsSectionLen = tsSectionLen;
    }

    /** §3: always 40, written explicitly and verified on read rather than assumed. */
    public int directoryOffset()
    {
        return HEADER_SIZE;
    }

    /** First payload offset past the directory; where the sections may start. */
    public int sectionsFrom()
    {
        return HEADER_SIZE + directoryLen;
    }

    // -----------------------------------------------------------------------------------------
    // §2 block model -- stated once, here
    // -----------------------------------------------------------------------------------------
    //
    // §2's four formulas are two lines each and that is exactly why they belong in one place: a
    // caller that re-derives blockIndex is a caller that can disagree with the encoder's blockCount
    // by one, and the §2 property the whole format rests on -- that block k of column A covers the
    // same rows as block k of column B, so a block set chosen from A's statistics applies to B
    // untranslated -- is only true while every axis derives its blocks from the same arithmetic.

    /** {@code 1 << blockSizeLog2}. */
    public int blockSize()
    {
        return 1 << blockSizeLog2;
    }

    /** §2: {@code (rowCount + blockSize - 1) >>> blockSizeLog2}. */
    public int blockCount()
    {
        return blockCount(rowCount, blockSizeLog2);
    }

    /** §2: {@code rowIndex >>> blockSizeLog2}. */
    public int blockIndex(int rowIndex)
    {
        checkRowIndex(rowIndex);
        return rowIndex >>> blockSizeLog2;
    }

    /** §2: {@code rowIndex & (blockSize - 1)}, the row's offset inside its block. */
    public int blockOffset(int rowIndex)
    {
        checkRowIndex(rowIndex);
        return rowIndex & (blockSize() - 1);
    }

    /** §2: {@code min(blockSize, rowCount - (k << blockSizeLog2))}; only the last block is short. */
    public int blockRows(int blockIndex)
    {
        return blockRows(rowCount, blockSizeLog2, blockIndex);
    }

    /** §2: {@code (rowCount + blockSize - 1) >>> blockSizeLog2}. */
    public static int blockCount(int rowCount, int blockSizeLog2)
    {
        checkRowCount(rowCount);
        checkBlockSizeLog2(blockSizeLog2);
        // rowCount <= 2^24 and blockSize <= 2^15, so the sum cannot overflow an int; done in long
        // anyway because this expression is the one every other formula here is derived from.
        return (int) (((long) rowCount + (1L << blockSizeLog2) - 1) >>> blockSizeLog2);
    }

    /** §2: {@code rowIndex >>> blockSizeLog2}. */
    public static int blockIndex(int rowIndex, int blockSizeLog2)
    {
        checkBlockSizeLog2(blockSizeLog2);
        if (rowIndex < 0)
            throw new IllegalArgumentException("negative rowIndex " + rowIndex);
        return rowIndex >>> blockSizeLog2;
    }

    /** §2: {@code rowIndex & (blockSize - 1)}. */
    public static int blockOffset(int rowIndex, int blockSizeLog2)
    {
        checkBlockSizeLog2(blockSizeLog2);
        if (rowIndex < 0)
            throw new IllegalArgumentException("negative rowIndex " + rowIndex);
        return rowIndex & ((1 << blockSizeLog2) - 1);
    }

    /**
     * §2: rows covered by block {@code blockIndex}, i.e. {@code blockSize} for every block but the
     * last. Never zero -- {@link BlockPresence} rejects a zero-row block for the same reason, that
     * a block with no rows is a miscomputed index rather than an empty block to tolerate.
     */
    public static int blockRows(int rowCount, int blockSizeLog2, int blockIndex)
    {
        int blocks = blockCount(rowCount, blockSizeLog2);
        if (blockIndex < 0 || blockIndex >= blocks)
            throw new IllegalArgumentException("blockIndex " + blockIndex + " outside 0.." + (blocks - 1));
        long start = (long) blockIndex << blockSizeLog2;
        return (int) Math.min(1L << blockSizeLog2, rowCount - start);
    }

    // -----------------------------------------------------------------------------------------
    // write
    // -----------------------------------------------------------------------------------------

    /** Serialises into a fresh {@link #HEADER_SIZE}-byte array. */
    public byte[] toBytes()
    {
        byte[] out = new byte[HEADER_SIZE];
        write(ByteBuffer.wrap(out));
        return out;
    }

    /**
     * Writes the forty bytes at {@code dst}'s position, advancing it by {@link #HEADER_SIZE} and
     * returning that count.
     *
     * <p>The ten fields tile the range exactly -- 1+4+8+8+1+2+4+4+4+4 = 40 -- so there is no
     * padding in the header and no byte of the destination is left holding whatever the caller's
     * scratch buffer had in it. §5 rule 5 is about padding, but the property it protects is that
     * identical input produces identical bytes, and a header with a gap would break it just as
     * effectively.
     */
    public int write(ByteBuffer dst)
    {
        requireBigEndian(dst);
        if (dst.remaining() < HEADER_SIZE)
            throw new IllegalArgumentException("destination holds " + dst.remaining() + " < header " + HEADER_SIZE);
        int base = dst.position();
        dst.put(base + OFFSET_VERSION, VERSION);
        dst.putInt(base + OFFSET_ROW_COUNT, rowCount);
        dst.putLong(base + OFFSET_FIRST_TIMESTAMP, firstTimestamp);
        dst.putLong(base + OFFSET_LAST_TIMESTAMP, lastTimestamp);
        dst.put(base + OFFSET_BLOCK_SIZE_LOG2, (byte) blockSizeLog2);
        dst.putShort(base + OFFSET_COLUMN_COUNT, (short) columnCount);
        dst.putInt(base + OFFSET_DIRECTORY_OFFSET, HEADER_SIZE);
        dst.putInt(base + OFFSET_DIRECTORY_LEN, directoryLen);
        dst.putInt(base + OFFSET_TS_SECTION_OFFSET, tsSectionOffset);
        dst.putInt(base + OFFSET_TS_SECTION_LEN, tsSectionLen);
        dst.position(base + HEADER_SIZE);
        return HEADER_SIZE;
    }

    // -----------------------------------------------------------------------------------------
    // read
    // -----------------------------------------------------------------------------------------

    /**
     * Parses and fully validates the header of {@code payload}, which must span exactly one chunk
     * from its position to its limit. The buffer is not modified.
     *
     * <p><b>A version byte other than 4 is corruption here, not an unsupported format.</b> That
     * looks like it contradicts §10, which routes a v1/v2/v3 payload to
     * {@link UnsupportedChunkFormatException} and forbids swallowing it -- but that dispatch happens
     * in {@link ChunkCodecs} on the version byte, before any layout is assumed. By the time control
     * reaches this method the payload has already been declared v4; a version byte that then reads
     * as something else means the bytes disagree with themselves, which is exactly what corruption
     * is. Routing it to {@code UnsupportedChunkFormatException} would propagate a corrupt chunk as a
     * cluster-wide format problem and stop the read path instead of skipping one chunk.
     *
     * <p>Every u32 is widened to a long before it is compared to anything, so a length near 2^32
     * fails a bound instead of arriving as a negative int that passes one.
     */
    public static ChunkV4Header read(ByteBuffer payload)
    {
        ByteBuffer buffer = payload.duplicate();
        buffer.order(ByteOrder.BIG_ENDIAN);
        int base = buffer.position();
        long payloadLength = buffer.remaining();
        if (payloadLength < HEADER_SIZE)
            throw new IllegalArgumentException("Corrupt v4 chunk: payload holds " + payloadLength +
                                               " bytes, header needs " + HEADER_SIZE);

        byte version = buffer.get(base + OFFSET_VERSION);
        if (version != VERSION)
            throw new IllegalArgumentException("Corrupt v4 chunk: version byte " + version + ", expected " + VERSION);

        long rowCount = u32(buffer, base + OFFSET_ROW_COUNT);
        long firstTimestamp = buffer.getLong(base + OFFSET_FIRST_TIMESTAMP);
        long lastTimestamp = buffer.getLong(base + OFFSET_LAST_TIMESTAMP);
        int blockSizeLog2 = buffer.get(base + OFFSET_BLOCK_SIZE_LOG2) & 0xFF;
        int columnCount = buffer.getShort(base + OFFSET_COLUMN_COUNT) & 0xFFFF;
        long directoryOffset = u32(buffer, base + OFFSET_DIRECTORY_OFFSET);
        long directoryLen = u32(buffer, base + OFFSET_DIRECTORY_LEN);
        long tsSectionOffset = u32(buffer, base + OFFSET_TS_SECTION_OFFSET);
        long tsSectionLen = u32(buffer, base + OFFSET_TS_SECTION_LEN);

        if (rowCount < 1 || rowCount > MAX_ROWS)
            throw new IllegalArgumentException("Corrupt v4 chunk: rowCount " + rowCount + " outside 1.." + MAX_ROWS);
        if (blockSizeLog2 < MIN_BLOCK_SIZE_LOG2 || blockSizeLog2 > MAX_BLOCK_SIZE_LOG2)
            throw new IllegalArgumentException("Corrupt v4 chunk: blockSizeLog2 " + blockSizeLog2 + " outside " +
                                               MIN_BLOCK_SIZE_LOG2 + ".." + MAX_BLOCK_SIZE_LOG2);
        // §3 states the constant instead of leaving it implied, so it is verified instead of
        // ignored: a reader that skipped this check would happily follow a corrupt offset into the
        // middle of the directory it was supposed to start at.
        if (directoryOffset != HEADER_SIZE)
            throw new IllegalArgumentException("Corrupt v4 chunk: directoryOffset " + directoryOffset +
                                               ", §3 fixes it at " + HEADER_SIZE);
        if (HEADER_SIZE + directoryLen > payloadLength)
            throw new IllegalArgumentException("Corrupt v4 chunk: directory ends at " + (HEADER_SIZE + directoryLen) +
                                               ", payload holds " + payloadLength);
        if (tsSectionOffset + tsSectionLen > payloadLength)
            throw new IllegalArgumentException("Corrupt v4 chunk: timestamp section ends at " +
                                               (tsSectionOffset + tsSectionLen) + ", payload holds " + payloadLength);

        // Both lengths are now known to fit the payload, so the narrowing casts are safe and the
        // constructor's remaining checks (alignment, ordering, directory overlap) run on ints.
        return new ChunkV4Header((int) rowCount, firstTimestamp, lastTimestamp, blockSizeLog2, columnCount,
                                 (int) directoryLen, (int) tsSectionOffset, (int) tsSectionLen);
    }

    // -----------------------------------------------------------------------------------------
    // O(1) peeks (§3)
    // -----------------------------------------------------------------------------------------
    //
    // These check the version byte and the header's own length, and nothing else. That is
    // deliberate on two counts. They must stay usable on a header-only prefix -- a future index or
    // a partially read frame has no sections to validate against -- and they must stay constant
    // time for a reader that asks a chunk for its bounds in order to decide not to read it. The value they return is the header's claim about itself; read() is what validates.

    /** §3, offset 1: the chunk's row count, without touching a section. */
    public static int rowCount(ByteBuffer payload)
    {
        return peek(payload).getInt(payload.position() + OFFSET_ROW_COUNT);
    }

    /** §3, offset 5: the timestamp of row 0, without touching a section. */
    public static long firstTimestamp(ByteBuffer payload)
    {
        return peek(payload).getLong(payload.position() + OFFSET_FIRST_TIMESTAMP);
    }

    /** §3, offset 13: the timestamp of the last row, without touching a section. */
    public static long lastTimestamp(ByteBuffer payload)
    {
        return peek(payload).getLong(payload.position() + OFFSET_LAST_TIMESTAMP);
    }

    private static ByteBuffer peek(ByteBuffer payload)
    {
        ByteBuffer buffer = payload.duplicate();
        buffer.order(ByteOrder.BIG_ENDIAN);
        if (buffer.remaining() < HEADER_SIZE)
            throw new IllegalArgumentException("Corrupt v4 chunk: truncated header, " + buffer.remaining() +
                                               " of " + HEADER_SIZE + " bytes");
        byte version = buffer.get(buffer.position() + OFFSET_VERSION);
        if (version != VERSION)
            throw new IllegalArgumentException("Corrupt v4 chunk: version byte " + version + ", expected " + VERSION);
        return buffer;
    }

    // -----------------------------------------------------------------------------------------
    // argument checks
    // -----------------------------------------------------------------------------------------

    private static void checkRowCount(int rowCount)
    {
        if (rowCount < 1 || rowCount > MAX_ROWS)
            throw new IllegalArgumentException("rowCount " + rowCount + " outside 1.." + MAX_ROWS);
    }

    private static void checkBlockSizeLog2(int blockSizeLog2)
    {
        if (blockSizeLog2 < MIN_BLOCK_SIZE_LOG2 || blockSizeLog2 > MAX_BLOCK_SIZE_LOG2)
            throw new IllegalArgumentException("blockSizeLog2 " + blockSizeLog2 + " outside " + MIN_BLOCK_SIZE_LOG2 +
                                               ".." + MAX_BLOCK_SIZE_LOG2);
    }

    private void checkRowIndex(int rowIndex)
    {
        if (rowIndex < 0 || rowIndex >= rowCount)
            throw new IllegalArgumentException("rowIndex " + rowIndex + " outside 0.." + (rowCount - 1));
    }

    private static void checkAligned(String what, int value)
    {
        // §6 pads every section boundary to 8 so that packed lanes, presence words and fixed-width
        // fields are all whole-getLong reads. An offset that is not a multiple of 8 therefore
        // cannot have been produced by a conforming encoder, which makes this a cheap corruption
        // check as well as an alignment one.
        if (value < 0)
            throw new IllegalArgumentException("negative " + what + ": " + value);
        if ((value & 7) != 0)
            throw new IllegalArgumentException(what + ' ' + value + " is not a multiple of 8 (v4 §6)");
    }

    private static long u32(ByteBuffer buffer, int index)
    {
        return buffer.getInt(index) & 0xFFFFFFFFL;
    }

    private static void requireBigEndian(ByteBuffer buffer)
    {
        if (buffer.order() != ByteOrder.BIG_ENDIAN)
            throw new IllegalArgumentException("v4 header fields are big-endian, buffer order is " + buffer.order());
    }
}
