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
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Pins the forty-byte header of chunk-format-v4 §3 and the row-to-block arithmetic of §2.
 *
 * <p>The golden vector is hand-computed from §3's table and committed as hex, because a round-trip
 * test cannot see a field reorder: swap {@code directoryLen} with {@code tsSectionOffset} and every
 * write-then-read assertion stays green while every existing chunk becomes unreadable.
 */
public class ChunkV4HeaderTest
{
    /**
     * §3's table, filled in by hand. Chosen to exercise the signs and the high words rather than to
     * look tidy: {@code firstTimestamp} is -1 so the i64 is all ones, {@code lastTimestamp} is 2^32
     * so a 32-bit read of it would produce zero, and {@code rowCount} 2050 is the multi-block count
     * §11 asks the layout tests to use (three 1024-row blocks, the last holding 2).
     */
    private static final String GOLDEN_HEX =
        "04" +                  // off  0  version              = 4
        "00000802" +            // off  1  rowCount             = 2050
        "FFFFFFFFFFFFFFFF" +    // off  5  firstTimestamp       = -1
        "0000000100000000" +    // off 13  lastTimestamp        = 4294967296
        "0A" +                  // off 21  blockSizeLog2        = 10 (1024, the v4.0 block)
        "0008" +                // off 22  columnCount          = 8
        "00000028" +            // off 24  directoryOffset      = 40
        "000000A8" +            // off 28  directoryLen         = 168
        "000000D0" +            // off 32  tsSectionOffset      = 208
        "00001000";             // off 36  tsSectionLen         = 4096

    private static final int GOLDEN_PAYLOAD_BYTES = 208 + 4096;

    private static ChunkV4Header golden()
    {
        return new ChunkV4Header(2050, -1L, 4294967296L, 10, 8, 168, 208, 4096);
    }

    // -----------------------------------------------------------------------------------------
    // layout
    // -----------------------------------------------------------------------------------------

    @Test
    public void goldenVectorFromSpecSection3()
    {
        assertEquals(40, GOLDEN_HEX.length() / 2);
        assertArrayEquals(hex(GOLDEN_HEX), golden().toBytes());

        ChunkV4Header read = ChunkV4Header.read(payloadWith(golden(), GOLDEN_PAYLOAD_BYTES));
        assertEquals(2050, read.rowCount);
        assertEquals(-1L, read.firstTimestamp);
        assertEquals(4294967296L, read.lastTimestamp);
        assertEquals(10, read.blockSizeLog2);
        assertEquals(8, read.columnCount);
        assertEquals(40, read.directoryOffset());
        assertEquals(168, read.directoryLen);
        assertEquals(208, read.tsSectionOffset);
        assertEquals(4096, read.tsSectionLen);
    }

    /**
     * The numbers §3 and §5 fix, asserted as numbers. A field reorder that still round-trips fails
     * here, which is the only thing standing between "the tests are green" and "no deployed chunk
     * can be read".
     *
     * <p>Offsets 0, 1, 5 and 13 additionally have to match version 3's header byte for byte, so
     * that {@code rowCount}/{@code firstTimestamp}/{@code lastTimestamp} stay O(1) peeks -- §3's
     * stated requirement, and the reason a reader can decide not to open a chunk for less than the
     * cost of opening it. The peek offsets are re-read here with the same absolute arithmetic
     * {@code ColumnarChunkCodec}'s peeks use ({@code position + 1}, {@code + 5}, {@code + 13}).
     */
    @Test
    public void everyFieldOffsetIsWhereTheSpecSays()
    {
        assertEquals(40, ChunkV4Header.HEADER_SIZE);
        assertEquals(4, ChunkV4Header.VERSION);

        assertEquals(0, ChunkV4Header.OFFSET_VERSION);
        assertEquals(1, ChunkV4Header.OFFSET_ROW_COUNT);
        assertEquals(5, ChunkV4Header.OFFSET_FIRST_TIMESTAMP);
        assertEquals(13, ChunkV4Header.OFFSET_LAST_TIMESTAMP);
        assertEquals(21, ChunkV4Header.OFFSET_BLOCK_SIZE_LOG2);
        assertEquals(22, ChunkV4Header.OFFSET_COLUMN_COUNT);
        assertEquals(24, ChunkV4Header.OFFSET_DIRECTORY_OFFSET);
        assertEquals(28, ChunkV4Header.OFFSET_DIRECTORY_LEN);
        assertEquals(32, ChunkV4Header.OFFSET_TS_SECTION_OFFSET);
        assertEquals(36, ChunkV4Header.OFFSET_TS_SECTION_LEN);

        // §5's three block-entry widths, asserted next to the header offsets because the same
        // reorder-invisible-to-round-trip risk applies and §11 groups them in one test.
        assertEquals(24, ChunkV4BlockTable.ENTRY_SIZE_STAT8);
        assertEquals(16, ChunkV4BlockTable.ENTRY_SIZE_STAT4);
        assertEquals(8, ChunkV4BlockTable.ENTRY_SIZE_STAT0);
        assertEquals(24, ChunkV4BlockTable.entrySize(8));
        assertEquals(16, ChunkV4BlockTable.entrySize(4));
        assertEquals(8, ChunkV4BlockTable.entrySize(0));

        ByteBuffer payload = payloadWith(golden(), GOLDEN_PAYLOAD_BYTES);
        int at = payload.position();
        assertEquals(4, payload.get(at));
        assertEquals(2050, payload.getInt(at + 1));
        assertEquals(-1L, payload.getLong(at + 5));
        assertEquals(4294967296L, payload.getLong(at + 13));
    }

    /**
     * The peeks answer from the first 21 bytes alone. Handed a payload that is nothing but a
     * header -- no directory, no sections, nothing the offsets point at -- they still answer, which
     * is what "never decodes a section" means and what makes a bounds check O(1).
     */
    @Test
    public void peeksAnswerFromAHeaderOnlyPrefix()
    {
        ByteBuffer headerOnly = ByteBuffer.wrap(golden().toBytes());
        assertEquals(2050, ChunkV4Header.rowCount(headerOnly));
        assertEquals(-1L, ChunkV4Header.firstTimestamp(headerOnly));
        assertEquals(4294967296L, ChunkV4Header.lastTimestamp(headerOnly));
        // Untouched: a peek that consumed its argument would break the next caller.
        assertEquals(0, headerOnly.position());
        assertEquals(40, headerOnly.remaining());
    }

    @Test
    public void peeksRespectTheBuffersPosition()
    {
        byte[] framed = new byte[64];
        System.arraycopy(golden().toBytes(), 0, framed, 17, 40);
        // Not sliced: the peeks must read relative to the buffer's position, because a chunk
        // arrives as a region of a larger Cassandra cell buffer and never at position zero.
        ByteBuffer payload = ByteBuffer.wrap(framed, 17, 40);
        assertEquals(2050, ChunkV4Header.rowCount(payload));
        assertEquals(-1L, ChunkV4Header.firstTimestamp(payload));
        assertEquals(4294967296L, ChunkV4Header.lastTimestamp(payload));
    }

    @Test
    public void peeksRejectAWrongVersionAndATruncatedHeader()
    {
        byte[] header = golden().toBytes();
        header[0] = 3;
        ByteBuffer v3 = ByteBuffer.wrap(header);
        assertThatThrownBy(() -> ChunkV4Header.rowCount(v3)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ChunkV4Header.firstTimestamp(v3)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ChunkV4Header.lastTimestamp(v3)).isInstanceOf(IllegalArgumentException.class);

        ByteBuffer short39 = ByteBuffer.wrap(Arrays.copyOf(golden().toBytes(), 39));
        assertThatThrownBy(() -> ChunkV4Header.rowCount(short39)).isInstanceOf(IllegalArgumentException.class);
    }

    // -----------------------------------------------------------------------------------------
    // round trip and determinism
    // -----------------------------------------------------------------------------------------

    @Test
    public void roundTripOverRandomisedHeaders()
    {
        Random random = new Random(4004L);
        for (int trial = 0; trial < 500; trial++)
        {
            ChunkV4Header header = randomHeader(random);
            int payloadLength = header.tsSectionOffset + header.tsSectionLen + 8 * random.nextInt(4);
            ChunkV4Header read = ChunkV4Header.read(payloadWith(header, payloadLength));
            assertEquals(header.rowCount, read.rowCount);
            assertEquals(header.firstTimestamp, read.firstTimestamp);
            assertEquals(header.lastTimestamp, read.lastTimestamp);
            assertEquals(header.blockSizeLog2, read.blockSizeLog2);
            assertEquals(header.columnCount, read.columnCount);
            assertEquals(header.directoryLen, read.directoryLen);
            assertEquals(header.tsSectionOffset, read.tsSectionOffset);
            assertEquals(header.tsSectionLen, read.tsSectionLen);
            assertArrayEquals(header.toBytes(), read.toBytes());
        }
    }

    /**
     * Written twice into a buffer pre-filled with garbage. The header has no padding, so the only
     * way stale bytes could survive is a field the writer forgot -- which a round-trip test cannot
     * see, because the reader would not look at that byte either.
     */
    @Test
    public void encodeTwiceIsByteIdentical()
    {
        Random random = new Random(919L);
        for (int trial = 0; trial < 200; trial++)
        {
            ChunkV4Header header = randomHeader(random);
            assertArrayEquals(intoDirtyBuffer(header, (byte) 0xFF), intoDirtyBuffer(header, (byte) 0x5A));
            assertArrayEquals(header.toBytes(), intoDirtyBuffer(header, (byte) 0xFF));
        }
    }

    // -----------------------------------------------------------------------------------------
    // §2 block model
    // -----------------------------------------------------------------------------------------

    /**
     * §2's four formulas, checked against a full row sweep rather than against themselves. The
     * property that matters is coverage: every row belongs to exactly one block, at the offset the
     * arithmetic claims, and the block sizes sum to {@code rowCount}. An off-by-one in
     * {@code blockCount} is what makes the last two rows of a chunk unreachable.
     */
    @Test
    public void blockArithmeticMatchesSpecSection2()
    {
        int[] rowCounts = { 1, 63, 64, 65, 1023, 1024, 1025, 2050, 32768, 32769, 100_000 };
        for (int blockSizeLog2 = ChunkV4Header.MIN_BLOCK_SIZE_LOG2;
             blockSizeLog2 <= ChunkV4Header.MAX_BLOCK_SIZE_LOG2;
             blockSizeLog2++)
        {
            int blockSize = 1 << blockSizeLog2;
            for (int rowCount : rowCounts)
            {
                int blocks = ChunkV4Header.blockCount(rowCount, blockSizeLog2);
                assertEquals((rowCount + blockSize - 1) / blockSize, blocks);

                int covered = 0;
                for (int k = 0; k < blocks; k++)
                {
                    int rows = ChunkV4Header.blockRows(rowCount, blockSizeLog2, k);
                    assertEquals(Math.min(blockSize, rowCount - k * blockSize), rows);
                    if (rows < 1)
                        fail("block " + k + " of " + rowCount + " rows is empty");
                    covered += rows;
                }
                assertEquals(rowCount, covered);

                for (int row = 0; row < Math.min(rowCount, 5000); row++)
                {
                    int k = ChunkV4Header.blockIndex(row, blockSizeLog2);
                    int offset = ChunkV4Header.blockOffset(row, blockSizeLog2);
                    assertEquals(row, (k << blockSizeLog2) + offset);
                    if (offset >= ChunkV4Header.blockRows(rowCount, blockSizeLog2, k))
                        fail("row " + row + " maps past the end of block " + k);
                }
            }
        }
    }

    @Test
    public void instanceBlockArithmeticAgreesWithTheStaticForm()
    {
        ChunkV4Header header = golden();
        assertEquals(1024, header.blockSize());
        assertEquals(3, header.blockCount());
        assertEquals(1024, header.blockRows(0));
        assertEquals(1024, header.blockRows(1));
        assertEquals(2, header.blockRows(2));
        assertEquals(2, header.blockIndex(2049));
        assertEquals(1, header.blockOffset(2049));
        assertThatThrownBy(() -> header.blockIndex(2050)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> header.blockRows(3)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ChunkV4Header.blockCount(0, 10)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ChunkV4Header.blockCount(1, 5)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ChunkV4Header.blockCount(1, 16)).isInstanceOf(IllegalArgumentException.class);
    }

    // -----------------------------------------------------------------------------------------
    // rejection
    // -----------------------------------------------------------------------------------------

    @Test
    public void constructorRejectsWhatTheReaderWouldReject()
    {
        // rowCount
        assertThatThrownBy(() -> new ChunkV4Header(0, 0, 0, 10, 1, 8, 48, 8))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ChunkV4Header(ChunkV4Header.MAX_ROWS + 1, 0, 1, 10, 1, 8, 48, 8))
            .isInstanceOf(IllegalArgumentException.class);
        // blockSizeLog2 outside §7's 6..15
        assertThatThrownBy(() -> new ChunkV4Header(10, 0, 1, 5, 1, 8, 48, 8))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ChunkV4Header(10, 0, 1, 16, 1, 8, 48, 8))
            .isInstanceOf(IllegalArgumentException.class);
        // inverted bounds, and a one-row chunk whose two bounds differ
        assertThatThrownBy(() -> new ChunkV4Header(10, 5, 4, 10, 1, 8, 48, 8))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ChunkV4Header(1, 4, 5, 10, 1, 8, 48, 8))
            .isInstanceOf(IllegalArgumentException.class);
        // §6 alignment
        assertThatThrownBy(() -> new ChunkV4Header(10, 0, 1, 10, 1, 7, 48, 8))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ChunkV4Header(10, 0, 1, 10, 1, 8, 49, 8))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ChunkV4Header(10, 0, 1, 10, 1, 8, 48, 9))
            .isInstanceOf(IllegalArgumentException.class);
        // the timestamp section may not start inside the directory
        assertThatThrownBy(() -> new ChunkV4Header(10, 0, 1, 10, 1, 16, 48, 8))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ChunkV4Header(10, 0, 1, 10, ChunkV4Header.MAX_COLUMNS + 1, 8, 48, 8))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void readRejectsAWrongVersionAsCorruptionNotAsAnUnsupportedFormat()
    {
        // §10 routes a v1/v2/v3 payload to UnsupportedChunkFormatException, but that dispatch is
        // ChunkCodecs' and happens on the version byte alone. Reaching this parser means the
        // payload was already declared v4, so a byte that then says otherwise is corruption -- and
        // must not be re-raised as a cluster-wide format problem that stops the read path.
        for (int version : new int[]{ 0, 1, 2, 3, 5, 255 })
        {
            byte[] payload = new byte[GOLDEN_PAYLOAD_BYTES];
            System.arraycopy(golden().toBytes(), 0, payload, 0, 40);
            payload[0] = (byte) version;
            assertThatThrownBy(() -> ChunkV4Header.read(ByteBuffer.wrap(payload)))
                .isInstanceOf(IllegalArgumentException.class)
                .isNotInstanceOf(UnsupportedChunkFormatException.class);
        }
    }

    @Test
    public void readRejectsOffsetsPastTheEndOfThePayload()
    {
        ChunkV4Header header = golden();
        // One byte short of what the timestamp section claims.
        assertThatThrownBy(() -> ChunkV4Header.read(payloadWith(header, GOLDEN_PAYLOAD_BYTES - 1)))
            .isInstanceOf(IllegalArgumentException.class);

        byte[] payload = new byte[GOLDEN_PAYLOAD_BYTES];
        System.arraycopy(header.toBytes(), 0, payload, 0, 40);
        // directoryOffset is fixed at 40 by §3 and stated in the bytes, so it is verified.
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        buffer.putInt(ChunkV4Header.OFFSET_DIRECTORY_OFFSET, 48);
        assertThatThrownBy(() -> ChunkV4Header.read(ByteBuffer.wrap(payload)))
            .isInstanceOf(IllegalArgumentException.class);

        // A length near 2^32 read as a signed int would be negative and would pass a naive bound.
        buffer.putInt(ChunkV4Header.OFFSET_DIRECTORY_OFFSET, 40);
        buffer.putInt(ChunkV4Header.OFFSET_DIRECTORY_LEN, 0xFFFFFFF8);
        assertThatThrownBy(() -> ChunkV4Header.read(ByteBuffer.wrap(payload)))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void truncateAtEveryPrefixLength()
    {
        byte[] payload = new byte[GOLDEN_PAYLOAD_BYTES];
        System.arraycopy(golden().toBytes(), 0, payload, 0, 40);
        for (int length = 0; length < GOLDEN_PAYLOAD_BYTES; length++)
        {
            ByteBuffer truncated = ByteBuffer.wrap(Arrays.copyOf(payload, length));
            try
            {
                ChunkV4Header.read(truncated);
                fail("a " + length + "-byte payload parsed as a header claiming " + GOLDEN_PAYLOAD_BYTES);
            }
            catch (IllegalArgumentException expected)
            {
                // the only permitted outcome
            }
        }
        // ... and the full length parses, so the loop above is testing truncation and not a typo.
        ChunkV4Header.read(ByteBuffer.wrap(payload));
    }

    /**
     * §11: no single-bit corruption may escape as an unchecked exception. Nothing here allocates
     * from a payload-supplied length -- the header is fixed size -- so the bound this test defends
     * is the exception type; a {@code BufferUnderflowException} leaking out would be caught by
     * callers as an unexpected failure instead of as a skippable corrupt chunk.
     */
    @Test
    public void singleBitFlipsNeverEscapeAsUncheckedOrOom()
    {
        byte[] payload = new byte[GOLDEN_PAYLOAD_BYTES];
        System.arraycopy(golden().toBytes(), 0, payload, 0, 40);
        int accepted = 0;
        for (int bit = 0; bit < 40 * 8; bit++)
        {
            byte[] flipped = payload.clone();
            flipped[bit >>> 3] ^= (byte) (1 << (bit & 7));
            try
            {
                ChunkV4Header.read(ByteBuffer.wrap(flipped));
                accepted++;
            }
            catch (IllegalArgumentException expected)
            {
                // the only permitted outcome
            }
            catch (RuntimeException unexpected)
            {
                fail("bit " + bit + " escaped as " + unexpected);
            }
        }
        // A sanity floor, not a target: the timestamp fields carry no redundancy (§3 declined a
        // CRC because SSTable compression already checksums), so many flips there are undetectable
        // by construction. If this ever reaches zero, the validation has become a tautology.
        if (accepted == 0)
            fail("every bit flip was rejected, which means the header is not being parsed at all");
    }

    // -----------------------------------------------------------------------------------------
    // helpers
    // -----------------------------------------------------------------------------------------

    private static ChunkV4Header randomHeader(Random random)
    {
        int rowCount = 1 + random.nextInt(200_000);
        // Kept clear of the long ends so the span below cannot overflow into an inverted pair; the
        // sign is still exercised, which is what the epoch-relative timestamps actually need.
        long first = random.nextLong() / 4;
        long last = rowCount == 1 ? first : first + random.nextInt(1_000_000);
        int blockSizeLog2 = ChunkV4Header.MIN_BLOCK_SIZE_LOG2
                            + random.nextInt(ChunkV4Header.MAX_BLOCK_SIZE_LOG2
                                             - ChunkV4Header.MIN_BLOCK_SIZE_LOG2 + 1);
        int columnCount = random.nextInt(16);
        int directoryLen = 8 * random.nextInt(64);
        int tsSectionOffset = ChunkV4Header.HEADER_SIZE + directoryLen + 8 * random.nextInt(4);
        int tsSectionLen = 8 * random.nextInt(512);
        return new ChunkV4Header(rowCount, first, last, blockSizeLog2, columnCount,
                                 directoryLen, tsSectionOffset, tsSectionLen);
    }

    private static ByteBuffer payloadWith(ChunkV4Header header, int payloadLength)
    {
        byte[] payload = new byte[payloadLength];
        header.write(ByteBuffer.wrap(payload));
        return ByteBuffer.wrap(payload);
    }

    private static byte[] intoDirtyBuffer(ChunkV4Header header, byte fill)
    {
        byte[] scratch = new byte[ChunkV4Header.HEADER_SIZE];
        Arrays.fill(scratch, fill);
        ByteBuffer dst = ByteBuffer.wrap(scratch);
        assertEquals(ChunkV4Header.HEADER_SIZE, header.write(dst));
        assertEquals(ChunkV4Header.HEADER_SIZE, dst.position());
        return scratch;
    }

    public static byte[] hex(String hex)
    {
        byte[] out = new byte[hex.length() / 2];
        for (int i = 0; i < out.length; i++)
            out[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        return out;
    }
}
