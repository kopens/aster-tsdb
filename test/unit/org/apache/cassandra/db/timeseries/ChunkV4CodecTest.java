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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.utils.FastByteOperations;

import static org.apache.cassandra.db.timeseries.ChunkV4HeaderTest.hex;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Pins the assembled chunk-format-v4 payload: header, directory, timestamp section and column
 * sections, in the §4.5 order, with the byte determinism §5 requires of the whole thing rather than
 * of one layer at a time.
 *
 * <p>The golden vector is hand-computed from the layers' field lists, not captured from a run. That
 * distinction is the point of having one: a capture pins whatever the encoder does today, including
 * a field written in the wrong place, and would round-trip perfectly inside this build while no
 * other replica reproduced it. Every number in {@link #GOLDEN_HEX} is derived below in a comment
 * from the closed-form size functions the encoders are required to use, so a layout change has to
 * disagree with the arithmetic and not merely with a blob.
 */
public class ChunkV4CodecTest
{
    // -----------------------------------------------------------------------------------------
    // the golden chunk
    // -----------------------------------------------------------------------------------------
    //
    // Four rows, timestamps 1000..1003, blockSizeLog2 6 so blockSize is 64 and there is one block.
    // Five columns, exercising every chunk-level decision this class makes:
    //
    //   c_const  INT64   [7, 7, 7, 7]                 CONSTANT + ALL_PRESENT -> a 0-byte section (§2)
    //   d_dbl    DOUBLE  [2.0, -, 2.0, -]             CONSTANT over the PRESENT values only, so it
    //                                                 still needs a section, whose one block is
    //                                                 CONSTANT (0-byte payload, value in the table)
    //   n_null   INT32   [-, -, -, -]                 ALL_NULL -> a 0-byte section
    //   p_part   INT64   [5, -, 9, -]                 partially null, non-constant, carries stats
    //   t_text   TEXT    [aaaaaaaa, bbbbbbbb, ...]    a dictionary that pays for itself
    //
    // Sizes, all from the closed forms:
    //
    //   directory   32 + 30 + 22 + 40 + 40 = 164, padded to 168
    //   ts section  block table 24 + body (0 presence + 8 width header + 8 lane) = 40
    //               FOR_BITPACK wins: residuals [0,1,2,3] fit 2 bits, so 8 + 8 = 16 against
    //               DELTA's 24 + 0 and RAW's roundUp8(4 * 9) = 40
    //   d_dbl       24 + (8 bitmap + 0) = 32
    //   p_part      24 + (8 bitmap + 8 header + 8 lane) = 48
    //               residuals [0,4] need 3 bits: 8 + 8 = 16, against DELTA's 24 and RAW's 24
    //   t_text      preamble roundUp8(8 + 9 + 9) = 32, table 8, body 8 = 48
    //               with a dictionary: 32 + 8 + packedLength(4,1) = 48
    //               without:            8 + 8 + roundUp8(4 * 9) = 56  -> the dictionary wins
    //
    // Offsets: directory at 40, ts at 40 + 168 = 208, d_dbl at 248, p_part at 280, t_text at 328,
    // and the payload is 376 bytes.

    private static final String GOLDEN_HEADER_HEX =
        "04" +                  // version
        "00000004" +            // rowCount
        "00000000000003E8" +    // firstTimestamp 1000
        "00000000000003EB" +    // lastTimestamp 1003
        "06" +                  // blockSizeLog2
        "0005" +                // columnCount
        "00000028" +            // directoryOffset, §3 fixes it at 40
        "000000A8" +            // directoryLen 168
        "000000D0" +            // tsSectionOffset 208
        "00000028";             // tsSectionLen 40

    private static final String GOLDEN_DIRECTORY_HEX =
        // c_const: INT64, ALL_PRESENT|CONSTANT, statOrder NONE (a constant carries no stats)
        "04" + "05" + "00" + "07" + "635F636F6E7374" +
        "00000000" + "00000000" + "00000000" + "08" + "0000000000000007" +
        // d_dbl: DOUBLE, CONSTANT but not all present, so section 248+32 and one block
        "01" + "04" + "00" + "05" + "645F64626C" +
        "000000F8" + "00000020" + "00000001" + "08" + "4000000000000000" +
        // n_null: INT32, ALL_NULL
        "03" + "02" + "00" + "06" + "6E5F6E756C6C" +
        "00000000" + "00000000" + "00000000" +
        // p_part: INT64, stats under SIGNED_INT, section 280+48
        "04" + "08" + "01" + "06" + "705F70617274" +
        "00000118" + "00000030" + "00000001" +
        "08" + "0000000000000005" + "08" + "0000000000000009" +
        // t_text: TEXT, ALL_PRESENT, stats under BYTES_UNSIGNED, section 328+48
        "06" + "09" + "04" + "06" + "745F74657874" +
        "00000148" + "00000030" + "00000001" +
        "08" + "6161616161616161" + "08" + "6262626262626262" +
        // §6 pad to 8, §5 rule 5 zero-filled
        "00000000";

    private static final String GOLDEN_TS_SECTION_HEX =
        "00000000000003E8" + "00000000000003EB" + "00000018" + "10" + "00" + "0000" +
        "02" + "00000000000000" + "00000000000000E4";

    private static final String GOLDEN_DBL_SECTION_HEX =
        "4000000000000000" + "4000000000000000" + "00000018" + "02" + "02" + "0000" +
        "0000000000000005";

    private static final String GOLDEN_PART_SECTION_HEX =
        "0000000000000005" + "0000000000000009" + "00000018" + "10" + "02" + "0000" +
        "0000000000000005" + "03" + "00000000000000" + "0000000000000020";

    private static final String GOLDEN_TEXT_SECTION_HEX =
        "0002" + "0000" + "00000012" +
        "08" + "6161616161616161" + "08" + "6262626262626262" + "000000000000" +
        "00000028" + "40" + "00" + "0000" +
        "000000000000000A";

    private static final String GOLDEN_HEX = GOLDEN_HEADER_HEX + GOLDEN_DIRECTORY_HEX + GOLDEN_TS_SECTION_HEX
                                             + GOLDEN_DBL_SECTION_HEX + GOLDEN_PART_SECTION_HEX
                                             + GOLDEN_TEXT_SECTION_HEX;

    private static final int GOLDEN_BLOCK_SIZE_LOG2 = 6;
    private static final long[] GOLDEN_TIMESTAMPS = { 1000, 1001, 1002, 1003 };

    private static SortedMap<String, ChunkV4Codec.ColumnInput> goldenColumns()
    {
        SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
        columns.put("c_const", column(ChunkV4Directory.TYPE_INT64, StatOrder.SIGNED_INT,
                                      bytes(7L), bytes(7L), bytes(7L), bytes(7L)));
        columns.put("d_dbl", column(ChunkV4Directory.TYPE_DOUBLE, StatOrder.IEEE754_TOTAL,
                                    bytes(2.0), null, bytes(2.0), null));
        columns.put("n_null", column(ChunkV4Directory.TYPE_INT32, StatOrder.SIGNED_INT,
                                     null, null, null, null));
        columns.put("p_part", column(ChunkV4Directory.TYPE_INT64, StatOrder.SIGNED_INT,
                                     bytes(5L), null, bytes(9L), null));
        columns.put("t_text", column(ChunkV4Directory.TYPE_TEXT, StatOrder.BYTES_UNSIGNED,
                                     bytes("aaaaaaaa"), bytes("bbbbbbbb"), bytes("aaaaaaaa"), bytes("bbbbbbbb")));
        return columns;
    }

    private static ByteBuffer golden()
    {
        return ChunkV4Codec.encode(GOLDEN_TIMESTAMPS, 4, goldenColumns(), GOLDEN_BLOCK_SIZE_LOG2);
    }

    @Test
    public void wholeChunkGoldenVector()
    {
        assertArrayEquals(hex(GOLDEN_HEX), payloadBytes(golden()));
        assertEquals("the hand-computed layout must add up", 376, hex(GOLDEN_HEX).length);
    }

    @Test
    public void goldenVectorDecodesToWhatItWasBuiltFrom()
    {
        ChunkV4Codec.Cursor cursor = ChunkV4Codec.cursor(golden(), null);

        assertTrue(cursor.advance());
        assertEquals(1000, cursor.timestamp());
        assertEquals(7L, asLong(cursor.getBytes("c_const")));
        assertEquals(2.0, asDouble(cursor.getBytes("d_dbl")), 0.0);
        assertTrue(cursor.isNull("n_null"));
        assertEquals(5L, asLong(cursor.getBytes("p_part")));
        assertEquals("aaaaaaaa", asText(cursor.getBytes("t_text")));

        assertTrue(cursor.advance());
        assertEquals(1001, cursor.timestamp());
        assertEquals(7L, asLong(cursor.getBytes("c_const")));
        assertTrue(cursor.isNull("d_dbl"));
        assertTrue(cursor.isNull("p_part"));
        assertEquals("bbbbbbbb", asText(cursor.getBytes("t_text")));

        assertTrue(cursor.advance());
        assertEquals(1002, cursor.timestamp());
        assertEquals(2.0, asDouble(cursor.getBytes("d_dbl")), 0.0);
        assertEquals(9L, asLong(cursor.getBytes("p_part")));

        assertTrue(cursor.advance());
        assertEquals(1003, cursor.timestamp());
        assertTrue(cursor.isNull("d_dbl"));
        assertTrue(cursor.isNull("p_part"));

        assertFalse(cursor.advance());
    }

    /**
     * Field offsets and entry sizes as numbers, over the assembled payload rather than over one
     * layer. A rearranged field passes every round-trip test in the suite -- the encoder and the
     * decoder move together -- and fails here.
     */
    @Test
    public void everyFieldOffsetIsWhereTheSpecSays()
    {
        // §3's table.
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
        assertEquals(40, ChunkV4Header.HEADER_SIZE);
        // §5's three block-entry sizes.
        assertEquals(24, ChunkV4BlockTable.ENTRY_SIZE_STAT8);
        assertEquals(16, ChunkV4BlockTable.ENTRY_SIZE_STAT4);
        assertEquals(8, ChunkV4BlockTable.ENTRY_SIZE_STAT0);

        ByteBuffer payload = golden();
        assertEquals(4, payload.get(ChunkV4Header.OFFSET_VERSION));
        assertEquals(4, payload.getInt(ChunkV4Header.OFFSET_ROW_COUNT));
        assertEquals(1000, payload.getLong(ChunkV4Header.OFFSET_FIRST_TIMESTAMP));
        assertEquals(1003, payload.getLong(ChunkV4Header.OFFSET_LAST_TIMESTAMP));
        assertEquals(6, payload.get(ChunkV4Header.OFFSET_BLOCK_SIZE_LOG2));
        assertEquals(5, payload.getShort(ChunkV4Header.OFFSET_COLUMN_COUNT));
        assertEquals(40, payload.getInt(ChunkV4Header.OFFSET_DIRECTORY_OFFSET));
        assertEquals(168, payload.getInt(ChunkV4Header.OFFSET_DIRECTORY_LEN));
        assertEquals(208, payload.getInt(ChunkV4Header.OFFSET_TS_SECTION_OFFSET));
        assertEquals(40, payload.getInt(ChunkV4Header.OFFSET_TS_SECTION_LEN));

        // The peeks read those fields and nothing else, on the assembled payload.
        assertEquals(4, ChunkV4Codec.rowCount(payload));
        assertEquals(1000, ChunkV4Codec.firstTimestamp(payload));
        assertEquals(1003, ChunkV4Codec.lastTimestamp(payload));

        // §4.5: sections start where the directory says, and the timestamp axis where the header does.
        ChunkV4Codec.Chunk chunk = ChunkV4Codec.open(payload);
        assertEquals(248, chunk.column("d_dbl").sectionOffset);
        assertEquals(280, chunk.column("p_part").sectionOffset);
        assertEquals(328, chunk.column("t_text").sectionOffset);
        // §5 rule 6: a zero-byte section has no offset.
        assertEquals(0, chunk.column("c_const").sectionOffset);
        assertEquals(0, chunk.column("n_null").sectionOffset);
        assertEquals(376, payload.remaining());
    }

    // -----------------------------------------------------------------------------------------
    // determinism (§5)
    // -----------------------------------------------------------------------------------------

    @Test
    public void encodeTwiceIsByteIdentical()
    {
        assertArrayEquals(payloadBytes(golden()), payloadBytes(golden()));

        Random random = new Random(20260803L);
        long[] timestamps = sequentialTimestamps(2050);
        SortedMap<String, ChunkV4Codec.ColumnInput> columns = productionShapedColumns(2050, random);
        byte[] first = payloadBytes(ChunkV4Codec.encode(timestamps, 2050, columns));
        byte[] second = payloadBytes(ChunkV4Codec.encode(timestamps, 2050, columns));
        assertArrayEquals(first, second);
    }

    /**
     * §5 rule 1's named trap. {@code new TreeMap<>(map)} adopts the source map's comparator instead
     * of forcing natural order, so a caller who handed in a reverse-ordered or case-insensitive
     * {@code SortedMap} would get a plausible, stable, wrong directory order -- and a chunk no other
     * replica reproduces byte for byte.
     */
    @Test
    public void encodeIsByteIdenticalUnderShuffledInputOrder()
    {
        byte[] natural = payloadBytes(golden());

        SortedMap<String, ChunkV4Codec.ColumnInput> reversed = new TreeMap<>(Comparator.reverseOrder());
        reversed.putAll(goldenColumns());
        assertArrayEquals(natural,
                          payloadBytes(ChunkV4Codec.encode(GOLDEN_TIMESTAMPS, 4, reversed, GOLDEN_BLOCK_SIZE_LOG2)));

        SortedMap<String, ChunkV4Codec.ColumnInput> caseInsensitive = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        caseInsensitive.putAll(goldenColumns());
        assertArrayEquals(natural,
                          payloadBytes(ChunkV4Codec.encode(GOLDEN_TIMESTAMPS, 4, caseInsensitive,
                                                           GOLDEN_BLOCK_SIZE_LOG2)));

        // And a map with no ordering at all, which is what a caller most often actually has.
        SortedMap<String, ChunkV4Codec.ColumnInput> viaHash = new TreeMap<>();
        List<String> names = new ArrayList<>(goldenColumns().keySet());
        Collections.shuffle(names, new Random(7));
        for (String name : names)
            viaHash.put(name, goldenColumns().get(name));
        assertArrayEquals(natural,
                          payloadBytes(ChunkV4Codec.encode(GOLDEN_TIMESTAMPS, 4, viaHash, GOLDEN_BLOCK_SIZE_LOG2)));
    }

    /**
     * {@code encode(decode(encode(x))) == encode(x)}: the encoder must be deterministic, so that a
     * re-encode of unchanged data yields unchanged bytes (§12).
     */
    @Test
    public void reencodeIsIdempotent()
    {
        assertReencodeIsIdempotent(golden());

        Random random = new Random(4242L);
        for (int blockSizeLog2 : new int[]{ 6, 10, 15 })
        {
            int rows = 2050;
            assertReencodeIsIdempotent(ChunkV4Codec.encode(sequentialTimestamps(rows), rows,
                                                           productionShapedColumns(rows, random), blockSizeLog2));
        }
    }

    private static void assertReencodeIsIdempotent(ByteBuffer payload)
    {
        ChunkV4Codec.Chunk chunk = ChunkV4Codec.open(payload);
        ByteBuffer again = ChunkV4Codec.encode(ChunkV4Codec.toTimestamps(payload),
                                               ChunkV4Codec.rowCount(payload),
                                               ChunkV4Codec.toColumnInputs(payload),
                                               chunk.blockSizeLog2());
        assertArrayEquals(payloadBytes(payload), payloadBytes(again));
    }

    /**
     * §5 rule 5. Every pad byte in the assembled payload is 0x00 and the decoder rejects any other
     * value -- the cheapest tripwire in the format, because an encoder leaking stale scratch bytes
     * into padding leaves every round-trip test green while livelocking the re-encoder.
     */
    @Test
    public void paddingIsZeroEverywhereAndVerifiedOnDecode()
    {
        byte[] payload = payloadBytes(golden());
        // The directory's pad to 8 (164..167) and the text preamble's (328 + 26 .. 328 + 31).
        int[] padBytes = { 204, 205, 206, 207, 354, 355, 356, 357, 358, 359 };
        for (int at : padBytes)
        {
            assertEquals("pad byte " + at, 0, payload[at]);
            byte[] corrupt = payload.clone();
            corrupt[at] = 1;
            assertThatThrownBy(() -> decodeFully(ByteBuffer.wrap(corrupt)))
                .isInstanceOf(IllegalArgumentException.class);
        }
    }

    // -----------------------------------------------------------------------------------------
    // value fidelity
    // -----------------------------------------------------------------------------------------

    /**
     * Every bit pattern a {@code double} column can hold, back exactly. A port of ALP that
     * normalises NaN passes a round trip over ordinary data and silently rewrites a real column;
     * {@code -0.0} flattened to {@code 0.0} is the same failure one exponent quieter.
     */
    @Test
    public void doubleBitPatternsRoundTripExactly()
    {
        long[] patterns = {
            Double.doubleToRawLongBits(0.0),
            Double.doubleToRawLongBits(-0.0),
            Double.doubleToRawLongBits(Double.POSITIVE_INFINITY),
            Double.doubleToRawLongBits(Double.NEGATIVE_INFINITY),
            Double.doubleToRawLongBits(Double.MAX_VALUE),
            Double.doubleToRawLongBits(-Double.MAX_VALUE),
            Double.doubleToRawLongBits(Double.MIN_VALUE),          // smallest subnormal
            Double.doubleToRawLongBits(Double.MIN_NORMAL),
            0x7FF8000000000000L,                                   // canonical quiet NaN
            0x7FF8000000000001L,                                   // NaN with a payload
            0x7FFDEADBEEF00000L,                                   // another payload
            0xFFF8000000000000L,                                   // negative NaN
            0x0000000000000001L,
            0xFFFFFFFFFFFFFFFFL,
            Double.doubleToRawLongBits(1.0),
            Double.doubleToRawLongBits(-1.5),
        };
        int n = patterns.length;
        ByteBuffer[] values = new ByteBuffer[n];
        for (int i = 0; i < n; i++)
            values[i] = ByteBuffer.wrap(rawBytes(patterns[i]));

        SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
        columns.put("v", new ChunkV4Codec.ColumnInput(ChunkV4Directory.TYPE_DOUBLE, StatOrder.IEEE754_TOTAL, values));

        ChunkV4Codec.Cursor cursor = ChunkV4Codec.cursor(ChunkV4Codec.encode(sequentialTimestamps(n), n, columns),
                                                         null);
        for (int i = 0; i < n; i++)
        {
            assertTrue(cursor.advance());
            byte[] decoded = cursor.getByteArray("v");
            assertArrayEquals("pattern " + Long.toHexString(patterns[i]), rawBytes(patterns[i]), decoded);
        }
        assertFalse(cursor.advance());
    }

    /**
     * §12's fourth risk, and the case only a corpus with a zero-length fixed-width value reaches. A
     * zero-length {@code bigint} is legal Cassandra ({@code blobAsBigint(0x)}), so it is not an
     * error: it downgrades the column to {@code OPAQUE} for this chunk -- <b>and the statOrder must
     * follow it to NONE</b>, or the column would keep extrema in an order that no longer describes
     * its values.
     */
    @Test
    public void aZeroLengthFixedWidthValueDowngradesToOpaqueAndDropsTheStatOrder()
    {
        ByteBuffer[] values = { bytes(1L), ByteBuffer.allocate(0), bytes(3L), bytes(4L) };
        SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
        columns.put("v", new ChunkV4Codec.ColumnInput(ChunkV4Directory.TYPE_INT64, StatOrder.SIGNED_INT, values));

        ByteBuffer payload = ChunkV4Codec.encode(sequentialTimestamps(4), 4, columns);
        ChunkV4Codec.Chunk chunk = ChunkV4Codec.open(payload);
        ChunkV4Directory.Entry entry = chunk.column("v");
        assertEquals(ChunkV4Directory.TYPE_OPAQUE, entry.typeCode);
        assertEquals(StatOrder.NONE, entry.statOrder);
        assertFalse("§4: an OPAQUE column carries no statistics", entry.hasStats());
        assertNull(chunk.chunkMin("v"));
        assertEquals(StatOrder.NONE, chunk.blockStatOrder("v"));

        // The values still round-trip byte for byte, empty one included -- a present, zero-length
        // value is not an absent one.
        ChunkV4Codec.Cursor cursor = chunk.cursor(null);
        assertTrue(cursor.advance());
        assertArrayEquals(rawBytes(1L), cursor.getByteArray("v"));
        assertTrue(cursor.advance());
        assertFalse("a zero-length value is present", cursor.isNull("v"));
        assertEquals(0, cursor.getBytes("v").remaining());
        assertTrue(cursor.advance());
        assertArrayEquals(rawBytes(3L), cursor.getByteArray("v"));

        // A boolean byte that is neither 0 nor 1 is the same trigger: v4 gives a boolean exactly one
        // byte form, so a second truthy byte is a value the type code cannot carry.
        SortedMap<String, ChunkV4Codec.ColumnInput> booleans = new TreeMap<>();
        booleans.put("b", new ChunkV4Codec.ColumnInput(ChunkV4Directory.TYPE_BOOLEAN, StatOrder.NONE,
                                                       new ByteBuffer[]{ ByteBuffer.wrap(new byte[]{ 0 }),
                                                                         ByteBuffer.wrap(new byte[]{ 2 }),
                                                                         ByteBuffer.wrap(new byte[]{ 1 }),
                                                                         ByteBuffer.wrap(new byte[]{ 1 }) }));
        ByteBuffer boolPayload = ChunkV4Codec.encode(sequentialTimestamps(4), 4, booleans);
        assertEquals(ChunkV4Directory.TYPE_OPAQUE, ChunkV4Codec.open(boolPayload).column("b").typeCode);
        ChunkV4Codec.Cursor boolCursor = ChunkV4Codec.cursor(boolPayload, null);
        assertTrue(boolCursor.advance());
        assertTrue(boolCursor.advance());
        assertArrayEquals(new byte[]{ 2 }, boolCursor.getByteArray("b"));
    }

    /**
     * Every value a cursor hands out must survive Cassandra's own comparison machinery.
     * {@code FastByteOperations} branches on {@code hasArray()} and, when it is false, dereferences
     * the buffer's {@code address} field -- which a read-only HEAP buffer (what
     * {@code asReadOnlyBuffer()} produces) reports as 0 while also answering false to
     * {@code isDirect()}. The result is a JVM-level SIGSEGV on the first cell comparison, which
     * ordinary reconciliation performs and no round-trip assertion notices, because reading the
     * bytes back works fine.
     */
    @Test
    public void decodedValuesSurviveCassandrasComparisonPaths()
    {
        int n = 8;
        ByteBuffer[] constantDouble = new ByteBuffer[n];   // 0-byte section, directory constant
        ByteBuffer[] varyingDouble = new ByteBuffer[n];    // ALP or RAW block bodies
        ByteBuffer[] text = new ByteBuffer[n];             // dictionary-shared arrays
        ByteBuffer[] opaque = new ByteBuffer[n];
        ByteBuffer[] ints = new ByteBuffer[n];
        ByteBuffer[] longs = new ByteBuffer[n];
        ByteBuffer[] partial = new ByteBuffer[n];          // partially null: presence + lane
        for (int i = 0; i < n; i++)
        {
            constantDouble[i] = bytes(1.5);
            varyingDouble[i] = bytes(i * 1.25);
            text[i] = bytes("label-" + (i % 3));
            opaque[i] = ByteBuffer.wrap(new byte[]{ (byte) i, (byte) (i + 1) });
            ints[i] = bytes(100 + i);
            longs[i] = bytes(1_000_000_000_000L + i);
            partial[i] = i % 2 == 0 ? bytes((long) (500 + i)) : null;
        }
        SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
        columns.put("constant", column(ChunkV4Directory.TYPE_DOUBLE, StatOrder.IEEE754_TOTAL, constantDouble));
        columns.put("varying", column(ChunkV4Directory.TYPE_DOUBLE, StatOrder.IEEE754_TOTAL, varyingDouble));
        columns.put("label", column(ChunkV4Directory.TYPE_TEXT, StatOrder.BYTES_UNSIGNED, text));
        columns.put("blobish", column(ChunkV4Directory.TYPE_OPAQUE, StatOrder.NONE, opaque));
        columns.put("counter32", column(ChunkV4Directory.TYPE_INT32, StatOrder.SIGNED_INT, ints));
        columns.put("counter64", column(ChunkV4Directory.TYPE_INT64, StatOrder.SIGNED_INT, longs));
        columns.put("partial", column(ChunkV4Directory.TYPE_INT64, StatOrder.SIGNED_INT, partial));

        ChunkV4Codec.Cursor cursor = ChunkV4Codec.cursor(ChunkV4Codec.encode(sequentialTimestamps(n), n, columns),
                                                         null);
        assertTrue(cursor.advance());
        assertComparable("constant", cursor.getBytes("constant"), bytes(1.5), bytes(9.0));
        assertComparable("varying", cursor.getBytes("varying"), bytes(0.0), bytes(9.0));
        assertComparable("label", cursor.getBytes("label"), bytes("label-0"), bytes("label-9"));
        assertComparable("blobish", cursor.getBytes("blobish"),
                         ByteBuffer.wrap(new byte[]{ 0, 1 }), ByteBuffer.wrap(new byte[]{ 9, 9 }));
        assertComparable("counter32", cursor.getBytes("counter32"), bytes(100), bytes(999));
        assertComparable("counter64", cursor.getBytes("counter64"),
                         bytes(1_000_000_000_000L), bytes(9_000_000_000_000L));
        assertComparable("partial", cursor.getBytes("partial"), bytes(500L), bytes(9_000L));

        assertTrue(cursor.advance());
        assertComparable("constant", cursor.getBytes("constant"), bytes(1.5), bytes(9.0));
        assertComparable("varying", cursor.getBytes("varying"), bytes(1.25), bytes(9.0));
        assertTrue("the odd rows of a partially-null column are absent", cursor.isNull("partial"));
    }

    // -----------------------------------------------------------------------------------------
    // round trip
    // -----------------------------------------------------------------------------------------

    @Test
    public void roundTripOverProductionShapedChunks()
    {
        Random random = new Random(31337L);
        for (int blockSizeLog2 : new int[]{ 6, 10, 15 })
        {
            for (int rows : new int[]{ 1, 63, 64, 65, 1024, 2050 })
            {
                SortedMap<String, ChunkV4Codec.ColumnInput> columns = productionShapedColumns(rows, random);
                long[] timestamps = sequentialTimestamps(rows);
                ByteBuffer payload = ChunkV4Codec.encode(timestamps, rows, columns, blockSizeLog2);
                assertDecodesTo(payload, timestamps, rows, columns, "rows=" + rows + " log2=" + blockSizeLog2);
            }
        }
    }

    @Test
    public void randomAccessMatchesSequentialScanAndNeverRanksPerRow()
    {
        Random random = new Random(99L);
        int rows = 2050;
        long[] timestamps = sequentialTimestamps(rows);
        ByteBuffer payload = ChunkV4Codec.encode(timestamps, rows, productionShapedColumns(rows, random), 10);
        List<String> names = ChunkV4Codec.open(payload).columnNames();

        // Sequential: a running value index, no rank() at all (BlockPresence.rank's normative note).
        ChunkV4Codec.Chunk sequentialChunk = ChunkV4Codec.open(payload);
        ChunkV4Codec.Cursor sequential = sequentialChunk.cursor(null);
        byte[][][] scanned = new byte[rows][names.size()][];
        for (int row = 0; row < rows; row++)
        {
            assertTrue(sequential.advance());
            assertEquals(timestamps[row], sequential.timestamp());
            for (int c = 0; c < names.size(); c++)
                scanned[row][c] = sequential.getByteArray(names.get(c));
        }
        assertEquals("a forward scan must not call rank() -- it carries a running value index",
                     0, sequential.rankCalls());

        // Random access: rank() is exactly what a discontinuous move is for, and it must land on the
        // same values. An off-by-one here returns another row's value without throwing (§12).
        ChunkV4Codec.Chunk seekChunk = ChunkV4Codec.open(payload);
        ChunkV4Codec.Cursor seeking = seekChunk.cursor(null);
        for (int probe = 0; probe < 400; probe++)
        {
            int row = random.nextInt(rows);
            seeking.seekTo(row);
            assertEquals(timestamps[row], seeking.timestamp());
            for (int c = 0; c < names.size(); c++)
                assertArrayEquals("row " + row + " column " + names.get(c),
                                  scanned[row][c], seeking.getByteArray(names.get(c)));
        }
        assertTrue("a seek must use rank()", seekChunk.rankCalls() > 0);

        // And advancing after a seek resumes the cheap path from wherever it landed.
        seeking.seekTo(0);
        for (int row = 1; row < rows; row++)
        {
            assertTrue(seeking.advance());
            for (int c = 0; c < names.size(); c++)
                assertArrayEquals(scanned[row][c], seeking.getByteArray(names.get(c)));
        }
    }

    // -----------------------------------------------------------------------------------------
    // projection
    // -----------------------------------------------------------------------------------------

    /**
     * §0 keeps v3's "skip an unprojected column" and strengthens it to a jump. Asserted with a byte
     * counter, not with a stopwatch: a full scan touches every byte of the payload exactly once --
     * header, directory, and each section's metadata and bodies, which together are its
     * {@code sectionLen} -- so a projected scan must touch exactly that total minus the sections it
     * skipped. The 0xFF overwrite is the corroborating half: unparseable bytes in a skipped section
     * cannot be parsed, so if the read succeeds they were never read.
     */
    @Test
    public void projectionSkipsUnprojectedSectionsByteForByte()
    {
        ByteBuffer payload = golden();
        int total = payload.remaining();

        ChunkV4Codec.Chunk full = ChunkV4Codec.open(payload);
        ChunkV4Codec.Cursor fullCursor = full.cursor(null);
        while (fullCursor.advance())
            for (String name : full.columnNames())
                fullCursor.getByteArray(name);
        assertEquals("a full scan reads the whole payload and no more", total, full.bytesOpened());

        Set<String> projection = Collections.singleton("p_part");
        int skipped = full.column("d_dbl").sectionLen + full.column("t_text").sectionLen;
        ChunkV4Codec.Chunk projected = ChunkV4Codec.open(payload);
        ChunkV4Codec.Cursor cursor = projected.cursor(projection);
        while (cursor.advance())
            cursor.getByteArray("p_part");
        assertEquals(total - skipped, projected.bytesOpened());
        assertTrue(projected.bytesOpened() < total);

        // The unprojected columns are not merely unread, they are unreadable -- and the projected
        // read still succeeds and still agrees with the unprojected one.
        byte[] scrambled = payloadBytes(payload);
        Arrays.fill(scrambled, 248, 248 + 32, (byte) 0xFF);          // d_dbl's whole section
        Arrays.fill(scrambled, 328, 328 + 48, (byte) 0xFF);          // t_text's whole section
        ChunkV4Codec.Cursor overScrambled = ChunkV4Codec.cursor(ByteBuffer.wrap(scrambled), projection);
        long[] expected = { 5, Long.MIN_VALUE, 9, Long.MIN_VALUE };
        for (int row = 0; row < 4; row++)
        {
            assertTrue(overScrambled.advance());
            assertEquals(GOLDEN_TIMESTAMPS[row], overScrambled.timestamp());
            if (expected[row] == Long.MIN_VALUE)
                assertTrue(overScrambled.isNull("p_part"));
            else
                assertEquals(expected[row], asLong(overScrambled.getBytes("p_part")));
            assertFalse("an unprojected column is not known to the cursor", overScrambled.hasColumn("d_dbl"));
            assertNull(overScrambled.getBytes("t_text"));
        }
        // Reading a scrambled column IS an error, which is what makes the assertion above mean
        // something: the bytes really are unparseable.
        assertThatThrownBy(() -> decodeFully(ByteBuffer.wrap(scrambled)))
            .isInstanceOf(IllegalArgumentException.class);
    }

    // -----------------------------------------------------------------------------------------
    // statistics and pruning (§1, §4)
    // -----------------------------------------------------------------------------------------

    @Test
    public void statsMatchBruteForceUnderDeclaredOrder()
    {
        Random random = new Random(5150L);
        int rows = 2050;
        SortedMap<String, ChunkV4Codec.ColumnInput> columns = productionShapedColumns(rows, random);
        ByteBuffer payload = ChunkV4Codec.encode(sequentialTimestamps(rows), rows, columns, 10);
        ChunkV4Codec.Chunk chunk = ChunkV4Codec.open(payload);

        int withStats = 0;
        for (ChunkV4Directory.Entry entry : chunk.directory().entries())
        {
            if (!entry.hasStats())
                continue;
            withStats++;
            StatOrder order = entry.statOrder;
            byte[] min = null;
            byte[] max = null;
            for (ByteBuffer value : columns.get(entry.name).values)
            {
                if (value == null)
                    continue;
                byte[] bytes = payloadBytes(value);
                if (min == null)
                {
                    min = bytes;
                    max = bytes;
                }
                else
                {
                    if (order.compare(bytes, min) < 0)
                        min = bytes;
                    if (order.compare(bytes, max) > 0)
                        max = bytes;
                }
            }
            assertArrayEquals(entry.name + " min", min, chunk.chunkMin(entry.name));
            assertArrayEquals(entry.name + " max", max, chunk.chunkMax(entry.name));
        }
        assertTrue("the corpus must actually produce statistics", withStats >= 3);
    }

    @Test
    public void constantAndAllNullColumnsCarryNoStats()
    {
        ChunkV4Codec.Chunk chunk = ChunkV4Codec.open(golden());
        for (String name : new String[]{ "c_const", "d_dbl", "n_null" })
        {
            ChunkV4Directory.Entry entry = chunk.column(name);
            assertFalse(name, entry.hasStats());
            assertEquals(name, StatOrder.NONE, entry.statOrder);
            assertNull(name, chunk.chunkMin(name));
            assertNull(name, chunk.chunkMax(name));
        }
        // §7: an extremum over 256 bytes means no statistics at all, because a truncated maximum is
        // not an upper bound -- it compares BELOW the value it was meant to bound.
        int n = 4;
        ByteBuffer[] longText = new ByteBuffer[n];
        for (int i = 0; i < n; i++)
        {
            char[] filler = new char[300 + i];
            Arrays.fill(filler, (char) ('a' + i));
            longText[i] = bytes(new String(filler));
        }
        SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
        columns.put("wide", column(ChunkV4Directory.TYPE_TEXT, StatOrder.BYTES_UNSIGNED, longText));
        ChunkV4Codec.Chunk wide = ChunkV4Codec.open(ChunkV4Codec.encode(sequentialTimestamps(n), n, columns));
        assertFalse(wide.column("wide").hasStats());
        assertEquals(StatOrder.NONE, wide.column("wide").statOrder);
    }

    /**
     * The test §11 says is the only one that catches treating a date as signed or a float as bytes:
     * whenever a statistic says "skip", brute force must agree that no row could have matched. The
     * other direction is deliberately not asserted -- §1 lets pruning be wrong only towards opening
     * something it need not have opened.
     */
    @Test
    public void pruningIsSound()
    {
        Random random = new Random(2718L);
        int rows = 2050;
        SortedMap<String, ChunkV4Codec.ColumnInput> columns = productionShapedColumns(rows, random);
        ByteBuffer payload = ChunkV4Codec.encode(sequentialTimestamps(rows), rows, columns, 10);
        ChunkV4Codec.Chunk chunk = ChunkV4Codec.open(payload);
        int blockSize = 1 << chunk.blockSizeLog2();

        int chunkPrunes = 0;
        int blockPrunes = 0;
        for (ChunkV4Directory.Entry entry : chunk.directory().entries())
        {
            StatOrder order = ChunkV4Codec.canonicalStatOrder(entry.typeCode);
            if (!order.hasOrder())
                continue;
            byte[][] values = new byte[rows][];
            ByteBuffer[] source = columns.get(entry.name).values;
            for (int row = 0; row < rows; row++)
                values[row] = source[row] == null ? null : payloadBytes(source[row]);

            for (int probe = 0; probe < 60; probe++)
            {
                byte[] lo = randomBound(random, values);
                byte[] hi = randomBound(random, values);
                if (lo != null && hi != null && order.compare(lo, hi) > 0)
                {
                    byte[] swap = lo;
                    lo = hi;
                    hi = swap;
                }

                if (!chunk.mayContain(entry.name, lo, hi))
                {
                    chunkPrunes++;
                    for (int row = 0; row < rows; row++)
                        if (matches(order, values[row], lo, hi))
                            fail("chunk pruning dropped a matching row " + row + " of " + entry.name);
                }
                for (int block = 0; block < chunk.blockCount(); block++)
                {
                    if (chunk.blockMayContain(entry.name, block, lo, hi))
                        continue;
                    blockPrunes++;
                    int from = block * blockSize;
                    int to = Math.min(rows, from + blockSize);
                    for (int row = from; row < to; row++)
                        if (matches(order, values[row], lo, hi))
                            fail("block pruning dropped a matching row " + row + " of " + entry.name);
                }
            }
        }
        assertTrue("the corpus must actually exercise pruning at chunk level", chunkPrunes > 0);
        assertTrue("the corpus must actually exercise pruning at block level", blockPrunes > 0);
    }

    @Test
    public void timePruningIsSoundAndCostsNoSection()
    {
        int rows = 2050;
        long[] timestamps = sequentialTimestamps(rows);
        ByteBuffer payload = ChunkV4Codec.encode(timestamps, rows, productionShapedColumns(rows, new Random(1L)), 10);

        // The chunk's own bounds are an O(1) header peek: §3's whole reason for keeping offsets 5
        // and 13 where v3 put them.
        ChunkV4Codec.Chunk chunk = ChunkV4Codec.open(payload);
        long headerOnly = chunk.bytesOpened();
        assertFalse(chunk.mayContainTime(Long.MIN_VALUE, timestamps[0] - 1));
        assertFalse(chunk.mayContainTime(timestamps[rows - 1] + 1, Long.MAX_VALUE));
        assertTrue(chunk.mayContainTime(timestamps[5], timestamps[5]));
        assertEquals("chunk-level time pruning must not open a section", headerOnly, chunk.bytesOpened());

        // Block bounds cost the axis's block table and not one body byte.
        int blockSize = 1 << chunk.blockSizeLog2();
        for (int block = 0; block < chunk.blockCount(); block++)
        {
            int from = block * blockSize;
            int to = Math.min(rows, from + blockSize) - 1;
            assertTrue(chunk.blockMayContainTime(block, timestamps[from], timestamps[to]));
            assertFalse(chunk.blockMayContainTime(block, timestamps[to] + 1, Long.MAX_VALUE));
            assertFalse(chunk.blockMayContainTime(block, Long.MIN_VALUE, timestamps[from] - 1));
        }
        long tsMetadata = ChunkV4BlockTable.tableLength(chunk.blockCount(), 8);
        assertEquals(headerOnly + tsMetadata, chunk.bytesOpened());
    }

    @Test
    public void blockExtremaAreReadableWithoutDecodingABody()
    {
        ChunkV4Codec.Chunk chunk = ChunkV4Codec.open(golden());
        long before = chunk.bytesOpened();
        assertArrayEquals(rawBytes(5L), chunk.blockMin("p_part", 0));
        assertArrayEquals(rawBytes(9L), chunk.blockMax("p_part", 0));
        // The section's block table, and nothing else: 24 bytes for one statWidth-8 entry.
        assertEquals(before + 24, chunk.bytesOpened());

        // §5 statWidth 0: TEXT has no per-block extrema field to read.
        assertNull(chunk.blockMin("t_text", 0));
        // §2's O(1) columns have no block table at all.
        assertNull(chunk.blockMin("c_const", 0));
        assertNull(chunk.blockMin("n_null", 0));
        assertEquals(StatOrder.SIGNED_INT, chunk.blockStatOrder("p_part"));
        assertEquals(StatOrder.NONE, chunk.blockStatOrder("t_text"));
        assertThatThrownBy(() -> chunk.blockMin("p_part", 1)).isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * §4's mapping rule, from the direction that bites. A {@code time} column is eight bytes and so
     * stored as {@code INT64}, whose block extrema are signed -- but {@code TimeType} compares
     * unsigned. Declaring the type's order would put the column's two families of statistics in two
     * different orders with only one declared, so the reduction is to NONE.
     */
    @Test
    public void aTypeWhoseOrderTheCodeCannotExpressDeclaresNone()
    {
        assertEquals(StatOrder.SIGNED_INT,
                     ChunkV4Codec.statOrderFor(ChunkV4Directory.TYPE_INT64, LongType.instance));
        assertEquals(StatOrder.IEEE754_TOTAL,
                     ChunkV4Codec.statOrderFor(ChunkV4Directory.TYPE_DOUBLE, DoubleType.instance));
        assertEquals(StatOrder.BYTES_UNSIGNED,
                     ChunkV4Codec.statOrderFor(ChunkV4Directory.TYPE_TEXT, UTF8Type.instance));
        assertEquals("time is BYTE_ORDER, i.e. unsigned, but is stored as INT64",
                     StatOrder.NONE, ChunkV4Codec.statOrderFor(ChunkV4Directory.TYPE_INT64, TimeType.instance));

        // And declaring it explicitly is refused rather than silently reduced, because at that point
        // the caller has stated something the bytes will not be.
        assertThatThrownBy(() -> new ChunkV4Codec.ColumnInput(ChunkV4Directory.TYPE_INT64, StatOrder.UNSIGNED_INT,
                                                              new ByteBuffer[0]))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ChunkV4Codec.ColumnInput(ChunkV4Directory.TYPE_OPAQUE, StatOrder.BYTES_UNSIGNED,
                                                              new ByteBuffer[0]))
            .isInstanceOf(IllegalArgumentException.class);
    }

    // -----------------------------------------------------------------------------------------
    // robustness (§11)
    // -----------------------------------------------------------------------------------------

    @Test
    public void truncateAtEveryPrefixLength()
    {
        byte[] full = payloadBytes(golden());
        for (int length = 0; length < full.length; length++)
        {
            byte[] truncated = Arrays.copyOf(full, length);
            try
            {
                decodeFully(ByteBuffer.wrap(truncated));
                fail("a " + length + "-byte prefix parsed a " + full.length + "-byte chunk");
            }
            catch (IllegalArgumentException expected)
            {
                // the only permitted outcome
            }
            catch (RuntimeException unexpected)
            {
                fail("prefix " + length + " escaped as " + unexpected);
            }
        }
        decodeFully(ByteBuffer.wrap(full));
    }

    @Test
    public void singleBitFlipsNeverEscapeAsUncheckedOrOom()
    {
        byte[] full = payloadBytes(golden());
        int accepted = 0;
        for (int bit = 0; bit < full.length * 8; bit++)
        {
            byte[] flipped = full.clone();
            flipped[bit >>> 3] ^= (byte) (1 << (bit & 7));
            try
            {
                decodeFully(ByteBuffer.wrap(flipped));
                accepted++;
            }
            catch (IllegalArgumentException expected)
            {
                // the only permitted outcome; UnsupportedChunkFormatException is a subtype and a
                // version byte one flip away from 4 never names a real format anyway
            }
            catch (RuntimeException unexpected)
            {
                fail("bit " + bit + " escaped as " + unexpected);
            }
            catch (OutOfMemoryError oom)
            {
                fail("bit " + bit + " reserved memory from a corrupt length");
            }
        }
        // Extrema, lanes and timestamps carry no redundancy, so some flips are undetectable by
        // design; a zero here would mean nothing is being parsed at all.
        assertTrue("every bit flip was rejected, which means the payload is not being parsed", accepted > 0);
    }

    /**
     * §10: a version byte naming a real-but-removed format is systematic -- it affects every chunk
     * that build wrote -- so it must propagate as {@link UnsupportedChunkFormatException} and never
     * be swallowed as one bad chunk. A byte naming nothing is what a corrupted first byte looks
     * like, and stays a skippable {@link IllegalArgumentException}.
     *
     * <p>When v4 is wired in, {@code ColumnarChunkCodec.VERSION} becomes 4 and
     * {@code ChunkCodecs.unsupportedVersion} has to gain an explicit branch for 3 beside 1 and 2, as
     * §10 requires. This assertion is what makes that step mandatory rather than optional.
     */
    @Test
    public void v1v2v3PayloadsRejectedAsUnsupportedNotCorrupt()
    {
        for (byte version : new byte[]{ 1, 2, 3 })
        {
            byte[] payload = payloadBytes(golden());
            payload[0] = version;
            assertThatThrownBy(() -> ChunkV4Codec.open(ByteBuffer.wrap(payload)))
                .as("version " + version)
                .isInstanceOf(UnsupportedChunkFormatException.class);
            assertThatThrownBy(() -> ChunkV4Codec.cursor(ByteBuffer.wrap(payload), null))
                .as("version " + version + " must not be swallowed by cursor()")
                .isInstanceOf(UnsupportedChunkFormatException.class);
        }
        for (byte version : new byte[]{ 0, 5, 9, (byte) 0xFF })
        {
            byte[] payload = payloadBytes(golden());
            payload[0] = version;
            assertThatThrownBy(() -> ChunkV4Codec.open(ByteBuffer.wrap(payload)))
                .as("version " + version)
                .isInstanceOf(IllegalArgumentException.class)
                .isNotInstanceOf(UnsupportedChunkFormatException.class);
        }
    }

    /**
     * §11: a corrupt length must throw, not reserve memory. The header's {@code rowCount} is the
     * dangerous one because every row-indexed structure downstream is sized from it, so it is bounded
     * against the timestamp block table the header itself declares -- one 24-byte entry per block --
     * which transitively bounds it against the payload.
     */
    @Test
    public void aCorruptRowCountCannotReserveMemory()
    {
        byte[] payload = payloadBytes(golden());
        ByteBuffer.wrap(payload).putInt(ChunkV4Header.OFFSET_ROW_COUNT, 16_000_000);
        assertThatThrownBy(() -> ChunkV4Codec.open(ByteBuffer.wrap(payload)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("tsSectionLen");

        // At the format's own ceiling, and one past it.
        byte[] atMax = payloadBytes(golden());
        ByteBuffer.wrap(atMax).putInt(ChunkV4Header.OFFSET_ROW_COUNT, ChunkV4Header.MAX_ROWS + 1);
        assertThatThrownBy(() -> ChunkV4Codec.open(ByteBuffer.wrap(atMax)))
            .isInstanceOf(IllegalArgumentException.class);

        // directoryLen is load-bearing for every offset after it, so no single-field corruption of it
        // can be self-consistent: 176 makes the timestamp section start inside the directory, 160
        // truncates the last entry, and either way one directory cannot acquire a second byte form
        // (§5) without being rejected.
        for (int badLength : new int[]{ 160, 176 })
        {
            byte[] badDirectory = payloadBytes(golden());
            ByteBuffer.wrap(badDirectory).putInt(ChunkV4Header.OFFSET_DIRECTORY_LEN, badLength);
            assertThatThrownBy(() -> ChunkV4Codec.open(ByteBuffer.wrap(badDirectory)))
                .as("directoryLen " + badLength)
                .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    public void theEncoderRejectsWhatItCannotRepresent()
    {
        SortedMap<String, ChunkV4Codec.ColumnInput> columns = goldenColumns();
        assertThatThrownBy(() -> ChunkV4Codec.encode(GOLDEN_TIMESTAMPS, 0, columns))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ChunkV4Codec.encode(new long[]{ 5, 5, 5, 5 }, 4, columns))
            .as("the header's bounds are only the extrema while the axis is ordered")
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ChunkV4Codec.encode(new long[]{ 4, 3, 2, 1 }, 4, columns))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ChunkV4Codec.encode(GOLDEN_TIMESTAMPS, 8, columns))
            .isInstanceOf(IllegalArgumentException.class);
        // §7 bounds the block size at 64..32,768 rows.
        assertThatThrownBy(() -> ChunkV4Codec.encode(GOLDEN_TIMESTAMPS, 4, columns, 5))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ChunkV4Codec.encode(GOLDEN_TIMESTAMPS, 4, columns, 16))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void aChunkWithNoColumnsIsStillAChunk()
    {
        long[] timestamps = sequentialTimestamps(100);
        ByteBuffer payload = ChunkV4Codec.encode(timestamps, 100, new TreeMap<>(), 6);
        ChunkV4Codec.Chunk chunk = ChunkV4Codec.open(payload);
        assertEquals(0, chunk.directory().size());
        assertEquals(0, chunk.header().directoryLen);
        assertEquals(ChunkV4Header.HEADER_SIZE, chunk.header().tsSectionOffset);
        assertArrayEquals(timestamps, ChunkV4Codec.toTimestamps(payload));
        assertArrayEquals(payloadBytes(payload),
                          payloadBytes(ChunkV4Codec.encode(timestamps, 100, new TreeMap<>(), 6)));
    }

    // -----------------------------------------------------------------------------------------
    // §11: the one test that must exist before v4 is enabled anywhere
    // -----------------------------------------------------------------------------------------

    /** Marks the forked JVM's hash line among whatever else the child happens to print. */
    private static final String JIT_HASH_PREFIX = "chunk-v4-corpus-sha256:";

    /**
     * §11/§12: the format's largest risk is not compression ratio but an encoder whose byte output
     * depends on the JIT tier -- same input, different bytes per node, every chunk reported changed
     * and rewritten on every re-encode cycle, looking like a capacity problem rather than a codec
     * bug -- and it is invisible to every single-JVM test, because one JVM compiles one way. So the
     * same production-shaped corpus is encoded twice: here, under whatever tiers this test run
     * reaches, and in a forked child JVM pinned to {@code -XX:TieredStopAtLevel=1} (C1 only, whose
     * floating-point and intrinsic compilation choices differ from C2's -- exactly where a
     * non-{@code strictfp} ALP search or a {@code Math.pow} would diverge). The SHA-256 of the
     * concatenated payloads must match exactly.
     *
     * <p>The child is this class's own {@code main}, launched with the parent's java binary
     * ({@code java.home}) and classpath ({@code java.class.path}), so the two JVMs run identical
     * bytecode and differ only in compilation tier. Its stdout is drained to EOF before waiting, so
     * the child can never block on a full pipe, and the wait is bounded.
     */
    @Test
    public void encoderIsDeterministicAcrossJitTiers() throws Exception
    {
        String parentHash = encodeCorpusHash();

        String javaBin = Paths.get(CassandraRelevantProperties.JAVA_HOME.getString(), "bin", "java").toString();
        Process child = new ProcessBuilder(javaBin, "-XX:TieredStopAtLevel=1", "-cp",
                                           CassandraRelevantProperties.JAVA_CLASS_PATH.getString(),
                                           ChunkV4CodecTest.class.getName())
                        .redirectErrorStream(true)
                        .start();

        List<String> output = new ArrayList<>();
        String childHash = null;
        try (BufferedReader reader = new BufferedReader(
                 new InputStreamReader(child.getInputStream(), StandardCharsets.UTF_8)))
        {
            String line;
            while ((line = reader.readLine()) != null)
            {
                output.add(line);
                if (line.startsWith(JIT_HASH_PREFIX))
                    childHash = line.substring(JIT_HASH_PREFIX.length());
            }
        }
        if (!child.waitFor(180, TimeUnit.SECONDS))
        {
            child.destroyForcibly();
            fail("tier-1 encoder JVM did not finish in 180s; output so far: " + output);
        }
        assertEquals("tier-1 encoder JVM failed; output: " + output, 0, child.exitValue());
        assertNotNull("tier-1 encoder JVM printed no hash line; output: " + output, childHash);
        assertEquals("same corpus, same build, different JIT tier, DIFFERENT bytes -- every chunk " +
                     "would be rewritten on every re-encode cycle (v4 §5, §12)", parentHash, childHash);
    }

    /** The forked half of {@link #encoderIsDeterministicAcrossJitTiers}. */
    public static void main(String[] args) throws Exception
    {
        System.out.println(JIT_HASH_PREFIX + encodeCorpusHash());
    }

    /**
     * SHA-256 over the concatenated payloads of a deterministic, production-shaped corpus:
     * {@link #productionShapedColumns} (constant, all-null, sparse, OPAQUE-downgraded, dictionary
     * text, boolean, date and random-walk double columns) over row counts that cross block
     * boundaries at both the default block size and near §7's minimum, with irregular timestamp
     * gaps. Bounded by design -- 160 chunks, seconds not minutes -- because it runs in the normal
     * battery, twice per invocation of the test above.
     */
    private static String encodeCorpusHash() throws Exception
    {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        Random random = new Random(20260804L);
        int[] rowCounts = { 4, 13, 64, 100, 257, 1024, 1500, 2050 };
        for (int round = 0; round < 20; round++)
        {
            for (int rows : rowCounts)
            {
                long ts = 1_700_000_000_000L + random.nextInt(1_000_000);
                long[] timestamps = new long[rows];
                for (int i = 0; i < rows; i++)
                {
                    ts += 1 + random.nextInt(5_000);
                    timestamps[i] = ts;
                }
                int blockSizeLog2 = round % 3 == 0 ? 6 : ChunkV4Header.DEFAULT_BLOCK_SIZE_LOG2;
                ByteBuffer payload = ChunkV4Codec.encode(timestamps, rows,
                                                         productionShapedColumns(rows, random), blockSizeLog2);
                digest.update(payload.duplicate());
            }
        }
        StringBuilder hex = new StringBuilder();
        for (byte b : digest.digest())
            hex.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
        return hex.toString();
    }

    // -----------------------------------------------------------------------------------------
    // helpers
    // -----------------------------------------------------------------------------------------

    /**
     * Opens a payload and reads every value of every column: the shape a robustness test needs,
     * because {@link ChunkV4Codec#open} deliberately parses only the header and directory and a
     * corruption further in would otherwise go unnoticed.
     */
    private static void decodeFully(ByteBuffer payload)
    {
        ChunkV4Codec.Chunk chunk = ChunkV4Codec.open(payload);
        List<String> names = chunk.columnNames();
        ChunkV4Codec.Cursor cursor = chunk.cursor(null);
        int rows = 0;
        while (cursor.advance())
        {
            cursor.timestamp();
            for (String name : names)
                cursor.getByteArray(name);
            rows++;
        }
        if (rows != chunk.rowCount())
            throw new IllegalArgumentException("Corrupt v4 chunk: walked " + rows + " of " + chunk.rowCount());
    }

    private static void assertDecodesTo(ByteBuffer payload, long[] timestamps, int rows,
                                        SortedMap<String, ChunkV4Codec.ColumnInput> columns, String what)
    {
        ChunkV4Codec.Chunk chunk = ChunkV4Codec.open(payload);
        assertEquals(what, rows, chunk.rowCount());
        assertEquals(what, columns.size(), chunk.directory().size());
        assertEquals(what, new TreeSet<>(columns.keySet()), new TreeSet<>(chunk.columnNames()));

        ChunkV4Codec.Cursor cursor = chunk.cursor(null);
        for (int row = 0; row < rows; row++)
        {
            assertTrue(what + " row " + row, cursor.advance());
            assertEquals(what + " timestamp " + row, timestamps[row], cursor.timestamp());
            for (String name : columns.keySet())
            {
                ByteBuffer expected = columns.get(name).values[row];
                byte[] actual = cursor.getByteArray(name);
                if (expected == null)
                {
                    assertTrue(what + ' ' + name + " row " + row, cursor.isNull(name));
                    assertNull(what + ' ' + name + " row " + row, actual);
                }
                else
                {
                    assertFalse(what + ' ' + name + " row " + row, cursor.isNull(name));
                    assertArrayEquals(what + ' ' + name + " row " + row, payloadBytes(expected), actual);
                }
            }
        }
        assertFalse(what, cursor.advance());
    }

    /**
     * The production shape from §8, plus the cases that only appear in a corpus built to contain
     * them: a constant column, an all-null one, a partially-null one, a column whose values are one
     * width short of its type code, and text both dictionary-friendly and not.
     */
    private static SortedMap<String, ChunkV4Codec.ColumnInput> productionShapedColumns(int rows, Random random)
    {
        ByteBuffer[] latency = new ByteBuffer[rows];
        ByteBuffer[] value = new ByteBuffer[rows];
        ByteBuffer[] tag = new ByteBuffer[rows];
        ByteBuffer[] blob = new ByteBuffer[rows];
        ByteBuffer[] flag = new ByteBuffer[rows];
        ByteBuffer[] count32 = new ByteBuffer[rows];
        ByteBuffer[] day = new ByteBuffer[rows];
        ByteBuffer[] constant = new ByteBuffer[rows];
        ByteBuffer[] sparse = new ByteBuffer[rows];
        ByteBuffer[] empty = new ByteBuffer[rows];
        ByteBuffer[] ragged = new ByteBuffer[rows];

        long walk = 1_000_000L;
        for (int row = 0; row < rows; row++)
        {
            walk += random.nextInt(200) - 100;
            latency[row] = bytes(walk);
            value[row] = bytes(random.nextDouble() * 100.0);
            tag[row] = bytes("host-" + (row % 7));
            blob[row] = ByteBuffer.wrap(new byte[]{ (byte) row, (byte) (row >> 8), (byte) random.nextInt(4) });
            flag[row] = ByteBuffer.wrap(new byte[]{ (byte) (row % 3 == 0 ? 1 : 0) });
            count32[row] = bytes(row * 3);
            // DATE32 is an unsigned day count whose serializer shifts by 2^31, so the epoch is
            // 0x80000000 and unsigned byte order is chronological order.
            day[row] = bytes((int) (0x80000000L + row));
            constant[row] = bytes(42L);
            sparse[row] = row % 17 == 0 ? bytes((long) row) : null;
            // A column whose values do not all fit its declared width: §12's OPAQUE downgrade.
            ragged[row] = row == rows / 2 ? ByteBuffer.allocate(0) : bytes((long) row);
        }

        SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
        columns.put("latency", column(ChunkV4Directory.TYPE_INT64, StatOrder.SIGNED_INT, latency));
        columns.put("value", column(ChunkV4Directory.TYPE_DOUBLE, StatOrder.IEEE754_TOTAL, value));
        columns.put("tag", column(ChunkV4Directory.TYPE_TEXT, StatOrder.BYTES_UNSIGNED, tag));
        columns.put("blob", column(ChunkV4Directory.TYPE_OPAQUE, StatOrder.NONE, blob));
        columns.put("flag", column(ChunkV4Directory.TYPE_BOOLEAN, StatOrder.NONE, flag));
        columns.put("count32", column(ChunkV4Directory.TYPE_INT32, StatOrder.SIGNED_INT, count32));
        columns.put("day", column(ChunkV4Directory.TYPE_DATE32, StatOrder.UNSIGNED_INT, day));
        columns.put("constant", column(ChunkV4Directory.TYPE_INT64, StatOrder.SIGNED_INT, constant));
        columns.put("sparse", column(ChunkV4Directory.TYPE_INT64, StatOrder.SIGNED_INT, sparse));
        columns.put("absent", column(ChunkV4Directory.TYPE_INT32, StatOrder.SIGNED_INT, empty));
        columns.put("ragged", column(ChunkV4Directory.TYPE_INT64, StatOrder.SIGNED_INT, ragged));
        return columns;
    }

    private static byte[] randomBound(Random random, byte[][] values)
    {
        if (random.nextInt(6) == 0)
            return null;
        for (int attempt = 0; attempt < 8; attempt++)
        {
            byte[] candidate = values[random.nextInt(values.length)];
            if (candidate != null)
                return candidate;
        }
        return null;
    }

    private static boolean matches(StatOrder order, byte[] value, byte[] lo, byte[] hi)
    {
        if (value == null)
            return false;
        if (lo != null && order.compare(value, lo) < 0)
            return false;
        return hi == null || order.compare(value, hi) <= 0;
    }

    private static void assertComparable(String column, ByteBuffer decoded, ByteBuffer same, ByteBuffer greater)
    {
        assertNotNull(column, decoded);
        // The structural invariant the fast path depends on. Asserted first so a regression names
        // the cause rather than crashing the JVM on the next line.
        assertTrue(column + ": a decoded value must be array-backed for FastByteOperations' fast path",
                   decoded.hasArray());
        assertFalse(column + ": a read-only heap buffer reports hasArray() == false", decoded.isReadOnly());

        assertEquals(column + ": must compare equal to its own bytes", 0,
                     FastByteOperations.compareUnsigned(decoded, same));
        assertEquals(column + ": must compare equal in the other direction", 0,
                     FastByteOperations.compareUnsigned(same, decoded));
        assertTrue(column + ": must sort before a larger value",
                   FastByteOperations.compareUnsigned(decoded, greater) < 0);
        assertTrue(column + ": a larger value must sort after it",
                   FastByteOperations.compareUnsigned(greater, decoded) > 0);
        assertEquals(column + ": ValueAccessor must agree", 0,
                     ValueAccessor.compare(decoded, ByteBufferAccessor.instance, same, ByteBufferAccessor.instance));
    }

    private static ChunkV4Codec.ColumnInput column(int typeCode, StatOrder statOrder, ByteBuffer... values)
    {
        return new ChunkV4Codec.ColumnInput(typeCode, statOrder, values);
    }

    private static long[] sequentialTimestamps(int n)
    {
        long[] timestamps = new long[n];
        for (int i = 0; i < n; i++)
            timestamps[i] = 1_700_000_000_000L + i * 1000L;
        return timestamps;
    }

    private static byte[] payloadBytes(ByteBuffer buffer)
    {
        byte[] out = new byte[buffer.remaining()];
        buffer.duplicate().get(out);
        return out;
    }

    private static byte[] rawBytes(long value)
    {
        byte[] out = new byte[8];
        for (int i = 0; i < 8; i++)
            out[i] = (byte) (value >>> (8 * (7 - i)));
        return out;
    }

    private static ByteBuffer bytes(long value)
    {
        return ByteBuffer.wrap(rawBytes(value));
    }

    private static ByteBuffer bytes(int value)
    {
        return ByteBuffer.wrap(new byte[]{ (byte) (value >>> 24), (byte) (value >>> 16),
                                           (byte) (value >>> 8), (byte) value });
    }

    private static ByteBuffer bytes(double value)
    {
        return ByteBuffer.wrap(rawBytes(Double.doubleToRawLongBits(value)));
    }

    private static ByteBuffer bytes(String value)
    {
        return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
    }

    private static long asLong(ByteBuffer value)
    {
        assertEquals(8, value.remaining());
        return value.duplicate().getLong();
    }

    private static double asDouble(ByteBuffer value)
    {
        return Double.longBitsToDouble(asLong(value));
    }

    private static String asText(ByteBuffer value)
    {
        return new String(payloadBytes(value), StandardCharsets.UTF_8);
    }
}
