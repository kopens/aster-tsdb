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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Test;

import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.utils.FastByteOperations;

import static org.apache.cassandra.db.timeseries.ChunkV4HeaderTest.hex;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link ColumnarChunkCodec} <em>entry point</em> against the chunk format it now routes to (v4, {@link ChunkV4Codec}). These are the
 * codec's cross-version CONTRACTS, deliberately re-proven through the facade rather than through
 * {@code ChunkV4Codec} directly, so a routing regression fails here even while the v4 tests stay
 * green: round-trip fidelity for every type-code family, O(1) constant/all-null columns,
 * projection skipping unread sections, encode determinism (including the caller-comparator trap),
 * NaN-payload bit exactness, {@code hasArray()} on every decoded buffer, corruption as
 * {@link IllegalArgumentException}, and removed formats -- now including v3 itself -- as
 * {@link UnsupportedChunkFormatException}.
 * <p>
 * The v3-layout-specific tests this file used to hold (golden v3 byte surgery, the 256-entry
 * dictionary threshold, the removed gorilla/chimp inner type codes) died with the v3 read path;
 * their v4 equivalents live in {@link ChunkV4CodecTest} and the per-layer v4 suites. The one v3
 * artifact kept is {@link #GOLDEN_V3_HEX}: a byte-exact v3 payload that must now be rejected as an
 * unsupported <em>format</em>, never as corruption.
 */
public class ColumnarChunkCodecTest
{
    /**
     * A byte-exact v3 chunk, hand-assembled from the retired v3 layout (25-byte header, directory,
     * raw first timestamp + empty delta-of-delta stream): one row at t=1000, one ALL_PRESENT
     * CONSTANT {@code int} column "v" = 7. Kept as a golden vector so the rejection test below
     * proves a <em>well-formed</em> v3 payload is refused by version dispatch, not by tripping over
     * its bytes.
     */
    private static final String GOLDEN_V3_HEX =
        "03" +                  // version 3 -- the removed columnar format
        "00000001" +            // rowCount 1
        "00000000000003E8" +    // firstTimestamp 1000
        "00000000000003E8" +    // lastTimestamp 1000
        "0001" +                // columnCount
        "000A" +                // dirSize 10
        "03" + "05" + "01" + "76" + "00" + "04" + "00000007" +   // "v": INT32, ALL_PRESENT|CONSTANT, const 7
        "00000000000003E8";     // timestamp section: first value raw, zero DoD bits

    @Test
    public void roundtripAllTypes()
    {
        int n = 200;
        long[] ts = sequentialTimestamps(n);

        double[] walk = new double[n];
        double w = 50.0;
        Random random = new Random(11);
        for (int i = 0; i < n; i++)
        {
            w += (random.nextInt(3) - 1) * 0.1;
            walk[i] = Math.round(w * 10.0) / 10.0;
        }

        String[] words = { "alpha", "beta", "gamma", "delta" };
        ByteBuffer[] doubleValues = new ByteBuffer[n];
        ByteBuffer[] boolValues = new ByteBuffer[n];
        ByteBuffer[] int32Values = new ByteBuffer[n];
        ByteBuffer[] int64Values = new ByteBuffer[n];
        ByteBuffer[] dateValues = new ByteBuffer[n];
        ByteBuffer[] textValues = new ByteBuffer[n];
        ByteBuffer[] opaqueValues = new ByteBuffer[n];
        for (int i = 0; i < n; i++)
        {
            doubleValues[i] = bytesOf(walk[i]);
            boolValues[i] = bytesOf(i % 3 == 0);
            int32Values[i] = bytesOf(i * 7 - 500);
            int64Values[i] = bytesOf((long) i * 1_000_000_007L - 12345L);
            // date serializes as an unsigned day count around the 2^31 epoch
            dateValues[i] = bytesOf((int) (0x80000000L + i));
            textValues[i] = bytesOf(words[i % words.length]);
            opaqueValues[i] = bytesOf(new byte[]{ (byte) i, (byte) (i >> 8), 0x42 });
        }

        SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
        columns.put("c_double", input(ChunkV4Directory.TYPE_DOUBLE, doubleValues));
        columns.put("c_bool", input(ChunkV4Directory.TYPE_BOOLEAN, boolValues));
        columns.put("c_int32", input(ChunkV4Directory.TYPE_INT32, int32Values));
        columns.put("c_int64", input(ChunkV4Directory.TYPE_INT64, int64Values));
        columns.put("c_date", input(ChunkV4Directory.TYPE_DATE32, dateValues));
        columns.put("c_text", input(ChunkV4Directory.TYPE_TEXT, textValues));
        columns.put("c_opaque", input(ChunkV4Directory.TYPE_OPAQUE, opaqueValues));

        ByteBuffer payload = ColumnarChunkCodec.encode(ts, n, columns);
        assertEquals(ColumnarChunkCodec.VERSION, payload.get(payload.position()));
        assertEquals(n, ColumnarChunkCodec.rowCount(payload));
        assertEquals(ts[0], ColumnarChunkCodec.firstTimestamp(payload));
        assertEquals(ts[n - 1], ColumnarChunkCodec.lastTimestamp(payload));

        ColumnarCursor cursor = ColumnarChunkCodec.cursor(payload, null);
        assertEquals(columns.keySet(), cursor.columns());
        for (int i = 0; i < n; i++)
        {
            assertTrue(cursor.advance());
            assertEquals(ts[i], cursor.timestamp());
            assertEquals("row " + i, walk[i], asDouble(cursor.getBytes("c_double")), 0.0);
            assertEquals("row " + i, i % 3 == 0, asBool(cursor.getBytes("c_bool")));
            assertEquals("row " + i, i * 7 - 500, asInt(cursor.getBytes("c_int32")));
            assertEquals("row " + i, (long) i * 1_000_000_007L - 12345L, asLong(cursor.getBytes("c_int64")));
            assertEquals("row " + i, (int) (0x80000000L + i), asInt(cursor.getBytes("c_date")));
            assertEquals("row " + i, words[i % words.length], asText(cursor.getBytes("c_text")));
            assertArrayEquals("row " + i, new byte[]{ (byte) i, (byte) (i >> 8), 0x42 }, asBytes(cursor.getBytes("c_opaque")));
        }
        assertFalse(cursor.advance());
    }

    @Test
    public void constantColumnCostsConstantBytes()
    {
        int constantSmall = encodeIntColumnSize(100, true);
        int constantLarge = encodeIntColumnSize(10_000, true);
        int variableSmall = encodeIntColumnSize(100, false);
        int variableLarge = encodeIntColumnSize(10_000, false);

        int constantGrowth = constantLarge - constantSmall;
        int variableGrowth = variableLarge - variableSmall;

        // both payloads pay the same timestamp-axis cost for the extra rows; the constant column
        // itself contributes ~0 additional bytes (a 0-byte section in every chunk size), while the
        // variable column's bit-packed blocks cost real bytes per row, so growth must differ widely
        assertTrue("constant grew " + constantGrowth + ", variable grew " + variableGrowth,
                   constantGrowth < variableGrowth / 4);
        assertTrue("constant payload size grew too much going 100 -> 10000 rows: " +
                   constantSmall + " -> " + constantLarge, constantLarge < constantSmall + 3000);
    }

    private static int encodeIntColumnSize(int n, boolean constant)
    {
        long[] ts = sequentialTimestamps(n);
        ByteBuffer[] values = new ByteBuffer[n];
        Random random = new Random(3);
        for (int i = 0; i < n; i++)
            values[i] = bytesOf(constant ? 192 : random.nextInt(1_000_000));
        SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
        columns.put("quality", input(ChunkV4Directory.TYPE_INT32, values));
        return ColumnarChunkCodec.encode(ts, n, columns).remaining();
    }

    @Test
    public void allNullColumn()
    {
        int n = 50;
        long[] ts = sequentialTimestamps(n);
        ByteBuffer[] values = new ByteBuffer[n];   // every entry null
        SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
        columns.put("maybe_col", input(ChunkV4Directory.TYPE_DOUBLE, values));

        ByteBuffer payload = ColumnarChunkCodec.encode(ts, n, columns);
        ColumnarCursor cursor = ColumnarChunkCodec.cursor(payload, null);
        for (int i = 0; i < n; i++)
        {
            assertTrue(cursor.advance());
            assertTrue(cursor.hasColumn("maybe_col"));
            assertTrue(cursor.isNull("maybe_col"));
            assertNull(cursor.getBytes("maybe_col"));
        }
    }

    @Test
    public void mixedNullsRoundtrip()
    {
        int n = 40;
        long[] ts = sequentialTimestamps(n);
        ByteBuffer[] values = new ByteBuffer[n];
        for (int i = 0; i < n; i++)
            values[i] = (i % 4 == 0) ? null : bytesOf(1000 + i);
        SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
        columns.put("latency", input(ChunkV4Directory.TYPE_INT32, values));

        ByteBuffer payload = ColumnarChunkCodec.encode(ts, n, columns);
        ColumnarCursor cursor = ColumnarChunkCodec.cursor(payload, null);
        for (int i = 0; i < n; i++)
        {
            assertTrue(cursor.advance());
            if (i % 4 == 0)
            {
                assertTrue("row " + i, cursor.isNull("latency"));
                assertNull("row " + i, cursor.getBytes("latency"));
            }
            else
            {
                assertFalse("row " + i, cursor.isNull("latency"));
                assertEquals("row " + i, 1000 + i, asInt(cursor.getBytes("latency")));
            }
        }
    }

    /**
     * Text must round-trip whatever the encoder's dictionary-vs-raw decision is at each
     * cardinality. (The exact crossover is a v4 per-block argmin, not v3's 256-entry cliff; which
     * side wins is pinned by the v4 layout tests, and here only fidelity matters.)
     */
    @Test
    public void textRoundTripsAcrossDictionaryAndRawShapes()
    {
        assertTextRoundtrip(200);
        assertTextRoundtrip(256);
        assertTextRoundtrip(257);
        assertTextRoundtrip(500);
    }

    private static void assertTextRoundtrip(int distinctCount)
    {
        int n = distinctCount;   // one row per distinct value keeps this simple and deterministic
        long[] ts = sequentialTimestamps(n);
        String[] expected = new String[n];
        ByteBuffer[] values = new ByteBuffer[n];
        for (int i = 0; i < n; i++)
        {
            expected[i] = "value-" + i;
            values[i] = bytesOf(expected[i]);
        }
        SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
        columns.put("tag", input(ChunkV4Directory.TYPE_TEXT, values));

        ByteBuffer payload = ColumnarChunkCodec.encode(ts, n, columns);
        ColumnarCursor cursor = ColumnarChunkCodec.cursor(payload, null);
        for (int i = 0; i < n; i++)
        {
            assertTrue(cursor.advance());
            assertEquals("distinctCount=" + distinctCount + " row=" + i, expected[i], asText(cursor.getBytes("tag")));
        }
    }

    @Test
    public void lowCardinalityTextIsMuchSmallerThanHighCardinality()
    {
        int n = 5000;
        long[] ts = sequentialTimestamps(n);

        ByteBuffer[] fewDistinct = new ByteBuffer[n];    // 10 distinct values repeated -> dictionary codes
        ByteBuffer[] manyDistinct = new ByteBuffer[n];   // all unique -> the bytes must be carried
        for (int i = 0; i < n; i++)
        {
            fewDistinct[i] = bytesOf("tag-" + (i % 10));
            manyDistinct[i] = bytesOf("unique-value-number-" + i);
        }

        SortedMap<String, ChunkV4Codec.ColumnInput> few = new TreeMap<>();
        few.put("tag", input(ChunkV4Directory.TYPE_TEXT, fewDistinct));
        SortedMap<String, ChunkV4Codec.ColumnInput> many = new TreeMap<>();
        many.put("tag", input(ChunkV4Directory.TYPE_TEXT, manyDistinct));

        int dictSize = ColumnarChunkCodec.encode(ts, n, few).remaining();
        int rawSize = ColumnarChunkCodec.encode(ts, n, many).remaining();

        assertTrue("dictionary encoding should be far smaller: dict=" + dictSize + " raw=" + rawSize,
                   dictSize < rawSize / 5);
    }

    @Test
    public void opaqueRoundtripFrozenMapLikePayload()
    {
        int n = 30;
        long[] ts = sequentialTimestamps(n);
        ByteBuffer[] values = new ByteBuffer[n];
        byte[][] expected = new byte[n][];
        Random random = new Random(5);
        for (int i = 0; i < n; i++)
        {
            // stand-in for a serialized frozen<map<text,text>> cell: an opaque, variable-length blob
            byte[] blob = new byte[8 + (i % 5) * 3];
            random.nextBytes(blob);
            blob[0] = (byte) i;
            expected[i] = blob;
            values[i] = bytesOf(blob);
        }
        SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
        columns.put("attribute", input(ChunkV4Directory.TYPE_OPAQUE, values));

        ByteBuffer payload = ColumnarChunkCodec.encode(ts, n, columns);
        ColumnarCursor cursor = ColumnarChunkCodec.cursor(payload, null);
        for (int i = 0; i < n; i++)
        {
            assertTrue(cursor.advance());
            assertArrayEquals("row " + i, expected[i], asBytes(cursor.getBytes("attribute")));
        }
    }

    @Test
    public void projectionSkipsUndecodedSectionBytes()
    {
        int n = 500;
        long[] ts = sequentialTimestamps(n);

        ByteBuffer[] keepValues = new ByteBuffer[n];
        ByteBuffer[] skipValues = new ByteBuffer[n];
        for (int i = 0; i < n; i++)
        {
            keepValues[i] = bytesOf(i);
            skipValues[i] = bytesOf("skip-me-" + i);   // 500 distinct values -> real per-row bytes
        }

        SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
        columns.put("aaa_keep", input(ChunkV4Directory.TYPE_INT32, keepValues));
        // "zzz_skip" sorts last among the two columns, so its section is the payload's tail
        columns.put("zzz_skip", input(ChunkV4Directory.TYPE_TEXT, skipValues));

        ByteBuffer clean = ColumnarChunkCodec.encode(ts, n, columns);

        // sanity: uncorrupted, projecting both columns round-trips both
        Set<String> both = new HashSet<>(Arrays.asList("aaa_keep", "zzz_skip"));
        ColumnarCursor sanity = ColumnarChunkCodec.cursor(clean.duplicate(), both);
        for (int i = 0; i < n; i++)
        {
            assertTrue(sanity.advance());
            assertEquals(i, asInt(sanity.getBytes("aaa_keep")));
            assertEquals("skip-me-" + i, asText(sanity.getBytes("zzz_skip")));
        }

        // corrupt the last 16 bytes of the payload -- guaranteed to fall inside "zzz_skip"'s
        // section since that column sorts last and its section is far larger than 16 bytes
        byte[] rawCopy = new byte[clean.remaining()];
        clean.duplicate().get(rawCopy);
        for (int i = rawCopy.length - 16; i < rawCopy.length; i++)
            rawCopy[i] = (byte) ~rawCopy[i];

        Set<String> projected = Collections.singleton("aaa_keep");
        ColumnarCursor cursor = ColumnarChunkCodec.cursor(ByteBuffer.wrap(rawCopy), projected);
        assertEquals(Collections.singleton("aaa_keep"), cursor.columns());
        assertFalse(cursor.hasColumn("zzz_skip"));
        for (int i = 0; i < n; i++)
        {
            assertTrue(cursor.advance());
            assertEquals(ts[i], cursor.timestamp());
            assertEquals(i, asInt(cursor.getBytes("aaa_keep")));
            assertNull(cursor.getBytes("zzz_skip"));   // excluded from the projection regardless of corruption
        }
        assertFalse(cursor.advance());
    }

    @Test
    public void encodeTwiceIsByteIdentical()
    {
        int n = 300;
        long[] ts = sequentialTimestamps(n);
        SortedMap<String, ChunkV4Codec.ColumnInput> natural = representativeColumns(n);

        ByteBuffer first = ColumnarChunkCodec.encode(ts, n, natural);
        ByteBuffer second = ColumnarChunkCodec.encode(ts, n, natural);
        assertEquals(first, second);

        // same entries, but built with a reversed comparator and a reversed insertion order, to
        // prove the directory's determinism comes from encode() re-sorting by name itself, not
        // from whatever order/comparator the caller's SortedMap happened to be built with
        // (v4 §5 rule 1 -- the `new TreeMap<>(map)` comparator-adoption trap)
        SortedMap<String, ChunkV4Codec.ColumnInput> reversed = new TreeMap<>(Collections.<String>reverseOrder());
        List<String> keysInReverseInsertOrder = new ArrayList<>(natural.keySet());
        Collections.reverse(keysInReverseInsertOrder);
        for (String key : keysInReverseInsertOrder)
            reversed.put(key, natural.get(key));

        ByteBuffer third = ColumnarChunkCodec.encode(ts, n, reversed);
        assertEquals(first, third);
    }

    /**
     * Every value a cursor hands out must be usable by Cassandra's own comparison machinery. This is
     * not a style preference: {@link FastByteOperations}'s unsafe path branches on
     * {@code hasArray()}, and if that is false it reads the buffer's {@code address} field and
     * dereferences it. A read-only HEAP buffer -- which is what {@code asReadOnlyBuffer()} produces --
     * answers {@code false} to both {@code hasArray()} and {@code isDirect()}, so its address is 0
     * and the comparison segfaults the JVM. The v3 decoder returned exactly such buffers from the
     * CONSTANT and TEXT/OPAQUE paths until it was fixed, and no round-trip assertion noticed, because
     * reading the bytes back out works fine; only comparing them crashes. v4 inherits the contract.
     * <p>
     * So this asserts the comparison RESULT (not merely that nothing throws -- a SIGSEGV would not
     * throw anyway) over one value from each decode path, plus the structural property that makes
     * the fast path safe.
     */
    @Test
    public void decodedValuesSurviveCassandrasComparisonPaths()
    {
        int n = 8;
        long[] ts = sequentialTimestamps(n);
        ByteBuffer[] constantDouble = new ByteBuffer[n];      // CONSTANT path (directory, 0-byte section)
        ByteBuffer[] varyingDouble = new ByteBuffer[n];       // normal fixed-width blocks
        ByteBuffer[] text = new ByteBuffer[n];                // TEXT dictionary path
        ByteBuffer[] opaque = new ByteBuffer[n];              // OPAQUE dictionary path
        ByteBuffer[] ints = new ByteBuffer[n];                // TYPE_INT32 fixed-width path
        ByteBuffer[] longs = new ByteBuffer[n];               // TYPE_INT64 fixed-width path
        ByteBuffer[] bools = new ByteBuffer[n];               // TYPE_BOOLEAN path
        for (int i = 0; i < n; i++)
        {
            constantDouble[i] = bytesOf(1.5);
            varyingDouble[i] = bytesOf(i * 1.25);
            text[i] = bytesOf("label-" + (i % 3));
            opaque[i] = ByteBuffer.wrap(new byte[]{ (byte) i, (byte) (i + 1) });
            ints[i] = bytesOf(100 + i);
            longs[i] = bytesOf(1_000_000_000_000L + i);
            bools[i] = bytesOf(i % 2 == 0);
        }
        SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
        columns.put("constant", input(ChunkV4Directory.TYPE_DOUBLE, constantDouble));
        columns.put("varying", input(ChunkV4Directory.TYPE_DOUBLE, varyingDouble));
        columns.put("label", input(ChunkV4Directory.TYPE_TEXT, text));
        columns.put("blobish", input(ChunkV4Directory.TYPE_OPAQUE, opaque));
        columns.put("counter32", input(ChunkV4Directory.TYPE_INT32, ints));
        columns.put("counter64", input(ChunkV4Directory.TYPE_INT64, longs));
        columns.put("flag", input(ChunkV4Directory.TYPE_BOOLEAN, bools));

        ColumnarCursor cursor = ColumnarChunkCodec.cursor(ColumnarChunkCodec.encode(ts, n, columns), null);
        assertTrue(cursor.advance());

        assertComparable("constant", cursor.getBytes("constant"), bytesOf(1.5), bytesOf(9.0));
        assertComparable("varying", cursor.getBytes("varying"), bytesOf(0.0), bytesOf(9.0));
        assertComparable("label", cursor.getBytes("label"), bytesOf("label-0"), bytesOf("label-9"));
        assertComparable("blobish", cursor.getBytes("blobish"),
                         ByteBuffer.wrap(new byte[]{ 0, 1 }), ByteBuffer.wrap(new byte[]{ 9, 9 }));
        assertComparable("counter32", cursor.getBytes("counter32"), bytesOf(100), bytesOf(999));
        assertComparable("counter64", cursor.getBytes("counter64"),
                         bytesOf(1_000_000_000_000L), bytesOf(9_000_000_000_000L));
        assertComparable("flag", cursor.getBytes("flag"), bytesOf(true), ByteBuffer.wrap(new byte[]{ 9 }));

        // A second row, so the per-row (non-constant) paths are exercised at an offset too.
        assertTrue(cursor.advance());
        assertComparable("constant", cursor.getBytes("constant"), bytesOf(1.5), bytesOf(9.0));
        assertComparable("varying", cursor.getBytes("varying"), bytesOf(1.25), bytesOf(9.0));
        assertComparable("counter32", cursor.getBytes("counter32"), bytesOf(101), bytesOf(999));
        assertComparable("counter64", cursor.getBytes("counter64"),
                         bytesOf(1_000_000_000_001L), bytesOf(9_000_000_000_000L));
        assertComparable("flag", cursor.getBytes("flag"), bytesOf(false), bytesOf(true));
    }

    /**
     * @param decoded a value straight out of the cursor
     * @param same    a separately-built buffer holding the identical bytes
     * @param greater a buffer that must sort strictly after {@code decoded}
     */
    private static void assertComparable(String column, ByteBuffer decoded, ByteBuffer same, ByteBuffer greater)
    {
        // The structural invariant the fast path depends on. Checked first so a regression names the
        // cause rather than crashing the JVM on the next line.
        assertTrue(column + ": a decoded value must be array-backed for FastByteOperations' fast path",
                   decoded.hasArray());

        // The exact route cell reconciliation takes (Cells.compareValues -> ValueAccessor.compare ->
        // FastByteOperations), asserting the ORDERING, not just the absence of an exception.
        assertEquals(column + ": must compare equal to its own bytes", 0,
                     FastByteOperations.compareUnsigned(decoded, same));
        assertEquals(column + ": must compare equal in the other direction", 0,
                     FastByteOperations.compareUnsigned(same, decoded));
        assertTrue(column + ": must sort before a larger value",
                   FastByteOperations.compareUnsigned(decoded, greater) < 0);
        assertTrue(column + ": a larger value must sort after it",
                   FastByteOperations.compareUnsigned(greater, decoded) > 0);
        // And through the accessor Cells actually uses, so the test tracks reconciliation rather than
        // one utility method.
        assertEquals(column + ": ValueAccessor must agree", 0,
                     ValueAccessor.compare(decoded, ByteBufferAccessor.instance, same, ByteBufferAccessor.instance));
    }

    private static SortedMap<String, ChunkV4Codec.ColumnInput> representativeColumns(int n)
    {
        ByteBuffer[] doubleValues = new ByteBuffer[n];
        ByteBuffer[] textValues = new ByteBuffer[n];
        ByteBuffer[] constantValues = new ByteBuffer[n];
        Random random = new Random(42);
        for (int i = 0; i < n; i++)
        {
            doubleValues[i] = (i % 7 == 0) ? null : bytesOf(random.nextDouble() * 100);
            textValues[i] = bytesOf("word-" + (i % 20));
            constantValues[i] = bytesOf(0);
        }
        SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
        columns.put("readings", input(ChunkV4Directory.TYPE_DOUBLE, doubleValues));
        columns.put("labels", input(ChunkV4Directory.TYPE_TEXT, textValues));
        columns.put("error_code", input(ChunkV4Directory.TYPE_INT32, constantValues));
        return columns;
    }

    @Test
    public void hasColumnFalseForUnknownName()
    {
        int n = 10;
        long[] ts = sequentialTimestamps(n);
        ByteBuffer[] values = new ByteBuffer[n];
        for (int i = 0; i < n; i++)
            values[i] = bytesOf(i);
        SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
        columns.put("existing_col", input(ChunkV4Directory.TYPE_INT32, values));

        ByteBuffer payload = ColumnarChunkCodec.encode(ts, n, columns);
        ColumnarCursor cursor = ColumnarChunkCodec.cursor(payload, null);
        assertTrue(cursor.hasColumn("existing_col"));
        assertFalse(cursor.hasColumn("added_later_col"));   // simulates an ALTER TABLE ADD after this chunk was written
        assertTrue(cursor.advance());
        assertTrue(cursor.isNull("added_later_col"));
        assertNull(cursor.getBytes("added_later_col"));
    }

    @Test
    public void truncatedPayloadThrowsIllegalArgument()
    {
        ByteBuffer payload = simpleTwoColumnPayload();
        byte[] bytes = new byte[payload.remaining()];
        payload.duplicate().get(bytes);
        for (int cut = 1; cut <= 5; cut++)
        {
            ByteBuffer truncated = ByteBuffer.wrap(bytes, 0, bytes.length - cut);
            assertThatThrownBy(() -> ColumnarChunkCodec.cursor(truncated, null))
                .as("cut=" + cut).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    public void corruptDirectoryEntryThrowsIllegalArgument()
    {
        ByteBuffer payload = simpleTwoColumnPayload();
        byte[] bytes = new byte[payload.remaining()];
        payload.duplicate().get(bytes);
        // the directory starts right after the 40-byte header; entry layout is
        // typeCode(1) colFlags(1) statOrder(1) nameLen(1) name ... -- corrupt nameLen so the name
        // overruns the directory region the header declares
        bytes[ChunkV4Header.HEADER_SIZE + 3] = (byte) 0xFF;
        ByteBuffer corrupted = ByteBuffer.wrap(bytes);
        assertThatThrownBy(() -> ColumnarChunkCodec.cursor(corrupted, null))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void corruptRowCountThrowsWithoutAllocating()
    {
        ByteBuffer payload = simpleTwoColumnPayload();
        byte[] bytes = new byte[payload.remaining()];
        payload.duplicate().get(bytes);

        // rowCount is the int32 right after the version byte (v4 kept v3's offset); ~536M rows is
        // over the format's MAX_ROWS and must be rejected before it can size any allocation
        byte[] corrupted = bytes.clone();
        corrupted[1] = (byte) 0x20;
        corrupted[2] = 0;
        corrupted[3] = 0;
        corrupted[4] = 0;

        assertThatThrownBy(() -> ColumnarChunkCodec.cursor(ByteBuffer.wrap(corrupted), null))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void headerPeeksOnTruncatedBufferThrowIllegalArgument()
    {
        ByteBuffer truncated = ByteBuffer.wrap(new byte[]{ 4, 0 });   // valid version byte, nothing else
        assertThatThrownBy(() -> ColumnarChunkCodec.rowCount(truncated)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ColumnarChunkCodec.firstTimestamp(truncated)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ColumnarChunkCodec.lastTimestamp(truncated)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void foreignVersionPayloadsAreRejectedAsUnsupported()
    {
        // Hand-built: the v1 (gorilla) and v2 (chimp128) encoders are deleted, so a bare version
        // byte plus padding is all a payload in those formats can be here -- which is also the
        // point: rejection must happen on the version byte alone, before any other byte is read.
        byte[] bytes = new byte[32];
        bytes[0] = 2;                                               // chimp128, the removed v2
        ByteBuffer chimpPayload = ByteBuffer.wrap(bytes.clone());
        bytes[0] = 1;                                               // gorilla, the removed v1
        ByteBuffer gorillaPayload = ByteBuffer.wrap(bytes);

        assertThatThrownBy(() -> ColumnarChunkCodec.cursor(gorillaPayload, null))
            .isInstanceOf(UnsupportedChunkFormatException.class);
        assertThatThrownBy(() -> ColumnarChunkCodec.cursor(chimpPayload, null))
            .isInstanceOf(UnsupportedChunkFormatException.class);
    }

    /**
     * §10 of the v4 spec, from the entry points the read path actually uses: a well-formed v3
     * payload names a real-but-removed format, so it must propagate as
     * {@link UnsupportedChunkFormatException} -- systematic, never swallowed, never skipped as one
     * corrupt chunk -- from the cursor and from every header peek alike.
     */
    @Test
    public void goldenV3PayloadIsRejectedAsUnsupportedNotCorrupt()
    {
        byte[] v3 = hex(GOLDEN_V3_HEX);

        assertThatThrownBy(() -> ColumnarChunkCodec.cursor(ByteBuffer.wrap(v3), null))
            .isInstanceOf(UnsupportedChunkFormatException.class)
            .hasMessageContaining("version: 3");
        assertThatThrownBy(() -> ColumnarChunkCodec.rowCount(ByteBuffer.wrap(v3)))
            .isInstanceOf(UnsupportedChunkFormatException.class);
        assertThatThrownBy(() -> ColumnarChunkCodec.firstTimestamp(ByteBuffer.wrap(v3)))
            .isInstanceOf(UnsupportedChunkFormatException.class);
        assertThatThrownBy(() -> ColumnarChunkCodec.lastTimestamp(ByteBuffer.wrap(v3)))
            .isInstanceOf(UnsupportedChunkFormatException.class);
    }

    /**
     * Doubles carry the one v4 double code whatever their distribution; which block encoding each
     * block got (decimal ALP or ALP-RD) is recorded inside the section, not in the directory, so
     * both fixtures must land on {@link ChunkV4Directory#TYPE_DOUBLE} and round-trip exactly.
     */
    @Test
    public void doubleColumnsCarryTheOneDoubleTypeCode()
    {
        int n = 400;
        Random random = new Random(17);
        ByteBuffer[] decimals = new ByteBuffer[n];      // decimal ALP's home ground
        ByteBuffer[] highEntropy = new ByteBuffer[n];   // nothing decimal here: ALP-RD territory
        double walk = 20.0;
        for (int i = 0; i < n; i++)
        {
            walk += (random.nextInt(3) - 1) * 0.01;
            decimals[i] = bytesOf(Math.round(walk * 100.0) / 100.0);
            highEntropy[i] = ByteBuffer.wrap(rawBytes(random.nextLong()));
        }

        for (ByteBuffer[] fixture : new ByteBuffer[][]{ decimals, highEntropy })
        {
            SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
            columns.put("v", input(ChunkV4Directory.TYPE_DOUBLE, fixture));
            ByteBuffer payload = ColumnarChunkCodec.encode(sequentialTimestamps(n), n, columns);

            assertEquals(ChunkV4Directory.TYPE_DOUBLE, ChunkV4Codec.open(payload).column("v").typeCode);
            ColumnarCursor cursor = ColumnarChunkCodec.cursor(payload, null);
            for (int i = 0; i < n; i++)
            {
                assertTrue(cursor.advance());
                assertArrayEquals("row " + i, asBytes(fixture[i]), asBytes(cursor.getBytes("v")));
            }
        }
    }

    /**
     * The whole columnar path, not just the codec: a double cell's bytes must come back out of a
     * cursor bit-identical, including the patterns Java's own {@code Double} helpers rewrite.
     * {@code ByteArrayUtil.bytes(double)} goes through {@code Double.doubleToLongBits}, which
     * canonicalises every NaN payload -- so a decoder that materialised doubles rather than raw bit
     * patterns would pass every other test here and still lose data on this one.
     */
    @Test
    public void doubleEdgeCaseBitPatternsSurviveTheWholeColumnarPath()
    {
        long[] patterns =
        {
            0x0000000000000000L,   // +0.0
            0x8000000000000000L,   // -0.0
            0x7FF0000000000000L,   // +Infinity
            0xFFF0000000000000L,   // -Infinity
            0x7FF8000000000000L,   // canonical NaN
            0x7FF8000000000001L,   // quiet NaN with a payload
            0x7FF0000000000001L,   // signalling NaN
            0xFFFFFFFFFFFFFFFFL,   // negative NaN, all payload bits set
            0x0000000000000001L,   // Double.MIN_VALUE
            0x000FFFFFFFFFFFFFL,   // largest subnormal
            0x0010000000000000L,   // Double.MIN_NORMAL
            0x7FEFFFFFFFFFFFFFL,   // Double.MAX_VALUE
            0xFFEFFFFFFFFFFFFFL,   // -Double.MAX_VALUE
            Double.doubleToRawLongBits(20.76),
            Double.doubleToRawLongBits(157.0),
            Double.doubleToRawLongBits(-0.1),
        };

        // Every second row null, so the presence machinery and the value-index bookkeeping are in
        // play too -- an off-by-one there would shift values onto neighbouring rows.
        int n = patterns.length * 2;
        long[] ts = sequentialTimestamps(n);
        ByteBuffer[] values = new ByteBuffer[n];
        for (int i = 0; i < patterns.length; i++)
            values[i * 2] = ByteBuffer.wrap(rawBytes(patterns[i]));

        SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
        columns.put("reading", input(ChunkV4Directory.TYPE_DOUBLE, values));

        ColumnarCursor cursor = ColumnarChunkCodec.cursor(ColumnarChunkCodec.encode(ts, n, columns), null);
        for (int i = 0; i < n; i++)
        {
            assertTrue(cursor.advance());
            if (i % 2 == 1)
            {
                assertTrue("row " + i + " must stay null", cursor.isNull("reading"));
                continue;
            }
            long expected = patterns[i / 2];
            ByteBuffer decoded = cursor.getBytes("reading");
            assertTrue("row " + i + ": decoded doubles must be array-backed (see the buffer contract)",
                       decoded.hasArray());
            assertArrayEquals("row " + i + ": " + String.format("0x%016X", expected),
                              rawBytes(expected), asBytes(decoded));
        }
        assertFalse(cursor.advance());
    }

    /**
     * Truncating a payload whose tail is a double column's section must be reported as corruption,
     * the same as every other section type. The double column sorts last here so its section really
     * is the payload's tail.
     */
    @Test
    public void truncatedTrailingDoubleSectionThrowsIllegalArgument()
    {
        int n = 300;
        long[] ts = sequentialTimestamps(n);
        ByteBuffer[] head = new ByteBuffer[n];
        ByteBuffer[] doubles = new ByteBuffer[n];
        Random random = new Random(23);
        double walk = 41.5;
        for (int i = 0; i < n; i++)
        {
            head[i] = bytesOf(i);
            walk += (random.nextInt(3) - 1) * 0.01;
            doubles[i] = bytesOf(Math.round(walk * 100.0) / 100.0);
        }
        SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
        columns.put("aaa_int", input(ChunkV4Directory.TYPE_INT32, head));
        columns.put("zzz_double", input(ChunkV4Directory.TYPE_DOUBLE, doubles));

        ByteBuffer payload = ColumnarChunkCodec.encode(ts, n, columns);
        byte[] bytes = new byte[payload.remaining()];
        payload.duplicate().get(bytes);

        for (int cut = 1; cut <= 24; cut++)
        {
            ByteBuffer truncated = ByteBuffer.wrap(bytes, 0, bytes.length - cut);
            assertThatThrownBy(() -> ColumnarChunkCodec.cursor(truncated, null))
                .as("cut=" + cut).isInstanceOf(IllegalArgumentException.class);
        }
    }

    private static byte[] rawBytes(long bits)
    {
        byte[] out = new byte[8];
        for (int i = 0; i < 8; i++)
            out[i] = (byte) (bits >>> (56 - 8 * i));
        return out;
    }

    @Test
    public void unknownColumnTypeCodeIsRejected()
    {
        SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
        columns.put("d", input(ChunkV4Directory.TYPE_INT32,
                               new ByteBuffer[]{ bytesOf(1), bytesOf(2), bytesOf(3) }));
        ByteBuffer payload = ColumnarChunkCodec.encode(new long[]{ 1L, 2L, 3L }, 3, columns);

        byte[] bytes = new byte[payload.remaining()];
        payload.duplicate().get(bytes);
        // the first directory entry starts right after the header; its first byte is the type code
        assertEquals(ChunkV4Directory.TYPE_INT32, bytes[ChunkV4Header.HEADER_SIZE]);
        bytes[ChunkV4Header.HEADER_SIZE] = 0x7F;

        // An unallocated code INSIDE a v4 payload is corruption, not a future format -- a future
        // format would carry a different version byte (v4 §9).
        assertThatThrownBy(() -> ColumnarChunkCodec.cursor(ByteBuffer.wrap(bytes), null))
            .isInstanceOf(IllegalArgumentException.class)
            .isNotInstanceOf(UnsupportedChunkFormatException.class);
    }

    @Test
    public void rejectsNonIncreasingTimestamps()
    {
        SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
        columns.put("c", input(ChunkV4Directory.TYPE_INT32,
                               new ByteBuffer[]{ bytesOf(1), bytesOf(2), bytesOf(3) }));

        assertThatThrownBy(() -> ColumnarChunkCodec.encode(new long[]{ 1000L, 999L, 1001L }, 3, columns))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ColumnarChunkCodec.encode(new long[]{ 1000L, 1000L, 1001L }, 3, columns))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsZeroCount()
    {
        ColumnarChunkCodec.encode(new long[0], 0, new TreeMap<>());
    }

    @Test
    public void headerPeeksDoNotConsumeBuffer()
    {
        ByteBuffer payload = simpleTwoColumnPayload();
        int position = payload.position();

        assertEquals(20, ColumnarChunkCodec.rowCount(payload));
        assertEquals(0L, ColumnarChunkCodec.firstTimestamp(payload));
        assertEquals(19_000L, ColumnarChunkCodec.lastTimestamp(payload));
        assertEquals(position, payload.position());
    }

    @Test
    public void cursorAccessBeforeAdvanceThrows()
    {
        ByteBuffer payload = simpleTwoColumnPayload();
        ColumnarCursor cursor = ColumnarChunkCodec.cursor(payload, null);

        assertThatThrownBy(cursor::timestamp).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> cursor.getBytes("colA")).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> cursor.isNull("colA")).isInstanceOf(IllegalStateException.class);
    }

    /** A {@link ChunkV4Codec.ColumnInput} declaring the type code's own canonical stat order. */
    private static ChunkV4Codec.ColumnInput input(int typeCode, ByteBuffer[] values)
    {
        return new ChunkV4Codec.ColumnInput(typeCode, ChunkV4Codec.canonicalStatOrder(typeCode), values);
    }

    private static ByteBuffer simpleTwoColumnPayload()
    {
        int n = 20;
        long[] ts = sequentialTimestamps(n);
        ByteBuffer[] a = new ByteBuffer[n];
        ByteBuffer[] b = new ByteBuffer[n];
        for (int i = 0; i < n; i++)
        {
            a[i] = bytesOf(i);
            b[i] = bytesOf("v" + i);
        }
        SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
        columns.put("colA", input(ChunkV4Directory.TYPE_INT32, a));
        columns.put("colB", input(ChunkV4Directory.TYPE_TEXT, b));
        return ColumnarChunkCodec.encode(ts, n, columns);
    }

    private static long[] sequentialTimestamps(int n)
    {
        long[] ts = new long[n];
        for (int i = 0; i < n; i++)
            ts[i] = i * 1000L;
        return ts;
    }

    private static ByteBuffer bytesOf(double v)
    {
        ByteBuffer b = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
        b.putDouble(v);
        b.flip();
        return b;
    }

    private static ByteBuffer bytesOf(int v)
    {
        ByteBuffer b = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
        b.putInt(v);
        b.flip();
        return b;
    }

    private static ByteBuffer bytesOf(long v)
    {
        ByteBuffer b = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
        b.putLong(v);
        b.flip();
        return b;
    }

    private static ByteBuffer bytesOf(boolean v)
    {
        ByteBuffer b = ByteBuffer.allocate(1);
        b.put((byte) (v ? 1 : 0));
        b.flip();
        return b;
    }

    private static ByteBuffer bytesOf(String v)
    {
        return ByteBuffer.wrap(v.getBytes(StandardCharsets.UTF_8));
    }

    private static ByteBuffer bytesOf(byte[] v)
    {
        return ByteBuffer.wrap(v);
    }

    private static double asDouble(ByteBuffer b)
    {
        return b.duplicate().order(ByteOrder.BIG_ENDIAN).getDouble();
    }

    private static int asInt(ByteBuffer b)
    {
        return b.duplicate().order(ByteOrder.BIG_ENDIAN).getInt();
    }

    private static long asLong(ByteBuffer b)
    {
        return b.duplicate().order(ByteOrder.BIG_ENDIAN).getLong();
    }

    private static boolean asBool(ByteBuffer b)
    {
        return b.duplicate().get() != 0;
    }

    private static String asText(ByteBuffer b)
    {
        return new String(asBytes(b), StandardCharsets.UTF_8);
    }

    private static byte[] asBytes(ByteBuffer b)
    {
        byte[] array = new byte[b.remaining()];
        b.duplicate().get(array);
        return array;
    }
}
