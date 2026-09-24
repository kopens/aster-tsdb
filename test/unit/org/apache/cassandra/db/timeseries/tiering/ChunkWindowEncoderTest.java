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
package org.apache.cassandra.db.timeseries.tiering;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.timeseries.ColumnarChunkCodec;
import org.apache.cassandra.db.timeseries.ColumnarCursor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The chunk-window encoding rules both chunk writers share (the re-encoder and the cold-window
 * flush), tested without a node: they used to exist only as two copies inside node-level paths.
 */
public class ChunkWindowEncoderTest
{
    private static final long WINDOW = 1_577_836_800_000L; // 2020-01-01T00:00Z
    private static final long WT = 1_650_000_000_000_000L; // micros

    private static TableMetadata metadata;
    private static ChunkWindowEncoder encoder;
    private static int valueSlot;
    private static int qualitySlot;

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
        metadata = TableMetadata.builder("cwe", "points")
                                .partitioner(Murmur3Partitioner.instance)
                                .addPartitionKeyColumn("tag_id", UTF8Type.instance)
                                .addClusteringColumn("timestamp", TimestampType.instance)
                                .addRegularColumn("value", DoubleType.instance)
                                .addRegularColumn("quality", Int32Type.instance)
                                .build();
        encoder = new ChunkWindowEncoder(metadata);
        for (int c = 0; c < encoder.valueCount(); c++)
        {
            if (encoder.valueRawName(c).equals("value"))
                valueSlot = c;
            else if (encoder.valueRawName(c).equals("quality"))
                qualitySlot = c;
        }
    }

    private static ByteBuffer[] sample(Double value, Integer quality)
    {
        ByteBuffer[] slots = new ByteBuffer[encoder.valueCount()];
        slots[valueSlot] = value == null ? null : DoubleType.instance.decompose(value);
        slots[qualitySlot] = quality == null ? null : Int32Type.instance.decompose(quality);
        return slots;
    }

    /** What the existing-chunk query returns for a chunk row holding {@code encoded}, written at {@code chunkWt}. */
    private static UntypedResultSet.Row storedChunk(ChunkWindowEncoder.Encoded encoded, long chunkWt)
    {
        List<ColumnSpecification> names = Arrays.asList(spec("payload", BytesType.instance),
                                                        spec("max_row_writetime", LongType.instance),
                                                        spec("chunk_wt", LongType.instance));
        return UntypedResultSet.Row.fromByteBuffers(names, Arrays.asList(encoded.payload.duplicate(),
                                                                         LongType.instance.decompose(encoded.maxWritetime),
                                                                         LongType.instance.decompose(chunkWt)));
    }

    private static ColumnSpecification spec(String name, org.apache.cassandra.db.marshal.AbstractType<?> type)
    {
        return new ColumnSpecification("cwe", "points__chunks", new ColumnIdentifier(name, true), type);
    }

    @Test
    public void mergeIsPerColumnSoAPartialUpdateKeepsTheOtherColumns()
    {
        // A stored sample, then a later UPDATE that wrote only `quality` for the same timestamp:
        // the merged chunk must hold both the stored value and the new quality -- per-cell
        // last-write-wins, exactly as Cassandra would answer the same read from rows.
        ChunkWindowEncoder.Window first = encoder.open(null);
        first.mergeSample(WINDOW, sample(21.5, 1));
        first.noteWritetime(WT);
        ChunkWindowEncoder.Encoded stored = first.encode();

        ChunkWindowEncoder.Window update = encoder.open(storedChunk(stored, WT + 1));
        update.mergeSample(WINDOW, sample(null, 7));
        update.noteWritetime(WT + 10);
        ChunkWindowEncoder.Encoded merged = update.encode();

        ColumnarCursor cursor = ColumnarChunkCodec.cursor(merged.payload, null);
        assertTrue(cursor.advance());
        assertEquals(WINDOW, cursor.timestamp());
        assertEquals(21.5, DoubleType.instance.compose(cursor.getBytes("value")), 0.0);
        assertEquals(7, (int) Int32Type.instance.compose(cursor.getBytes("quality")));
        assertFalse(cursor.advance());
        assertEquals(WT + 10, merged.maxWritetime);
    }

    @Test
    public void reEncodingIdenticalContentAtTheSameWritetimeIsUnchanged()
    {
        // The re-run / replayed-flush / other-replica case: nothing new, so nothing to write.
        ChunkWindowEncoder.Window first = encoder.open(null);
        first.mergeSample(WINDOW, sample(1.0, 1));
        first.mergeSample(WINDOW + 1000, sample(2.0, null));
        first.noteWritetime(WT);
        ChunkWindowEncoder.Encoded stored = first.encode();
        assertFalse("a window with no stored chunk is always written", stored.unchanged);

        ChunkWindowEncoder.Window again = encoder.open(storedChunk(stored, WT + 1));
        again.mergeSample(WINDOW + 1000, sample(2.0, null));
        again.noteWritetime(WT);
        assertTrue(again.encode().unchanged);

        // Same content but a newer writetime is NOT unchanged: max_row_writetime moves, and the
        // range delete that follows is sized by it.
        ChunkWindowEncoder.Window newer = encoder.open(storedChunk(stored, WT + 1));
        newer.mergeSample(WINDOW + 1000, sample(2.0, null));
        newer.noteWritetime(WT + 5);
        assertFalse(newer.encode().unchanged);
    }

    @Test
    public void insertTimestampIsStrictlyAfterBothTheRowsAndTheReplacedChunk()
    {
        ChunkWindowEncoder.Window first = encoder.open(null);
        first.mergeSample(WINDOW, sample(1.0, 1));
        first.noteWritetime(WT);
        ChunkWindowEncoder.Encoded stored = first.encode();
        assertEquals(WT + 1, stored.insertTimestamp);

        // The stored chunk row was itself written later than maxWt + 1 (crash-then-backfill): the
        // replacement must still land strictly after it, or the two tie and can tear per column.
        long chunkWt = WT + 1_000;
        ChunkWindowEncoder.Window late = encoder.open(storedChunk(stored, chunkWt));
        late.mergeSample(WINDOW + 500, sample(3.0, 3));
        late.noteWritetime(WT + 2);
        assertEquals(chunkWt + 1, late.encode().insertTimestamp);
    }

    @Test
    public void aWindowWithoutAnyWritetimeHasNone()
    {
        ChunkWindowEncoder.Window bare = encoder.open(null);
        bare.mergeSample(WINDOW, sample(null, null));
        assertFalse("no cell writetime and no stored chunk: both writers must leave the window alone",
                    bare.haveWritetime());
        assertEquals(1, bare.sampleCount());
    }

    @Test
    public void insertValuesBindInTheStatementsColumnOrder()
    {
        ChunkWindowEncoder.Window window = encoder.open(null);
        window.mergeSample(WINDOW, sample(1.0, 1));
        window.mergeSample(WINDOW + 1, sample(2.0, 2));
        window.noteWritetime(WT);
        ChunkWindowEncoder.Encoded encoded = window.encode();

        ByteBuffer tag = UTF8Type.instance.decompose("pump-01");
        List<ByteBuffer> values = encoded.insertValues(Collections.singletonList(tag), WINDOW);
        // (<tag>, window_start, codec, samples, max_row_writetime, payload) USING TIMESTAMP ?
        assertEquals(7, values.size());
        assertEquals(tag, values.get(0));
        assertEquals(WINDOW, TimestampType.instance.compose(values.get(1)).getTime());
        assertEquals(encoded.payload.get(encoded.payload.position()), (byte) ByteType.instance.compose(values.get(2)));
        assertEquals(2, (int) Int32Type.instance.compose(values.get(3)));
        assertEquals(WT, (long) LongType.instance.compose(values.get(4)));
        assertEquals(encoded.payload, values.get(5));
        assertEquals(WT + 1, (long) LongType.instance.compose(values.get(6)));

        String insert = ChunkWindowEncoder.insertChunkQuery("cwe.points__chunks", "tag_id", "?");
        assertTrue(insert, insert.contains("(tag_id, window_start, codec, samples, max_row_writetime, payload)"));
    }
}
