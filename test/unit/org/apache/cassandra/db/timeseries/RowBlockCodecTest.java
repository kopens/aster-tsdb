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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.timeseries.tiering.ChunkColumnTypes;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RowBlockCodecTest
{
    static
    {
        DatabaseDescriptor.toolInitialization();
    }

    private static final long NOW_SEC = 1_800_000_000L;

    private static TableMetadata table(boolean descending)
    {
        return TableMetadata.builder("ks", "t")
                            .addPartitionKeyColumn("tag", UTF8Type.instance)
                            .addClusteringColumn("ts", descending ? ReversedType.getInstance(TimestampType.instance) : TimestampType.instance)
                            .addRegularColumn("b", BooleanType.instance)
                            .addRegularColumn("m", MapType.getInstance(UTF8Type.instance, UTF8Type.instance, false))
                            .addRegularColumn("n", Int32Type.instance)
                            .addRegularColumn("s", UTF8Type.instance)
                            .addRegularColumn("v", DoubleType.instance)
                            .build();
    }

    private static ColumnMetadata col(TableMetadata t, String name)
    {
        return t.getColumn(ColumnIdentifier.getInterned(name, true));
    }

    private static Row.Builder newRow(long tsMillis)
    {
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(Clustering.make(TimestampType.instance.fromTimeInMillis(tsMillis)));
        return builder;
    }

    private static List<Row> roundTrip(TableMetadata t, List<Row> rows)
    {
        ByteBuffer payload = RowBlockCodec.encode(t, rows);
        assertTrue("these rows must be encodable", payload != null);
        return RowBlockCodec.decode(t, payload, null);
    }

    private static Row full(TableMetadata t, long tsMillis, long writetime)
    {
        Row.Builder b = newRow(tsMillis);
        b.addPrimaryKeyLivenessInfo(LivenessInfo.create(writetime));
        b.addCell(BufferCell.live(col(t, "b"), writetime, BooleanType.instance.decompose(true)));
        b.addCell(BufferCell.live(col(t, "m"), writetime, col(t, "m").type.decomposeUntyped(Map.of("k", "x"))));
        b.addCell(BufferCell.live(col(t, "n"), writetime, Int32Type.instance.decompose(7)));
        b.addCell(BufferCell.live(col(t, "s"), writetime, UTF8Type.instance.decompose("on")));
        b.addCell(BufferCell.live(col(t, "v"), writetime, DoubleType.instance.decompose(20.5)));
        return b.build();
    }

    @Test
    public void liveRowsRoundTripExactly()
    {
        TableMetadata t = table(false);
        Row.Builder p = newRow(2_000);
        p.addPrimaryKeyLivenessInfo(LivenessInfo.create(1001));
        p.addCell(BufferCell.live(col(t, "v"), 1001, DoubleType.instance.decompose(1.5)));
        p.addCell(BufferCell.live(col(t, "s"), 1001, UTF8Type.instance.decompose("")));     // present but empty
        Row partial = p.build();
        Row.Builder k = newRow(3_000);
        k.addPrimaryKeyLivenessInfo(LivenessInfo.create(1002));                              // key only, every column null
        List<Row> rows = List.of(full(t, 1_000, 1000), partial, k.build());
        assertEquals(rows, roundTrip(t, rows));
    }

    @Test
    public void updateOnlyRowsKeepEmptyLiveness()
    {
        TableMetadata t = table(false);
        Row.Builder b = newRow(1_000);
        b.addCell(BufferCell.live(col(t, "v"), 4000, DoubleType.instance.decompose(3.0)));
        b.addCell(BufferCell.live(col(t, "n"), 4100, Int32Type.instance.decompose(3)));
        List<Row> rows = List.of(b.build());
        List<Row> back = roundTrip(t, rows);
        assertEquals(rows, back);
        assertTrue(back.get(0).primaryKeyLivenessInfo().isEmpty());
    }

    @Test
    public void descendingTablesRoundTripInTableOrder()
    {
        TableMetadata t = table(true);
        List<Row> rows = List.of(full(t, 3_000, 30), full(t, 2_000, 20), full(t, 1_000, 10));
        assertEquals(rows, roundTrip(t, rows));
    }

    /**
     * The read path's column filter fetches every regular column but queries only some; row format then keeps a
     * fetched-but-unqueried cell only when it is not older than the row marker, with its value skipped. Decoding must
     * do exactly the same, or a replica reading this format returns different cells (and a different digest).
     */
    @Test
    public void columnFilterIsAppliedExactlyAsRowFormatDoes()
    {
        TableMetadata t = table(false);
        Row.Builder late = newRow(2_000);
        late.addPrimaryKeyLivenessInfo(LivenessInfo.create(2000));
        late.addCell(BufferCell.live(col(t, "v"), 2000, DoubleType.instance.decompose(2.0)));
        late.addCell(BufferCell.live(col(t, "n"), 1999, Int32Type.instance.decompose(1)));  // older than the row marker
        late.addCell(BufferCell.live(col(t, "s"), 2500, UTF8Type.instance.decompose("x"))); // newer than the row marker
        List<Row> rows = List.of(full(t, 1_000, 1000), late.build());

        ColumnFilter onlyV = ColumnFilter.selection(t, RegularAndStaticColumns.of(col(t, "v")), false);
        List<Row> back = RowBlockCodec.decode(t, RowBlockCodec.encode(t, rows), onlyV);

        assertEquals(2, back.size());
        Row first = back.get(0);
        assertEquals(rows.get(0).getCell(col(t, "v")), first.getCell(col(t, "v")));
        for (String other : new String[]{ "b", "m", "n", "s" })
            assertEquals(other, rows.get(0).getCell(col(t, other)).withSkippedValue(), first.getCell(col(t, other)));
        Row second = back.get(1);
        assertEquals(rows.get(1).primaryKeyLivenessInfo(), second.primaryKeyLivenessInfo());
        assertNull("older than the row marker and unqueried: dropped", second.getCell(col(t, "n")));
        assertEquals(rows.get(1).getCell(col(t, "s")).withSkippedValue(), second.getCell(col(t, "s")));
    }

    /**
     * With one replica the value-less cells of unqueried plain columns are left out -- but only where they prove
     * nothing: a row without a marker, with an expiring marker, or a column with any cell of its own timestamp, TTL
     * or tombstone in the block keeps them, because those cells can be all that keeps the row alive.
     */
    @Test
    public void redundantCellsAreSkippedOnlyWhereTheyProveNothing()
    {
        TableMetadata t = table(false);
        ColumnFilter onlyV = ColumnFilter.selection(t, RegularAndStaticColumns.of(col(t, "v")), false);

        List<Row> plain = List.of(full(t, 1_000, 1000), full(t, 2_000, 1001));
        for (Row row : RowBlockCodec.decode(t, RowBlockCodec.encode(t, plain), onlyV, true))
        {
            assertEquals("only the queried column", 1, row.columnCount());
            assertTrue(!row.primaryKeyLivenessInfo().isEmpty());
        }

        Row.Builder noMarker = newRow(1_000);                                 // made by UPDATE alone
        noMarker.addCell(BufferCell.live(col(t, "s"), 500, UTF8Type.instance.decompose("x")));
        Row.Builder expiring = newRow(2_000);
        expiring.addPrimaryKeyLivenessInfo(LivenessInfo.expiring(600, 60, NOW_SEC));
        expiring.addCell(BufferCell.live(col(t, "s"), 600, UTF8Type.instance.decompose("y")));
        List<Row> kept = RowBlockCodec.decode(t, RowBlockCodec.encode(t, List.of(noMarker.build(), expiring.build())), onlyV, true);
        assertEquals("the row has no marker: its cell is its liveness", 1, kept.get(0).columnCount());
        assertEquals("the marker can expire before the cell", 1, kept.get(1).columnCount());

        Row.Builder late = newRow(1_000);
        late.addPrimaryKeyLivenessInfo(LivenessInfo.create(700));
        late.addCell(BufferCell.live(col(t, "s"), 900, UTF8Type.instance.decompose("z")));   // newer than the marker
        assertEquals("a cell newer than the marker can outlive it", 1,
                     RowBlockCodec.decode(t, RowBlockCodec.encode(t, List.of(late.build())), onlyV, true).get(0).columnCount());
    }

    @Test
    public void cellWritetimesSurviveLateColumnUpdates()
    {
        TableMetadata t = table(false);
        Row.Builder b = newRow(1_000);
        b.addPrimaryKeyLivenessInfo(LivenessInfo.create(1000));
        b.addCell(BufferCell.live(col(t, "v"), 1000, DoubleType.instance.decompose(1.0)));
        b.addCell(BufferCell.live(col(t, "n"), 5000, Int32Type.instance.decompose(9)));   // a later UPDATE of one column
        b.addCell(BufferCell.live(col(t, "s"), 999, UTF8Type.instance.decompose("old"))); // older than the row marker
        List<Row> rows = List.of(b.build());
        assertEquals(rows, roundTrip(t, rows));
    }

    @Test
    public void expiringCellsAndLivenessKeepTtlAndDeletionTime()
    {
        TableMetadata t = table(false);
        Row.Builder b = newRow(1_000);
        b.addPrimaryKeyLivenessInfo(LivenessInfo.expiring(1000, 3600, NOW_SEC));
        b.addCell(BufferCell.expiring(col(t, "v"), 1000, 3600, NOW_SEC, DoubleType.instance.decompose(1.0)));
        b.addCell(BufferCell.live(col(t, "n"), 1000, Int32Type.instance.decompose(1)));
        Row.Builder expired = newRow(2_000);
        expired.addPrimaryKeyLivenessInfo(LivenessInfo.create(1001));
        expired.addCell(BufferCell.expiring(col(t, "v"), 1001, 60, NOW_SEC - 86_400, DoubleType.instance.decompose(2.0)));
        List<Row> rows = List.of(b.build(), expired.build());
        List<Row> back = roundTrip(t, rows);
        assertEquals(rows, back);
        assertEquals(rows.get(1).getCell(col(t, "v")).isLive(NOW_SEC), back.get(1).getCell(col(t, "v")).isLive(NOW_SEC));
    }

    @Test
    public void cellTombstonesRoundTrip()
    {
        TableMetadata t = table(false);
        Row.Builder b = newRow(1_000);                                      // what INSERT ... VALUES (..., null) writes
        b.addPrimaryKeyLivenessInfo(LivenessInfo.create(1000));
        b.addCell(BufferCell.live(col(t, "v"), 1000, DoubleType.instance.decompose(1.0)));
        b.addCell(BufferCell.tombstone(col(t, "s"), 1000, NOW_SEC));
        b.addCell(BufferCell.tombstone(col(t, "n"), 1200, NOW_SEC - 5));
        List<Row> rows = List.of(b.build());
        assertEquals(rows, roundTrip(t, rows));
    }

    @Test
    public void unencodableRowsReturnNull()
    {
        TableMetadata t = table(false);
        Row.Builder deleted = newRow(1_000);
        deleted.addRowDeletion(Row.Deletion.regular(DeletionTime.build(500, NOW_SEC)));
        assertNull("a row deletion", RowBlockCodec.encode(t, List.of(deleted.build())));

        assertNull("duplicate clusterings", RowBlockCodec.encode(t, List.of(full(t, 1_000, 1), full(t, 1_000, 2))));
        assertNull("out of clustering order", RowBlockCodec.encode(t, List.of(full(t, 2_000, 1), full(t, 1_000, 2))));
        assertNull("no rows", RowBlockCodec.encode(t, List.of()));

        TableMetadata counters = TableMetadata.builder("ks", "c")
                                              .addPartitionKeyColumn("tag", UTF8Type.instance)
                                              .addClusteringColumn("ts", TimestampType.instance)
                                              .addRegularColumn("hits", CounterColumnType.instance)
                                              .isCounter(true)
                                              .build();
        assertTrue(!RowBlockCodec.supports(counters));
        TableMetadata multiCell = TableMetadata.builder("ks", "mc")
                                               .addPartitionKeyColumn("tag", UTF8Type.instance)
                                               .addClusteringColumn("ts", TimestampType.instance)
                                               .addRegularColumn("tags", MapType.getInstance(UTF8Type.instance, UTF8Type.instance, true))
                                               .build();
        assertTrue(!RowBlockCodec.supports(multiCell));
        TableMetadata twoClusterings = TableMetadata.builder("ks", "tc")
                                                    .addPartitionKeyColumn("tag", UTF8Type.instance)
                                                    .addClusteringColumn("ts", TimestampType.instance)
                                                    .addClusteringColumn("seq", Int32Type.instance)
                                                    .addRegularColumn("v", DoubleType.instance)
                                                    .build();
        assertTrue(!RowBlockCodec.supports(twoClusterings));
    }

    @Test
    public void encodingIsDeterministic()
    {
        TableMetadata t = table(false);
        List<Row> rows = randomRows(t, new Random(7), 500);
        ByteBuffer a = RowBlockCodec.encode(t, rows);
        ByteBuffer b = RowBlockCodec.encode(t, new ArrayList<>(rows));
        assertEquals(a, b);
    }

    @Test
    public void randomRowsRoundTrip()
    {
        for (int seed = 0; seed < 300; seed++)
        {
            Random random = new Random(seed);
            boolean descending = random.nextBoolean();
            TableMetadata t = table(descending);
            List<Row> rows = randomRows(t, random, 1 + random.nextInt(2_000));
            if (descending)
                Collections.reverse(rows);
            assertEquals("seed " + seed, rows, roundTrip(t, rows));
        }
    }

    /** The tm_tag_point shape at its usual state: one writetime per row, no TTL, no tombstones. */
    @Test
    public void uniformRowsCostLittleOverValuesAlone()
    {
        TableMetadata t = table(false);
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < 900; i++)
            rows.add(full(t, 1_000L * i, 1_000_000L + i));
        int withMetadata = RowBlockCodec.encode(t, rows).remaining();

        long[] timestamps = new long[rows.size()];
        SortedMap<String, ChunkV4Codec.ColumnInput> valuesOnly = new TreeMap<>();
        for (ColumnMetadata c : t.regularColumns())
        {
            ByteBuffer[] values = new ByteBuffer[rows.size()];
            for (int i = 0; i < rows.size(); i++)
                values[i] = rows.get(i).getCell(c).buffer();
            valuesOnly.put(c.name.toString(), new ChunkV4Codec.ColumnInput(ChunkColumnTypes.typeCodeFor(c.type), ChunkColumnTypes.statOrderFor(c.type), values));
        }
        for (int i = 0; i < rows.size(); i++)
            timestamps[i] = 1_000L * i;
        ByteBuffer plain = ColumnarChunkCodec.encode(timestamps, rows.size(), valuesOnly);
        // The row-timestamp column is real data (one writetime per row); everything else must fold to O(1).
        assertTrue("metadata overhead " + withMetadata + " vs values alone " + plain.remaining(),
                   withMetadata <= plain.remaining() * 1.25 + 2_048);
    }

    private static List<Row> randomRows(TableMetadata t, Random random, int count)
    {
        List<Row> rows = new ArrayList<>(count);
        ColumnMetadata last = null;
        for (ColumnMetadata c : t.regularColumns())
            last = c;
        long ts = random.nextInt(1_000_000);
        for (int i = 0; i < count; i++)
        {
            ts += 1 + random.nextInt(random.nextBoolean() ? 3 : 5_000);
            long writetime = 1_700_000_000_000_000L + i * 1_000L + random.nextInt(1_000);
            Row.Builder b = newRow(ts);
            int liveness = random.nextInt(10);
            if (liveness < 7)
                b.addPrimaryKeyLivenessInfo(LivenessInfo.create(writetime));
            else if (liveness < 9)
                b.addPrimaryKeyLivenessInfo(LivenessInfo.expiring(writetime, 1 + random.nextInt(100_000), NOW_SEC - random.nextInt(200_000)));
            boolean anyCell = liveness < 9;
            for (ColumnMetadata c : t.regularColumns())
            {
                int kind = random.nextInt(12);
                long cellWt = writetime + (random.nextInt(8) == 0 ? random.nextInt(2_000_000) - 1_000_000 : 0);
                if (kind < 3 && (anyCell || c != last))
                    continue;                                                         // no cell (but never an empty row)
                anyCell = true;
                if (kind == 3)
                    b.addCell(BufferCell.tombstone(c, cellWt, NOW_SEC - random.nextInt(100_000)));
                else if (kind == 4)
                    b.addCell(BufferCell.expiring(c, cellWt, 1 + random.nextInt(100_000), NOW_SEC - random.nextInt(200_000), value(c.type, random)));
                else
                    b.addCell(BufferCell.live(c, cellWt, value(c.type, random)));
            }
            rows.add(b.build());
        }
        return rows;
    }

    private static ByteBuffer value(AbstractType<?> type, Random random)
    {
        if (type instanceof DoubleType)
            return DoubleType.instance.decompose(random.nextInt(4) == 0 ? 20.0 : random.nextGaussian() * 100);
        if (type instanceof Int32Type)
            return Int32Type.instance.decompose(random.nextInt(5));
        if (type instanceof BooleanType)
            return BooleanType.instance.decompose(random.nextBoolean());
        if (type instanceof UTF8Type)
            return UTF8Type.instance.decompose(random.nextInt(5) == 0 ? "" : "s" + random.nextInt(20));
        return type.decomposeUntyped(Map.of("k" + random.nextInt(3), "v" + random.nextInt(3)));
    }
}
