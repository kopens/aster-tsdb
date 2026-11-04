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

package org.apache.cassandra.io.sstable.format.bti;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.timeseries.RowBlockCodec;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Writes one partition of a {@link TscolFormat} sstable. Key, partition deletion and static row are written exactly as
 * BTI writes them; then a kind byte and the body:
 * <ul>
 * <li>{@link #KIND_COLUMNAR}: an unsigned vint block count, then per block {@code long minMillis, long maxMillis,
 *     vint rowCount, vint length}, then the blocks -- each one {@link RowBlockCodec#encode} of the rows whose
 *     clustering falls in one epoch-aligned {@link #BLOCK_MILLIS} span, in the order the rows arrived (the table's
 *     clustering order) -- then BTI's end-of-partition byte.</li>
 * <li>{@link #KIND_ROWS}: BTI's own row serialization, for a partition holding anything the codec refuses (a range
 *     tombstone, a row deletion). Blocks already encoded for it are decoded and replayed -- the codec round-trips
 *     exactly, so the replayed rows are the rows that came in.</li>
 * </ul>
 * Rows are only buffered one block at a time; encoded blocks are kept until {@link #finish} writes the directory in
 * front of them. Nothing is written after the static row until then, which also keeps early-open happy: a partition
 * is either wholly in the data file or not at all. The row index (Rows.db) is never used: every partition is
 * "unindexed" and the block directory plays its part.
 */
class TscolPartitionWriter extends BtiFormatPartitionWriter
{
    static final int KIND_ROWS = 0;
    static final int KIND_COLUMNAR = 1;
    static final long BLOCK_MILLIS = 900_000L;

    private final TableMetadata metadata;
    private final SequentialWriter dataWriter;

    private boolean rowsMode;
    private final List<Row> pending = new ArrayList<>();
    private long pendingBlock;
    private final List<ByteBuffer> blocks = new ArrayList<>();
    private final List<long[]> directory = new ArrayList<>();          // {minMillis, maxMillis, rowCount}

    TscolPartitionWriter(SerializationHeader header,
                         TableMetadata metadata,
                         SequentialWriter dataWriter,
                         SequentialWriter rowIndexWriter,
                         Version version)
    {
        // An index block size no partition reaches: rows are never indexed in Rows.db (see the class comment).
        super(header, metadata.comparator, dataWriter, rowIndexWriter, version, Integer.MAX_VALUE);
        this.metadata = metadata;
        this.dataWriter = dataWriter;
    }

    @Override
    public void reset()
    {
        super.reset();
        rowsMode = false;
        pending.clear();
        blocks.clear();
        directory.clear();
    }

    @Override
    public void addUnfiltered(Unfiltered unfiltered) throws IOException
    {
        if (rowsMode)
        {
            super.addUnfiltered(unfiltered);
            return;
        }
        if (!(unfiltered instanceof Row))
        {
            switchToRows();
            super.addUnfiltered(unfiltered);
            return;
        }

        Row row = (Row) unfiltered;
        long block = Math.floorDiv(millis(row), BLOCK_MILLIS);
        if (!pending.isEmpty() && block != pendingBlock && !encodePending())
        {
            switchToRows();
            super.addUnfiltered(row);
            return;
        }
        pendingBlock = block;
        pending.add(row);
    }

    @Override
    public long finish() throws IOException
    {
        if (!rowsMode && !pending.isEmpty() && !encodePending())
            switchToRows();

        if (!rowsMode)
        {
            dataWriter.writeByte(KIND_COLUMNAR);
            dataWriter.writeUnsignedVInt32(blocks.size());
            for (int i = 0; i < blocks.size(); i++)
            {
                long[] entry = directory.get(i);
                dataWriter.writeLong(entry[0]);
                dataWriter.writeLong(entry[1]);
                dataWriter.writeUnsignedVInt32((int) entry[2]);
                dataWriter.writeUnsignedVInt32(blocks.get(i).remaining());
            }
            for (ByteBuffer block : blocks)
                dataWriter.write(block.duplicate());
        }
        // BTI's end-of-partition byte in both cases; with no row ever added this returns -1, i.e. no row index.
        return super.finish();
    }

    /** @return false if the codec refused the pending rows, which stay pending for {@link #switchToRows}. */
    private boolean encodePending()
    {
        ByteBuffer encoded = RowBlockCodec.encode(metadata, pending);
        if (encoded == null)
            return false;
        long first = millis(pending.get(0)), last = millis(pending.get(pending.size() - 1));
        directory.add(new long[]{ Math.min(first, last), Math.max(first, last), pending.size() });
        blocks.add(encoded);
        pending.clear();
        return true;
    }

    private void switchToRows() throws IOException
    {
        rowsMode = true;
        dataWriter.writeByte(KIND_ROWS);
        for (ByteBuffer block : blocks)
            for (Row row : RowBlockCodec.decode(metadata, block, null))
                super.addUnfiltered(row);
        for (Row row : pending)
            super.addUnfiltered(row);
        blocks.clear();
        directory.clear();
        pending.clear();
    }

    private static long millis(Row row)
    {
        return TimestampType.instance.compose(row.clustering().bufferAt(0)).getTime();
    }
}
