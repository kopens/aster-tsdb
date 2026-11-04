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

import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.db.timeseries.RowBlockCodec;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.schema.TableMetadata;

/**
 * The columnar half of a {@link TscolFormat} partition body, after its kind byte (layout: {@link TscolPartitionWriter}).
 * Public for {@code SSTableSimpleIterator}, the sequential reader compaction, repair validation, scrub, verify and index
 * builds go through.
 */
public final class TscolBody
{
    public static final int KIND_ROWS = TscolPartitionWriter.KIND_ROWS;
    public static final int KIND_COLUMNAR = TscolPartitionWriter.KIND_COLUMNAR;

    /** Decides from a block's {@code [minMillis, maxMillis]} whether it is decoded or skipped. */
    public interface BlockSelector
    {
        boolean wants(long minMillis, long maxMillis);
    }

    public static final BlockSelector ALL_BLOCKS = (min, max) -> true;

    private TscolBody()
    {
    }

    /**
     * Reads the columnar body and the end-of-partition byte after it.
     * @param columns the read's column filter, or {@code null} for every column
     * @return the selected blocks' rows, in the table's clustering order
     */
    public static List<Row> readColumnar(TableMetadata metadata, DataInputPlus in, ColumnFilter columns, BlockSelector selector) throws IOException
    {
        int blockCount = in.readUnsignedVInt32();
        long[] min = new long[blockCount], max = new long[blockCount];
        int[] length = new int[blockCount];
        for (int i = 0; i < blockCount; i++)
        {
            min[i] = in.readLong();
            max[i] = in.readLong();
            in.readUnsignedVInt32();                                           // row count, for tools
            length[i] = in.readUnsignedVInt32();
        }
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < blockCount; i++)
        {
            if (!selector.wants(min[i], max[i]))
            {
                in.skipBytesFully(length[i]);
                continue;
            }
            byte[] block = new byte[length[i]];
            in.readFully(block);
            rows.addAll(RowBlockCodec.decode(metadata, ByteBuffer.wrap(block), columns));
        }
        if (!UnfilteredSerializer.isEndOfPartition(in.readUnsignedByte()))
            throw new IOException("columnar partition body of " + metadata + " is not followed by an end-of-partition byte");
        return rows;
    }
}
