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
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Reads one {@link TscolFormat} partition body (layout: {@link TscolPartitionWriter}) into an
 * {@link ImmutableBTreePartition}, which then slices, reverses and clips range tombstones exactly as an in-memory
 * partition does. Columns are filtered while decoding, by the same rules the row-format deserializer applies, so the
 * rows handed out are the ones a BTI sstable holding the same data would hand out.
 */
final class ColumnarPartitions
{
    private ColumnarPartitions()
    {
    }

    /**
     * @param in positioned just after the partition key; left just after the partition's end-of-partition byte
     * @param slices the clusterings the caller will read, used only to skip blocks that cannot intersect them;
     *               {@link Slices#ALL} decodes every block
     */
    static ImmutableBTreePartition read(SSTableReader sstable, FileDataInput in, DecoratedKey key,
                                        ColumnFilter columns, Slices slices) throws IOException
    {
        TableMetadata metadata = sstable.metadata();
        DeserializationHelper helper = new DeserializationHelper(metadata, sstable.descriptor.version.correspondingMessagingVersion(),
                                                                 DeserializationHelper.Flag.LOCAL, columns);
        DeletionTime partitionDeletion = DeletionTime.getSerializer(sstable.descriptor.version).deserialize(in);

        Row staticRow = Rows.EMPTY_STATIC_ROW;
        if (sstable.header.hasStatic())
        {
            if (columns.fetchedColumns().statics.isEmpty())
                UnfilteredSerializer.serializer.skipStaticRow(in, sstable.header, helper);
            else
                staticRow = UnfilteredSerializer.serializer.deserializeStaticRow(in, sstable.header, helper);
        }

        List<Unfiltered> unfiltereds = new ArrayList<>();
        int kind = in.readUnsignedByte();
        if (kind == TscolPartitionWriter.KIND_ROWS)
        {
            Row.Builder builder = BTreeRow.sortedBuilder();
            Unfiltered unfiltered;
            while ((unfiltered = UnfilteredSerializer.serializer.deserialize(in, sstable.header, helper, builder)) != null)
                unfiltereds.add(unfiltered);
        }
        else if (kind == TscolPartitionWriter.KIND_COLUMNAR)
        {
            unfiltereds.addAll(TscolBody.readColumnar(metadata, in, columns, (min, max) -> mayIntersect(metadata, slices, min, max)));
        }
        else
        {
            throw new CorruptSSTableException(new IOException("unknown partition body kind " + kind + " for " + key), in.getPath());
        }

        return ImmutableBTreePartition.create(new ListIterator(metadata, key, partitionDeletion, columns, staticRow, unfiltereds, sstable),
                                              unfiltereds.size(), true);
    }

    /** The partition, restricted to {@code slices} in the requested direction. */
    static UnfilteredRowIterator iterator(ImmutableBTreePartition partition, Slices slices, boolean reversed)
    {
        return partition.unfilteredIterator(ColumnFilter.all(partition.columns()), slices, reversed);
    }

    private static boolean mayIntersect(TableMetadata metadata, Slices slices, long minMillis, long maxMillis)
    {
        if (slices == Slices.ALL)
            return true;
        Clustering<ByteBuffer> low = Clustering.make(TimestampType.instance.fromTimeInMillis(minMillis));
        Clustering<ByteBuffer> high = Clustering.make(TimestampType.instance.fromTimeInMillis(maxMillis));
        // In a DESC table the newest clustering sorts first.
        boolean descending = metadata.clusteringColumns().get(0).type.isReversed();
        Slice block = Slice.make(ClusteringBound.inclusiveStartOf(descending ? high : low),
                                 ClusteringBound.inclusiveEndOf(descending ? low : high));
        return slices.intersects(block);
    }

    private static final class ListIterator extends AbstractUnfilteredRowIterator
    {
        private final Iterator<Unfiltered> source;

        ListIterator(TableMetadata metadata, DecoratedKey key, DeletionTime partitionDeletion, ColumnFilter columns,
                     Row staticRow, List<Unfiltered> unfiltereds, SSTableReader sstable)
        {
            super(metadata, key, partitionDeletion, columns.fetchedColumns(), staticRow, false, sstable.stats());
            this.source = unfiltereds.iterator();
        }

        @Override
        protected Unfiltered computeNext()
        {
            return source.hasNext() ? source.next() : endOfData();
        }
    }
}
