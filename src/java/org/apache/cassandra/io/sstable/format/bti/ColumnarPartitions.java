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
import org.apache.cassandra.db.timeseries.RowBlockCodec;
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
     * @param in positioned just after the partition key; left just after the partition's end-of-partition byte (every
     *           byte the iterator needs is read before this returns, so the caller may close {@code in} at once)
     * @param slices the clusterings to return; blocks that cannot intersect them are not even read
     */
    static UnfilteredRowIterator read(SSTableReader sstable, FileDataInput in, DecoratedKey key,
                                      ColumnFilter columns, Slices slices, boolean reversed) throws IOException
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

        int kind = in.readUnsignedByte();
        if (kind == TscolPartitionWriter.KIND_COLUMNAR)
        {
            // No range tombstones and no row deletions in a columnar body: rows in clustering order, filtered by the
            // slices, are the whole answer. Decoded lazily, one block at a time, in the direction asked for.
            List<ByteBuffer> blocks = TscolBody.readBlocks(metadata, in, (min, max) -> mayIntersect(metadata, slices, min, max));
            return new BlockIterator(metadata, key, partitionDeletion, columns, staticRow, reversed, sstable, blocks, slices);
        }
        if (kind != TscolPartitionWriter.KIND_ROWS)
            throw new CorruptSSTableException(new IOException("unknown partition body kind " + kind + " for " + key), in.getPath());

        // The row-format fallback (range tombstones, row deletions): materialised so slicing clips markers exactly as
        // an in-memory partition does. Rare by construction.
        List<Unfiltered> unfiltereds = new ArrayList<>();
        Row.Builder builder = BTreeRow.sortedBuilder();
        Unfiltered unfiltered;
        while ((unfiltered = UnfilteredSerializer.serializer.deserialize(in, sstable.header, helper, builder)) != null)
            unfiltereds.add(unfiltered);
        ImmutableBTreePartition partition = ImmutableBTreePartition.create(new ListIterator(metadata, key, partitionDeletion, columns, staticRow, unfiltereds, sstable),
                                                                           unfiltereds.size(), true);
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

    /** A columnar partition's rows, decoded block by block as they are pulled, kept to {@code slices}. */
    private static final class BlockIterator extends AbstractUnfilteredRowIterator
    {
        private final TableMetadata table;
        private final List<ByteBuffer> blocks;
        private final ColumnFilter columns;
        private final Slices slices;
        private int nextBlock;
        private List<Row> current = List.of();
        private int position;

        BlockIterator(TableMetadata metadata, DecoratedKey key, DeletionTime partitionDeletion, ColumnFilter columns,
                      Row staticRow, boolean reversed, SSTableReader sstable, List<ByteBuffer> blocks, Slices slices)
        {
            super(metadata, key, partitionDeletion, columns.fetchedColumns(), staticRow, reversed, sstable.stats());
            this.table = metadata;
            this.blocks = blocks;
            this.columns = columns;
            this.slices = slices;
            this.nextBlock = reversed ? blocks.size() - 1 : 0;
            this.position = reversed ? -1 : 0;                       // nothing decoded yet, in either direction
        }

        @Override
        protected Unfiltered computeNext()
        {
            while (true)
            {
                while (isReverseOrder ? position >= 0 : position < current.size())
                {
                    Row row = current.get(isReverseOrder ? position-- : position++);
                    if (slices.selects(row.clustering()))
                        return row;
                }
                if (isReverseOrder ? nextBlock < 0 : nextBlock >= blocks.size())
                    return endOfData();
                current = RowBlockCodec.decode(table, blocks.get(isReverseOrder ? nextBlock-- : nextBlock++), columns);
                position = isReverseOrder ? current.size() - 1 : 0;
            }
        }
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
