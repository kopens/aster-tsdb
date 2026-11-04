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
package org.apache.cassandra.io.sstable;

import java.io.IOError;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.bti.TscolBody;
import org.apache.cassandra.io.sstable.format.bti.TscolFormat;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;

/**
 * Utility class to handle deserializing atom from sstables.
 *
 * Note that this is not a full fledged UnfilteredRowIterator. It's also not closeable, it is always
 * the job of the user to close the underlying ressources.
 */

public abstract class SSTableSimpleIterator extends AbstractIterator<Unfiltered> implements Iterator<Unfiltered>
{
    final TableMetadata metadata;
    protected final DataInputPlus in;
    protected final DeserializationHelper helper;

    private SSTableSimpleIterator(TableMetadata metadata, DataInputPlus in, DeserializationHelper helper)
    {
        this.metadata = metadata;
        this.in = in;
        this.helper = helper;
    }

    public static SSTableSimpleIterator create(TableMetadata metadata, DataInputPlus in, SerializationHeader header, DeserializationHelper helper, DeletionTime partitionDeletion)
    {
        return new CurrentFormatIterator(metadata, in, header, helper);
    }

    public static SSTableSimpleIterator createTombstoneOnly(TableMetadata metadata, DataInputPlus in, SerializationHeader header, DeserializationHelper helper, DeletionTime partitionDeletion)
    {
        return new CurrentFormatTombstoneIterator(metadata, in, header, helper);
    }

    // aster-tsdb (tscol): a tscol partition body is a kind byte after the static row, then either these same rows or
    // columnar blocks; every sequential reader (compaction, validation, scrub, verify, index builds) comes through here.
    public static SSTableSimpleIterator create(SSTableReader sstable, TableMetadata metadata, DataInputPlus in,
                                              DeserializationHelper helper, DeletionTime partitionDeletion, boolean tombstoneOnly)
    {
        SSTableSimpleIterator rows = tombstoneOnly
                                     ? createTombstoneOnly(metadata, in, sstable.header, helper, partitionDeletion)
                                     : create(metadata, in, sstable.header, helper, partitionDeletion);
        return TscolFormat.is(sstable.descriptor.getFormat()) ? new ColumnarFormatIterator(metadata, in, helper, rows, tombstoneOnly) : rows;
    }

    public abstract Row readStaticRow() throws IOException;

    private static class ColumnarFormatIterator extends SSTableSimpleIterator
    {
        private final SSTableSimpleIterator rows;
        private final boolean tombstoneOnly;
        private Iterator<Row> columnar;

        private ColumnarFormatIterator(TableMetadata metadata, DataInputPlus in, DeserializationHelper helper,
                                       SSTableSimpleIterator rows, boolean tombstoneOnly)
        {
            super(metadata, in, helper);
            this.rows = rows;
            this.tombstoneOnly = tombstoneOnly;
        }

        public Row readStaticRow() throws IOException
        {
            Row staticRow = rows.readStaticRow();
            int kind = in.readUnsignedByte();
            if (kind == TscolBody.KIND_COLUMNAR)
            {
                // Columnar blocks hold no range tombstones and no row deletions, so a tombstone-only read takes nothing.
                List<Row> decoded = TscolBody.readColumnar(metadata, in, null, TscolBody.ALL_BLOCKS);
                columnar = tombstoneOnly ? Collections.emptyIterator() : decoded.iterator();
            }
            else if (kind != TscolBody.KIND_ROWS)
            {
                throw new IOException("unknown tscol partition body kind " + kind);
            }
            return staticRow;
        }

        protected Unfiltered computeNext()
        {
            if (columnar != null)
                return columnar.hasNext() ? columnar.next() : endOfData();
            return rows.hasNext() ? rows.next() : endOfData();
        }
    }

    private static class CurrentFormatIterator extends SSTableSimpleIterator
    {
        private final SerializationHeader header;

        private final Row.Builder builder;

        private CurrentFormatIterator(TableMetadata metadata, DataInputPlus in, SerializationHeader header, DeserializationHelper helper)
        {
            super(metadata, in, helper);
            this.header = header;
            this.builder = BTreeRow.sortedBuilder();
        }

        public Row readStaticRow() throws IOException
        {
            return header.hasStatic() ? UnfilteredSerializer.serializer.deserializeStaticRow(in, header, helper) : Rows.EMPTY_STATIC_ROW;
        }

        protected Unfiltered computeNext()
        {
            try
            {
                Unfiltered unfiltered = UnfilteredSerializer.serializer.deserialize(in, header, helper, builder);
                return unfiltered == null ? endOfData() : unfiltered;
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }
    }

    private static class CurrentFormatTombstoneIterator extends SSTableSimpleIterator
    {
        private final SerializationHeader header;

        private CurrentFormatTombstoneIterator(TableMetadata metadata, DataInputPlus in, SerializationHeader header, DeserializationHelper helper)
        {
            super(metadata, in, helper);
            this.header = header;
        }

        public Row readStaticRow() throws IOException
        {
            if (header.hasStatic())
            {
                Row staticRow = UnfilteredSerializer.serializer.deserializeStaticRow(in, header, helper);
                if (!staticRow.deletion().isLive())
                    return BTreeRow.emptyDeletedRow(staticRow.clustering(), staticRow.deletion());
            }
            return Rows.EMPTY_STATIC_ROW;
        }

        protected Unfiltered computeNext()
        {
            try
            {
                Unfiltered unfiltered = UnfilteredSerializer.serializer.deserializeTombstonesOnly((FileDataInput) in, header, helper);
                return unfiltered == null ? endOfData() : unfiltered;
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }
    }

    @VisibleForTesting
    public static class EmptySSTableSimpleIterator extends SSTableSimpleIterator
    {
        public EmptySSTableSimpleIterator(TableMetadata metadata)
        {
            super(metadata, null, null);
        }

        public Row readStaticRow() throws IOException
        {
            return Rows.EMPTY_STATIC_ROW;
        }

        protected Unfiltered computeNext()
        {
            return null;
        }

        public boolean hasNext()
        {
            return false;
        }
    }
}
