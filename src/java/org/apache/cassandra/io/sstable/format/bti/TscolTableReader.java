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

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * BTI's reader for {@link TscolFormat} sstables: partition lookup, bloom filter, key iteration and scanners are BTI's;
 * only turning a partition's body into rows differs. Every row read of this reader -- point reads and the scanner's
 * per-partition reads alike -- funnels through the one {@code rowIterator} overload overridden here.
 */
public class TscolTableReader extends BtiTableReader
{
    public TscolTableReader(Builder builder, Owner owner)
    {
        super(builder, owner);
    }

    @Override
    protected BtiTableReader.Builder newBuilder()
    {
        return new Builder(descriptor);
    }

    @Override
    UnfilteredRowIterator rowIterator(FileDataInput dataFileInput,
                                      boolean ownsDataFileInput,
                                      DecoratedKey key,
                                      TrieIndexEntry indexEntry,
                                      Slices slices,
                                      ColumnFilter selectedColumns,
                                      boolean reversed)
    {
        if (indexEntry == null)
            return super.rowIterator(dataFileInput, ownsDataFileInput, key, indexEntry, slices, selectedColumns, reversed);

        boolean close = ownsDataFileInput || dataFileInput == null;
        FileDataInput in = dataFileInput == null ? getFileDataInput(indexEntry.position) : dataFileInput;
        try
        {
            in.seek(indexEntry.position);
            ByteBufferUtil.skipShortLength(in);                               // the partition key
            return ColumnarPartitions.read(this, in, key, selectedColumns, slices, reversed);
        }
        catch (IOException | CorruptSSTableException e)
        {
            markSuspect();
            throw e instanceof CorruptSSTableException ? (CorruptSSTableException) e : new CorruptSSTableException(e, in.getPath());
        }
        finally
        {
            if (close)
                FileUtils.closeQuietly(in);
        }
    }

    public static class Builder extends BtiTableReader.Builder
    {
        public Builder(Descriptor descriptor)
        {
            super(descriptor);
        }

        @Override
        protected BtiTableReader buildInternal(Owner owner)
        {
            return new TscolTableReader(this, owner);
        }
    }
}
