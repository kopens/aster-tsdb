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

import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.SequentialWriter;

/**
 * BTI's writer with {@link TscolPartitionWriter} bodies. Statistics, the bloom filter, the partition index and index
 * observers are all driven per row by {@code SortedTableWriter.append} before the partition writer sees the row, so
 * they are exactly what BTI produces for the same data.
 */
public class TscolTableWriter extends BtiTableWriter
{
    public TscolTableWriter(Builder builder, ILifecycleTransaction txn, Owner owner)
    {
        super(builder, txn, owner);
    }

    @Override
    protected BtiTableReader.Builder newReaderBuilder()
    {
        return new TscolTableReader.Builder(descriptor);
    }

    public static class Builder extends BtiTableWriter.Builder
    {
        public Builder(Descriptor descriptor)
        {
            super(descriptor);
        }

        @Override
        protected BtiFormatPartitionWriter newPartitionWriter(SequentialWriter dataWriter, SequentialWriter rowIndexWriter)
        {
            return new TscolPartitionWriter(getSerializationHeader(), getTableMetadataRef().getLocal(), dataWriter,
                                            rowIndexWriter, descriptor.version);
        }

        @Override
        protected BtiTableWriter newWriter(ILifecycleTransaction txn, Owner owner)
        {
            return new TscolTableWriter(this, txn, owner);
        }
    }
}
