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

import java.util.Map;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * The columnar frozen-window format ({@code tscol}): BTI's partition index, bloom filter, statistics and other
 * components unchanged, with each partition's rows stored as {@link org.apache.cassandra.db.timeseries.RowBlockCodec}
 * blocks instead of one serialized row after another
 * (docs/superpowers/specs/2026-11-04-columnar-frozen-sstable-design.md section 4.1). Only time-series compaction's
 * freeze writes it; {@code selected_format} never names it, so nothing else does.
 */
public class TscolFormat extends BtiFormat
{
    public static final String NAME = "tscol";

    public TscolFormat(Map<String, String> options)
    {
        super(NAME, options);
    }

    /** The registered instance (one per process, like every format). */
    public static TscolFormat instance()
    {
        return (TscolFormat) DatabaseDescriptor.getSSTableFormats().get(NAME);
    }

    public static boolean is(SSTableFormat<?, ?> format)
    {
        return format.name().equals(NAME);
    }

    private static final TscolReaderFactory tscolReaderFactory = new TscolReaderFactory();
    private static final TscolWriterFactory tscolWriterFactory = new TscolWriterFactory();

    @Override
    public BtiTableReaderFactory getReaderFactory()
    {
        return tscolReaderFactory;
    }

    @Override
    public BtiTableWriterFactory getWriterFactory()
    {
        return tscolWriterFactory;
    }

    static class TscolReaderFactory extends BtiTableReaderFactory
    {
        @Override
        public SSTableReader.Builder<BtiTableReader, BtiTableReader.Builder> builder(Descriptor descriptor)
        {
            return new TscolTableReader.Builder(descriptor);
        }
    }

    static class TscolWriterFactory extends BtiTableWriterFactory
    {
        @Override
        public BtiTableWriter.Builder builder(Descriptor descriptor)
        {
            return new TscolTableWriter.Builder(descriptor);
        }
    }

    @SuppressWarnings("unused")
    public static class TscolFormatFactory implements Factory
    {
        @Override
        public String name()
        {
            return NAME;
        }

        @Override
        public SSTableFormat<?, ?> getInstance(Map<String, String> options)
        {
            return new TscolFormat(options);
        }
    }
}
