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
package org.apache.cassandra.db.virtual;

import java.util.Date;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.TimeSeriesCompactionStrategy;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.TableMetadata;

/**
 * {@code system_views.timeseries_windows}: one row per TimeSeriesCompactionStrategy window of every table
 * using it, on this node. Answers the questions the 2026-09-24 production timeout needed answered by hand:
 * is any sstable spanning windows ({@code spanning_sstables > 0} should never happen once flushes and
 * delegate compactions are window-contained), and does a window hold far more rows than it should (rows
 * the tiering re-encoder has deleted but no rewrite has yet merged with their tombstones)?
 * <pre>
 * SELECT * FROM system_views.timeseries_windows WHERE keyspace_name = 'pp' AND table_name = 'tm_tag_point';
 * </pre>
 */
final class TimeseriesWindowsTable extends AbstractVirtualTable
{
    private static final String KEYSPACE_NAME = "keyspace_name";
    private static final String TABLE_NAME = "table_name";
    private static final String WINDOW_START = "window_start";
    private static final String STATE = "state";
    private static final String PARKED = "parked";
    private static final String SSTABLES = "sstables";
    private static final String SPANNING_SSTABLES = "spanning_sstables";
    private static final String BYTES_ON_DISK = "bytes_on_disk";
    private static final String ROWS = "rows";
    private static final String DROPPABLE_TOMBSTONE_RATIO = "droppable_tombstone_ratio";

    TimeseriesWindowsTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "timeseries_windows")
                           .comment("per-window state of TimeSeriesCompactionStrategy tables on this node")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(CompositeType.getInstance(UTF8Type.instance, UTF8Type.instance)))
                           .addPartitionKeyColumn(KEYSPACE_NAME, UTF8Type.instance)
                           .addPartitionKeyColumn(TABLE_NAME, UTF8Type.instance)
                           .addClusteringColumn(WINDOW_START, TimestampType.instance)
                           .addRegularColumn(STATE, UTF8Type.instance)
                           .addRegularColumn(PARKED, BooleanType.instance)
                           .addRegularColumn(SSTABLES, Int32Type.instance)
                           .addRegularColumn(SPANNING_SSTABLES, Int32Type.instance)
                           .addRegularColumn(BYTES_ON_DISK, LongType.instance)
                           .addRegularColumn(ROWS, LongType.instance)
                           .addRegularColumn(DROPPABLE_TOMBSTONE_RATIO, DoubleType.instance)
                           .build());
    }

    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        for (Keyspace keyspace : Keyspace.all())
        {
            for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
            {
                for (TimeSeriesCompactionStrategy.WindowReport r : cfs.getTimeSeriesWindowReports())
                {
                    result.row(keyspace.getName(), cfs.getTableName(), new Date(r.windowStart))
                          .column(STATE, r.state)
                          .column(PARKED, r.parked)
                          .column(SSTABLES, r.sstables)
                          .column(SPANNING_SSTABLES, r.spanning)
                          .column(BYTES_ON_DISK, r.bytesOnDisk)
                          .column(ROWS, r.rows)
                          .column(DROPPABLE_TOMBSTONE_RATIO, r.droppableTombstoneRatio);
                }
            }
        }
        return result;
    }
}
