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

import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.timeseries.tiering.TieredStorageService;
import org.apache.cassandra.db.timeseries.tiering.TieringPolicy;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;

/**
 * {@code system_views.timeseries_tiering}: one row per table with a {@code timeseries_tiering} policy
 * (see {@link TieringPolicy}), showing the policy's windows/interval alongside
 * {@link TieredStorageService}'s last-completed-run stats for that table.
 */
final class TimeseriesTieringTable extends AbstractVirtualTable
{
    private static final String KEYSPACE_NAME = "keyspace_name";
    private static final String TABLE_NAME = "table_name";
    private static final String HOT_WINDOW_MS = "hot_window_ms";
    private static final String CHUNK_WINDOW_MS = "chunk_window_ms";
    private static final String COLD_WINDOW_MS = "cold_window_ms";
    private static final String INTERVAL_MS = "interval_ms";
    private static final String LAST_RUN_AT = "last_run_at";
    private static final String WINDOWS_ENCODED = "windows_encoded";
    private static final String ROWS_ENCODED = "rows_encoded";
    private static final String LATE_MERGES = "late_merges";
    private static final String CHUNKS_EXPIRED = "chunks_expired";
    private static final String TAGS_SKIPPED = "tags_skipped";
    private static final String LAST_ATTEMPT_AT = "last_attempt_at";
    private static final String CONSECUTIVE_FAILURES = "consecutive_failures";
    private static final String LAST_ERROR = "last_error";

    TimeseriesTieringTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "timeseries_tiering")
                           .comment("tiered storage policy and re-encode status for time-series tables")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(KEYSPACE_NAME, UTF8Type.instance)
                           .addClusteringColumn(TABLE_NAME, UTF8Type.instance)
                           .addRegularColumn(HOT_WINDOW_MS, LongType.instance)
                           .addRegularColumn(CHUNK_WINDOW_MS, LongType.instance)
                           .addRegularColumn(COLD_WINDOW_MS, LongType.instance)
                           .addRegularColumn(INTERVAL_MS, LongType.instance)
                           .addRegularColumn(LAST_RUN_AT, LongType.instance)
                           .addRegularColumn(WINDOWS_ENCODED, LongType.instance)
                           .addRegularColumn(ROWS_ENCODED, LongType.instance)
                           .addRegularColumn(LATE_MERGES, LongType.instance)
                           .addRegularColumn(CHUNKS_EXPIRED, LongType.instance)
                           .addRegularColumn(TAGS_SKIPPED, LongType.instance)
                           .addRegularColumn(LAST_ATTEMPT_AT, LongType.instance)
                           .addRegularColumn(CONSECUTIVE_FAILURES, LongType.instance)
                           .addRegularColumn(LAST_ERROR, UTF8Type.instance)
                           .build());
    }

    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        for (KeyspaceMetadata keyspace : Schema.instance.getUserKeyspaces())
        {
            for (TableMetadata table : keyspace.tables)
            {
                TieringPolicy policy;
                try
                {
                    policy = TieringPolicy.fromTable(table);
                }
                catch (ConfigurationException e)
                {
                    // Shown, not omitted: a table whose policy does not parse is not being tiered at
                    // all, and dropping its row made that the easiest state to miss.
                    result.row(keyspace.name, table.name)
                          .column(LAST_ERROR, "invalid timeseries_tiering policy: " + e.getMessage());
                    continue;
                }
                if (policy == null)
                    continue;

                TieredStorageService.TierRunStats stats =
                        TieredStorageService.instance.lastStats(keyspace.name, table.name);
                Long lastRunAt = TieredStorageService.instance.lastRunAtMillis(keyspace.name, table.name);
                Long lastAttemptAt = TieredStorageService.instance.lastAttemptAtMillis(keyspace.name, table.name);

                result.row(keyspace.name, table.name)
                      .column(HOT_WINDOW_MS, policy.hotWindowMillis)
                      .column(CHUNK_WINDOW_MS, policy.chunkWindowMillis)
                      .column(COLD_WINDOW_MS, policy.coldWindowMillis)
                      .column(INTERVAL_MS, policy.intervalMillis)
                      .column(LAST_RUN_AT, lastRunAt == null ? -1L : lastRunAt)
                      .column(WINDOWS_ENCODED, stats == null ? 0L : stats.windowsEncoded)
                      .column(ROWS_ENCODED, stats == null ? 0L : stats.rowsEncoded)
                      .column(LATE_MERGES, stats == null ? 0L : stats.lateMerges)
                      .column(CHUNKS_EXPIRED, stats == null ? 0L : stats.chunksExpired)
                      // Non-zero means the last completed cycle did NOT encode everything below the
                      // cutoff: those tags failed and were skipped, so the table is not tiered as far
                      // as the other counters suggest.
                      .column(TAGS_SKIPPED, stats == null ? 0L : stats.tagsSkipped)
                      .column(LAST_ATTEMPT_AT, lastAttemptAt == null ? -1L : lastAttemptAt)
                      // Non-zero means tiering of this table is currently failing -- last_run_at only
                      // moves on runs that complete, so it alone just looks stale.
                      .column(CONSECUTIVE_FAILURES,
                              (long) TieredStorageService.instance.consecutiveFailures(keyspace.name, table.name))
                      .column(LAST_ERROR, TieredStorageService.instance.lastError(keyspace.name, table.name));
            }
        }

        return result;
    }
}
