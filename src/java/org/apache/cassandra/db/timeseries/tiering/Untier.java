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

package org.apache.cassandra.db.timeseries.tiering;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.transport.Dispatcher;

import static java.lang.String.format;

/**
 * {@code nodetool untier}: the one-way exit from shadow-table tiering, ahead of its removal
 * (docs/superpowers/specs/2026-11-04-columnar-frozen-sstable-design.md section 4.8). Every chunk in
 * {@code <table>__chunks} is decoded back into rows of the base table, written at {@code max_row_writetime + 1} --
 * one above the range tombstone the re-encoder deleted the originals with, so they become visible again -- then read
 * back and compared, and only then is that chunk row deleted. Time-series compaction then freezes the restored
 * windows as usual (into {@code tscol} with {@code frozen_format = 'columnar'}).
 * <p>
 * Order matters and makes the run safe to stop and repeat:
 * <ol>
 * <li>the tiering policy is detached first, so the re-encoder does not chunk the restored rows again; transparent
 *     reads keep merging the chunks that are left (they follow the chunk table, not the policy), so no window is
 *     ever invisible while this runs;</li>
 * <li>a chunk is deleted only after every row decoded from it is read back from the base table with the restored
 *     value (or a newer one written since);</li>
 * <li>rewriting a window that was already restored writes the same cells at the same timestamp, so a rerun after a
 *     crash is harmless;</li>
 * <li>the shadow tables are dropped only once the chunk table is empty.</li>
 * </ol>
 * Restored rows carry no TTL: the chunks did not keep one either, and the table's compaction retention still drops
 * the windows they land in.
 */
public final class Untier
{
    private static final Logger logger = LoggerFactory.getLogger(Untier.class);

    private Untier()
    {
    }

    /** What one run did. */
    public static final class Result
    {
        public long chunksRestored;
        public long rowsRestored;
        public boolean shadowTablesDropped;

        @Override
        public String toString()
        {
            return format("restored %d rows from %d chunks; shadow tables %s", rowsRestored, chunksRestored,
                          shadowTablesDropped ? "dropped" : "kept (chunks remain -- run again)");
        }
    }

    public static Result run(String keyspace, String table, int rowsPerSecond)
    {
        TableMetadata base = Schema.instance.getTableMetadata(keyspace, table);
        if (base == null)
            throw new IllegalArgumentException(format("%s.%s does not exist", keyspace, table));
        if (Schema.instance.getTableMetadata(keyspace, ChunkTables.chunkTableName(table)) == null)
            throw new IllegalArgumentException(format("%s.%s has no chunk table: nothing to untier", keyspace, table));

        ConsistencyLevel cl = consistencyFor(base);
        detachPolicy(base);

        RateLimiter limiter = RateLimiter.create(Math.max(1, rowsPerSecond));
        Result result = new Result();
        String chunkRef = TieredStorageService.quotedRef(keyspace, ChunkTables.chunkTableName(table));
        List<ColumnMetadata> tagColumns = base.partitionKeyColumns();
        String tagList = TieredStorageService.columnList(tagColumns);
        String tagPredicate = TieredStorageService.equalityPredicate(tagColumns);
        List<String> tagNames = TieredStorageService.rawNames(tagColumns);

        TransparentReads.enterInternalBypass();
        try
        {
            for (UntypedResultSet.Row tagRow : TieredStorageService.pagedSelect(format("SELECT DISTINCT %s FROM %s", tagList, chunkRef),
                                                                                cl, List.of()))
            {
                List<ByteBuffer> tag = new ArrayList<>(tagNames.size());
                for (String name : tagNames)
                    tag.add(tagRow.getBytes(name));
                DecoratedKey key = base.partitioner.decorateKey(tag.size() == 1 ? tag.get(0) : CompositeType.build(org.apache.cassandra.db.marshal.ByteBufferAccessor.instance,
                                                                                                                  tag.toArray(new ByteBuffer[0])));

                List<UntypedResultSet.Row> windows = TieredStorageService.pagedSelect(format("SELECT window_start FROM %s WHERE %s", chunkRef, tagPredicate), cl, tag);
                for (UntypedResultSet.Row window : windows)
                {
                    ByteBuffer windowStart = window.getBytes("window_start");
                    UntypedResultSet chunk = QueryProcessor.process(format("SELECT max_row_writetime, payload FROM %s WHERE %s AND window_start = ?", chunkRef, tagPredicate),
                                                                    cl, TieredStorageService.boundTo(tag, windowStart));
                    if (chunk.isEmpty())
                        continue;
                    long maxWt = chunk.one().getLong("max_row_writetime");
                    List<Row> rows = restoredRows(base, chunk.one().getBytes("payload"), maxWt);
                    if (!rows.isEmpty())
                    {
                        limiter.acquire(rows.size());
                        write(base, key, rows, cl);
                        verify(base, tag, tagPredicate, rows, maxWt, cl);
                    }
                    QueryProcessor.process(format("DELETE FROM %s WHERE %s AND window_start = ?", chunkRef, tagPredicate),
                                           cl, TieredStorageService.boundTo(tag, windowStart));
                    result.chunksRestored++;
                    result.rowsRestored += rows.size();
                }
            }
        }
        finally
        {
            TransparentReads.exitInternalBypass();
        }

        if (QueryProcessor.process(format("SELECT window_start FROM %s LIMIT 1", chunkRef), cl).isEmpty())
        {
            for (String shadow : List.of(ChunkTables.chunkTableName(table), ChunkTables.coverageTableName(table), ChunkTables.tagsTableName(table)))
                if (Schema.instance.getTableMetadata(keyspace, shadow) != null)
                    QueryProcessor.process(format("DROP TABLE IF EXISTS %s", TieredStorageService.quotedRef(keyspace, shadow)), ConsistencyLevel.ONE);
            ChunkCoverage.invalidateAll();
            result.shadowTablesDropped = true;
        }
        logger.info("Untier of {}.{}: {}", keyspace, table, result);
        return result;
    }

    private static ConsistencyLevel consistencyFor(TableMetadata base)
    {
        try
        {
            TieringPolicy policy = TieringPolicy.fromTable(base);
            if (policy != null)
                return policy.consistency;
        }
        catch (ConfigurationException ignored)
        {
            // an unreadable policy: the default below
        }
        return ConsistencyLevel.LOCAL_QUORUM;
    }

    /** Removes the {@code timeseries_tiering} extension, keeping any other. */
    private static void detachPolicy(TableMetadata base)
    {
        if (!base.params.extensions.containsKey(TieringPolicy.EXTENSION_KEY))
            return;
        StringBuilder map = new StringBuilder();
        for (Map.Entry<String, ByteBuffer> e : base.params.extensions.entrySet())
        {
            if (e.getKey().equals(TieringPolicy.EXTENSION_KEY))
                continue;
            map.append(map.length() == 0 ? "" : ", ").append('\'').append(e.getKey().replace("'", "''")).append("': 0x")
               .append(org.apache.cassandra.utils.ByteBufferUtil.bytesToHex(e.getValue()));
        }
        QueryProcessor.process(format("ALTER TABLE %s WITH extensions = {%s}", TieredStorageService.quotedRef(base.keyspace, base.name), map),
                               ConsistencyLevel.ONE);
    }

    /** The chunk's rows at {@code max_row_writetime + 1}, each with a primary-key liveness like the INSERT it came from. */
    private static List<Row> restoredRows(TableMetadata base, ByteBuffer payload, long maxWt)
    {
        long restoredAt = maxWt == Long.MAX_VALUE ? Long.MAX_VALUE : maxWt + 1;
        List<Row> rows = new ArrayList<>();
        Iterator<Row> decoded = ChunkReadSupport.rowsFromChunk(base, payload, maxWt, Long.MIN_VALUE, Long.MAX_VALUE, false, null);
        Row.Builder builder = BTreeRow.sortedBuilder();
        while (decoded.hasNext())
        {
            Row row = decoded.next();
            builder.newRow(row.clustering());
            builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(restoredAt));
            for (ColumnData data : row)
                builder.addCell(((Cell<?>) data).withUpdatedTimestampAndLocalDeletionTime(restoredAt, Cell.NO_DELETION_TIME));
            rows.add(builder.build());
        }
        return rows;
    }

    private static void write(TableMetadata base, DecoratedKey key, List<Row> rows, ConsistencyLevel cl)
    {
        PartitionUpdate.Builder update = new PartitionUpdate.Builder(base, key, base.regularAndStaticColumns(), rows.size());
        for (Row row : rows)
            update.add(row);
        StorageProxy.mutate(List.of(new Mutation(update.build())), cl, Dispatcher.RequestTime.forImmediateExecution());
    }

    /**
     * Every restored row must read back from the base table with the restored value -- or with a newer one, written
     * after the window was chunked. Throws before the chunk is deleted otherwise.
     */
    private static void verify(TableMetadata base, List<ByteBuffer> tag, String tagPredicate, List<Row> rows, long maxWt, ConsistencyLevel cl)
    {
        ColumnMetadata ts = base.clusteringColumns().get(0);
        StringBuilder select = new StringBuilder(ts.name.toCQLString());
        List<ColumnMetadata> regulars = new ArrayList<>();
        base.regularColumns().forEach(regulars::add);
        for (ColumnMetadata c : regulars)
            select.append(", ").append(c.name.toCQLString()).append(", writetime(").append(c.name.toCQLString()).append(") AS \"wt_").append(c.name).append('"');

        long first = millis(rows.get(0)), last = millis(rows.get(rows.size() - 1));
        List<UntypedResultSet.Row> read = TieredStorageService.pagedSelect(
            format("SELECT %s FROM %s WHERE %s AND %s >= ? AND %s <= ?", select, TieredStorageService.quotedRef(base.keyspace, base.name),
                   tagPredicate, ts.name.toCQLString(), ts.name.toCQLString()),
            cl, TieredStorageService.boundTo(tag, TimestampType.instance.fromTimeInMillis(Math.min(first, last)),
                                             TimestampType.instance.fromTimeInMillis(Math.max(first, last))));
        Map<Long, UntypedResultSet.Row> byTime = new HashMap<>();
        for (UntypedResultSet.Row r : read)
            byTime.put(r.getTimestamp(ts.name.toString()).getTime(), r);

        for (Row row : rows)
        {
            UntypedResultSet.Row back = byTime.get(millis(row));
            if (back == null)
                throw new IllegalStateException(format("untier of %s: restored row %s is not readable back; the chunk is kept",
                                                       base, row.clustering().toString(base)));
            for (ColumnData data : row)
            {
                Cell<?> cell = (Cell<?>) data;
                String name = cell.column().name.toString();
                ByteBuffer value = back.has(name) ? back.getBytes(name) : null;
                long writetime = back.has("wt_" + name) ? back.getLong("wt_" + name) : Long.MIN_VALUE;
                if (writetime > maxWt + 1)
                    continue;                                             // overwritten since; the newer value wins
                if (!Objects.equals(value, cell.buffer()))
                    throw new IllegalStateException(format("untier of %s: column %s of %s reads back differently; the chunk is kept",
                                                           base, name, row.clustering().toString(base)));
            }
        }
    }

    private static long millis(Row row)
    {
        return TimestampType.instance.compose(row.clustering().bufferAt(0)).getTime();
    }
}
