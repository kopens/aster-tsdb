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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.timeseries.ColumnarChunkCodec;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static java.lang.String.format;

/**
 * Flush-time chunk encoding for {@link org.apache.cassandra.db.memtable.TimeSeriesMemtable} (stage 4
 * of the time-series memtable design, docs/superpowers/specs/2026-08-02-timeseries-memtable-design.md
 * section 5.1): when a memtable holding a tiering-enabled table flushes rows whose chunk window is
 * already colder than the policy's {@code hot_window}, those rows are encoded straight into the
 * {@code __chunks} shadow table and <b>omitted</b> from the base sstable, instead of being written as
 * rows for {@link TieredStorageService} to later read back, encode, and range-delete. That round trip
 * dominates bulk backfill and archive loads; this removes it.
 *
 * <h2>The durability ordering rule</h2>
 * The base memtable's commitlog span is released when its flush completes, so a row omitted from the
 * base sstable before its chunk is durable is simply gone after a crash. The order is therefore
 * fixed, per the design spec:
 * <ol>
 *   <li><b>write the chunk and make it durable</b> -- the {@code INSERT} into the chunk table goes
 *       through the ordinary write path (commitlog + chunk-table memtable), and
 *       {@link CommitLog#sync} then forces the entry to disk;</li>
 *   <li><b>widen the coverage ledger</b> ({@link ChunkCoverage#claim}), so the read path knows cold
 *       data reaches these windows even after the policy is later changed or dropped;</li>
 *   <li><b>only then omit the rows</b> -- the {@link RowOmissions} this returns is what the memtable
 *       consults while writing the base sstable, and it is only ever returned after 1 and 2
 *       succeeded. Every failure path before that returns {@link #NONE}, which flushes every row
 *       exactly as today.</li>
 * </ol>
 * A crash (or any failure) between the steps is safe in both directions: before step 3 takes effect
 * the rows are still in the flushing memtable and its commitlog span, so replay restores them and
 * the re-flush finds the already-written chunk byte-identical (the encoding and its
 * {@code USING TIMESTAMP} are deterministic) and converges. The one ordering difference from the
 * re-encoder -- which claims coverage <em>before</em> writing the chunk -- is deliberate: the
 * re-encoder's step 3 is a distributed range delete whose only protection is the ledger, whereas
 * here the base rows remain readable (memtable, then sstable) until step 3, so a crash between chunk
 * write and claim merely leaves a redundant chunk that the next cycle re-recognises.
 *
 * <h2>What may be chunked</h2>
 * Only windows entirely below {@code windowStartFor(}{@link ColdBoundary#hotBoundaryMs}{@code )} --
 * the same cutoff the re-encoder computes. Hot data stays as rows: chunks are immutable and the
 * cold-write guard ({@link TieredWrites}) refuses un-writing below the cold boundary, so chunking
 * fresh data would break late backfill and ordinary updates and make {@code hot_window} meaningless.
 * On top of that, a window is left as rows whenever encoding it cannot be proven equivalent to what
 * the re-encoder would have produced:
 * <ul>
 *   <li>any live sstable's covered clustering range intersects the window -- rows already on disk
 *       for the window may pre-date the chunk, and the re-encoder's later per-column merge assumes
 *       every live base row post-dates the chunk it merges into (its range delete used to guarantee
 *       that; omission does not);</li>
 *   <li>an older, not-yet-flushed memtable still exists -- its rows for the window are about to
 *       become exactly such sstable rows;</li>
 *   <li>the partition carries any tombstone (partition, range, row or cell level) or an expired
 *       cell -- chunks encode live values only, and a dropped tombstone could resurrect what it
 *       shadows;</li>
 *   <li>a memtable cell's writetime is at or below the existing chunk's {@code max_row_writetime} --
 *       under the re-encoder such a cell would have been shadowed by its range tombstone, and
 *       encoding it would overwrite newer chunk content with older data;</li>
 *   <li>the window carries no cell writetime at all (and no prior chunk to inherit one from) --
 *       mirroring the re-encoder, which refuses to touch such windows;</li>
 *   <li>the merged window exceeds {@link TieredStorageService#maxSamplesPerWindow}.</li>
 * </ul>
 * The schema gate is exactly {@link TieringPolicy#unsupportedSchemaError} -- no stricter. A table
 * without a tiering policy pays one cached policy lookup and is otherwise untouched, and a table
 * whose chunk table does not exist yet (the re-encoder creates it on its first cycle) flushes as
 * rows rather than issuing DDL from the flush path.
 *
 * <h2>Equivalence with the re-encoder</h2>
 * The encoded chunk is built with the same inputs, in the same order, through the same
 * {@link ColumnarChunkCodec#encode} call, and written with the same statement, at the policy's
 * consistency level, with the same deterministic {@code USING TIMESTAMP max(maxWt+1, chunkWt+1)} --
 * so the payload a cold flush writes is byte-identical to what a re-encode cycle over the same rows
 * would have written, replicas that encode the same data converge without coordination, and the
 * re-encoder's own {@code chunkUnchanged} suppression recognises it. Late rows for an
 * already-chunked window are merged with the existing chunk (read at quorum strength, like the
 * re-encoder's late merge) rather than overwriting it.
 *
 * <h2>Known residual limits (documented, not silent)</h2>
 * Each replica encodes its <em>own</em> copy of the window rather than a quorum view, so a replica
 * that permanently missed a write encodes a chunk without it; determinism plus the existing-chunk
 * merge make later flushes converge on the superset, and rows redelivered by hints or repair land as
 * base rows the re-encoder merges in on its next cycle. Rows written with a client-supplied
 * {@code USING TIMESTAMP} at or below an already-written chunk's {@code max_row_writetime} remain
 * subject to the same explicit-timestamp hazards {@link ChunkReadSupport} documents for the
 * re-encoder path.
 */
public final class ColdWindowChunkFlush
{
    private static final Logger logger = LoggerFactory.getLogger(ColdWindowChunkFlush.class);

    /**
     * Serializes cold-window encoding per table across concurrently flushing memtables: two flushes
     * encoding the same (tag, window) must see each other's chunk through the existing-chunk read,
     * or the second write would silently drop the first flush's samples.
     */
    private static final ConcurrentHashMap<TableId, Object> LOCKS = new ConcurrentHashMap<>();

    /**
     * Test seam, invoked with the window start just before a chunk {@code INSERT} is issued. Lets a
     * test fail the chunk write for a chosen window and assert that the window's rows still reach the
     * base sstable -- the durability ordering this class exists to keep.
     */
    @VisibleForTesting
    public static volatile LongConsumer beforeChunkInsertForTesting;

    /** The no-omissions answer: flush every row exactly as before. */
    public static final RowOmissions NONE = new RowOmissions(Collections.emptyMap(), 0);

    private ColdWindowChunkFlush()
    {
    }

    /**
     * Which rows of a flushing memtable were encoded into durable chunks and must therefore be left
     * out of the base sstable. Immutable; only ever built after the chunk writes are durable and the
     * coverage ledger is widened.
     */
    public static final class RowOmissions
    {
        private final Map<DecoratedKey, PartitionOmission> byKey;
        private final long chunkWindowMillis;

        private RowOmissions(Map<DecoratedKey, PartitionOmission> byKey, long chunkWindowMillis)
        {
            this.byKey = byKey;
            this.chunkWindowMillis = chunkWindowMillis;
        }

        public boolean isEmpty()
        {
            return byKey.isEmpty();
        }

        /** True if any row of {@code key}'s partition is to be omitted. */
        public boolean containsKey(DecoratedKey key)
        {
            return byKey.containsKey(key);
        }

        /** True if every row of {@code key}'s partition was chunked, so nothing of it needs flushing. */
        public boolean fullyOmitted(DecoratedKey key)
        {
            PartitionOmission omission = byKey.get(key);
            return omission != null && omission.full;
        }

        /** True if {@code clustering}'s row was encoded into a chunk and must not reach the sstable. */
        public boolean shouldOmit(DecoratedKey key, Clustering<?> clustering)
        {
            PartitionOmission omission = byKey.get(key);
            if (omission == null)
                return false;
            long ts = ColdBoundary.clusteringMs(clustering);
            return omission.windows.contains(ts - Math.floorMod(ts, chunkWindowMillis));
        }
    }

    private static final class PartitionOmission
    {
        final Set<Long> windows;
        final boolean full;

        PartitionOmission(Set<Long> windows, boolean full)
        {
            this.windows = windows;
            this.full = full;
        }
    }

    /**
     * One chunk window's worth of one partition's memtable rows, collected while scanning the
     * partition. {@code unchunkable} is sticky: one disqualifying row leaves the whole window to the
     * ordinary row flush (and, later, the re-encoder).
     */
    private static final class WindowBucket
    {
        final TreeMap<Long, ByteBuffer[]> samples = new TreeMap<>();
        long maxCellWt = Long.MIN_VALUE;
        long minCellWt = Long.MAX_VALUE;
        boolean anyCellWt;
        boolean unchunkable;
    }

    /**
     * Encodes {@code memtable}'s cold windows into chunks, following the durability order in the
     * class javadoc.
     *
     * @param memtable   the flushing memtable (already switched out, so its contents are immutable)
     * @param partitions the memtable's merged flush view, one entry per distinct partition key
     * @return the rows to omit from the base sstable; {@link #NONE} (omit nothing) whenever cold
     *         flushing does not apply or anything at all went wrong -- falling back to rows is
     *         always safe, because a chunk without its rows omitted is merely redundant
     */
    public static RowOmissions encode(Memtable memtable, Supplier<Iterator<Partition>> partitions)
    {
        TableMetadata metadata = memtable.metadata();
        try
        {
            return encodeInternal(memtable, metadata, partitions);
        }
        catch (RuntimeException e)
        {
            // The flush itself must never fail because of this optimisation: rows are the fallback.
            logger.error("Cold-window chunk flush failed for {}.{}; flushing every window as rows instead " +
                         "(nothing is lost, the re-encoder will pick the windows up)",
                         metadata.keyspace, metadata.name, e);
            return NONE;
        }
    }

    private static RowOmissions encodeInternal(Memtable memtable, TableMetadata metadata,
                                               Supplier<Iterator<Partition>> partitions)
    {
        TieringPolicy policy;
        try
        {
            policy = TieringPolicy.fromTable(metadata);
        }
        catch (ConfigurationException e)
        {
            return NONE;
        }
        if (policy == null)
            return NONE;
        // The same gate the re-encoder applies, and no stricter: a schema tiering cannot handle
        // simply flushes as rows, exactly as before.
        if (TieringPolicy.unsupportedSchemaError(metadata) != null)
            return NONE;
        if (metadata.regularColumns().isEmpty())
            return NONE;
        if (!ColdBoundary.hasTimestampClustering(metadata))
            return NONE;
        // No DDL from the flush path: until the re-encoder's first cycle has created the chunk table
        // (and its ledger), cold windows flush as rows.
        if (Schema.instance.getTableMetadata(metadata.keyspace, ChunkTables.chunkTableName(metadata.name)) == null)
            return NONE;

        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(metadata.id);
        if (cfs == null)
            return NONE;

        // The same bypass bracket the re-encoder holds: our own chunk-table reads and writes must not
        // re-enter the transparent-read merge or the cold-write guard.
        TransparentReads.enterInternalBypass();
        try
        {
            synchronized (LOCKS.computeIfAbsent(metadata.id, ignored -> new Object()))
            {
                return encodeLocked(memtable, metadata, cfs, policy, partitions);
            }
        }
        finally
        {
            TransparentReads.exitInternalBypass();
        }
    }

    private static RowOmissions encodeLocked(Memtable memtable, TableMetadata metadata, ColumnFamilyStore cfs,
                                             TieringPolicy policy, Supplier<Iterator<Partition>> partitions)
    {
        // Coverage must be readable now, or the claim in step 2 would fail after the chunk writes;
        // check before doing any work.
        if (!ChunkCoverage.forTable(metadata, policy.consistency).known())
        {
            logger.warn("Cold-window chunk flush skipped for {}.{}: the chunk coverage ledger cannot be read, " +
                        "so coverage could not be widened before omitting rows; flushing as rows",
                        metadata.keyspace, metadata.name);
            return NONE;
        }

        // An older memtable that has not finished flushing may still hold rows for the same windows;
        // they would land in an sstable this flush cannot see, with writetimes possibly below the
        // chunk's. Let the older flush finish (its sstable then trips the coverage check below).
        for (Memtable other : cfs.getTracker().getView().getAllMemtables())
        {
            if (other != memtable && !other.isClean() && other.compareTo(memtable) <= 0)
                return NONE;
        }

        Encoder encoder = new Encoder(metadata, cfs, policy);
        Map<DecoratedKey, PartitionOmission> omitted = new HashMap<>();

        Iterator<Partition> iterator = partitions.get();
        while (iterator.hasNext())
            encoder.encodePartition(iterator.next(), omitted);

        if (omitted.isEmpty())
            return NONE;

        // Step 1, completed: force the chunk INSERTs' commitlog entries to disk. The writes above went
        // through the ordinary write path (commitlog + chunk memtable); this makes them durable before
        // any base row is allowed to go missing.
        try
        {
            CommitLog.instance.sync(true);
        }
        catch (IOException e)
        {
            logger.error("Cold-window chunk flush for {}.{}: commitlog sync failed after writing {} chunk(s); " +
                         "flushing every row as rows (the chunks are redundant, not authoritative)",
                         metadata.keyspace, metadata.name, encoder.windowsEncoded, e);
            return NONE;
        }

        // Step 2: widen the ledger. Throws when it cannot -- caught by encode()'s fallback, so the
        // rows are then flushed and the already-written chunks are merely redundant.
        ChunkCoverage.claim(metadata, policy.consistency, encoder.minClaimedWindow, encoder.maxClaimedWindow,
                            policy.chunkWindowMillis);

        logger.info("Cold-window chunk flush for {}.{}: encoded {} window(s) / {} row(s) into {} " +
                    "({} window(s) left to the row flush)",
                    metadata.keyspace, metadata.name, encoder.windowsEncoded, encoder.rowsEncoded,
                    ChunkTables.chunkTableName(metadata.name), encoder.windowsSkipped);

        // Step 3: only now may the rows be omitted from the base sstable.
        return new RowOmissions(omitted, policy.chunkWindowMillis);
    }

    /** The per-flush encoding state: prepared statements, gates and counters shared by every partition. */
    private static final class Encoder
    {
        private final TableMetadata metadata;
        private final TieringPolicy policy;
        private final ConsistencyLevel cl;
        private final List<ColumnMetadata> tagColumns;
        /** Shared with the re-encoder, so both write byte-identical chunks for identical content. */
        private final ChunkWindowEncoder windowEncoder;
        /** Regular column -> its slot in {@link #windowEncoder}'s order. */
        private final Map<ColumnMetadata, Integer> valueIndex = new HashMap<>();
        private final String existingChunkQuery;
        private final String insertChunkQuery;
        private final long cutoff;
        private final long nowInSec;
        private final int maxSamples;
        /** Live sstables' covered clustering ranges as {@code [lowestMs, highestMsExclusive)} pairs. */
        private final List<long[]> sstableRanges = new ArrayList<>();

        long minClaimedWindow = Long.MAX_VALUE;
        long maxClaimedWindow = Long.MIN_VALUE;
        int windowsEncoded;
        int windowsSkipped;
        int rowsEncoded;

        Encoder(TableMetadata metadata, ColumnFamilyStore cfs, TieringPolicy policy)
        {
            this.metadata = metadata;
            this.policy = policy;
            this.cl = policy.consistency;
            this.tagColumns = metadata.partitionKeyColumns();
            this.windowEncoder = new ChunkWindowEncoder(metadata);
            for (int c = 0; c < windowEncoder.valueCount(); c++)
                valueIndex.put(metadata.getColumn(ColumnIdentifier.getInterned(windowEncoder.valueRawName(c), true)), c);

            // The same statements the re-encoder issues, against the same chunk table, at the policy's
            // consistency level -- so what a cold flush writes is indistinguishable from what a
            // re-encode cycle writes.
            String chunkRef = format("%s.%s", ColumnIdentifier.maybeQuote(metadata.keyspace),
                                     ColumnIdentifier.maybeQuote(ChunkTables.chunkTableName(metadata.name)));
            StringBuilder tagCqlList = new StringBuilder();
            StringBuilder tagPredicate = new StringBuilder();
            StringBuilder bindMarkers = new StringBuilder();
            for (ColumnMetadata column : tagColumns)
            {
                if (tagCqlList.length() > 0)
                {
                    tagCqlList.append(", ");
                    tagPredicate.append(" AND ");
                    bindMarkers.append(", ");
                }
                tagCqlList.append(column.name.toCQLString());
                tagPredicate.append(column.name.toCQLString()).append(" = ?");
                bindMarkers.append('?');
            }
            this.existingChunkQuery = ChunkWindowEncoder.existingChunkQuery(chunkRef, tagPredicate.toString());
            this.insertChunkQuery = ChunkWindowEncoder.insertChunkQuery(chunkRef, tagCqlList.toString(), bindMarkers.toString());

            // The exact cutoff the re-encoder computes: the hot boundary, floored to a window start.
            this.cutoff = policy.windowStartFor(ColdBoundary.hotBoundaryMs(policy));
            this.nowInSec = FBUtilities.nowInSeconds();
            this.maxSamples = TieredStorageService.instance.maxSamplesPerWindow;

            boolean descending = ColdBoundary.isDescending(metadata);
            for (SSTableReader sstable : cfs.getLiveSSTables())
            {
                Slice covered = sstable.getSSTableMetadata().coveredClustering;
                // An sstable that never saw a clustering — static rows or partition deletions only —
                // records the EMPTY slice (MAX_START, MIN_END), whose bounds carry no clustering
                // component without being BOTTOM or TOP. It covers no clustered range, so it cannot
                // intersect any window; reading a timestamp out of those bounds is what produced the
                // production AIOOBE, and treating them as unbounded instead would veto every window
                // for as long as one static-only sstable is live.
                if (covered.isEmpty(metadata.comparator))
                    continue;
                sstableRanges.add(new long[]{ ColdBoundary.lowestMs(covered, descending),
                                              ColdBoundary.highestMsExclusive(covered, descending) });
            }
        }

        /**
         * Scans one partition, then encodes each of its chunkable cold windows; successful windows
         * are recorded in {@code omitted}. A partition carrying any deletion is left entirely to the
         * row flush: tombstones cannot be encoded, and dropping one would resurrect what it shadows.
         */
        void encodePartition(Partition partition, Map<DecoratedKey, PartitionOmission> omitted)
        {
            DecoratedKey key = partition.partitionKey();
            TreeMap<Long, WindowBucket> buckets = new TreeMap<>();
            int totalRows = 0;
            boolean hasStatic;

            try (UnfilteredRowIterator rows = partition.unfilteredIterator())
            {
                if (!rows.partitionLevelDeletion().isLive())
                    return;
                hasStatic = !rows.staticRow().isEmpty();
                while (rows.hasNext())
                {
                    Unfiltered unfiltered = rows.next();
                    if (!unfiltered.isRow())
                        return;                              // a range tombstone: whole partition stays rows
                    Row row = (Row) unfiltered;
                    totalRows++;
                    collectRow(row, buckets);
                }
            }

            List<ByteBuffer> tag = tagValues(key);
            Set<Long> chunkedWindows = null;
            int omittedRows = 0;
            for (Map.Entry<Long, WindowBucket> entry : buckets.entrySet())
            {
                long window = entry.getKey();
                WindowBucket bucket = entry.getValue();
                if (bucket.unchunkable || bucket.samples.isEmpty())
                {
                    windowsSkipped++;
                    continue;
                }
                try
                {
                    if (!encodeWindow(tag, window, bucket))
                    {
                        windowsSkipped++;
                        continue;
                    }
                }
                catch (RuntimeException e)
                {
                    // One window's trouble (a failed chunk write, an unreadable existing chunk, an
                    // unavailable replica) must not fail the flush or spoil the other windows: this
                    // window's rows simply flush as rows, and the re-encoder retries it later.
                    windowsSkipped++;
                    logger.error("Cold-window chunk flush for {}.{}: window [{}, {}) of partition {} could not be " +
                                 "encoded; its rows will be flushed as rows instead", metadata.keyspace, metadata.name,
                                 window, window + policy.chunkWindowMillis,
                                 metadata.partitionKeyType.getString(key.getKey()), e);
                    continue;
                }
                if (chunkedWindows == null)
                    chunkedWindows = new HashSet<>();
                chunkedWindows.add(window);
                omittedRows += bucket.samples.size();
                windowsEncoded++;
                rowsEncoded += bucket.samples.size();
                minClaimedWindow = Math.min(minClaimedWindow, window);
                maxClaimedWindow = Math.max(maxClaimedWindow, window);
            }

            if (chunkedWindows != null)
                omitted.put(key, new PartitionOmission(chunkedWindows, !hasStatic && omittedRows == totalRows));
        }

        /** Routes {@code row} into its window's bucket, or poisons the bucket if it cannot be chunked. */
        private void collectRow(Row row, TreeMap<Long, WindowBucket> buckets)
        {
            long ts = ColdBoundary.clusteringMs(row.clustering());
            long window = policy.windowStartFor(ts);
            if (window >= cutoff)
                return;                                      // hot: never touched
            WindowBucket bucket = buckets.computeIfAbsent(window, ignored -> new WindowBucket());
            if (bucket.unchunkable)
                return;
            // Rows already on disk for this window may pre-date anything in the memtable; a chunk
            // written over them would later have the re-encoder merge those OLDER rows on top of it.
            if (intersectsSSTables(window, window + policy.chunkWindowMillis))
            {
                bucket.unchunkable = true;
                return;
            }
            ByteBuffer[] values = new ByteBuffer[windowEncoder.valueCount()];
            if (!collectCells(row, values, bucket))
            {
                bucket.unchunkable = true;
                return;
            }
            bucket.samples.put(ts, values);
        }

        /**
         * @return false when the row cannot be encoded: it deletes something (row deletion, cell
         * tombstone, expired or expiring-to-nothing cell) or carries a cell this table's regular
         * columns cannot place. Live values -- including a bare primary-key insert with every column
         * null -- are collected exactly as the re-encoder's CQL read would have seen them.
         */
        private boolean collectCells(Row row, ByteBuffer[] values, WindowBucket bucket)
        {
            if (!row.deletion().isLive())
                return false;
            for (ColumnData data : row)
            {
                if (!data.column().isSimple())
                    return false;                            // multi-cell: the schema gate rejects these tables
                Cell<?> cell = (Cell<?>) data;
                if (cell.isTombstone() || !cell.isLive(nowInSec))
                    return false;
                Integer index = valueIndex.get(data.column());
                if (index == null)
                    return false;
                values[index] = cell.buffer();
                long writetime = cell.timestamp();
                bucket.maxCellWt = Math.max(bucket.maxCellWt, writetime);
                bucket.minCellWt = Math.min(bucket.minCellWt, writetime);
                bucket.anyCellWt = true;
            }
            return true;
        }

        /**
         * Encodes one (tag, window) into the chunk table, merging with any existing chunk exactly as
         * the re-encoder's late merge does.
         *
         * @return true when the window's rows are covered by a durable-once-synced chunk and may be
         *         omitted; false when the window must flush as rows
         */
        private boolean encodeWindow(List<ByteBuffer> tag, long window, WindowBucket bucket)
        {
            UntypedResultSet existingRs = QueryProcessor.process(existingChunkQuery, cl,
                    boundTo(tag, TimestampType.instance.fromTimeInMillis(window)));
            UntypedResultSet.Row existingRow = (existingRs == null || existingRs.isEmpty()) ? null : existingRs.one();

            // A memtable cell at or below the chunk's max writetime would have been shadowed by the
            // re-encoder's range tombstone; encoding it could replace newer chunk content with older
            // data. Leave the window to the row flush and the re-encoder's own merge. Checked before
            // decoding the existing chunk, which is the expensive part of open().
            if (existingRow != null && bucket.anyCellWt && bucket.minCellWt <= existingRow.getLong("max_row_writetime"))
                return false;

            ChunkWindowEncoder.Window encoding = windowEncoder.open(existingRow);
            for (Map.Entry<Long, ByteBuffer[]> sample : bucket.samples.entrySet())
                encoding.mergeSample(sample.getKey(), sample.getValue());
            if (bucket.anyCellWt)
                encoding.noteWritetime(bucket.maxCellWt);
            // No cell writetime anywhere and no prior chunk to inherit one from: mirroring the
            // re-encoder, the window is left completely untouched.
            if (!encoding.haveWritetime())
                return false;

            int count = encoding.sampleCount();
            if (count > maxSamples)
            {
                logger.error("Cold-window chunk flush for {}.{}: window [{}, {}) merges to {} samples, over the " +
                             "{}-sample per-chunk limit; flushing it as rows (lower the policy's chunk_window)",
                             metadata.keyspace, metadata.name, window, window + policy.chunkWindowMillis,
                             count, maxSamples);
                return false;
            }

            ChunkWindowEncoder.Encoded encoded = encoding.encode();
            if (!encoded.unchanged)
            {
                LongConsumer hook = beforeChunkInsertForTesting;
                if (hook != null)
                    hook.accept(window);
                QueryProcessor.process(insertChunkQuery, cl, encoded.insertValues(tag, window));
            }
            return true;
        }

        private boolean intersectsSSTables(long startMs, long endMsExclusive)
        {
            for (long[] range : sstableRanges)
            {
                if (range[0] < endMsExclusive && startMs < range[1])
                    return true;
            }
            return false;
        }

        /** A composite partition key arrives as one buffer; split it into the chunk table's key columns. */
        private List<ByteBuffer> tagValues(DecoratedKey key)
        {
            List<ByteBuffer> values = new ArrayList<>(tagColumns.size());
            Collections.addAll(values, tagColumns.size() == 1
                                       ? new ByteBuffer[]{ key.getKey() }
                                       : ((CompositeType) metadata.partitionKeyType).split(key.getKey()));
            return values;
        }

        private static List<ByteBuffer> boundTo(List<ByteBuffer> tag, ByteBuffer... rest)
        {
            List<ByteBuffer> values = new ArrayList<>(tag.size() + rest.length);
            values.addAll(tag);
            Collections.addAll(values, rest);
            return values;
        }
    }
}
