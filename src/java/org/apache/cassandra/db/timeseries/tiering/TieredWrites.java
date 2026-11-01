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

import java.util.Iterator;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Enforces the one rule that makes tiering's read path sound: <b>cold data is immutable</b>.
 * <p>
 * Once the re-encoder has moved a window into the {@code __chunks} shadow table, the base rows are
 * gone and the chunk is the only copy. A tombstone written against that window can therefore only
 * ever be a <em>temporary</em> mask: {@link TransparentReads} merges the chunk's rows back in, and
 * the tombstone shadows them only until {@code gc_grace_seconds} purges it -- after which the
 * deleted data reappears, permanently and silently. Documenting that would have been shipping a
 * data-resurrection bug on a timer, so instead such writes are rejected outright, and
 * {@code cold_window} is the supported way to remove cold data.
 * <p>
 * Rejected when the write reaches a clustering below {@link ColdBoundary#coldBelowMs}: partition-level
 * deletes (which necessarily reach cold data -- they have no clustering bound at all), range deletes,
 * row deletes, and deletes of individual cells. A cell tombstone is also what
 * {@code UPDATE ... SET col = null} and {@code INSERT ... VALUES (..., null)} write, so those are
 * caught by the same check -- the test is what the mutation actually contains, not which statement
 * produced it.
 * <p>
 * That boundary is the <b>same one {@link TransparentReads} merges below</b>, and it is derived from
 * what the chunk table actually holds rather than from the current policy. The two have to agree: a
 * row the read path merges out of a chunk is a row this guard must refuse to tombstone. Keying the
 * guard on {@code policy != null} instead let the agreement break in one direction -- drop the
 * {@code timeseries_tiering} extension (or write a typo into it) and reads still serve the encoded
 * history while a {@code DELETE} against it was accepted, masking chunk rows until
 * {@code gc_grace_seconds} purged the tombstone and the deleted data came back.
 * <p>
 * When coverage cannot be established at all the guard still <b>rejects</b> (see
 * {@link ColdBoundary#coldBelowMs}) -- a wrongly refused write is loud and fixable in the moment, a
 * wrongly accepted one is silent until gc_grace. Partition, range and row deletions are refused
 * outright. Cell-level tombstones are refused only below the boundary that is still provable without
 * the ledger (the hot boundary, or the newest coverage this node saw established), because above it
 * they are indistinguishable from what an ordinary live write emits (assigning any non-frozen
 * collection or UDT produces a complex deletion) and refusing them while the ledger is unreadable
 * would break plain current-timestamp writes -- an outage larger than the one being prevented.
 * <p>
 * <b>Not</b> rejected: anything confined to the hot window (ordinary current-data deletes are
 * untouched), and writes of real values to cold clusterings -- a late {@code UPDATE ... SET x = 1}
 * against a chunked row is a supported operation that the re-encoder merges into the chunk per
 * column on its next cycle. Only <em>un</em>writing cold data is refused.
 */
public final class TieredWrites
{
    private TieredWrites()
    {
    }

    /**
     * @throws InvalidRequestException if any of {@code mutations} tombstones data below the cold
     * boundary of a table the chunk table covers (see {@link ColdBoundary#coldBelowMs})
     */
    public static void guardColdMutations(List<? extends IMutation> mutations)
    {
        guardColdMutations(mutations, false);
    }

    /**
     * @param fromInsertOnly whether every statement that produced {@code mutations} was an
     * {@code INSERT}. Only the caller knows this, and only before the mutations are built: statements
     * in one batch that target the same partition <em>and the same clustering</em> are merged into a
     * single {@link Row} before this method ever runs, and that merge destroys the distinction. A row
     * carrying an INSERT's primary-key liveness marker beside a DELETE's cell tombstone is
     * indistinguishable, by inspection, from an INSERT that bound null -- including by timestamp,
     * since a batch stamps every statement alike. Deciding here from the row was therefore wrong:
     * `BEGIN BATCH INSERT ...; DELETE col ... APPLY BATCH` against one cold clustering was accepted.
     */
    public static void guardColdMutations(List<? extends IMutation> mutations, boolean fromInsertOnly)
    {
        // The re-encoder's own range delete is exactly the write this guard exists to reject -- it is
        // also the one write that is allowed to do it, because it deletes rows it has just copied
        // into a chunk. It brackets its whole cycle with the same bypass the read path uses.
        if (TransparentReads.inInternalBypass())
            return;

        for (IMutation mutation : mutations)
            for (PartitionUpdate update : mutation.getPartitionUpdates())
            {
                noteTag(update);
                guard(update, fromInsertOnly);
            }
    }

    /**
     * As {@link #guardColdMutations}, for the one update a conditional (LWT/CAS) write builds --
     * {@code CQL3CasRequest.makeUpdates} never goes through {@code getMutations}, so without this
     * {@code DELETE ... IF EXISTS} would walk straight past the rule. Covers conditional BATCH too:
     * that path builds its update through the same request object.
     */
    public static void guardColdUpdate(PartitionUpdate update)
    {
        if (TransparentReads.inInternalBypass())
            return;
        noteTag(update);
        // A conditional (LWT) write is never treated as insert-only: CQL3CasRequest merges the
        // condition's updates the same way a batch does, so the same ambiguity applies.
        guard(update, false);
    }

    /**
     * Refuses a conditional (LWT) write to a clustering that may live only in a chunk.
     * <p>
     * Paxos evaluates the condition with its own read, which does not merge chunks
     * (tiered-storage.md §5.1.2). Against a chunked row that read sees nothing, so
     * {@code INSERT ... IF NOT EXISTS} applied over an existing row and {@code UPDATE ... IF col = v}
     * never matched a value a SERIAL read had just returned -- wrong answers to the question the
     * caller asked, which TieredStorageDefectReproTest.s3 reproduced. Refused instead, against the
     * same boundary the cell-tombstone rule uses (the established coverage, or what is provable
     * without it). Conditions on static columns are untouched: statics are never chunked.
     * <p>
     * Called from {@code CQL3CasRequest.readCommand}, i.e. before Paxos reads the condition rows, for
     * conditional statements and batches alike: a write-time check would only see conditions that
     * came out true, and a condition that came out false against an unseen chunk is the other half of
     * the wrong answer.
     *
     * @param conditionRows the clusterings the request conditions on (no static clustering)
     */
    public static void refuseConditionsOnColdData(TableMetadata metadata, Iterable<Clustering<?>> conditionRows)
    {
        if (TransparentReads.inInternalBypass() || !ColdBoundary.hasTimestampClustering(metadata))
            return;
        boolean anyRegularRow = false;
        long lowestMs = Long.MAX_VALUE;
        for (Clustering<?> clustering : conditionRows)
        {
            if (clustering == Clustering.STATIC_CLUSTERING)
                continue;
            anyRegularRow = true;
            lowestMs = Math.min(lowestMs, ColdBoundary.clusteringMs(clustering));
        }
        if (!anyRegularRow)
            return;

        TieringPolicy policy;
        try
        {
            policy = TieringPolicy.fromTable(metadata);
        }
        catch (ConfigurationException e)
        {
            policy = null;
        }
        ChunkCoverage.Coverage coverage = coverage(metadata, policy);
        long coldBelowMs = coverage.known() ? ColdBoundary.coldBelowMs(coverage, policy)
                                            : provablyColdBelowMs(metadata, policy);
        if (coldBelowMs == Long.MIN_VALUE || lowestMs >= coldBelowMs)
            return;

        TieringMetrics.record(metadata, TieringMetrics.Event.COLD_WRITE_REFUSED);
        throw new InvalidRequestException(
            String.format("A conditional (LWT) write to %s.%s at a clustering older than the hot window is not " +
                          "allowed: its condition is decided by a Paxos read that does not see rows already encoded " +
                          "into %s, so IF NOT EXISTS / IF col = ... would be judged against the hot rows alone. " +
                          "Write without the condition, or only condition rows inside the hot window.",
                          metadata.keyspace, metadata.name, ChunkTables.chunkTableName(metadata.name)));
    }

    /**
     * Registers the update's tag with {@link TagRegistry} on the way past, so the re-encoder can
     * enumerate tags from a small shadow table instead of {@code SELECT DISTINCT}-ing the base table
     * every cycle. Gated on the same free clustering-shape check {@link #guard} opens with, so a
     * write to a table that was never a candidate for tiering pays nothing new at all.
     */
    private static void noteTag(PartitionUpdate update)
    {
        TableMetadata metadata = update.metadata();
        if (!ColdBoundary.hasTimestampClustering(metadata))
            return;
        TagRegistry.noteTagIfTiered(metadata, update.partitionKey());
    }

    private static void guard(PartitionUpdate update, boolean fromInsertOnly)
    {
        TableMetadata metadata = update.metadata();
        // Same shape the read path requires: without exactly one timestamp clustering column nothing
        // has ever been chunked, so there is no cold copy for a tombstone to mask. Checked first
        // because it is free, and because everything below reads a timestamp out of a clustering --
        // on any other clustering type that would throw MarshalException mid-DELETE.
        if (!ColdBoundary.hasTimestampClustering(metadata))
            return;

        refusePastTimestampsIntoColdData(update);

        // CONTENT FIRST, coverage second. Coverage can cost a ledger read, and the overwhelming
        // majority of writes -- every plain INSERT/UPDATE of real values -- cannot un-write anything
        // whatever the coverage turns out to be. Asking coverage before looking at the update made
        // every write to the table pay for the ledger, so a ledger that was timing out blocked a
        // request thread per mutation.
        Tombstones tombstones = tombstonesIn(update, ColdBoundary.isDescending(metadata), fromInsertOnly);
        if (tombstones == null)
            return;

        TieringPolicy policy;
        try
        {
            policy = TieringPolicy.fromTable(metadata);
        }
        catch (ConfigurationException e)
        {
            // An invalid policy must not break writes -- and must not un-protect already-encoded
            // data either. Treated as an absent policy, exactly as the read path treats it, so the
            // answer still comes from coverage.
            policy = null;
        }

        // Where cold begins, asked of what the chunk table REALLY holds rather than of the policy:
        // the policy can be raised, invalidated or dropped after data has been encoded, and none of
        // those makes a chunk's rows writable again. This is the same call the read path makes to
        // decide whether to merge, off the same per-table cache.
        ChunkCoverage.Coverage coverage = coverage(metadata, policy);
        long coldBelowMs = ColdBoundary.coldBelowMs(coverage, policy);
        // Nothing has ever been encoded and no policy calls anything cold: there is no cold copy for
        // any tombstone in this update to mask, whatever it contains. (Load-bearing: the
        // partition-delete rejection below is otherwise unconditional.)
        if (coldBelowMs == Long.MIN_VALUE)
            return;

        if (tombstones.partitionDelete)
            throw reject(metadata, "a partition-level DELETE", "it has no clustering bound, so it necessarily " +
                                                               "covers data that has already been chunked");
        if (tombstones.lowestRangeMs < coldBelowMs)
            throw reject(metadata, "a range DELETE", describeBoundary(coldBelowMs));
        if (tombstones.lowestRowDeleteMs < coldBelowMs)
            throw reject(metadata, "a row DELETE", describeBoundary(coldBelowMs));

        // Cell-level tombstones. When coverage could not be established, coldBelowMs is
        // Long.MAX_VALUE -- EVERY clustering is nominally cold -- and refusing this shape against that
        // would reject ordinary current-timestamp writes (a complex deletion is what assigning any
        // non-frozen collection or UDT emits) for as long as the ledger stayed unreadable. But letting
        // every cell tombstone through was wider than that needed: an UPDATE ... SET col = null or a
        // DELETE col against a chunked clustering got through too, and the re-encoder keeps a chunk's
        // value for a cell it reads as absent, so the tombstone only masked the chunk until gc_grace
        // purged it and the value came back. So against an unknown coverage the boundary is the one
        // still provable without the ledger (provablyColdBelowMs): current-timestamp writes all sit
        // above it, and nothing below it is a live write's business. (An INSERT's null never gets
        // here -- fromInsertOnly -- so a backlogged writer is unaffected either way.)
        long cellColdBelowMs = coverage.known() ? coldBelowMs : provablyColdBelowMs(metadata, policy);
        if (tombstones.lowestCellMs < cellColdBelowMs)
            throw reject(metadata, "deleting a column (or writing a null value into one)",
                         describeBoundary(coldBelowMs));
    }

    /**
     * Where cold data provably begins without reading the ledger: the later of the newest top this
     * node has seen established (the ledger only widens, so what was chunked then is chunked still)
     * and, with a policy installed, the hot boundary (nothing older can still be hot). Without either
     * there is nothing provable, and {@link Long#MIN_VALUE} lets the cell tombstone through as before.
     */
    private static long provablyColdBelowMs(TableMetadata metadata, TieringPolicy policy)
    {
        long lastKnownTop = ChunkCoverage.lastKnownTopExclusiveMs(metadata);
        return policy == null ? lastKnownTop : Math.max(lastKnownTop, ColdBoundary.hotBoundaryMs(policy));
    }

    /**
     * Refuses a client write below quorum strength to a tiered table whose keyspace keeps more than one
     * replica.
     * <p>
     * The re-encoder reads a window at its policy's quorum consistency, encodes what it read, and
     * deletes the window's rows with a range tombstone at the newest writetime it saw. A row on fewer
     * replicas than a quorum may be missing from that read; if its writetime is below the tombstone's it
     * is then deleted on every replica without ever having been encoded. TieredStorageDefectReproTest.s1
     * reproduced exactly that with a CL ONE write (6 rows written, 5 left). Quorum writes intersect the
     * re-encoder's quorum read, so they cannot be missed this way. With a single replica every level is
     * quorum strength and nothing is refused (node 41 runs RF 1).
     * <p>
     * Like the cold-write guards, only tiering's own bypass is exempt, not every internal client state:
     * a coordinated write at ONE is just as lossy from an internal caller. Local {@code executeInternal}
     * writes never reach this (they do not go through {@code ModificationStatement#execute}).
     */
    public static void guardWriteConsistency(TableMetadata metadata, ConsistencyLevel cl)
    {
        if (TransparentReads.inInternalBypass() || TieringPolicy.isQuorumStrength(cl))
            return;
        if (!ColdBoundary.hasTimestampClustering(metadata))
            return;
        TieringPolicy policy;
        try
        {
            policy = TieringPolicy.fromTable(metadata);
        }
        catch (ConfigurationException e)
        {
            policy = null;
        }
        if (policy == null)
            return;
        int replicas = Keyspace.open(metadata.keyspace).getReplicationStrategy().getReplicationFactor().allReplicas;
        if (replicas <= 1)
            return;
        TieringMetrics.record(metadata, TieringMetrics.Event.COLD_WRITE_REFUSED);
        throw new InvalidRequestException(
            String.format("Writes to the tiered table %s.%s must use a quorum consistency level (QUORUM, LOCAL_QUORUM, " +
                          "EACH_QUORUM or ALL), not %s: the re-encoder reads at quorum and range-deletes what it " +
                          "encoded, so a row on fewer replicas than a quorum can be deleted without being encoded.",
                          metadata.keyspace, metadata.name, cl));
    }

    /**
     * Test-only seam. The tiering tests pin their results with tiny explicit writetimes
     * ({@code USING TIMESTAMP 101}) on 1970 clusterings -- exactly the shape
     * {@link #refusePastTimestampsIntoColdData} refuses -- and assert what the re-encoder does with
     * them. Production never sets it.
     */
    @VisibleForTesting
    public static volatile boolean acceptPastTimestampsIntoColdDataForTesting;

    /**
     * Refuses a live write into the cold region that carries a write timestamp older than the hot
     * boundary -- a client-supplied past timestamp, typically {@code USING TIMESTAMP <measurement
     * time>} on a backfill.
     * <p>
     * The re-encoder removes the rows it encoded with a range tombstone at the window's newest
     * writetime ({@code USING TIMESTAMP maxWt}, tiered-storage.md §5.1). A write that reaches the
     * window after that and carries a timestamp at or below {@code maxWt} is shadowed by it the moment
     * it lands: it never shows in a read and the next cycle never sees it to encode, so it is lost
     * without an error. With server timestamps that cannot happen (a write made after the cycle read
     * the window is newer than anything the cycle read); with measurement-time timestamps it is the
     * normal case for late data -- an edge device's backlog stamped with the times it measured. Both
     * conditions below hold for exactly that write: its clustering is cold, and so is its stamp.
     * Refused loudly so the writer keeps it and resends without the explicit timestamp, which the
     * next cycle merges into the chunk. Current-time writes, and measurement-time stamps on hot data
     * (ordinary collection, and what TSCS windows by), are untouched.
     * <p>
     * Needs the policy's hot window to say what is cold; a table whose policy was detached is not
     * re-encoded any more, so there is no future tombstone to be shadowed by.
     */
    private static void refusePastTimestampsIntoColdData(PartitionUpdate update)
    {
        if (acceptPastTimestampsIntoColdDataForTesting)
            return;

        TableMetadata metadata = update.metadata();
        TieringPolicy policy;
        try
        {
            policy = TieringPolicy.fromTable(metadata);
        }
        catch (ConfigurationException e)
        {
            // Not being re-encoded with an invalid policy either.
            return;
        }
        if (policy == null)
            return;

        long hotBoundaryMs = ColdBoundary.hotBoundaryMs(policy);
        long hotBoundaryMicros = hotBoundaryMs * 1000;
        for (Row row : update)
        {
            if (row.clustering() == Clustering.STATIC_CLUSTERING)
                continue;
            if (ColdBoundary.clusteringMs(row.clustering()) >= hotBoundaryMs)
                continue;
            long oldest = Long.MAX_VALUE;
            if (!row.primaryKeyLivenessInfo().isEmpty())
                oldest = row.primaryKeyLivenessInfo().timestamp();
            for (Cell<?> cell : row.cells())
                if (!cell.isTombstone())
                    oldest = Math.min(oldest, cell.timestamp());
            if (oldest < hotBoundaryMicros)
            {
                TieringMetrics.record(metadata, TieringMetrics.Event.COLD_WRITE_REFUSED);
                throw new InvalidRequestException(
                    String.format("Writing to %s.%s at a clustering older than the hot window with a write timestamp " +
                                  "(%d) also older than it -- a client-supplied past timestamp, such as USING TIMESTAMP " +
                                  "<measurement time> -- is not allowed on a tiered table: the re-encoder removes rows it " +
                                  "has encoded with a range tombstone at the window's newest write timestamp, which would " +
                                  "shadow this write silently and lose it. Resend it without USING TIMESTAMP (the server's " +
                                  "timestamp); tiering merges it into the chunk on its next cycle.",
                                  metadata.keyspace, metadata.name, oldest));
            }
        }
    }

    /**
     * The chunk coverage this mutation must be judged against.
     * <p>
     * Read at the policy's own consistency level so the guard sees the ledger with the same reach the
     * re-encoder wrote it with; when the policy is gone -- the case this exists for -- at the same
     * {@code LOCAL_QUORUM} default the re-encoder falls back to. Never at {@code null} (the
     * internal, node-local path): a coordinator that holds no replica of the ledger partition would
     * read it as empty and conclude that nothing is cold, which is precisely the mistake being
     * prevented, and it would cache that answer for the read path to make as well.
     * ({@link ChunkCoverage#ledgerConsistency} enforces the same floor on every other caller.)
     */
    private static ChunkCoverage.Coverage coverage(TableMetadata metadata, TieringPolicy policy)
    {
        return ChunkCoverage.forTable(metadata, policy != null ? policy.consistency
                                                               : ChunkCoverage.DEFAULT_CONSISTENCY);
    }

    /**
     * The oldest clustering an update tombstones, split by how much the shape says about intent.
     * <p>
     * The split exists because the guard has to behave differently when coverage is unknown. A
     * partition, range or row deletion is produced by nothing but a {@code DELETE}, so it is
     * unambiguously an attempt to remove data and is refused whether or not coverage could be
     * established. A cell tombstone or a complex (collection/UDT) deletion is <em>also</em> what an
     * ordinary {@code INSERT}/{@code UPDATE} of live data emits, so it is only refused against a
     * coverage that is actually known.
     */
    private static final class Tombstones
    {
        boolean partitionDelete;
        long lowestRangeMs = Long.MAX_VALUE;
        long lowestRowDeleteMs = Long.MAX_VALUE;
        long lowestCellMs = Long.MAX_VALUE;

        boolean any()
        {
            return partitionDelete
                   || lowestRangeMs != Long.MAX_VALUE
                   || lowestRowDeleteMs != Long.MAX_VALUE
                   || lowestCellMs != Long.MAX_VALUE;
        }
    }

    /** @return what {@code update} tombstones, or {@code null} if it tombstones nothing at all. */
    private static Tombstones tombstonesIn(PartitionUpdate update, boolean descending, boolean fromInsertOnly)
    {
        Tombstones found = new Tombstones();
        found.partitionDelete = !update.partitionLevelDeletion().isLive();

        DeletionInfo deletionInfo = update.deletionInfo();
        if (deletionInfo.hasRanges())
        {
            Iterator<RangeTombstone> ranges = deletionInfo.rangeIterator(false);
            while (ranges.hasNext())
                found.lowestRangeMs = Math.min(found.lowestRangeMs,
                                               ColdBoundary.lowestMs(ranges.next().deletedSlice(), descending));
        }

        for (Row row : update)
        {
            // Static cells are never chunked (the re-encoder deletes a clustering RANGE, which cannot
            // touch them), so deleting one is always safe.
            if (row.clustering() == Clustering.STATIC_CLUSTERING)
                continue;
            if (!row.deletion().isLive())
                found.lowestRowDeleteMs = Math.min(found.lowestRowDeleteMs,
                                                   ColdBoundary.clusteringMs(row.clustering()));
            else if (hasCellTombstone(row) && !fromInsertOnly)
                found.lowestCellMs = Math.min(found.lowestCellMs, ColdBoundary.clusteringMs(row.clustering()));
        }

        return found.any() ? found : null;
    }


    private static boolean hasCellTombstone(Row row)
    {
        if (row.hasComplexDeletion())
            return true;
        for (Cell<?> cell : row.cells())
            if (cell.isTombstone())
                return true;
        return false;
    }

    private static String describeBoundary(long coldBelowMs)
    {
        return "it reaches clusterings that are already cold (before epoch millis " + coldBelowMs +
               ", which is the later of the hot window and the newest timestamp the chunk table covers)";
    }

    private static InvalidRequestException reject(TableMetadata metadata, String what, String why)
    {
        TieringMetrics.record(metadata, TieringMetrics.Event.COLD_WRITE_REFUSED);
        return new InvalidRequestException(
            String.format("%s is not allowed on %s.%s: %s, and data older than the hot window is immutable once " +
                          "timeseries_tiering has encoded it into %s (the base rows are gone, so a tombstone would " +
                          "only mask the chunk's copy until gc_grace_seconds purged it, after which the deleted data " +
                          "would silently reappear). Use the policy's cold_window to expire cold data. A whole " +
                          "partition cannot be deleted from a tiered table at all -- the chunk coverage only ever " +
                          "widens -- so remove one by letting cold_window expire it, or TRUNCATE the table (which " +
                          "cascades to %s).",
                          what, metadata.keyspace, metadata.name, why,
                          ChunkTables.chunkTableName(metadata.name), ChunkTables.chunkTableName(metadata.name)));
    }
}
