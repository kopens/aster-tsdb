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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.SinglePartitionReadQuery;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;

/**
 * SP3 transparent reads (design spec section 3.3.1): decides whether a SELECT on a tiering-enabled
 * table must merge in decoded chunk rows, fetches and decodes the relevant chunks at the user's
 * consistency level, and wraps the hot {@link UnfilteredPartitionIterator} with the merge decorator.
 *
 * Activation requires ALL of: a single-partition (or IN) read group; a slice or names clustering
 * filter; a chunk table that exists; and a requested time range reaching below the point where cold
 * data can start. Everything else returns the hot iterator untouched - the non-tiered fast path pays
 * nothing beyond one schema-extension lookup and one cached {@link ChunkCoverage} lookup.
 *
 * <b>The merge gate is real coverage, not the current policy.</b> The only sound question is "do
 * chunks exist at or below this query's timestamps?", answered by {@link ChunkCoverage}; asking the
 * live {@link TieringPolicy} instead lets three ordinary tuning actions -- raising {@code hot_window},
 * dropping the extension, shrinking {@code chunk_window} -- silently hide history whose base rows
 * were deleted when it was encoded. The policy still contributes a floor
 * ({@code max(coverageTop, hotBoundary)}): anything the re-encoder has just written is by definition
 * below the current hot boundary, so a node whose cached coverage is a minute stale still merges it.
 *
 * <b>The wrap happens on the UNFILTERED stream</b>, before {@code Filter} purges tombstones, so
 * deletes are reconciled by the ordinary read machinery instead of being invisible to the merge --
 * see {@link ChunkMergeUnfilteredIterator}. Every unfiltered-to-filtered conversion point on a
 * single-partition read path therefore calls {@link #maybeWrap}: the local ones
 * ({@code SinglePartitionReadQuery.Group#executeInternal}, {@code AbstractReadQuery#executeInternal})
 * and the coordinator ones -- {@code DigestResolver#getData}, which serves a read whose digests
 * agreed, and {@code DataResolver#resolveInternal}, which is the single point every reconciling path
 * funnels through: the digest-<em>mismatch</em> retry ({@code AbstractReadRepair#startRepair} ->
 * {@code DataResolver#resolve}), the transient-replica branch of {@code DigestResolver#getData},
 * short-read protection and replica filtering protection.
 *
 * <p><b>Where in {@code resolveInternal} is load-bearing.</b> The wrap goes on the output of
 * {@code UnfilteredPartitionIterators.merge(results, mergeListener)}, i.e. strictly after read
 * repair's merge listener has been attached to the replica sources. A synthetic chunk row reaching
 * that listener would look like a row present in the merged result and absent from every replica, so
 * read repair would write it back into the base table as a real row -- undoing the compression and
 * resurrecting data the re-encoder deliberately deleted. {@code TieredStorageDistributedTest#
 * digestMismatchStillMergesChunksWithoutRepairingThemBack} holds both halves of that in place.
 */
public final class TransparentReads
{
    /**
     * The re-encoder's own base-table reads run through the same SELECT machinery; merging chunks
     * into THEM would make already-encoded windows look like live base rows again (re-encode loops,
     * broken idempotency). The tiering machinery brackets its work with this per-thread bypass.
     */
    private static final ThreadLocal<Integer> INTERNAL_BYPASS = ThreadLocal.withInitial(() -> 0);

    private TransparentReads()
    {
    }

    /**
     * Nesting-safe on purpose: several unrelated things bracket reads with this now (the re-encoder's
     * own cycle, and each CAS precondition read), and a plain boolean would let an inner
     * {@code exit} re-enable merging while an outer bracket was still meant to hold -- silently, and
     * on a path where the consequence is a linearizability violation or a JVM crash.
     */
    public static void enterInternalBypass()
    {
        INTERNAL_BYPASS.set(INTERNAL_BYPASS.get() + 1);
    }

    public static void exitInternalBypass()
    {
        int depth = INTERNAL_BYPASS.get() - 1;
        INTERNAL_BYPASS.set(Math.max(0, depth));
    }

    /** True while this thread is inside tiering's own machinery (see {@link #enterInternalBypass}). */
    public static boolean inInternalBypass()
    {
        return INTERNAL_BYPASS.get() > 0;
    }

    /**
     * @param cl the user query's consistency level, or null for the internal/local execution path
     * @return the merged iterator, or {@code hot} itself when transparent reading does not apply
     */
    public static UnfilteredPartitionIterator maybeWrap(TableMetadata metadata, ReadQuery query,
                                                        UnfilteredPartitionIterator hot, ConsistencyLevel cl)
    {
        if (inInternalBypass())
            return hot;

        TieringPolicy policy;
        try
        {
            policy = TieringPolicy.fromTable(metadata);
        }
        catch (ConfigurationException e)
        {
            // An invalid policy must never break reads -- and must not hide chunks either. A typo in
            // the extension JSON is a configuration error, not an instruction to make every encoded
            // window disappear, so fall through to coverage exactly as an absent policy does.
            policy = null;
        }

        // What the chunk table ACTUALLY holds, cached per table. A table with no chunk table has
        // provably never encoded anything (the re-encoder creates it, and waits for it to be visible
        // cluster-wide, before writing the first chunk), so this is where every non-tiered read --
        // and every tiered read before the first cycle -- leaves, at the cost of one cached lookup.
        ChunkCoverage.Coverage coverage = ChunkCoverage.forTable(metadata, cl);
        if (policy == null && !coverage.chunkTableExists())
            return hot;
        // The hook fires both per read GROUP (the local path executes a whole group at once) and per
        // read COMMAND (the coordinator resolves one partition at a time), so accept either shape and
        // normalise to the list of single-partition queries this iterator will carry.
        List<? extends SinglePartitionReadQuery> commands;
        if (query instanceof SinglePartitionReadQuery.Group)
            commands = ((SinglePartitionReadQuery.Group<?>) query).queries;
        else if (query instanceof SinglePartitionReadQuery)
            commands = Collections.singletonList((SinglePartitionReadQuery) query);
        else
        {
            // Range scans cannot merge chunks (no partition context). A client's range scan that
            // could reach chunked data never gets here -- guardRangeRead refused it before it was
            // issued -- so what arrives is either confined to the hot window (complete) or an
            // internal caller, which reads the base table as it stands on purpose. The warning is
            // for the latter. The hook fires from several places per query, so de-duplicate against
            // the warnings already accumulated for THIS request rather than sending the same
            // sentence three times.
            String warning = "tiered table " + metadata.keyspace + '.' + metadata.name +
                             ": range scans read hot rows only; query by partition key for merged hot+cold reads";
            List<String> existing = ClientWarn.instance.getWarnings();
            if (existing == null || !existing.contains(warning))
                ClientWarn.instance.warn(warning);
            return hot;
        }

        if (commands.isEmpty())
            return hot;

        // Every timestamp read below assumes exactly one clustering column, of type timestamp -- what
        // TieringPolicy.unsupportedSchemaError enforces before anything is ever chunked. A table
        // carrying the extension but not that shape has no chunks to merge, and must not have its
        // ordinary SELECTs broken by this code path (composing a timestamp out of, say, an int
        // clustering throws MarshalException mid-query).
        if (!ColdBoundary.hasTimestampClustering(metadata))
            return hot;

        // Policy present, but nothing encoded yet: the shadow table has never been created, so there
        // is no cold data and (unlike before) no chunk read to fail and warn about on every
        // historical read of a freshly-configured table.
        if (!coverage.chunkTableExists())
            return hot;

        SinglePartitionReadQuery first = commands.get(0);
        ClusteringIndexFilter filter = first.clusteringIndexFilter();
        // WITH CLUSTERING ORDER BY (ts DESC) wraps the clustering type in ReversedType, so both
        // Slices and the names filter's NavigableSet are ordered by DESCENDING timestamp: the
        // comparator-order start bound is then the range's UPPER time bound, and the comparator-order
        // end bound its LOWER one. Reading them as if ascending produces a nonsensical (usually
        // empty) time range and silently serves no cold rows at all -- on exactly the DESC shape that
        // is the dominant time-series idiom.
        boolean clusteringDescending = ColdBoundary.isDescending(metadata);
        // The order rows must be EMITTED in, expressed as timestamps: the hot iterator runs in
        // clustering-comparator order unless the filter reverses it, and comparator order is already
        // descending-by-timestamp on a DESC table.
        boolean emitDescending = clusteringDescending != filter.isReversed();

        long startMs;
        long endMsExcl;
        NavigableSet<Clustering<?>> exactClusterings = null;
        // Only set when the filter carries MORE THAN ONE slice: the [start, end) range derived below
        // is then the hull of all of them, and a chunk row falling in a gap BETWEEN slices would be
        // injected into a result the hot side never selected. Unreachable with a single timestamp
        // clustering column (CQL cannot express a disjunction over it), but the hull is what the code
        // computes, so the gap is closed explicitly rather than left to the schema to prevent.
        Slices gappedSlices = null;
        if (filter instanceof ClusteringIndexSliceFilter)
        {
            Slices slices = ((ClusteringIndexSliceFilter) filter).requestedSlices();
            if (slices.isEmpty())
                return hot;
            startMs = ColdBoundary.lowestMs(slices.get(0), clusteringDescending);
            endMsExcl = ColdBoundary.highestMsExclusive(slices.get(slices.size() - 1), clusteringDescending);
            if (slices.size() > 1)
                gappedSlices = slices;
        }
        else if (filter instanceof ClusteringIndexNamesFilter)
        {
            exactClusterings = ((ClusteringIndexNamesFilter) filter).requestedRows();
            if (exactClusterings.isEmpty())
                return hot;
            Clustering<?> comparatorFirst = exactClusterings.first();
            Clustering<?> comparatorLast = exactClusterings.last();
            startMs = ColdBoundary.clusteringMs(clusteringDescending ? comparatorLast : comparatorFirst);
            endMsExcl = ColdBoundary.clusteringMs(clusteringDescending ? comparatorFirst : comparatorLast) + 1;
        }
        else
        {
            return hot;
        }

        // The one correct question: can any chunk hold data at or above this query's start? Asked
        // through the definition of cold that TieredWrites enforces on the other side, so the two can
        // never disagree about which rows a chunk owns (ColdBoundary.coldBelowMs).
        long mergeBelowMs = ColdBoundary.coldBelowMs(coverage, policy);
        if (startMs >= mergeBelowMs)
            return hot;                                   // no chunk reaches up here: fast path

        List<DecoratedKey> keys = new ArrayList<>(commands.size());
        for (SinglePartitionReadQuery command : commands)
            keys.add(command.partitionKey());

        // The WIDEST chunk_window any existing chunk was written with, not the configured one: after
        // an operator shrinks chunk_window, every wider legacy chunk sits further before the range
        // start than one current window, and a look-back of one current window would never find it.
        long lookbackMs = coverage.lookbackMillis();
        // Built once per query, not once per partition key per query: everything it holds -- the two
        // chunk SELECTs, the bounds, the projection -- depends only on the query, not on the key. The
        // source is told the TIMESTAMP order to emit in; the merge decorator is told whether the
        // iterators run against clustering-comparator order, which is filter.isReversed() -- the
        // comparator itself already encodes the DESC-ness.
        ChunkRowSource chunks = new ChunkRowSource(metadata, lookbackMs, startMs, endMsExcl,
                                                   exactClusterings, gappedSlices, emitDescending,
                                                   // Decode only the columns this query reads.
                                                   chunkProjection(query.columnFilter()), cl);
        return ChunkMergeUnfilteredIterator.wrap(hot, keys, chunks::rowsFor, metadata, filter.isReversed());
    }

    /**
     * Refuses a client's range scan (any read without a partition key: a full scan, {@code count(*)},
     * {@code DISTINCT}, a token range -- Spark's full-table read) when it could reach chunked data.
     * <p>
     * {@link #maybeWrap} can only merge chunks into a partition it is told about, and a range scan
     * names none, so on a tiered table it used to come back with the hot rows alone behind a client
     * warning most drivers never surface: {@code count(*)} over 20M rows answered 500
     * (tiering-benchmark.md). A successful query silently missing every cold row is the worst answer
     * there is; an error is not.
     * <p>
     * Allowed through: internal callers (the server reads base tables as they stand on purpose), and
     * a scan whose time range starts at or above {@link ColdBoundary#coldBelowMs} -- no chunk reaches
     * it, so its answer is complete. An unknowable coverage puts that boundary at
     * {@link Long#MAX_VALUE}, so every scan is refused: the same fail-closed reading the merge and
     * the write guard take.
     *
     * @throws InvalidRequestException if the scan could reach data that only a chunk holds
     */
    public static void guardRangeRead(TableMetadata metadata, ClusteringIndexFilter filter,
                                      ClientState state, ConsistencyLevel cl)
    {
        if (state.isInternal || inInternalBypass())
            return;
        // Free, and first: nothing without exactly one timestamp clustering column was ever chunked,
        // and every timestamp read below would throw on any other shape.
        if (!ColdBoundary.hasTimestampClustering(metadata))
            return;

        TieringPolicy policy;
        try
        {
            policy = TieringPolicy.fromTable(metadata);
        }
        catch (ConfigurationException e)
        {
            // As in maybeWrap: a typo in the extension does not make encoded windows disappear.
            policy = null;
        }

        ChunkCoverage.Coverage coverage = ChunkCoverage.forTable(metadata, cl);
        if (!coverage.chunkTableExists())
            return;
        long coldBelowMs = ColdBoundary.coldBelowMs(coverage, policy);
        if (coldBelowMs == Long.MIN_VALUE || lowestRequestedMs(filter, ColdBoundary.isDescending(metadata)) >= coldBelowMs)
            return;

        TieringMetrics.record(metadata, TieringMetrics.Event.RANGE_SCAN_REFUSED);
        String hotOnly = coldBelowMs == Long.MAX_VALUE
                         ? "(its chunk coverage cannot be read right now, so no time range is provably hot)"
                         : "or restrict its time range to at or above epoch millis " + coldBelowMs + " (the cold boundary)";
        throw new InvalidRequestException(
            String.format("A range scan (a query without the partition key: a full scan, an aggregate, DISTINCT or a " +
                          "token range) is not supported on %s.%s, because part of its data lives only in %s and a " +
                          "range scan cannot merge it; it would return the hot rows alone as a complete answer. " +
                          "Restrict the query by partition key %s.",
                          metadata.keyspace, metadata.name, ChunkTables.chunkTableName(metadata.name), hotOnly));
    }

    /** @return the oldest timestamp {@code filter} can select, {@link Long#MIN_VALUE} if unbounded below. */
    private static long lowestRequestedMs(ClusteringIndexFilter filter, boolean descending)
    {
        if (filter instanceof ClusteringIndexSliceFilter)
        {
            long lowest = Long.MAX_VALUE;
            for (Slice slice : ((ClusteringIndexSliceFilter) filter).requestedSlices())
                lowest = Math.min(lowest, ColdBoundary.lowestMs(slice, descending));
            return lowest;
        }
        if (filter instanceof ClusteringIndexNamesFilter)
        {
            long lowest = Long.MAX_VALUE;
            for (Clustering<?> clustering : ((ClusteringIndexNamesFilter) filter).requestedRows())
                if (clustering != Clustering.STATIC_CLUSTERING)
                    lowest = Math.min(lowest, ColdBoundary.clusteringMs(clustering));
            return lowest;
        }
        // An unknown filter shape is not evidence the scan stays hot.
        return Long.MIN_VALUE;
    }

    /**
     * The columns a chunk actually has to be decoded for, or {@code null} for "all of them".
     * <p>
     * Deliberately {@link ColumnFilter#queriedColumns()} and not {@code fetchedColumns()}: every CQL
     * SELECT is built through {@code ColumnFilter.allRegularColumnsBuilder}, so its <em>fetched</em>
     * set is always every regular column and projecting on it would save nothing. Cassandra fetches
     * that superset so it can tell "row exists, your column is null" from "no row"; the synthetic
     * rows carry that distinction as primary-key liveness instead, which is what makes the narrower
     * set safe -- see {@link ChunkReadSupport}'s class javadoc for the full argument and its one
     * precondition.
     * <p>
     * Static columns are left out: the re-encoder never chunks them (its range delete cannot reach
     * them), so no chunk directory has ever carried one.
     */
    @VisibleForTesting
    static Set<String> chunkProjection(ColumnFilter filter)
    {
        // A wildcard SELECT queries everything, so an intersection would only cost hashing.
        if (filter == null || filter.allFetchedColumnsAreQueried())
            return null;

        Set<String> names = new HashSet<>();
        for (ColumnMetadata column : filter.queriedColumns().regulars)
            names.add(column.name.toString());
        return names;
    }
}
