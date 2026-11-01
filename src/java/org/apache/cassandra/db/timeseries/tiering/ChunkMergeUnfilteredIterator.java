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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.Throwables;

/**
 * Merges rows decoded from the {@code __chunks} shadow table into a read's <b>unfiltered</b>
 * partition iterator, i.e. while range tombstones, row deletions and cell tombstones are still in
 * the stream.
 * <p>
 * The timing is the whole point. The synthetic chunk rows go to
 * {@link UnfilteredRowIterators#merge(List)} as just another source, so Cassandra's ordinary
 * reconciliation decides what survives -- a partition deletion, a range tombstone covering the
 * sample's clustering, a row deletion or a newer cell tombstone all shadow the reconstructed data
 * exactly as they would shadow a real base row. Merging after {@code Filter} has run (what SP3
 * originally did) cannot honour a delete at all: {@code Filter} purges every tombstone and drops
 * every row left empty, so the merge would be handed a stream with no deletion information in it.
 * <p>
 * This works only because the reconstruction post-dates the re-encoder's own delete: see
 * {@link ChunkReadSupport}, which stamps every synthetic row at {@code max_row_writetime + 1}. At
 * {@code max_row_writetime} the tiering tombstone -- issued at exactly that timestamp -- would
 * shadow everything this class produces.
 * <p>
 * Partitions the hot iterator does not contain at all (fully chunked tags) are synthesized in the
 * position given by {@code expectedKeys}, which must list the query's partition keys in the same
 * order the hot iterator yields them (the read command/group order).
 * <p>
 * <b>The chunk side is pulled, not handed over.</b> {@code chunkRows} yields a
 * {@link CloseableIterator}, not a list, so a window is fetched and decoded only when the merge
 * actually reaches it: a {@code LIMIT} satisfied from the newest window leaves every older one
 * untouched, which is the difference between a tiered unbounded read costing one window and costing
 * the whole retention period ({@link ChunkRowSource}). The corollary is that this class now owns a
 * closeable per partition it is working on, and every path out -- merged, synthesized, empty, or
 * thrown -- has to release it.
 * <p>
 * <b>That ordering is an enforced invariant, not an assumption.</b> If the hot iterator ever yields a
 * key {@code expectedKeys} has already passed -- which is one wiring change away, since
 * {@code SinglePartitionReadQuery.Group#executeLocally} sorts by <em>token</em> while {@code queries}
 * is sorted by partition-key <em>value</em> -- the old "synthesize and carry on" handling emitted that
 * partition <b>twice</b>: once chunk-only, once merged. Two partitions with the same key in one result
 * is silent duplication that no downstream stage rejects, so a divergence fails the query instead (see
 * {@link #outOfOrder}).
 */
public final class ChunkMergeUnfilteredIterator implements UnfilteredPartitionIterator
{
    private final UnfilteredPartitionIterator hot;
    private final Iterator<DecoratedKey> expectedKeys;
    /** The expected keys not yet consumed, for the order check -- see {@link #outOfOrder}. */
    private final Set<DecoratedKey> pendingKeys;
    private final Function<DecoratedKey, CloseableIterator<Row>> chunkRows;
    private final TableMetadata metadata;
    private final boolean reversed;

    private UnfilteredRowIterator peekedHot;
    private UnfilteredRowIterator next;

    private ChunkMergeUnfilteredIterator(UnfilteredPartitionIterator hot,
                                         List<DecoratedKey> expectedKeys,
                                         Function<DecoratedKey, CloseableIterator<Row>> chunkRows,
                                         TableMetadata metadata,
                                         boolean reversed)
    {
        this.hot = hot;
        this.expectedKeys = expectedKeys.iterator();
        this.pendingKeys = new HashSet<>(expectedKeys);
        this.chunkRows = chunkRows;
        this.metadata = metadata;
        this.reversed = reversed;
    }

    /**
     * @param expectedKeys the query's partition keys, in the order the hot iterator yields them
     * @param chunkRows    per-key synthetic rows (already range-filtered and ordered to match
     *                     {@code reversed}), produced lazily; an exhausted iterator means "no chunks
     *                     for this key". This class closes every iterator it obtains from here.
     * @param reversed     whether the read runs in reverse clustering-comparator order, i.e. the hot
     *                     iterators' {@code isReverseOrder()}; the synthetic iterator must report the
     *                     same or {@code merge} compares clusterings the wrong way round
     */
    public static UnfilteredPartitionIterator wrap(UnfilteredPartitionIterator hot,
                                                   List<DecoratedKey> expectedKeys,
                                                   Function<DecoratedKey, CloseableIterator<Row>> chunkRows,
                                                   TableMetadata metadata,
                                                   boolean reversed)
    {
        return new ChunkMergeUnfilteredIterator(hot, expectedKeys, chunkRows, metadata, reversed);
    }

    @Override
    public TableMetadata metadata()
    {
        return metadata;
    }

    @Override
    public boolean hasNext()
    {
        while (next == null)
        {
            if (peekedHot == null && hot.hasNext())
                peekedHot = hot.next();

            if (!expectedKeys.hasNext())
            {
                if (peekedHot == null)
                    return false;
                // A hot partition left over after every expected key has been consumed: either its key
                // was never requested, or it arrived after the position expectedKeys gave it. Merging
                // it here would emit a partition this iterator has already emitted chunk-only.
                throw outOfOrder(peekedHot.partitionKey());
            }

            DecoratedKey expected = expectedKeys.next();
            pendingKeys.remove(expected);
            if (peekedHot != null && peekedHot.partitionKey().equals(expected))
            {
                // peekedHot is cleared only once merge() has taken ownership: if it throws (a corrupt
                // or unreadable chunk fails the query from inside the first pull), close() must still
                // find the hot partition to release.
                next = merge(peekedHot, expected);
                peekedHot = null;
            }
            else
            {
                // Skipping `expected` is only legitimate if the peeked hot partition is still ahead of
                // us in expectedKeys, i.e. the hot side simply has nothing for `expected` (a fully
                // chunked tag). A peeked key that is NOT still pending means the two orders diverged.
                if (peekedHot != null && !pendingKeys.contains(peekedHot.partitionKey()))
                    throw outOfOrder(peekedHot.partitionKey());

                next = syntheticOrNull(metadata, expected, reversed, chunkRows.apply(expected));
                // null: no hot rows and no chunks for this key - nothing to emit, try the next key
            }
        }
        return true;
    }

    @Override
    public UnfilteredRowIterator next()
    {
        if (next == null && !hasNext())
            throw new NoSuchElementException();
        UnfilteredRowIterator result = next;
        next = null;
        return result;
    }

    @Override
    public void close()
    {
        // Everything held must be closed, not just `hot`. A limit (or an exception out of chunkRows,
        // including the deliberate UnsupportedChunkFormatException rethrow) can stop the read while a
        // peeked hot partition or an already-built merged partition is still open, and those wrap
        // live SSTable/memtable iterators -- dropping them leaks the ref-count and the file
        // descriptor ("LEAK DETECTED"). Cassandra's own BaseIterator.close() closes its held `next`
        // for exactly this reason. `hot` is closed last and unconditionally.
        Throwable failure = closeQuietly(peekedHot, null);
        failure = closeQuietly(next, failure);
        peekedHot = null;
        next = null;
        failure = closeQuietly(hot, failure);
        Throwables.maybeFail(failure);
    }

    /**
     * The hot iterator and {@code expectedKeys} disagree about partition order. Reported rather than
     * worked around: the only ways to "continue" are to emit the partition twice or to drop one of the
     * two copies, and a duplicated or dropped partition is a wrong answer that nothing downstream can
     * detect. Every path wired today (the local group read and the coordinator's per-command resolve)
     * satisfies the invariant, so this firing means a caller changed.
     */
    private IllegalStateException outOfOrder(DecoratedKey key)
    {
        return new IllegalStateException(
            String.format("Tiered read of %s.%s: the hot partition iterator yielded %s out of the order the read's " +
                          "partition keys were given in. The chunk merge cannot place it without emitting some " +
                          "partition twice, so the query is failed instead of returning duplicated rows.",
                          metadata.keyspace, metadata.name, key));
    }

    private static Throwable closeQuietly(AutoCloseable closeable, Throwable failure)
    {
        if (closeable == null)
            return failure;
        try
        {
            closeable.close();
        }
        catch (Throwable t)
        {
            return Throwables.merge(failure, t);
        }
        return failure;
    }

    private UnfilteredRowIterator merge(UnfilteredRowIterator hotPartition, DecoratedKey key)
    {
        return mergeWith(hotPartition, chunkRows.apply(key), metadata);
    }

    /**
     * {@code hotPartition} with {@code synthetic}'s chunk rows merged in, or {@code hotPartition} itself
     * when there are none. Takes ownership of {@code synthetic} on every path, the throwing ones
     * included. Shared with {@link ChunkRangeMergeIterator}.
     */
    static UnfilteredRowIterator mergeWith(UnfilteredRowIterator hotPartition, CloseableIterator<Row> synthetic,
                                           TableMetadata metadata)
    {
        boolean handedOn = false;
        try
        {
            if (!synthetic.hasNext())
                return hotPartition;                      // nothing cold here: hand the hot side back

            List<UnfilteredRowIterator> sources = new ArrayList<>(2);
            sources.add(hotPartition);
            // isReverseOrder must match the hot iterator's own, not the wrapper's idea of it, or the
            // merge compares clusterings in one order while a source produced them in the other.
            sources.add(synthetic(metadata, hotPartition.partitionKey(), hotPartition.isReverseOrder(), synthetic));
            UnfilteredRowIterator merged = UnfilteredRowIterators.merge(sources);
            handedOn = true;                              // merged.close() now owns `synthetic`
            return merged;
        }
        finally
        {
            if (!handedOn)
                synthetic.close();
        }
    }

    /**
     * A chunk-only partition for {@code key}, or {@code null} when {@code rows} turns out to be empty.
     * Takes ownership of {@code rows} on every path. Shared with {@link ChunkRangeMergeIterator}.
     */
    static UnfilteredRowIterator syntheticOrNull(TableMetadata metadata, DecoratedKey key, boolean reversed,
                                                 CloseableIterator<Row> rows)
    {
        boolean handedOn = false;
        try
        {
            // hasNext() is what fetches and decodes the first window, so it is also where a chunk read
            // fails. Either way the iterator is this method's to release until it is handed on.
            if (!rows.hasNext())
                return null;
            UnfilteredRowIterator partition = synthetic(metadata, key, reversed, rows);
            handedOn = true;
            return partition;
        }
        finally
        {
            if (!handedOn)
                rows.close();
        }
    }

    /**
     * A partition made only of chunk-decoded rows: no partition deletion, no static row, no
     * tombstones. Closing it closes {@code rows} -- that is the only thing that stops the chunk
     * source fetching windows nobody asked for, and the reason both {@code merge}'s
     * {@link UnfilteredRowIterators#merge(List)} (which closes its sources) and this class's own
     * {@link #close()} are enough to keep the lazy iterator from being dropped on the floor.
     */
    private static UnfilteredRowIterator synthetic(TableMetadata metadata,
                                                   DecoratedKey key,
                                                   boolean reversed,
                                                   CloseableIterator<Row> rows)
    {
        return new AbstractUnfilteredRowIterator(metadata,
                                                 key,
                                                 DeletionTime.LIVE,
                                                 metadata.regularAndStaticColumns(),
                                                 Rows.EMPTY_STATIC_ROW,
                                                 reversed,
                                                 EncodingStats.NO_STATS)
        {
            @Override
            protected Unfiltered computeNext()
            {
                return rows.hasNext() ? rows.next() : endOfData();
            }

            @Override
            public void close()
            {
                rows.close();
            }
        };
    }
}
