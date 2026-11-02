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
import java.util.NoSuchElementException;
import java.util.function.Function;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.Throwables;

/**
 * The range-scan counterpart of {@link ChunkMergeUnfilteredIterator}: a sort-merge join of the
 * replicas' merged hot partitions with the keys the chunk table holds in the same sub-range, both in
 * token order (design: range-scan-merge-design.md section 1.1).
 * <ul>
 *   <li>a key only the hot side has passes through untouched -- no window listing, so a scan over
 *       mostly-hot partitions pays one chunk-key enumeration per sub-range, not one query per key;</li>
 *   <li>a key both sides have is merged exactly as a single-partition read merges it;</li>
 *   <li>a key only the chunk table has (a fully tiered partition) is synthesized.</li>
 * </ul>
 * <b>The hot side is always advanced first.</b> Before a chunk-only partition is emitted, the hot
 * iterator's {@code hasNext()} runs, which is where short-read protection extends a replica that
 * stopped at its limit. Emitting the chunk-only key first could put it ahead of a hot key SRP had
 * yet to fetch -- out of token order.
 * <p>
 * <b>Order is enforced, not assumed.</b> Every emitted key must be strictly greater than the one
 * before it; anything else would be a duplicated or misplaced partition that nothing downstream can
 * detect (and that a range pager would then resume from in the wrong place), so it fails the query.
 */
final class ChunkRangeMergeIterator implements UnfilteredPartitionIterator
{
    private final UnfilteredPartitionIterator hot;
    private final Iterator<DecoratedKey> chunkKeys;
    private final Function<DecoratedKey, CloseableIterator<Row>> chunkRows;
    private final TableMetadata metadata;
    private final boolean reversed;

    private UnfilteredRowIterator peekedHot;
    private DecoratedKey peekedChunkKey;
    private DecoratedKey lastEmitted;
    private UnfilteredRowIterator next;

    /**
     * @param chunkKeys keys holding at least one chunk window in the query's time range, within the
     *                  sub-range being resolved, in token order (lazily enumerated)
     * @param chunkRows per-key synthetic rows; this class closes every iterator it obtains
     * @param reversed  the read's {@code isReversed()} (see {@link ChunkMergeUnfilteredIterator#wrap})
     */
    ChunkRangeMergeIterator(UnfilteredPartitionIterator hot,
                            Iterator<DecoratedKey> chunkKeys,
                            Function<DecoratedKey, CloseableIterator<Row>> chunkRows,
                            TableMetadata metadata,
                            boolean reversed)
    {
        this.hot = hot;
        this.chunkKeys = chunkKeys;
        this.chunkRows = chunkRows;
        this.metadata = metadata;
        this.reversed = reversed;
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
            if (peekedChunkKey == null && chunkKeys.hasNext())
                peekedChunkKey = chunkKeys.next();

            if (peekedHot == null && peekedChunkKey == null)
                return false;

            int cmp = peekedHot == null ? 1
                    : peekedChunkKey == null ? -1
                    : peekedHot.partitionKey().compareTo(peekedChunkKey);
            if (cmp < 0)
            {
                checkOrder(peekedHot.partitionKey());
                next = peekedHot;
                peekedHot = null;
            }
            else if (cmp == 0)
            {
                DecoratedKey key = peekedChunkKey;
                checkOrder(key);
                // peekedHot is cleared only once mergeWith has taken ownership, so close() still finds
                // it if the first chunk pull throws.
                next = ChunkMergeUnfilteredIterator.mergeWith(peekedHot, chunkRows.apply(key), metadata);
                peekedHot = null;
                peekedChunkKey = null;
            }
            else
            {
                DecoratedKey key = peekedChunkKey;
                checkOrder(key);
                peekedChunkKey = null;
                // null when no window of this key actually reaches the rows this read selects (the
                // enumeration lists by window, the filter is by row): nothing to emit, try the next key
                next = ChunkMergeUnfilteredIterator.syntheticOrNull(metadata, key, reversed, chunkRows.apply(key));
            }
        }
        return true;
    }

    private void checkOrder(DecoratedKey key)
    {
        if (lastEmitted != null && key.compareTo(lastEmitted) <= 0)
            throw new IllegalStateException(
                String.format("Tiered range scan of %s.%s: partition %s arrived after %s, out of token order. The " +
                              "chunk merge cannot place it without emitting a partition twice or out of order, so " +
                              "the query is failed instead.",
                              metadata.keyspace, metadata.name, key, lastEmitted));
        lastEmitted = key;
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
        // As ChunkMergeUnfilteredIterator.close: a limit or a failed chunk read can stop the scan while
        // a peeked or built partition is still open over live sstable iterators.
        Throwable failure = closeQuietly(peekedHot, null);
        failure = closeQuietly(next, failure);
        peekedHot = null;
        next = null;
        failure = closeQuietly(hot, failure);
        Throwables.maybeFail(failure);
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
}
