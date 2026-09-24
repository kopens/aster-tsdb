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

package org.apache.cassandra.db.compaction;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * Extends the ordinary {@link CompactionController#getFullyExpiredSSTables()} tombstone/TTL check with an
 * unconditional retention-cutoff drop: every sstable in this task's originals whose max timestamp falls
 * before {@code retentionCutoffMillis} is treated as fully expired, regardless of whether its data would
 * otherwise still shadow live rows in overlapping sstables.
 * <p>
 * This is retention doing what {@link TimeWindowCompactionController}'s {@code ignoreOverlaps} flag does for
 * TWCS: skip the overlap-safety check entirely for these sstables. Dropping past-retention data is a
 * deliberate policy decision, not a correctness-preserving tombstone purge - all replicas apply the same
 * retention policy, so a brief resurrection of rows near the cutoff (e.g. via repair from a clock-skewed
 * replica that has not yet caught up to the same cutoff) is an accepted trade-off of this feature, not a bug.
 * <p>
 * Cross-replica clock skew is not the only case this trades away. Two purely local, single-replica cases
 * follow from the same "whole sstable, by max timestamp" policy:
 * <ul>
 *   <li>An sstable is classified by its <i>max</i> timestamp, so a single arbitrarily-old row co-resident with
 *       a recent one keeps that old row alive until the whole sstable's window passes retention - retention is
 *       therefore a floor on how long data survives, not an exact per-row guarantee.</li>
 *   <li>Dropping an expired sstable that holds a tombstone whose shadowed row lives in a different,
 *       newer/mixed-window sstable permanently resurrects that row - the tombstone is gone, the row it was
 *       covering is not, and no future compaction will re-encounter both together.</li>
 * </ul>
 * Exact per-row retention and safe tombstone handling in the presence of mixed-age sstables require that an
 * sstable's data never crosses a window boundary (design spec section 4 invariant). T3's flush-time window
 * splitting makes that true of flushes, but not of every sstable: the UCS delegate compacts across all active
 * windows and can emit a window-spanning sstable, and a split-refreeze that overflows its budget is parked
 * with the spanning sstable intact. For those, both cases above still apply.
 */
public class TimeSeriesCompactionController extends CompactionController
{
    private final Set<SSTableReader> compacting;
    private final long retentionCutoffMillis;
    private final TimeUnit timestampResolution;

    public TimeSeriesCompactionController(ColumnFamilyStore cfs,
                                          Set<SSTableReader> compacting,
                                          long gcBefore,
                                          long retentionCutoffMillis,
                                          TimeUnit timestampResolution)
    {
        super(cfs, compacting, gcBefore);
        this.compacting = compacting;
        this.retentionCutoffMillis = retentionCutoffMillis;
        this.timestampResolution = timestampResolution;
    }

    @Override
    public Set<SSTableReader> getFullyExpiredSSTables()
    {
        Set<SSTableReader> expired = new HashSet<>(super.getFullyExpiredSSTables());
        // sstables whose window closed before the retention cutoff are dropped whole, independent of
        // overlap/tombstone status - see class javadoc.
        for (SSTableReader sstable : compacting)
            if (TimeUnit.MILLISECONDS.convert(sstable.getMaxTimestamp(), timestampResolution) < retentionCutoffMillis)
                expired.add(sstable);
        return expired;
    }
}
