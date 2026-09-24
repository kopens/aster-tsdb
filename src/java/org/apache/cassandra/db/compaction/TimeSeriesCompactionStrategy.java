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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.compaction.timeseries.TimeWindowSplittingMultiWriter;
import org.apache.cassandra.db.compaction.unified.Controller;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.TimeUUID;

/**
 * Time-series compaction: SSTables are classified into fixed time windows by max timestamp.
 * Active windows (current, or closed less than freeze_after ago) delegate their compaction to an
 * internal {@link UnifiedCompactionStrategy}; windows past the configured retention are dropped
 * whole via {@link TimeSeriesCompactionTask}/{@link TimeSeriesCompactionController}
 * (see {@link TimeSeriesCompactionStrategyOptions#isExpiredWindow}). Closed windows are frozen to a
 * single sstable per window (per strategy-instance slice) by
 * {@link FreezeCompactionTask}, which fires {@link org.apache.cassandra.db.compaction.timeseries.WindowFrozenListener}
 * post-commit. Late-data isolation (flush/streaming window splitting) landed in T3: see
 * {@link org.apache.cassandra.db.compaction.timeseries.TimeWindowSplittingMultiWriter} and
 * {@link SplitRefreezeCompactionTask} (design spec sections 4 and 10).
 * <p>
 * <b>TTL reclaim, precisely.</b> Because the freeze runs a real {@link CompactionController} with the
 * caller's gcBefore, data that is already expired <em>at the moment the window freezes</em> is purged
 * there, with no {@code retention} configured. That is the whole of the guarantee: once a window is
 * down to one sstable it is never a freeze candidate again ({@link #nextFreezeCandidate(Round)} skips windows
 * with fewer than two sstables), so data that expires <em>after</em> the freeze is not reclaimed by
 * this strategy. Configure {@code retention} to bound how long such data survives.
 * <p>
 * Instances only ever see the sstable slice the CompactionStrategyManager assigns them (per
 * repair status and per disk), so all window state is derived per call and never assumed complete.
 */
public class TimeSeriesCompactionStrategy extends AbstractCompactionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesCompactionStrategy.class);

    private final TimeSeriesCompactionStrategyOptions tsOptions;
    private final UnifiedCompactionStrategy delegate;
    private final Set<SSTableReader> sstables = new HashSet<>();
    // the set of expired-window sstables selected on the most recent background round, so
    // getEstimatedRemainingTasks() can report the pending whole-window drop without recomputing it.
    private volatile Set<SSTableReader> lastExpiredSelection = Set.of();
    // number of FREEZING windows (>= 2 sstables) seen on the most recent background round, for
    // getEstimatedRemainingTasks() - freezes starve behind other tables' work if the CSM cannot see them.
    private volatile int freezeBacklog;
    private volatile int splitBacklog;
    // the freeze candidate that failed tryModify on the previous round: cross-round version of TWCS's
    // previousCandidate guard (:100-106) - warn when the same candidate is stuck two rounds running,
    // but keep retrying (unlike TWCS's intra-call loop, skipping here would never retry at all).
    // Split-refreeze keeps its own copy because a refused freeze no longer suppresses the split branch
    // (see getNextBackgroundTasksAt): both paths can now refuse in the same round, and one shared field
    // could only ever remember the later of the two, silently disarming the other path's repeat-WARN.
    private volatile Set<SSTableReader> previousFreezeCandidate = Set.of();
    private volatile Set<SSTableReader> previousSplitCandidate = Set.of();
    // sstables excluded from every automatic path because their window is far in the future; refreshed
    // each round by syncDelegate so operators can see the accumulation, not just a throttled WARN.
    private volatile Set<SSTableReader> farFutureSSTables = Set.of();
    // Per-window no-progress bookkeeping for the guard below; guarded by this.
    private final Map<Long, WindowProgress> windowProgress = new HashMap<>();
    // windows the guard has parked, with the sstables they are parked at; refreshed each round so
    // operators can see them - a parked window is deliberately absent from the backlogs, so it would
    // otherwise be invisible except for one WARN at the transition. See getParkedWindows().
    private volatile NavigableMap<Long, Set<SSTableReader>> parkedWindows = Collections.emptyNavigableMap();

    /**
     * How many <em>completed</em> rewrites of one window may return it to a shape the current chain of
     * rewrites has already produced before it is parked; a genuinely stuck window is therefore
     * rewritten a bounded number of times and then never again. Two allows one futile rewrite - enough
     * to absorb a rewrite that lost a race with concurrent work - while keeping a stuck window's cost
     * bounded instead of unbounded.
     */
    @VisibleForTesting
    static final int NO_PROGRESS_STRIKES = 2;

    /**
     * How many distinct shapes one chain remembers. Only short cycles need catching - the freeze
     * <-> split alternation is a 2-cycle and a futile rewrite is a 1-cycle - and a chain that has
     * produced this many <em>different</em> shapes in a row is making progress by any reading, so the
     * eldest is evicted rather than letting the set grow with the chain.
     */
    private static final int MAX_TRACKED_SHAPES = 8;

    /**
     * What the guard remembers about one uninterrupted <em>chain</em> of rewrites over a window: a run
     * of completed rewrites in which each one started from the shape the previous one left behind, so
     * nothing outside compaction has touched the window in between. See {@link #recordCompletedRewrite}.
     */
    private static final class WindowProgress
    {
        /** Every shape this chain has produced, in order; see {@link #windowSignature}. */
        private final Set<Long> shapes = new LinkedHashSet<>();
        /** The shape the most recent rewrite in this chain left behind. */
        private long lastSignature;
        private int strikes;
        private boolean parked;

        /**
         * @return {@code true} if this chain has already produced {@code signature} - the window has
         * come back to a shape these rewrites have been at before, so the rewrites are going round in
         * a circle. Records it otherwise.
         */
        private boolean revisits(long signature)
        {
            if (shapes.contains(signature))
                return true;
            if (shapes.size() == MAX_TRACKED_SHAPES)
                shapes.remove(shapes.iterator().next());
            shapes.add(signature);
            return false;
        }
    }

    public TimeSeriesCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        this(cfs, options, new TimeSeriesCompactionStrategyOptions(options));
    }

    // Builds the delegate from tsOptions built exactly once by the caller (either public ctor above, or the
    // @VisibleForTesting ctor below), rather than each ctor building its own TimeSeriesCompactionStrategyOptions.
    private TimeSeriesCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options, TimeSeriesCompactionStrategyOptions tsOptions)
    {
        this(cfs, options, tsOptions, new UnifiedCompactionStrategy(cfs, tsOptions.delegateOptions(options)));
    }

    @VisibleForTesting
    TimeSeriesCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options, UnifiedCompactionStrategy delegate)
    {
        this(cfs, options, new TimeSeriesCompactionStrategyOptions(options), delegate);
    }

    private TimeSeriesCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options,
                                         TimeSeriesCompactionStrategyOptions tsOptions, UnifiedCompactionStrategy delegate)
    {
        super(cfs, options);
        this.tsOptions = tsOptions;
        this.delegate = delegate;
    }

    @Override
    public Collection<AbstractCompactionTask> getNextBackgroundTasks(long gcBefore)
    {
        return getNextBackgroundTasksAt(Clock.Global.currentTimeMillis(), gcBefore);
    }

    // synchronized: freezeBacklog/splitBacklog/previousFreezeCandidate and the no-progress bookkeeping
    // are all read-modify-written across this method, and several background threads can select work for
    // the same strategy instance concurrently (T2-M10). The body only picks work - the compaction itself
    // runs outside this lock - so serialising selection costs nothing measurable.
    @VisibleForTesting
    synchronized Collection<AbstractCompactionTask> getNextBackgroundTasksAt(long nowMillis, long gcBefore)
    {
        Round round = snapshot(nowMillis);
        syncDelegate(round);

        Set<SSTableReader> expired = round.expired;
        lastExpiredSelection = expired;

        // Both candidates are computed every round, before any early return: their side effect is to
        // refresh freezeBacklog/splitBacklog, which feed getEstimatedRemainingTasks() and hence how the
        // CompactionStrategyManager ranks this table against every other one. Computing them only on
        // rounds that reach them left the backlogs stale on every retention-drop round (T2-M8). The
        // delegate's own term had the same defect for longer - see refreshDelegateBacklog.
        RewriteCandidate freeze = nextFreezeCandidate(round);
        RewriteCandidate split = nextSplitRefreezeCandidate(round);
        pruneWindowProgress(round);

        if (!expired.isEmpty())
        {
            // Mirror UCS's zombie-sstable filter (UnifiedCompactionStrategy#getSSTables, CASSANDRA-18342):
            // only ever try to mark sstables the tracker still considers live, and never a suspect one, for
            // compaction. expiredSSTables() classifies purely off this instance's own bookkeeping, which can
            // be briefly stale relative to the tracker's live set.
            Set<SSTableReader> toDrop = new HashSet<>(AbstractCompactionStrategy.filterSuspectSSTables(expired));
            toDrop.retainAll(cfs.getLiveSSTables());
            if (!toDrop.isEmpty())
            {
                LifecycleTransaction txn = cfs.getTracker().tryModify(toDrop, OperationType.COMPACTION);
                if (txn != null)
                {
                    long cutoff = nowMillis - tsOptions.retentionMillis;
                    logger.info("Dropping {} expired-window sstables from {} (retention cutoff {})",
                                toDrop.size(), cfs.getTableName(), cutoff);
                    // A drop round attempts neither rewrite, so it breaks the consecutive-failure chains
                    // the repeat-WARN below relies on (T2-M8).
                    previousFreezeCandidate = Set.of();
                    previousSplitCandidate = Set.of();
                    refreshDelegateBacklog(gcBefore);
                    return List.of(new TimeSeriesCompactionTask(cfs, txn, gcBefore, cutoff, tsOptions.timestampResolution));
                }
                logger.debug("Unable to mark {} expired-window sstables for compaction in {}; probably a background " +
                             "compaction or the retention drop from a previous round got to them first",
                             toDrop.size(), cfs.getTableName());
            }
        }

        // Rounds with no attempt on a path (no candidate, or a lone survivor) break that path's
        // "consecutive failures" chain: its repeat-WARN must mean two attempts in a row on the same set.
        // Each branch therefore publishes its refusal - empty when it did not attempt anything - rather
        // than leaving the field for a trailing reset that an early return would skip.
        Set<SSTableReader> freezeRefused = Set.of();
        if (freeze != null)
        {
            // Same zombie filter as the expired branch: only live, non-suspect sstables. Note this
            // starts from the ELIGIBLE subset, never the whole window: see RewriteCandidate.
            Set<SSTableReader> toFreeze = new HashSet<>(AbstractCompactionStrategy.filterSuspectSSTables(freeze.eligible));
            toFreeze.retainAll(cfs.getLiveSSTables());
            if (toFreeze.size() > 1)                      // a single survivor is already "frozen enough"; self-heals next round
            {
                LifecycleTransaction txn = cfs.getTracker().tryModify(toFreeze, OperationType.COMPACTION);
                if (txn != null)
                {
                    previousFreezeCandidate = Set.of();
                    previousSplitCandidate = Set.of();    // the split branch made no attempt this round
                    logger.debug("Freezing window {} of {}: {} sstables -> 1", freeze.windowStart, cfs.getTableName(), toFreeze.size());
                    long window = freeze.windowStart;
                    // Signed on the UNFILTERED window, never on toFreeze - see RewriteCandidate.window.
                    long before = windowSignature(freeze.window);
                    refreshDelegateBacklog(gcBefore);
                    return List.of(new FreezeCompactionTask(cfs, txn, gcBefore, window,
                                                            () -> recordCompletedRewrite(window, before, "freeze")));
                }
                // Refused: a race the eligibility filter cannot pre-empt (the tracker marked these
                // compacting between the snapshot and here). Fall through - to the split branch and then
                // to the delegate - and retry next round (spec section 8).
                warnRepeatedRefusal("freezing", freeze.windowStart, toFreeze, previousFreezeCandidate);
                freezeRefused = toFreeze;
            }
        }
        previousFreezeCandidate = freezeRefused;

        // Legacy SPANNING sstables (single sstable failing containment in a closed window) are
        // split-rewritten into per-window sstables - lower priority than regular freezes (plan D7), so
        // this is reached only when no freeze task was handed out. It is deliberately NOT gated on
        // whether the freeze branch *attempted* anything: one freeze candidate stuck behind a refusing
        // tracker used to suppress every split for as long as it stayed stuck, so a table with one
        // wedged freeze did no split work at all, round after round.
        Set<SSTableReader> splitRefused = Set.of();
        if (split != null)
        {
            Set<SSTableReader> toSplit = new HashSet<>(AbstractCompactionStrategy.filterSuspectSSTables(split.eligible));
            toSplit.retainAll(cfs.getLiveSSTables());
            if (toSplit.size() == 1)
            {
                LifecycleTransaction txn = cfs.getTracker().tryModify(toSplit, OperationType.COMPACTION);
                if (txn != null)
                {
                    previousSplitCandidate = Set.of();
                    logger.debug("Split-refreezing spanning sstable in window {} of {}", split.windowStart, cfs.getTableName());
                    long window = split.windowStart;
                    long before = windowSignature(split.window);   // unfiltered, as above
                    refreshDelegateBacklog(gcBefore);
                    return List.of(new SplitRefreezeCompactionTask(cfs, txn, gcBefore,
                                                                   tsOptions::windowStartFor, tsOptions.timestampResolution,
                                                                   () -> recordCompletedRewrite(window, before, "split-refreeze")));
                }
                warnRepeatedRefusal("split-refreezing", split.windowStart, toSplit, previousSplitCandidate);
                splitRefused = toSplit;
            }
        }
        previousSplitCandidate = splitRefused;

        // This refreshes the delegate's own estimatedRemainingTasks as a side effect, which is why the
        // fall-through path needs no refreshDelegateBacklog call (and must not make one).
        return delegate.getNextBackgroundTasks(gcBefore);
    }

    /**
     * The "same candidate refused twice running" WARN, throttled.
     * <p>
     * A refusal is not by itself a fault - it means something else is already rewriting these sstables -
     * so only a repeat is worth an operator's attention. But "repeat" is not rare: a candidate whose
     * sstables are held for the length of a large compaction is refused on every background round for as
     * long as that runs, and this logged unthrottled, so one contended window produced a WARN per round
     * for minutes on end. Keyed per table and per path so one table's (or one path's) stuck candidate
     * cannot mask another's.
     */
    private void warnRepeatedRefusal(String what, long windowStart, Set<SSTableReader> candidate, Set<SSTableReader> previous)
    {
        if (candidate.equals(previous))
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN,
                             cfs.getKeyspaceName() + '.' + cfs.getTableName() + ":refused:" + what,
                             1, TimeUnit.MINUTES,
                             "Could not acquire references for {} sstables {} which is not a problem per se," +
                             " unless it happens frequently, in which case it must be reported. Will retry later.",
                             what, candidate);
        else
            logger.debug("Unable to mark window {} of {} for {}; will retry next round",
                         windowStart, cfs.getTableName(), what);
    }

    /**
     * Recomputes the UCS delegate's {@code estimatedRemainingTasks}, which is the term
     * {@link #getEstimatedRemainingTasks()} contributes to the CompactionStrategyManager's ranking of
     * this table against every other one.
     * <p>
     * UCS only ever writes that field inside its own {@code getNextCompactionPick}, i.e. only on rounds
     * where it is actually asked for work. TSCS returns before the delegate on every expired/freeze/split
     * round, so on precisely the busy rounds - the ones where the ranking matters - the delegate's term
     * was whatever it happened to be when the delegate last ran, possibly many minutes stale and possibly
     * describing an sstable set that no longer exists. Same defect the freeze/split backlogs had (T2-M8),
     * fixed the same way: refresh it on every round.
     * <p>
     * Called only from the early-return paths, and deliberately so. {@code getNextCompactionPick} also
     * consumes UCS's throttled fully-expired-sstable check ({@code maybeGetExpiredSSTables} stamps
     * {@code lastExpiredCheck} and TSCS discards the result), so calling it unconditionally at the top of
     * the round would mean the check is always spent by us microseconds before the delegate's own call
     * finds it not due - UCS would never drop a fully expired sstable again. On rounds that reach the
     * delegate it refreshes the term itself, so those need nothing; on rounds that do not, UCS was going
     * to be given no chance to act on the check anyway, so spending it only defers the next check by at
     * most one expired_sstable_check_frequency. The pick itself is a plain ArrayList of readers - it
     * holds no references and no transaction - so discarding it leaks nothing.
     */
    private void refreshDelegateBacklog(long gcBefore)
    {
        delegate.getNextCompactionPick(gcBefore);
    }

    /**
     * Everything one background round needs to know about this instance's sstable slice, derived in a
     * single pass.
     * <p>
     * Selection used to walk the sstable set once per question - delegate sync, expired set, freeze
     * candidate, split candidate, guard pruning - the last three rebuilding a whole {@code TreeMap} of
     * windows through {@link TimeSeriesCompactionStrategy#windows()} first: five walks of a set that on
     * a large time-series table is tens of thousands of readers, three throwaway maps and (with the
     * availability filter) three {@code Tracker.getCompacting()} calls, every background round, for
     * every strategy instance the CompactionStrategyManager holds (per repair status, per disk). They
     * all already ran inside the same {@code synchronized} block, so they were guaranteed to observe
     * identical state; deriving it once is therefore a pure cost reduction, and must stay one. Nothing
     * here may filter or reshape what the classifier sees - the compacting/EARLY filter is a separate,
     * explicitly-requested view ({@link #eligible}), never baked into {@link #windows}.
     * <p>
     * Deliberately not reused past the round:
     * {@link TimeSeriesCompactionStrategy#recordCompletedRewrite} runs post-commit, when the window has
     * changed by definition, and recomputes {@link TimeSeriesCompactionStrategy#windows()} for itself.
     * Feeding it this snapshot would compare the shape a rewrite produced against the shape it started
     * from and conclude "nothing changed" for every rewrite ever run.
     */
    private static final class Round
    {
        final long nowMillis;
        /** Ascending by window start, so "oldest first" is just iteration order. */
        final NavigableMap<Long, Set<SSTableReader>> windows;
        final Set<SSTableReader> active;
        final Set<SSTableReader> farFuture;
        final Set<SSTableReader> expired;
        /**
         * One {@code Tracker.getCompacting()} call per round, held by reference exactly as
         * {@code UnifiedCompactionStrategy#getCompactableSSTables} holds it.
         */
        final Set<SSTableReader> compacting;

        Round(long nowMillis, NavigableMap<Long, Set<SSTableReader>> windows, Set<SSTableReader> active,
              Set<SSTableReader> farFuture, Set<SSTableReader> expired, Set<SSTableReader> compacting)
        {
            this.nowMillis = nowMillis;
            this.windows = windows;
            this.active = active;
            this.farFuture = farFuture;
            this.expired = expired;
            this.compacting = compacting;
        }

        /**
         * The members of {@code window} this round may actually hand to {@code Tracker.tryModify}:
         * {@code UnifiedCompactionStrategy#isSuitableForCompaction} (not suspect, not an EARLY reader
         * opened by a compaction still in flight) plus its caller's not-already-compacting test.
         * <p>
         * <b>Eligibility only.</b> An sstable being unavailable this instant says nothing about the
         * window's shape, so this view must never reach {@link TimeSeriesCompactionStrategy#classify}
         * or {@code windowSignature} - see {@link RewriteCandidate}.
         */
        Set<SSTableReader> eligible(Set<SSTableReader> window)
        {
            Set<SSTableReader> eligible = new HashSet<>(window.size());
            for (SSTableReader sstable : window)
                if (!compacting.contains(sstable)
                    && sstable.openReason != SSTableReader.OpenReason.EARLY
                    && !sstable.isMarkedSuspect())
                    eligible.add(sstable);
            return eligible;
        }
    }

    /**
     * One window selected for a rewrite, carrying the two views of it that must not be confused.
     * <p>
     * {@link #window} is the window as it is: every sstable whose max timestamp falls in it. That is
     * what {@link TimeSeriesCompactionStrategy#classify} judged and what {@code windowSignature} signs,
     * and it has to be, because {@link TimeSeriesCompactionStrategy#recordCompletedRewrite} compares the
     * signature taken here against one it recomputes
     * post-commit from the full window. Sign the filtered set instead and the two are drawn from
     * different populations: any sstable that starts or stops compacting between hand-out and completion
     * changes the "before" without anything having happened to the window, the guard's chain breaks, and
     * the no-progress guard silently stops catching the freeze/split livelock it exists to bound.
     * <p>
     * {@link #eligible} is the subset this round may actually try to mark compacting.
     */
    @VisibleForTesting
    static final class RewriteCandidate
    {
        final long windowStart;
        final Set<SSTableReader> window;
        final Set<SSTableReader> eligible;

        RewriteCandidate(long windowStart, Set<SSTableReader> window, Set<SSTableReader> eligible)
        {
            this.windowStart = windowStart;
            this.window = window;
            this.eligible = eligible;
        }
    }

    private synchronized Round snapshot(long nowMillis)
    {
        NavigableMap<Long, Set<SSTableReader>> windows = new TreeMap<>();
        Set<SSTableReader> active = new HashSet<>();
        Set<SSTableReader> farFuture = new HashSet<>();
        Set<SSTableReader> expired = new HashSet<>();
        for (SSTableReader sstable : sstables)
        {
            long windowStart = tsOptions.windowStartFor(maxTimestampMillis(sstable));
            windows.computeIfAbsent(windowStart, start -> new HashSet<>()).add(sstable);
            // Tested before the far-future "continue" because the pass this replaced (expiredSSTables)
            // had no far-future filter at all. The two are mutually exclusive under today's predicates -
            // an expired window start is in the past - so this changes nothing; it keeps the exclusion a
            // property of the predicates, as the old code did, rather than of this loop's shape.
            if (tsOptions.isExpiredWindow(windowStart, nowMillis))
                expired.add(sstable);
            if (tsOptions.isFarFutureWindow(windowStart, nowMillis))
            {
                farFuture.add(sstable);
                // Garbage/misconfigured-writer timestamps: keep them out of both the UCS delegate and the
                // freeze machinery, and complain (throttled, keyed per table so one table's warn does not
                // suppress another's) so the operator investigates (design spec section 8).
                NoSpamLogger.log(logger, NoSpamLogger.Level.WARN,
                                 cfs.getKeyspaceName() + '.' + cfs.getTableName() + ":far-future-window",
                                 1, TimeUnit.MINUTES,
                                 "{}.{} now holds {} sstable(s) with max timestamp beyond now + {}ms (e.g. {} in " +
                                 "window {}). They are excluded from delegate compaction, from freeze, from split " +
                                 "and from retention, so they accumulate permanently - one per flush while the bad " +
                                 "clock persists. Remedy: fix the writer clock or USING TIMESTAMP input, then " +
                                 "rewrite them with a user-defined or maximal compaction (both still include them). " +
                                 "The current set is on the table's MBean as FarFutureTimeSeriesSSTables.",
                                 cfs.getKeyspaceName(), cfs.getTableName(), farFuture.size(),
                                 tsOptions.maxFutureWindowMillis, sstable, windowStart);
                continue;
            }
            if (tsOptions.isActiveWindow(windowStart, nowMillis))
                active.add(sstable);
        }
        return new Round(nowMillis, windows, active, farFuture, expired, cfs.getTracker().getCompacting());
    }

    /** Sstables whose whole window has closed before the configured retention cutoff, if any. */
    @VisibleForTesting
    synchronized Set<SSTableReader> expiredSSTables(long nowMillis)
    {
        return snapshot(nowMillis).expired;
    }

    private synchronized void syncDelegate(Round round)
    {
        farFutureSSTables = round.farFuture;

        Set<SSTableReader> inDelegate = new HashSet<>(delegate.getSSTables());
        Set<SSTableReader> toRemove = new HashSet<>(inDelegate);
        toRemove.removeAll(round.active);
        Set<SSTableReader> toAdd = new HashSet<>(round.active);
        toAdd.removeAll(inDelegate);
        if (!toRemove.isEmpty())
            delegate.removeSSTables(toRemove);
        if (!toAdd.isEmpty())
            delegate.addSSTables(toAdd);
    }

    long maxTimestampMillis(SSTableReader sstable)
    {
        return TimeUnit.MILLISECONDS.convert(sstable.getMaxTimestamp(), tsOptions.timestampResolution);
    }

    long minTimestampMillis(SSTableReader sstable)
    {
        return TimeUnit.MILLISECONDS.convert(sstable.getMinTimestamp(), tsOptions.timestampResolution);
    }

    /** Window states, derived statelessly from sstable min/max timestamps every round (design spec section 3). */
    public enum WindowState { CURRENT, CLOSING, FREEZING, FROZEN, EXPIRED }

    /**
     * Classifies one window of this instance's sstable slice. Stateless: nothing is persisted, the state is a
     * pure function of (window key, sstables, now) - restart-safe, and a FROZEN window that gains a late
     * sstable reverts to FREEZING simply because the derivation changes (design spec sections 3-4).
     * <p>
     * FROZEN demands a single sstable <b>fully contained</b> in the window ({@code windowStartFor(min) ==
     * windowStartFor(max)}). A single sstable spanning window boundaries classifies FREEZING - it is not
     * frozen - but freeze selection ({@link #nextFreezeCandidate(Round)}) will not pick a single-sstable window:
     * merging one sstable into one sstable cannot restore containment. Splitting it can, which is what
     * {@link #nextSplitRefreezeCandidate(Round)} selects it for.
     * <p>
     * Scope: the CompactionStrategyManager splits strategy instances per repair status and per disk, so this
     * judgment (and the frozen event) is per instance slice, never per table.
     * <p>
     * Precondition: callers filter far-future windows first via
     * {@link TimeSeriesCompactionStrategyOptions#isFarFutureWindow} (design spec section 8).
     */
    @VisibleForTesting
    WindowState classify(long windowStartMillis, Set<SSTableReader> windowSSTables, long nowMillis)
    {
        if (tsOptions.isExpiredWindow(windowStartMillis, nowMillis))
            return WindowState.EXPIRED;
        if (tsOptions.isCurrentWindow(windowStartMillis, nowMillis))
            return WindowState.CURRENT;
        if (tsOptions.isActiveWindow(windowStartMillis, nowMillis))
            return WindowState.CLOSING;
        if (windowSSTables.size() == 1)
        {
            SSTableReader only = windowSSTables.iterator().next();
            if (tsOptions.windowStartFor(minTimestampMillis(only)) == tsOptions.windowStartFor(maxTimestampMillis(only)))
                return WindowState.FROZEN;
        }
        return WindowState.FREEZING;
    }

    /**
     * The window map, recomputed. The background round does not call this - it reads {@link Round#windows}
     * - but {@link #recordCompletedRewrite} and {@link #getMaximalTasksAt} deliberately do: both need the
     * set as it is at the moment they run, not as some earlier selection saw it.
     */
    @VisibleForTesting
    synchronized Map<Long, Set<SSTableReader>> windows()
    {
        Map<Long, Set<SSTableReader>> result = new TreeMap<>();
        for (SSTableReader sstable : sstables)
            result.computeIfAbsent(tsOptions.windowStartFor(maxTimestampMillis(sstable)), start -> new HashSet<>()).add(sstable);
        return result;
    }

    @VisibleForTesting
    synchronized RewriteCandidate nextFreezeCandidate(long nowMillis)
    {
        return nextFreezeCandidate(snapshot(nowMillis));
    }

    /**
     * The oldest FREEZING window with at least two sstables that the no-progress guard has not parked
     * <em>and that this round can actually start</em>, or null. Single-sstable FREEZING windows (a
     * spanning sstable failing containment) are not candidates: merging one sstable into one sstable
     * cannot restore containment, and selecting it would recompact it every round forever -
     * {@link #nextSplitRefreezeCandidate(Round)} owns those.
     * <p>
     * <b>Head-of-line blocking.</b> "Oldest" used to mean oldest full stop, so one sstable of the oldest
     * FREEZING window being compacted by anything else - a delegate compaction that began while the
     * window was still CLOSING, an operator's user-defined compaction - made every round pick that
     * window, get refused by the tracker, and hand out nothing: every newer freeze and (because a
     * refusal also suppressed the split branch) every split starved for the whole length of that
     * compaction. Windows are therefore skipped when {@link Round#eligible} leaves them too small to
     * rewrite, and the next-oldest is taken instead.
     * <p>
     * <b>Shape questions are asked of the whole window.</b> The size test, {@link #classify} and
     * {@link #isParked} all see the unfiltered window; only the final "can this round start it" test
     * sees the eligible subset. A window whose members are momentarily unavailable has not changed
     * shape, and treating it as though it had would corrupt the no-progress guard (see
     * {@link RewriteCandidate}) and make the backlog vanish and reappear as unrelated compactions come
     * and go.
     * <p>
     * Side effect: refreshes {@link #freezeBacklog} with the number of windows that need freezing,
     * blocked ones included - they are pending work whether or not this particular round can start them,
     * and dropping them would tell the CompactionStrategyManager this table has nothing to do.
     */
    private RewriteCandidate nextFreezeCandidate(Round round)
    {
        RewriteCandidate oldest = null;
        int backlog = 0;
        for (Map.Entry<Long, Set<SSTableReader>> window : round.windows.entrySet())
        {
            // Precondition hygiene for classify's documented contract (callers filter far-future windows
            // first; spec section 8). Currently redundant defense-in-depth: isCurrentWindow also holds for
            // any future window, so classify would report CURRENT and the window would be skipped anyway -
            // but that is a property of today's predicates, not a guarantee. Keep the explicit filter so a
            // future predicate change cannot silently start freeze-judging garbage timestamps.
            if (tsOptions.isFarFutureWindow(window.getKey(), round.nowMillis))
                continue;
            if (window.getValue().size() < 2)
                continue;
            if (classify(window.getKey(), window.getValue(), round.nowMillis) != WindowState.FREEZING)
                continue;
            if (isParked(window.getKey(), window.getValue()))
                continue;                                 // freezing it again provably changes nothing; see recordCompletedRewrite
            backlog++;
            if (oldest != null)
                continue;                                 // already picked; keep counting the backlog
            Set<SSTableReader> eligible = round.eligible(window.getValue());
            if (eligible.size() < 2)
                continue;                                 // blocked this round, not done: try the next-oldest
            oldest = new RewriteCandidate(window.getKey(), window.getValue(), eligible);
        }
        freezeBacklog = backlog;
        return oldest;
    }

    @VisibleForTesting
    synchronized RewriteCandidate nextSplitRefreezeCandidate(long nowMillis)
    {
        return nextSplitRefreezeCandidate(snapshot(nowMillis));
    }

    /**
     * The oldest closed window whose single sstable fails containment, that the no-progress guard has
     * not parked, and that this round can start. Such an sstable is either legacy (pre-T3 data or a
     * strategy switch) or one TSCS wrote unsplit on purpose - a partition too large to window-route, or
     * a window folded onto another writer at the writer cap. These classify FREEZING but cannot be fixed
     * by freezing (merging one sstable into one sstable never restores containment) - they need
     * {@link SplitRefreezeCompactionTask}. Side effect: refreshes {@link #splitBacklog}, blocked windows
     * included. Filtering, classification and backlog discipline are exactly as in
     * {@link #nextFreezeCandidate(Round)}; here a window is blocked when its single sstable is the
     * unavailable one.
     */
    private RewriteCandidate nextSplitRefreezeCandidate(Round round)
    {
        RewriteCandidate oldest = null;
        int backlog = 0;
        for (Map.Entry<Long, Set<SSTableReader>> window : round.windows.entrySet())
        {
            if (tsOptions.isFarFutureWindow(window.getKey(), round.nowMillis))
                continue;                                 // same precondition hygiene as nextFreezeCandidate
            if (window.getValue().size() != 1)
                continue;
            if (classify(window.getKey(), window.getValue(), round.nowMillis) != WindowState.FREEZING)
                continue;                                 // FROZEN (contained) or still active/expired
            if (isParked(window.getKey(), window.getValue()))
                continue;                                 // splitting it again provably changes nothing; see recordCompletedRewrite
            backlog++;
            if (oldest != null)
                continue;
            Set<SSTableReader> eligible = round.eligible(window.getValue());
            if (eligible.size() != 1)
                continue;                                 // its one sstable is compacting/EARLY: blocked, not done
            oldest = new RewriteCandidate(window.getKey(), window.getValue(), eligible);
        }
        splitBacklog = backlog;
        return oldest;
    }

    /**
     * The no-progress guard, in one place for both rewrite paths.
     * <p>
     * Freeze and split-refreeze share a failure shape: the task's postcondition is not what
     * {@link #classify} tests, so the classifier re-selects the very same window on every background
     * round and the rewrite runs forever. Two real instances:
     * <ul>
     *   <li>a split whose output still spans window boundaries (a row whose timestamps straddle a
     *       boundary, before element-granularity routing; or a partition too large to route, which is
     *       written unsplit on purpose);</li>
     *   <li>a freeze that emits more than one sstable because {@code CompactionAwareWriter} switched
     *       location at a JBOD disk boundary, leaving the window at two sstables (T2-I3).</li>
     * </ul>
     * Candidate equality cannot detect either: every rewrite produces a fresh generation, so the
     * candidate set differs each round while the window's <em>shape</em> is unchanged. The guard
     * therefore tracks the shape - {@link #windowSignature} - immediately before and immediately after
     * each rewrite, and parks a window whose rewrites keep returning it to a shape they have already
     * produced ({@link #NO_PROGRESS_STRIKES} times).
     * <p>
     * <b>Chains, not single rewrites.</b> "The rewrite changed nothing" is too narrow a test, because
     * the two paths can undo <em>each other</em> while each one changes the window. A closed window
     * whose single spanning sstable holds an un-routable (over-budget) partition <em>and</em> ordinary
     * data alternates forever: the split writes the overflowing partition unsplit into one sstable and
     * the ordinary data into another, so the window goes 1 sstable -> 2 and classifies FREEZING; the
     * freeze merges those 2 back into 1 still-spanning sstable, so it classifies FREEZING again and is
     * re-split. Both rewrites change the shape, so a per-rewrite test resets on every one of them and
     * the giant partition is rewritten end to end forever. (Parking on shape alone only ever worked
     * when the sstable held <em>nothing but</em> the overflowing partition.)
     * <p>
     * So the unit is a <b>chain</b>: consecutive completed rewrites in which each one started from the
     * shape the previous one left behind. Inside a chain nothing but compaction has touched the
     * window, so a shape the chain has produced before means the rewrites are going round in a circle,
     * whichever paths they came from - which is exactly what has to be bounded. A rewrite that starts
     * from a shape no rewrite produced (a late flush, streaming, an operator's compaction landed in
     * between) begins a fresh chain and forgets everything.
     * <p>
     * <b>Chains, not round-to-round.</b> Comparing the shape a window has when a task is handed out
     * with the shape it had at the previous hand-out cannot tell a stuck window from a healthy one
     * under steady late data: a closed window that receives one late flush per round oscillates 2
     * sstables -> 1 -> 2, and every round samples it at 2 with an identical signature, so three
     * <em>successful</em> freezes would park a window that is working perfectly. Chaining fixes that
     * for the same reason it catches the alternation: the freeze left the window at 1 sstable and the
     * next freeze starts from 2, so the late flush - not a rewrite - is what moved it, the chain
     * breaks, and nothing is scored.
     * <p>
     * <b>Completed rewrites only.</b> This is called strictly post-commit by the task itself, so a
     * freeze aborted by the disk-space pre-flight, stopped by {@code nodetool stop COMPACTION} or
     * killed by an IO error never scores. Counting hand-outs instead meant three consecutive full-disk
     * rounds parked a window, and freeing the disk did not un-park it: nothing about the window had
     * changed, so nothing reset the guard, and only a restart or a shape change recovered it.
     * <p>
     * Parking is deliberately self-healing: it is keyed on the shape the chain was parked at, so any
     * change to the window (a late flush, an operator's {@code nodetool relocatesstables} or major
     * compaction) silently un-parks it. Parking is a safety net, not a fix - a parked window never
     * freezes, so {@link org.apache.cassandra.db.compaction.timeseries.WindowFrozenListener} never
     * fires for it and downstream consumers never see it. That is why it warns, and why
     * {@link #getParkedWindows()} exposes the current set.
     *
     * @param signatureBefore the window's shape when this rewrite was handed out
     */
    @VisibleForTesting
    synchronized void recordCompletedRewrite(long windowStartMillis, long signatureBefore, String what)
    {
        Set<SSTableReader> after = windows().getOrDefault(windowStartMillis, Set.of());
        long signatureAfter = windowSignature(after);

        WindowProgress progress = windowProgress.get(windowStartMillis);
        if (progress == null || progress.lastSignature != signatureBefore)
        {
            // This rewrite started from a shape no rewrite of this window produced, so something
            // outside compaction moved it: whatever the previous chain was doing is history.
            progress = new WindowProgress();
            progress.revisits(signatureBefore);
            windowProgress.put(windowStartMillis, progress);
        }
        progress.lastSignature = signatureAfter;

        if (!progress.revisits(signatureAfter))
        {
            // A shape this chain has never been at: the rewrites are still getting somewhere.
            progress.strikes = 0;
            return;
        }
        if (++progress.strikes < NO_PROGRESS_STRIKES)
            return;

        progress.parked = true;
        logger.warn("Parking window {} of {}.{}: {} completed {} time(s) returning the window to a shape its " +
                    "rewrites had already produced ({} sstable(s), spanning windows {}). Not re-selecting it " +
                    "for freeze or split until something outside compaction changes the window - it will not " +
                    "freeze, so downstream consumers of the frozen-window event will not see it. Investigate: " +
                    "for a window alternating between one spanning sstable and two, check for a partition too " +
                    "large to window-route sharing an sstable with ordinary data; for a window stuck at several " +
                    "sstables on JBOD, run nodetool relocatesstables.",
                    windowStartMillis, cfs.getKeyspaceName(), cfs.getTableName(), what, progress.strikes,
                    after.size(), describeSpans(after));
    }

    /** True if the guard has parked this window at its current shape. */
    @VisibleForTesting
    synchronized boolean isParked(long windowStartMillis, Set<SSTableReader> windowSSTables)
    {
        WindowProgress progress = windowProgress.get(windowStartMillis);
        return progress != null && progress.parked && progress.lastSignature == windowSignature(windowSSTables);
    }

    /**
     * A window's "shape": how many sstables it holds, and how many windows those sstables span in
     * total. A freeze that works shrinks the count; a split that works shrinks the span (usually to the
     * point where the window holds one contained sstable and is not a candidate at all). Anything a
     * rewrite can legitimately achieve moves one of the two, so an unchanged pair means the rewrite
     * achieved nothing.
     */
    private long windowSignature(Set<SSTableReader> windowSSTables)
    {
        long spanTotal = 0;
        for (SSTableReader sstable : windowSSTables)
            spanTotal += windowsSpanned(sstable);
        return ((long) windowSSTables.size() << 32) | Math.min(spanTotal, 0xFFFFFFFFL);
    }

    /** How many windows this sstable's min..max write-timestamp range covers; 1 when contained. */
    private long windowsSpanned(SSTableReader sstable)
    {
        long first = tsOptions.windowStartFor(minTimestampMillis(sstable));
        long last = tsOptions.windowStartFor(maxTimestampMillis(sstable));
        long width = last - first;
        if (width <= 0)                                   // contained, inverted, or overflowed on garbage timestamps
            return 1;
        return Math.min(width / tsOptions.windowSizeMillis + 1, 1 << 20);
    }

    private String describeSpans(Set<SSTableReader> windowSSTables)
    {
        StringBuilder sb = new StringBuilder();
        for (SSTableReader sstable : windowSSTables)
            sb.append(sb.length() == 0 ? "" : ", ")
              .append(sstable)
              .append('[').append(tsOptions.windowStartFor(minTimestampMillis(sstable)))
              .append("..").append(tsOptions.windowStartFor(maxTimestampMillis(sstable))).append(']');
        return sb.toString();
    }

    /**
     * Drops guard bookkeeping for windows this instance no longer holds, so the map cannot grow
     * unbounded, and republishes {@link #parkedWindows} for {@link #getParkedWindows()}.
     */
    private synchronized void pruneWindowProgress(Round round)
    {
        Map<Long, Set<SSTableReader>> current = round.windows;
        if (!windowProgress.isEmpty())
            windowProgress.keySet().retainAll(current.keySet());

        NavigableMap<Long, Set<SSTableReader>> parked = new TreeMap<>();
        for (Map.Entry<Long, Set<SSTableReader>> window : current.entrySet())
            if (isParked(window.getKey(), window.getValue()))
                parked.put(window.getKey(), Set.copyOf(window.getValue()));
        parkedWindows = Collections.unmodifiableNavigableMap(parked);
    }

    /**
     * Windows the no-progress guard has parked, mapped to the sstables they are parked at. Exposed for
     * the same reason as {@link #getFarFutureSSTables()}: a parked window is deliberately skipped by
     * {@link #nextFreezeCandidate(Round)} and {@link #nextSplitRefreezeCandidate(Round)}, so it drops out of
     * {@code freezeBacklog}/{@code splitBacklog} and out of {@link #getEstimatedRemainingTasks()} - it
     * looks exactly like a table with nothing to do. It is not: a parked window never reaches FROZEN,
     * so nothing that only a rewrite of the window does happens for it -- in particular, rows shadowed by
     * a later deletion routed into the same window (the tiering re-encoder's range deletes, above all)
     * stay on disk and are read and merged by every query over that range until the window changes
     * shape or leaves retention. (The tiering re-encoder itself runs on its own scheduler and does not
     * wait for a freeze.) Without this, the only signal is a single WARN at the moment of parking.
     * <p>
     * Refreshed on every background round; empty is the healthy state. Un-parking is automatic on any
     * change to the window - see {@link #recordCompletedRewrite}.
     * <p>
     * Reaches operators via the table's own MBean, aggregated over this table's strategy instances:
     * {@link org.apache.cassandra.db.ColumnFamilyStoreMBean#getParkedTimeSeriesWindows()}.
     */
    public NavigableMap<Long, Set<SSTableReader>> getParkedWindows()
    {
        return parkedWindows;
    }

    /**
     * SSTables excluded from every automatic path (UCS delegate, freeze, split, and retention - an
     * expired window can never be a future one) because their window lies beyond {@code
     * max_future_window}. They accumulate permanently until an operator intervenes, so this is exposed
     * rather than only warned about; the remedy is a user-defined or maximal compaction over them
     * (both still include far-future sstables) once the writer clock or {@code USING TIMESTAMP} input
     * that produced them is fixed.
     * <p>
     * Reaches operators via the table's own MBean, aggregated over this table's strategy instances:
     * {@link org.apache.cassandra.db.ColumnFamilyStoreMBean#getFarFutureTimeSeriesSSTables()}.
     */
    public Set<SSTableReader> getFarFutureSSTables()
    {
        return farFutureSSTables;
    }

    @VisibleForTesting
    UnifiedCompactionStrategy delegate()
    {
        return delegate;
    }

    @Override
    public synchronized void addSSTable(SSTableReader added)
    {
        sstables.add(added);
    }

    @Override
    public synchronized void removeSSTable(SSTableReader removed)
    {
        sstables.remove(removed);
        delegate.removeSSTable(removed);
    }

    @Override
    protected synchronized Set<SSTableReader> getSSTables()
    {
        return new HashSet<>(sstables);
    }

    @Override
    public List<AbstractCompactionTask> getMaximalTasks(long gcBefore, boolean splitOutput)
    {
        return getMaximalTasksAt(Clock.Global.currentTimeMillis(), gcBefore, splitOutput);
    }

    @VisibleForTesting
    List<AbstractCompactionTask> getMaximalTasksAt(long nowMillis, long gcBefore, boolean splitOutput)
    {
        // Preserve the window invariant for maximal compaction too: never cross windows, one task per window.
        // A window that is itself expired is routed through TimeSeriesCompactionTask so its retention cutoff
        // is honoured here too, rather than silently rewriting it via a plain CompactionTask.
        List<AbstractCompactionTask> tasks = new ArrayList<>();
        for (Map.Entry<Long, Set<SSTableReader>> entry : windows().entrySet())
        {
            Collection<SSTableReader> window = AbstractCompactionStrategy.filterSuspectSSTables(entry.getValue());
            if (window.isEmpty())
                continue;
            LifecycleTransaction txn = cfs.getTracker().tryModify(window, OperationType.COMPACTION);
            if (txn == null)
                continue;
            tasks.add(tsOptions.isExpiredWindow(entry.getKey(), nowMillis)
                      ? new TimeSeriesCompactionTask(cfs, txn, gcBefore, nowMillis - tsOptions.retentionMillis, tsOptions.timestampResolution)
                      : new CompactionTask(cfs, txn, gcBefore));
        }
        return tasks;                                     // never null, unlike TWCS
    }

    @Override
    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> toCompact, long gcBefore)
    {
        assert !toCompact.isEmpty();
        LifecycleTransaction txn = cfs.getTracker().tryModify(toCompact, OperationType.COMPACTION);
        if (txn == null)
        {
            logger.trace("Unable to mark {} for compaction; probably a background compaction got to it first", toCompact);
            return null;
        }
        return new CompactionTask(cfs, txn, gcBefore).setUserDefined(true);
    }

    /**
     * Four volatile reads and nothing else. This is polled by the CompactionStrategyManager to rank
     * tables against one another, so it must not select work of its own; every term is refreshed by the
     * background round instead ({@link #refreshDelegateBacklog}, {@link #nextFreezeCandidate(Round)},
     * {@link #nextSplitRefreezeCandidate(Round)}), which is where the cost belongs.
     */
    @Override
    public int getEstimatedRemainingTasks()
    {
        return delegate.getEstimatedRemainingTasks()
               + (lastExpiredSelection.isEmpty() ? 0 : 1)
               + freezeBacklog
               + splitBacklog;
    }

    @Override
    public long getMaxSSTableBytes()
    {
        return Long.MAX_VALUE;
    }

    /**
     * Flush and streaming both create sstables through this hook (via
     * {@code ColumnFamilyStore.createSSTableMultiWriter}); splitting them at window boundaries here
     * upholds the "every sstable belongs to exactly one window" invariant (design spec section 4)
     * that whole-window drops and per-window freezing rely on. Note this intentionally forgoes the
     * UCS delegate's token sharding for flushes (plan D6).
     */
    @Override
    public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor,
                                                       long keyCount,
                                                       long repairedAt,
                                                       TimeUUID pendingRepair,
                                                       boolean isTransient,
                                                       IntervalSet<CommitLogPosition> commitLogPositions,
                                                       int sstableLevel,
                                                       SerializationHeader header,
                                                       Collection<Index.Group> indexGroups,
                                                       ILifecycleTransaction txn)
    {
        return new TimeWindowSplittingMultiWriter(cfs,
                                                  descriptor,
                                                  keyCount,
                                                  repairedAt,
                                                  pendingRepair,
                                                  isTransient,
                                                  commitLogPositions,
                                                  sstableLevel,
                                                  header,
                                                  indexGroups,
                                                  txn,
                                                  tsOptions::windowStartFor,
                                                  tsOptions.timestampResolution);
    }

    @Override
    public void startup()
    {
        super.startup();
        delegate.startup();
    }

    @Override
    public void shutdown()
    {
        super.shutdown();
        delegate.shutdown();
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        Map<String, String> unchecked = AbstractCompactionStrategy.validateOptions(options);
        unchecked = TimeSeriesCompactionStrategyOptions.validateOptions(options, unchecked);
        unchecked = Controller.validateOptions(unchecked);
        unchecked.remove(CompactionParams.Option.MIN_THRESHOLD.toString());
        unchecked.remove(CompactionParams.Option.MAX_THRESHOLD.toString());
        return unchecked;
    }

    @Override
    public String toString()
    {
        return String.format("TimeSeriesCompactionStrategy[window=%dms/freeze=%dms/retention=%dms]",
                             tsOptions.windowSizeMillis, tsOptions.freezeAfterMillis, tsOptions.retentionMillis);
    }
}
