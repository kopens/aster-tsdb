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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TimeSeriesCompactionStrategyTest
{
    private static final long NOW = 1_700_000_000_000L;
    private static final long HOUR = 3_600_000L;

    // AbstractCompactionStrategy's constructor calls cfs.getDirectories(); with RETURNS_DEEP_STUBS that makes
    // Mockito build a real Directories mock, whose class init touches DatabaseDescriptor (see
    // UnifiedCompactionStrategyTest#setUpClass for the same requirement).
    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static Map<String, String> options()
    {
        Map<String, String> map = new HashMap<>();
        map.put(TimeSeriesCompactionStrategyOptions.WINDOW_SIZE, "1h");
        map.put(TimeSeriesCompactionStrategyOptions.FREEZE_AFTER, "2h");
        return map;
    }

    /** UnifiedCompactionStrategyTest.mockSSTable 축소판: 창 분류에 필요한 것만 스텁 */
    static SSTableReader sstableAt(long maxTimestampMillis)
    {
        return sstableSpanning(maxTimestampMillis - 1, maxTimestampMillis);
    }

    /** min·max를 각각 지정하는 상태 기계용 확장판 (기존 sstableAt(max)는 min = max − 1ms 고정) */
    static SSTableReader sstableSpanning(long minTimestampMillis, long maxTimestampMillis)
    {
        SSTableReader sstable = mock(SSTableReader.class, Mockito.RETURNS_DEEP_STUBS);
        when(sstable.getMaxTimestamp()).thenReturn(maxTimestampMillis * 1000);   // timestampResolution 기본 MICROSECONDS
        when(sstable.getMinTimestamp()).thenReturn(minTimestampMillis * 1000);
        // AbstractCompactionTask.validateSSTables는 2개 이상 묶을 때 수리 상태 일관성을 검사한다 — 딥스텁은
        // getPendingRepair()에 목 TimeUUID를 내주면서 isPendingRepair()는 false라 자기모순이 된다
        // (UnifiedCompactionStrategyTest.mockSSTable과 같은 처방)
        when(sstable.isRepaired()).thenReturn(false);
        when(sstable.getPendingRepair()).thenReturn(null);
        return sstable;
    }

    /**
     * tryModify가 "요청 집합을 originals로 갖는 오프라인 txn 목"을 내주도록 스텁.
     * isOffline=true로 AbstractCompactionTask 생성자의 compacting-marked 단언(:52-58)을 우회한다 —
     * 딥스텁 기본값에 맡기면 originals가 목 Set이 되어 태스크 라우팅 단언이 불가능하다.
     */
    @SuppressWarnings("unchecked")
    private static void stubTracker(ColumnFamilyStore cfs)
    {
        when(cfs.getTracker().tryModify(anyCollection(), any(OperationType.class)))
            .thenAnswer(invocation -> {
                Set<SSTableReader> requested = new HashSet<>((Collection<SSTableReader>) invocation.getArgument(0));
                LifecycleTransaction txn = mock(LifecycleTransaction.class);
                when(txn.originals()).thenReturn(requested);
                when(txn.isOffline()).thenReturn(true);
                return txn;
            });
    }

    /**
     * As {@link #stubTracker}, but modelling the half of the real {@code Tracker.tryModify} contract the
     * always-granting stub cannot: a request that touches an sstable somebody else already holds is
     * refused wholesale. Without this a mock test cannot reproduce head-of-line blocking at all - in
     * production a freeze candidate containing one compacting sstable is *always* refused, which is
     * exactly why the round used to hand out nothing.
     */
    @SuppressWarnings("unchecked")
    private static void stubTrackerRefusing(ColumnFamilyStore cfs, Set<SSTableReader> unavailable)
    {
        when(cfs.getTracker().tryModify(anyCollection(), any(OperationType.class)))
            .thenAnswer(invocation -> {
                Set<SSTableReader> requested = new HashSet<>((Collection<SSTableReader>) invocation.getArgument(0));
                for (SSTableReader sstable : requested)
                    if (unavailable.contains(sstable))
                        return null;
                LifecycleTransaction txn = mock(LifecycleTransaction.class);
                when(txn.originals()).thenReturn(requested);
                when(txn.isOffline()).thenReturn(true);
                return txn;
            });
    }

    private static TimeSeriesCompactionStrategy strategy(UnifiedCompactionStrategy delegate)
    {
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        return new TimeSeriesCompactionStrategy(cfs, options(), delegate);
    }

    @Test
    public void classifiesSSTablesIntoWindows()
    {
        TimeSeriesCompactionStrategy tscs = strategy(mock(UnifiedCompactionStrategy.class));
        SSTableReader current = sstableAt(NOW - 60_000);
        SSTableReader old = sstableAt(NOW - 10 * HOUR);
        tscs.addSSTable(current);
        tscs.addSSTable(old);

        Map<Long, Set<SSTableReader>> windows = tscs.windows();
        assertEquals(2, windows.size());
        for (Map.Entry<Long, Set<SSTableReader>> window : windows.entrySet())
            for (SSTableReader sstable : window.getValue())
            {
                long ts = sstable.getMaxTimestamp() / 1000;
                assertTrue(window.getKey() <= ts && ts < window.getKey() + HOUR);
            }
    }

    @Test
    public void addAndRemoveAreIdempotent()
    {
        TimeSeriesCompactionStrategy tscs = strategy(mock(UnifiedCompactionStrategy.class));
        SSTableReader sstable = sstableAt(NOW);
        tscs.addSSTable(sstable);
        tscs.addSSTable(sstable);                       // 중복 통지 (CSM:1054)
        assertEquals(1, tscs.getSSTables().size());
        tscs.removeSSTable(sstable);
        tscs.removeSSTable(sstable);
        assertEquals(0, tscs.getSSTables().size());
    }

    @Test
    public void delegateSeesOnlyActiveWindows()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        TimeSeriesCompactionStrategy tscs = strategy(delegate);

        SSTableReader current = sstableAt(NOW - 60_000);                 // CURRENT
        SSTableReader closing = sstableAt(NOW - 2 * HOUR - 60_000);      // CLOSING (freeze_after 2h 이내)
        SSTableReader frozen = sstableAt(NOW - 10 * HOUR);               // 비활성
        tscs.addSSTable(current);
        tscs.addSSTable(closing);
        tscs.addSSTable(frozen);

        tscs.getNextBackgroundTasksAt(NOW, 0);                           // 테스트용 시각 주입 오버로드

        // One delegate per active window, so the adds arrive per window: check their union.
        @SuppressWarnings("unchecked")
        org.mockito.ArgumentCaptor<Iterable<SSTableReader>> captor = org.mockito.ArgumentCaptor.forClass(Iterable.class);
        verify(delegate, Mockito.atLeastOnce()).addSSTables(captor.capture());
        Set<SSTableReader> added = new java.util.HashSet<>();
        for (Iterable<SSTableReader> batch : captor.getAllValues())
            batch.forEach(added::add);
        assertTrue(added.contains(current) && added.contains(closing) && !added.contains(frozen));
        assertEquals("current and closing are different windows, so different delegates",
                     2, tscs.delegates().size());
    }

    @Test
    public void delegateIsPrunedWhenWindowsAge()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        TimeSeriesCompactionStrategy tscs = strategy(delegate);

        SSTableReader sstable = sstableAt(NOW - 60_000);
        tscs.addSSTable(sstable);
        tscs.getNextBackgroundTasksAt(NOW, 0);                           // 위임에 추가됨
        when(delegate.getSSTables()).thenReturn(Set.of(sstable));

        tscs.getNextBackgroundTasksAt(NOW + 4 * HOUR, 0);                // 창이 늙음 → 위임에서 제거
        verify(delegate).removeSSTables(Mockito.argThat(iterable ->
            com.google.common.collect.Sets.newHashSet(iterable).contains(sstable)));
    }

    @Test
    public void backgroundTasksComeFromDelegate()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        AbstractCompactionTask task = mock(AbstractCompactionTask.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of(task));
        TimeSeriesCompactionStrategy tscs = strategy(delegate);
        tscs.addSSTable(sstableAt(NOW - 60_000));

        assertEquals(List.of(task), List.copyOf(tscs.getNextBackgroundTasksAt(NOW, 0)));
    }

    @Test
    public void estimatedTasksSumsDelegate()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getEstimatedRemainingTasks()).thenReturn(3);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        TimeSeriesCompactionStrategy tscs = strategy(delegate);
        tscs.getNextBackgroundTasksAt(NOW, 0);                           // delegates exist from the first round on
        assertEquals(3, tscs.getEstimatedRemainingTasks());
    }

    @Test
    public void expiredWindowsAreSelectedForWholeDrop()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        Map<String, String> opts = options();
        opts.put(TimeSeriesCompactionStrategyOptions.RETENTION, "30d");
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, opts, delegate);

        SSTableReader live = sstableAt(NOW - HOUR);
        SSTableReader expired1 = sstableAt(NOW - 31L * 24 * HOUR);
        SSTableReader expired2 = sstableAt(NOW - 31L * 24 * HOUR + 60_000);   // same expired window
        tscs.addSSTable(live);
        tscs.addSSTable(expired1);
        tscs.addSSTable(expired2);

        Set<SSTableReader> selected = tscs.expiredSSTables(NOW);
        assertEquals(Set.of(expired1, expired2), selected);
    }

    @Test
    public void nothingExpiresWithoutRetention()
    {
        TimeSeriesCompactionStrategy tscs = strategy(mock(UnifiedCompactionStrategy.class));
        tscs.addSSTable(sstableAt(NOW - 400L * 24 * HOUR));
        assertEquals(Set.of(), tscs.expiredSSTables(NOW));
    }

    @Test
    public void expiredSelectionIsFilteredThroughLiveSSTablesAndNeverTriesTheTracker()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        AbstractCompactionTask delegateTask = mock(AbstractCompactionTask.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of(delegateTask));

        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        // Simulate a zombie: this instance's own bookkeeping still classifies the sstable as an expired
        // window, but the tracker no longer considers it live (e.g. removed via another path already).
        // Mirrors UnifiedCompactionStrategy#getSSTables' zombie filter (CASSANDRA-18342).
        when(cfs.getLiveSSTables()).thenReturn(Set.of());

        Map<String, String> opts = options();
        opts.put(TimeSeriesCompactionStrategyOptions.RETENTION, "30d");
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, opts, delegate);
        tscs.addSSTable(sstableAt(NOW - 31L * 24 * HOUR));               // expired per classification, but not live

        Collection<AbstractCompactionTask> tasks = tscs.getNextBackgroundTasksAt(NOW, 0);

        // Nothing survived the live-set filter, so the tracker is never asked to mark anything compacting,
        // and background tasks fall through to the delegate untouched.
        Mockito.verify(cfs.getTracker(), Mockito.never()).tryModify(Mockito.anyCollection(), Mockito.any(OperationType.class));
        assertEquals(List.of(delegateTask), List.copyOf(tasks));
    }

    @Test
    public void classifierDerivesAllFiveStates()
    {
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        Map<String, String> opts = options();
        opts.put(TimeSeriesCompactionStrategyOptions.RETENTION, "30d");
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, opts, mock(UnifiedCompactionStrategy.class));
        TimeSeriesCompactionStrategyOptions o = new TimeSeriesCompactionStrategyOptions(opts);

        SSTableReader current = sstableAt(NOW - 60_000);
        SSTableReader closing = sstableAt(NOW - 2 * HOUR - 60_000);
        SSTableReader freezing1 = sstableAt(NOW - 10 * HOUR);
        SSTableReader freezing2 = sstableAt(NOW - 10 * HOUR + 60_000);
        SSTableReader frozen = sstableAt(NOW - 20 * HOUR);                       // 단일 + min·max 같은 창(포함)
        SSTableReader expired = sstableAt(NOW - 31L * 24 * HOUR);

        assertEquals(TimeSeriesCompactionStrategy.WindowState.CURRENT,
                     tscs.classify(o.windowStartFor(NOW - 60_000), Set.of(current), NOW));
        assertEquals(TimeSeriesCompactionStrategy.WindowState.CLOSING,
                     tscs.classify(o.windowStartFor(NOW - 2 * HOUR - 60_000), Set.of(closing), NOW));
        assertEquals(TimeSeriesCompactionStrategy.WindowState.FREEZING,
                     tscs.classify(o.windowStartFor(NOW - 10 * HOUR), Set.of(freezing1, freezing2), NOW));
        assertEquals(TimeSeriesCompactionStrategy.WindowState.FROZEN,
                     tscs.classify(o.windowStartFor(NOW - 20 * HOUR), Set.of(frozen), NOW));
        assertEquals(TimeSeriesCompactionStrategy.WindowState.EXPIRED,
                     tscs.classify(o.windowStartFor(NOW - 31L * 24 * HOUR), Set.of(expired), NOW));
    }

    @Test
    public void singleSpanningSSTableIsFreezingNotFrozen()
    {
        // FROZEN은 "창 경계 안에 완전히 포함된 단일 sstable" — min이 이전 창에 걸치면 동결 상태가 아니다
        TimeSeriesCompactionStrategy tscs = strategy(mock(UnifiedCompactionStrategy.class));
        TimeSeriesCompactionStrategyOptions o = new TimeSeriesCompactionStrategyOptions(options());
        SSTableReader spanning = sstableSpanning(NOW - 11 * HOUR, NOW - 10 * HOUR);
        assertEquals(TimeSeriesCompactionStrategy.WindowState.FREEZING,
                     tscs.classify(o.windowStartFor(NOW - 10 * HOUR), Set.of(spanning), NOW));
    }

    @Test
    public void lateDataRevertsFrozenToFreezingByPureDerivation()
    {
        // 상태 저장이 없으므로 FROZEN 창에 sstable이 더해지면 판정이 그 자리에서 FREEZING으로 되돌아간다 (스펙 §4)
        TimeSeriesCompactionStrategy tscs = strategy(mock(UnifiedCompactionStrategy.class));
        TimeSeriesCompactionStrategyOptions o = new TimeSeriesCompactionStrategyOptions(options());
        long windowStart = o.windowStartFor(NOW - 10 * HOUR);
        SSTableReader frozen = sstableAt(NOW - 10 * HOUR);
        SSTableReader late = sstableAt(NOW - 10 * HOUR + 30_000);
        assertEquals(TimeSeriesCompactionStrategy.WindowState.FROZEN, tscs.classify(windowStart, Set.of(frozen), NOW));
        assertEquals(TimeSeriesCompactionStrategy.WindowState.FREEZING, tscs.classify(windowStart, Set.of(frozen, late), NOW));
    }

    @Test
    public void farFutureSSTablesAreExcludedFromDelegate()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        TimeSeriesCompactionStrategy tscs = strategy(delegate);

        SSTableReader current = sstableAt(NOW - 60_000);
        SSTableReader farFuture = sstableAt(NOW + 3L * 24 * HOUR);   // 기본 max_future_window 1d 초과
        tscs.addSSTable(current);
        tscs.addSSTable(farFuture);

        tscs.getNextBackgroundTasksAt(NOW, 0);

        verify(delegate).addSSTables(Mockito.argThat(iterable -> {
            Set<SSTableReader> added = com.google.common.collect.Sets.newHashSet(iterable);
            return added.contains(current) && !added.contains(farFuture);
        }));
    }

    @Test
    public void farFutureSSTablesAreNeverHandedToADelegate()
    {
        // Delegates are created fresh per window and only ever hold what syncDelegate handed them, so there is
        // no "already in the delegate from before the guard" state left to prune: the guard is simply that a
        // far-future sstable is never handed over in the first place.
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        TimeSeriesCompactionStrategy tscs = strategy(delegate);

        SSTableReader farFuture = sstableAt(NOW + 3L * 24 * HOUR);
        tscs.addSSTable(farFuture);
        tscs.getNextBackgroundTasksAt(NOW, 0);

        verify(delegate, Mockito.never()).addSSTables(Mockito.argThat(iterable ->
            com.google.common.collect.Sets.newHashSet(iterable).contains(farFuture)));
    }

    @Test
    public void maximalTasksBuildOneTaskPerWindowAndRouteExpiredThroughRetentionTask()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        stubTracker(cfs);
        Map<String, String> opts = options();
        opts.put(TimeSeriesCompactionStrategyOptions.RETENTION, "30d");
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, opts, delegate);

        SSTableReader live = sstableAt(NOW - HOUR);
        SSTableReader expired = sstableAt(NOW - 31L * 24 * HOUR);
        tscs.addSSTable(live);
        tscs.addSSTable(expired);

        List<AbstractCompactionTask> tasks = tscs.getMaximalTasksAt(NOW, 0, false);
        assertEquals(2, tasks.size());                               // 창당 1개, 창을 절대 넘지 않는다
        for (AbstractCompactionTask task : tasks)
        {
            boolean coversExpired = task.transaction.originals().contains(expired);
            assertEquals("expired 창만 retention 태스크로 라우팅", coversExpired, task instanceof TimeSeriesCompactionTask);
        }
    }

    @Test
    public void maximalTasksReturnEmptyListWhenTrackerRefuses()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        when(cfs.getTracker().tryModify(anyCollection(), any(OperationType.class))).thenReturn(null);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);
        tscs.addSSTable(sstableAt(NOW - HOUR));

        assertEquals(List.of(), tscs.getMaximalTasksAt(NOW, 0, false));   // null이 아니라 빈 리스트 (TWCS 함정)
    }

    @Test
    public void userDefinedTaskMarksAndWraps()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        stubTracker(cfs);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);
        SSTableReader sstable = sstableAt(NOW - HOUR);

        AbstractCompactionTask task = tscs.getUserDefinedTask(List.of(sstable), 0);
        assertNotNull(task);
        assertTrue(task.isUserDefined);                              // 같은 패키지라 protected 필드 접근 가능
        assertEquals(Set.of(sstable), task.transaction.originals());
    }

    @Test
    public void userDefinedTaskReturnsNullWhenTrackerRefuses()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        when(cfs.getTracker().tryModify(anyCollection(), any(OperationType.class))).thenReturn(null);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);

        assertNull(tscs.getUserDefinedTask(List.of(sstableAt(NOW - HOUR)), 0));
    }

    @Test
    public void freezeTaskIsCreatedForClosedMultiSSTableWindow()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        stubTracker(cfs);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);

        SSTableReader current = sstableAt(NOW - 60_000);
        SSTableReader old1 = sstableAt(NOW - 10 * HOUR);
        SSTableReader old2 = sstableAt(NOW - 10 * HOUR + 60_000);    // 같은 닫힌 창
        tscs.addSSTable(current);
        tscs.addSSTable(old1);
        tscs.addSSTable(old2);
        when(cfs.getLiveSSTables()).thenReturn(Set.of(current, old1, old2));

        Collection<AbstractCompactionTask> tasks = tscs.getNextBackgroundTasksAt(NOW, 0);

        AbstractCompactionTask task = List.copyOf(tasks).get(0);
        assertEquals(1, tasks.size());                               // 라운드당 동결 최대 1개
        assertTrue(task instanceof FreezeCompactionTask);
        assertEquals(Set.of(old1, old2), task.transaction.originals());
        verify(delegate, Mockito.never()).getNextBackgroundTasks(anyLong());   // 동결이 위임보다 우선
    }

    @Test
    public void oldestFreezingWindowIsSelectedFirst()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        stubTracker(cfs);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);

        SSTableReader newer1 = sstableAt(NOW - 10 * HOUR);
        SSTableReader newer2 = sstableAt(NOW - 10 * HOUR + 60_000);
        SSTableReader older1 = sstableAt(NOW - 20 * HOUR);
        SSTableReader older2 = sstableAt(NOW - 20 * HOUR + 60_000);
        for (SSTableReader s : List.of(newer1, newer2, older1, older2))
            tscs.addSSTable(s);
        when(cfs.getLiveSSTables()).thenReturn(Set.of(newer1, newer2, older1, older2));

        Collection<AbstractCompactionTask> tasks = tscs.getNextBackgroundTasksAt(NOW, 0);

        assertEquals(1, tasks.size());
        assertEquals(Set.of(older1, older2), List.copyOf(tasks).get(0).transaction.originals());   // 오래된 창 우선
    }

    @Test
    public void frozenWindowIsNotReselected()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);
        TimeSeriesCompactionStrategyOptions o = new TimeSeriesCompactionStrategyOptions(options());

        SSTableReader contained = sstableAt(NOW - 10 * HOUR);        // 단일 + 포함(min = max − 1ms) = FROZEN
        tscs.addSSTable(contained);
        Collection<AbstractCompactionTask> tasks = tscs.getNextBackgroundTasksAt(NOW, 0);

        assertEquals(List.of(), List.copyOf(tasks));
        verify(cfs.getTracker(), Mockito.never()).tryModify(anyCollection(), any(OperationType.class));

        // Discriminating (T2-M7): assert it is CONTAINMENT that spares this window, not the freeze
        // path's "size < 2" filter. The split-refreeze path does consider single-sstable windows, so if
        // min and max fell in different windows this same sstable would be selected there.
        assertEquals(TimeSeriesCompactionStrategy.WindowState.FROZEN,
                     tscs.classify(o.windowStartFor(NOW - 10 * HOUR), Set.of(contained), NOW));
        assertNull(tscs.nextSplitRefreezeCandidate(NOW));
    }

    @Test
    public void spanningSingleSSTableIsNeverFreezeSelected()
    {
        // 포함 실패로 FREEZING이지만, 단일 sstable을 합쳐 봐야 포함이 복구되지 않는다 →
        // 동결 후보가 아니어야 무한 재컴팩션 루프가 없다. 대신 분할 재작성 후보다.
        TimeSeriesCompactionStrategy tscs = strategy(mock(UnifiedCompactionStrategy.class));
        TimeSeriesCompactionStrategyOptions o = new TimeSeriesCompactionStrategyOptions(options());
        SSTableReader spanning = sstableSpanning(NOW - 11 * HOUR, NOW - 10 * HOUR);
        tscs.addSSTable(spanning);

        assertNull(tscs.nextFreezeCandidate(NOW));

        // Discriminating (T2-M7): the FREEZING verdict really does come from failed containment, and the
        // window is not simply invisible to every path.
        assertEquals(TimeSeriesCompactionStrategy.WindowState.FREEZING,
                     tscs.classify(o.windowStartFor(NOW - 10 * HOUR), Set.of(spanning), NOW));
        assertNotNull(tscs.nextSplitRefreezeCandidate(NOW));
    }

    // ---------------------------------------------------------------------------------------------
    // The no-progress guard: one guard in the strategy for both livelock shapes.
    //
    // Strikes are scored by the task itself, post-commit, through the callback the strategy hands it -
    // not at hand-out. These tests therefore have to run that callback to simulate a rewrite that
    // actually committed; dispatching alone must score nothing (see guardScoresOnlyRewritesThatCompleted).
    // ---------------------------------------------------------------------------------------------

    /** Runs a dispatched rewrite task's post-commit guard callback: "this rewrite durably committed". */
    private static void complete(AbstractCompactionTask task)
    {
        if (task instanceof FreezeCompactionTask)
            ((FreezeCompactionTask) task).onCompleted().run();
        else if (task instanceof SplitRefreezeCompactionTask)
            ((SplitRefreezeCompactionTask) task).onCompleted().run();
        else
            throw new AssertionError("not a rewrite task: " + task);
    }

    private static AbstractCompactionTask onlyTask(Collection<AbstractCompactionTask> tasks)
    {
        assertEquals("expected exactly one task, got " + tasks, 1, tasks.size());
        return List.copyOf(tasks).get(0);
    }

    /** Replaces this instance's whole sstable set, as a committed rewrite or a late flush would. */
    private static void setWindow(TimeSeriesCompactionStrategy tscs, ColumnFamilyStore cfs, Set<SSTableReader> window)
    {
        for (SSTableReader s : tscs.getSSTables())
            tscs.removeSSTable(s);
        for (SSTableReader s : window)
            tscs.addSSTable(s);
        when(cfs.getLiveSSTables()).thenReturn(window);
    }

    private static long oldWindow()
    {
        return new TimeSeriesCompactionStrategyOptions(options()).windowStartFor(NOW - 10 * HOUR);
    }

    /**
     * T2-I3's shape: a freeze that keeps emitting two sstables (a JBOD disk-boundary writer switch)
     * leaves the window at two sstables, so it classifies FREEZING and is re-selected forever. Here the
     * sstable set never changes across the rewrite, which is exactly what the classifier sees in that
     * case. Candidate equality cannot catch it in production because each rewrite has a fresh
     * generation, so the guard compares the window's shape before and after the rewrite instead.
     */
    @Test
    public void freezeThatNeverChangesTheWindowIsParked()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        AbstractCompactionTask delegateTask = mock(AbstractCompactionTask.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of(delegateTask));
        when(delegate.getEstimatedRemainingTasks()).thenReturn(0);
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        stubTracker(cfs);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);

        SSTableReader a = sstableAt(NOW - 10 * HOUR);
        SSTableReader b = sstableAt(NOW - 10 * HOUR + 60_000);
        setWindow(tscs, cfs, Set.of(a, b));

        // Bounded: exactly NO_PROGRESS_STRIKES freezes actually run, each leaving the window untouched.
        for (int round = 0; round < TimeSeriesCompactionStrategy.NO_PROGRESS_STRIKES; round++)
        {
            AbstractCompactionTask task = onlyTask(tscs.getNextBackgroundTasksAt(NOW, 0));
            assertTrue("round " + round, task instanceof FreezeCompactionTask);
            complete(task);
        }

        // From here on every round falls straight through to the delegate.
        assertEquals(List.of(delegateTask), List.copyOf(tscs.getNextBackgroundTasksAt(NOW, 0)));
        assertEquals(List.of(delegateTask), List.copyOf(tscs.getNextBackgroundTasksAt(NOW, 0)));
        assertTrue(tscs.isParked(oldWindow(), Set.of(a, b)));
        // A parked window is no longer pending work, so it must not inflate the backlog either.
        assertEquals(0, tscs.getEstimatedRemainingTasks());
    }

    /** C1's shape: a split whose output still spans the same number of windows never will stop spanning. */
    @Test
    public void splitThatNeverReducesTheSpanIsParked()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        AbstractCompactionTask delegateTask = mock(AbstractCompactionTask.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of(delegateTask));
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        stubTracker(cfs);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);

        SSTableReader spanning = sstableSpanning(NOW - 11 * HOUR, NOW - 10 * HOUR);
        setWindow(tscs, cfs, Set.of(spanning));

        for (int round = 0; round < TimeSeriesCompactionStrategy.NO_PROGRESS_STRIKES; round++)
        {
            AbstractCompactionTask task = onlyTask(tscs.getNextBackgroundTasksAt(NOW, 0));
            assertTrue("round " + round, task instanceof SplitRefreezeCompactionTask);
            complete(task);
        }

        assertEquals(List.of(delegateTask), List.copyOf(tscs.getNextBackgroundTasksAt(NOW, 0)));
        assertTrue(tscs.isParked(oldWindow(), Set.of(spanning)));
    }

    /**
     * The freeze <-> split alternation, which parking on "this rewrite changed nothing" could not
     * catch because <em>both</em> rewrites change the window.
     * <p>
     * A closed window holding one spanning sstable that carries an over-budget (un-routable) partition
     * <b>and</b> ordinary data: the split writes the overflowing partition unsplit into one sstable and
     * the ordinary rows into their own, so the window goes 1 sstable -> 2 and is now a FREEZE
     * candidate; the freeze merges those 2 back into 1 still-spanning sstable, so it is a SPLIT
     * candidate again. Each path changed the shape, so each reset the other's strikes and the giant
     * partition was rewritten end to end forever.
     * <p>
     * Deliberately many more rounds than {@code NO_PROGRESS_STRIKES}: the loop must stop on its own,
     * and the assertion is that it stopped, not that it stopped after some particular count.
     */
    @Test
    public void freezeAndSplitCannotClearEachOthersStrikes()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        stubTracker(cfs);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);

        // The whole window keys off max timestamp, so both the spanning sstable and the ordinary one
        // sit in the same (old, closed) window - which is what makes the two paths fight over it.
        SSTableReader spanning = sstableSpanning(NOW - 11 * HOUR, NOW - 10 * HOUR);
        setWindow(tscs, cfs, Set.of(spanning));

        int rewrites = 0;
        for (int round = 0; round < 4 * TimeSeriesCompactionStrategy.NO_PROGRESS_STRIKES + 4; round++)
        {
            Collection<AbstractCompactionTask> tasks = tscs.getNextBackgroundTasksAt(NOW, 0);
            if (tasks.isEmpty())
                break;                                    // the guard parked the window: the loop ended

            AbstractCompactionTask task = onlyTask(tasks);
            rewrites++;
            if (task instanceof SplitRefreezeCompactionTask)
            {
                // The split re-routes the un-routable partition unsplit (same span as before) and lands
                // the ordinary rows in their own, contained sstable: two sstables, same window.
                setWindow(tscs, cfs, Set.of(sstableSpanning(NOW - 11 * HOUR, NOW - 10 * HOUR),
                                            sstableAt(NOW - 10 * HOUR + 60_000)));
            }
            else
            {
                assertTrue("round " + round, task instanceof FreezeCompactionTask);
                // The freeze merges them back into one sstable that still spans, because the partition
                // it cannot split is still in it.
                setWindow(tscs, cfs, Set.of(sstableSpanning(NOW - 11 * HOUR, NOW - 10 * HOUR)));
            }
            complete(task);
        }

        assertTrue("the alternation must stop, not run for every round: " + rewrites + " rewrites",
                   rewrites <= 4 * TimeSeriesCompactionStrategy.NO_PROGRESS_STRIKES);
        assertTrue("the window must end up parked", tscs.isParked(oldWindow(), tscs.getSSTables()));
        assertEquals(List.of(), List.copyOf(tscs.getNextBackgroundTasksAt(NOW, 0)));
    }

    /**
     * I1: a rewrite that never ran must never score a strike.
     * <p>
     * The guard used to count hand-outs, so a freeze aborted by the disk-space pre-flight, stopped by
     * {@code nodetool stop COMPACTION} or thrown from still scored. Three consecutive full-disk rounds
     * parked the window - and freeing the disk did not un-park it, because parking is keyed on the
     * window's shape and nothing about the window had changed. Only a restart or a shape change
     * recovered; until then the window never froze and the frozen-window event never fired for it.
     * Here nothing is ever completed, so no number of rounds may park the window.
     */
    @Test
    public void guardScoresOnlyRewritesThatCompleted()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        stubTracker(cfs);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);

        SSTableReader a = sstableAt(NOW - 10 * HOUR);
        SSTableReader b = sstableAt(NOW - 10 * HOUR + 60_000);
        setWindow(tscs, cfs, Set.of(a, b));

        for (int round = 0; round < 3 * TimeSeriesCompactionStrategy.NO_PROGRESS_STRIKES + 2; round++)
        {
            AbstractCompactionTask task = onlyTask(tscs.getNextBackgroundTasksAt(NOW, 0));
            assertTrue("round " + round + " must still be dispatched", task instanceof FreezeCompactionTask);
            // deliberately NOT completed: this is the aborted / stopped / failed rewrite
        }
        assertFalse(tscs.isParked(oldWindow(), Set.of(a, b)));
        assertTrue(tscs.getParkedWindows().isEmpty());
    }

    /**
     * Parking is keyed on the window's shape, so it is self-healing: late data, an operator's
     * relocatesstables or a major compaction all change the shape and silently un-park the window. A
     * park that could never be undone would strand the window - it would never freeze, so the
     * frozen-window event would never fire for it.
     */
    @Test
    public void parkedWindowIsUnparkedWhenTheWindowChanges()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        stubTracker(cfs);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);

        SSTableReader a = sstableAt(NOW - 10 * HOUR);
        SSTableReader b = sstableAt(NOW - 10 * HOUR + 60_000);
        setWindow(tscs, cfs, Set.of(a, b));

        for (int round = 0; round < TimeSeriesCompactionStrategy.NO_PROGRESS_STRIKES; round++)
            complete(onlyTask(tscs.getNextBackgroundTasksAt(NOW, 0)));
        assertTrue(tscs.isParked(oldWindow(), Set.of(a, b)));

        SSTableReader late = sstableAt(NOW - 10 * HOUR + 120_000);
        setWindow(tscs, cfs, Set.of(a, b, late));

        assertFalse(tscs.isParked(oldWindow(), Set.of(a, b, late)));
        assertTrue(onlyTask(tscs.getNextBackgroundTasksAt(NOW, 0)) instanceof FreezeCompactionTask);
    }

    /**
     * I2/I5: a window that keeps changing must never be parked, however many rounds it takes - and that
     * includes the steady-late-data shape that used to false-positive.
     * <p>
     * A closed window receiving one late flush per round oscillates 2 sstables -> 1 -> 2, and every
     * round samples it at 2 with an identical {@code (count, spanTotal)} signature. A guard that
     * compares one hand-out with the previous hand-out sees "nothing changed" three times running and
     * parks a window whose freezes are all working perfectly - after which it never freezes again and
     * the frozen-window event stops firing for it. Attributing a change to the rewrite that caused it
     * is what fixes this: the freeze took the window from 2 sstables to 1, and the guard sees that.
     * <p>
     * Deliberately many more rounds than {@code NO_PROGRESS_STRIKES}: the previous version of this test
     * ran two rounds against {@code NO_PROGRESS_STRIKES == 2}, so deleting the signature comparison
     * outright still left it green.
     */
    @Test
    public void progressResetsTheGuard()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        stubTracker(cfs);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);

        for (int round = 0; round < 3 * TimeSeriesCompactionStrategy.NO_PROGRESS_STRIKES + 2; round++)
        {
            // The window holds two sstables of an identical shape at the start of every round.
            setWindow(tscs, cfs, Set.of(sstableAt(NOW - 10 * HOUR), sstableAt(NOW - 10 * HOUR + 60_000)));

            AbstractCompactionTask task = onlyTask(tscs.getNextBackgroundTasksAt(NOW, 0));
            assertTrue("round " + round, task instanceof FreezeCompactionTask);

            // The freeze commits and leaves one sstable behind - real progress, whatever arrives next.
            setWindow(tscs, cfs, Set.of(sstableAt(NOW - 10 * HOUR)));
            complete(task);
            assertFalse("round " + round, tscs.isParked(oldWindow(), tscs.getSSTables()));
        }
    }

    /**
     * I3: a parked window has to be visible. {@link TimeSeriesCompactionStrategy#isParked} is consulted
     * before the backlog counter, so a parked window drops out of freezeBacklog/splitBacklog and hence
     * out of getEstimatedRemainingTasks() - the table then looks exactly like one with nothing to do,
     * while in fact a window has stopped freezing and every downstream consumer of the frozen-window
     * event has silently stopped receiving it for that window. One WARN at the transition is not enough.
     */
    @Test
    public void parkedWindowsAreExposedForOperators()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        when(delegate.getEstimatedRemainingTasks()).thenReturn(0);
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        stubTracker(cfs);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);

        SSTableReader a = sstableAt(NOW - 10 * HOUR);
        SSTableReader b = sstableAt(NOW - 10 * HOUR + 60_000);
        setWindow(tscs, cfs, Set.of(a, b));
        assertTrue("nothing is parked in the healthy state", tscs.getParkedWindows().isEmpty());

        for (int round = 0; round < TimeSeriesCompactionStrategy.NO_PROGRESS_STRIKES; round++)
            complete(onlyTask(tscs.getNextBackgroundTasksAt(NOW, 0)));

        // The snapshot is republished on the next background round, like every other operator-visible
        // figure this strategy keeps.
        tscs.getNextBackgroundTasksAt(NOW, 0);

        assertEquals(Set.of(oldWindow()), tscs.getParkedWindows().keySet());
        assertEquals(Set.of(a, b), tscs.getParkedWindows().get(oldWindow()));
        // ...which is exactly the information getEstimatedRemainingTasks() cannot carry.
        assertEquals(0, tscs.getEstimatedRemainingTasks());
    }

    @Test
    public void farFutureWindowNeverCountsTowardFreeze()
    {
        // Pins end-to-end behavior (spec section 8): a far-future window is never a freeze candidate and
        // never counts toward the backlog, however many sstables it has. Note this cannot distinguish the
        // explicit far-future filter in nextFreezeCandidate from the classifier's verdict (any future
        // window classifies CURRENT under today's predicates, which also skips it) - it pins the
        // behavior, not the filter; see the precondition-hygiene comment at the filter site.
        TimeSeriesCompactionStrategy tscs = strategy(mock(UnifiedCompactionStrategy.class));
        tscs.addSSTable(sstableAt(NOW + 3L * 24 * HOUR));
        tscs.addSSTable(sstableAt(NOW + 3L * 24 * HOUR + 60_000));

        assertNull(tscs.nextFreezeCandidate(NOW));
        assertEquals(0, tscs.getEstimatedRemainingTasks());
    }

    @Test
    public void freezeSkipsWhenZombieFilterLeavesSingleSurvivor()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);

        SSTableReader old1 = sstableAt(NOW - 10 * HOUR);
        SSTableReader old2 = sstableAt(NOW - 10 * HOUR + 60_000);
        tscs.addSSTable(old1);
        tscs.addSSTable(old2);
        when(cfs.getLiveSSTables()).thenReturn(Set.of(old1));        // old2는 좀비: 트래커 기준 이미 비활성

        tscs.getNextBackgroundTasksAt(NOW, 0);

        // 생존자 1개짜리 동결은 무의미 — 다음 라운드에 집합이 안정되면 자연 치유된다
        verify(cfs.getTracker(), Mockito.never()).tryModify(anyCollection(), any(OperationType.class));
    }

    @Test
    public void tryModifyRefusalSkipsRoundAndRetriesUntilItSucceeds()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        AbstractCompactionTask delegateTask = mock(AbstractCompactionTask.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of(delegateTask));
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);

        SSTableReader old1 = sstableAt(NOW - 10 * HOUR);
        SSTableReader old2 = sstableAt(NOW - 10 * HOUR + 60_000);
        tscs.addSSTable(old1);
        tscs.addSSTable(old2);
        when(cfs.getLiveSSTables()).thenReturn(Set.of(old1, old2));
        when(cfs.getTracker().tryModify(anyCollection(), any(OperationType.class))).thenReturn(null);

        // 1·2라운드: 트래커 거부(예: CLOSING 시절 시작된 위임 컴팩션 진행 중) → 위임으로 폴스루.
        // 2라운드째 동일 후보 재실패는 TWCS previousCandidate 가드(:100-106)의 크로스-라운드 판으로 WARN만 남긴다.
        assertEquals(List.of(delegateTask), List.copyOf(tscs.getNextBackgroundTasksAt(NOW, 0)));
        assertEquals(List.of(delegateTask), List.copyOf(tscs.getNextBackgroundTasksAt(NOW, 0)));
        verify(cfs.getTracker(), Mockito.times(2)).tryModify(anyCollection(), any(OperationType.class));

        // 3라운드: 경합이 풀리면 정상 동결 — 가드가 살아있는 후보를 영구히 굶기지 않는다는 증명
        stubTracker(cfs);
        Collection<AbstractCompactionTask> tasks = tscs.getNextBackgroundTasksAt(NOW, 0);
        assertEquals(1, tasks.size());
        assertTrue(List.copyOf(tasks).get(0) instanceof FreezeCompactionTask);
    }

    @Test
    public void freezeBacklogCountsInEstimatedRemainingTasks()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        when(delegate.getEstimatedRemainingTasks()).thenReturn(3);
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        stubTracker(cfs);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);

        SSTableReader a1 = sstableAt(NOW - 10 * HOUR);
        SSTableReader a2 = sstableAt(NOW - 10 * HOUR + 60_000);
        SSTableReader b1 = sstableAt(NOW - 20 * HOUR);
        SSTableReader b2 = sstableAt(NOW - 20 * HOUR + 60_000);
        for (SSTableReader s : List.of(a1, a2, b1, b2))
            tscs.addSSTable(s);
        when(cfs.getLiveSSTables()).thenReturn(Set.of(a1, a2, b1, b2));

        tscs.getNextBackgroundTasksAt(NOW, 0);                       // 백로그 계산은 배경 라운드의 부수효과

        // 위임 3 + 만료 0 + FREEZING 창 2 — 백로그를 안 세면 CSM 정렬에서 다른 테이블에 밀려 동결이 기아한다
        assertEquals(5, tscs.getEstimatedRemainingTasks());
    }

    @Test
    public void spanningSingleIsSelectedForSplitRefreeze()
    {
        // T3: 레거시 걸침 단일 sstable은 동결 후보는 아니지만(위 테스트) 분할 재작성 후보다 (스펙 §4/§10)
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        stubTracker(cfs);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);

        SSTableReader spanning = sstableSpanning(NOW - 11 * HOUR, NOW - 10 * HOUR);
        tscs.addSSTable(spanning);
        when(cfs.getLiveSSTables()).thenReturn(Set.of(spanning));

        Collection<AbstractCompactionTask> tasks = tscs.getNextBackgroundTasksAt(NOW, 0);

        assertEquals(1, tasks.size());
        AbstractCompactionTask task = List.copyOf(tasks).get(0);
        assertTrue(task instanceof SplitRefreezeCompactionTask);
        assertEquals(Set.of(spanning), task.transaction.originals());
        verify(delegate, Mockito.never()).getNextBackgroundTasks(anyLong());   // 분할이 위임보다 우선
    }

    @Test
    public void normalFreezeBeatsSplitRefreeze()
    {
        // 우선순위: 만료 > 동결 > 분할 재작성 > 위임 (라운드당 태스크 1개)
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        stubTracker(cfs);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);

        SSTableReader spanning = sstableSpanning(NOW - 31 * HOUR, NOW - 30 * HOUR);   // 더 오래된 걸침 창
        SSTableReader f1 = sstableAt(NOW - 10 * HOUR);
        SSTableReader f2 = sstableAt(NOW - 10 * HOUR + 60_000);
        for (SSTableReader s : List.of(spanning, f1, f2))
            tscs.addSSTable(s);
        when(cfs.getLiveSSTables()).thenReturn(Set.of(spanning, f1, f2));

        Collection<AbstractCompactionTask> tasks = tscs.getNextBackgroundTasksAt(NOW, 0);

        assertEquals(1, tasks.size());
        assertTrue(List.copyOf(tasks).get(0) instanceof FreezeCompactionTask);   // 걸침이 더 오래돼도 동결 우선
    }

    @Test
    public void splitBacklogCountsInEstimatedRemainingTasks()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        when(delegate.getEstimatedRemainingTasks()).thenReturn(0);
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        stubTracker(cfs);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);

        SSTableReader spanA = sstableSpanning(NOW - 11 * HOUR, NOW - 10 * HOUR);
        SSTableReader spanB = sstableSpanning(NOW - 21 * HOUR, NOW - 20 * HOUR);
        tscs.addSSTable(spanA);
        tscs.addSSTable(spanB);
        when(cfs.getLiveSSTables()).thenReturn(Set.of(spanA, spanB));

        tscs.getNextBackgroundTasksAt(NOW, 0);

        // 위임 0 + 만료 0 + 동결 0 + 걸침 2 — 분할 백로그도 CSM 정렬에 보여야 기아하지 않는다
        assertEquals(2, tscs.getEstimatedRemainingTasks());
    }

    // ---------------------------------------------------------------------------------------------
    // Availability: what the round may start, versus what the window IS.
    //
    // Selection now filters out sstables another operation already holds (Tracker.getCompacting()) and
    // EARLY readers, mirroring UnifiedCompactionStrategy#isSuitableForCompaction + #getCompactableSSTables.
    // The filter decides ELIGIBILITY ONLY: classification, the backlog counters and the no-progress
    // guard's signature all keep looking at the whole window, because an sstable being busy right now is
    // not a change in the window's shape.
    // ---------------------------------------------------------------------------------------------

    /**
     * Head-of-line blocking. One sstable of the OLDEST FREEZING window is being compacted by something
     * else - a delegate compaction that began while the window was still CLOSING, an operator's
     * user-defined compaction - so the tracker refuses that window. Selection picked the oldest window
     * unconditionally, so it picked the refused one on every round for the whole length of that
     * compaction and handed out nothing: every newer freeze waited behind it, and (see
     * {@code splitIsNotSuppressedByABlockedFreeze}) so did every split.
     */
    @Test
    public void freezeSkipsABlockedWindowAndTakesTheNextEligibleOne()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        when(delegate.getEstimatedRemainingTasks()).thenReturn(0);
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);

        SSTableReader older1 = sstableAt(NOW - 20 * HOUR);
        SSTableReader older2 = sstableAt(NOW - 20 * HOUR + 60_000);
        SSTableReader newer1 = sstableAt(NOW - 10 * HOUR);
        SSTableReader newer2 = sstableAt(NOW - 10 * HOUR + 60_000);
        for (SSTableReader s : List.of(older1, older2, newer1, newer2))
            tscs.addSSTable(s);
        when(cfs.getLiveSSTables()).thenReturn(Set.of(older1, older2, newer1, newer2));
        when(cfs.getTracker().getCompacting()).thenReturn(Set.of(older1));
        stubTrackerRefusing(cfs, Set.of(older1));

        AbstractCompactionTask task = onlyTask(tscs.getNextBackgroundTasksAt(NOW, 0));

        assertTrue(task instanceof FreezeCompactionTask);
        assertEquals(Set.of(newer1, newer2), task.transaction.originals());
        // The blocked window is not done, only unstartable this round: it must still read as pending
        // work, or the CompactionStrategyManager deprioritises this table while a freeze is waiting.
        assertEquals(2, tscs.getEstimatedRemainingTasks());
    }

    /**
     * The companion to the test above: skipping a blocked window must not degenerate into "take any
     * eligible window". Three FREEZING windows with the MIDDLE one blocked - the oldest is still the one
     * selected, so freezes keep coming out in window order and the oldest data still freezes first.
     */
    @Test
    public void oldestEligibleFreezingWindowIsStillTakenOldestFirst()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        when(delegate.getEstimatedRemainingTasks()).thenReturn(0);
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);

        SSTableReader oldest1 = sstableAt(NOW - 30 * HOUR);
        SSTableReader oldest2 = sstableAt(NOW - 30 * HOUR + 60_000);
        SSTableReader blocked1 = sstableAt(NOW - 20 * HOUR);
        SSTableReader blocked2 = sstableAt(NOW - 20 * HOUR + 60_000);
        SSTableReader newest1 = sstableAt(NOW - 10 * HOUR);
        SSTableReader newest2 = sstableAt(NOW - 10 * HOUR + 60_000);
        for (SSTableReader s : List.of(oldest1, oldest2, blocked1, blocked2, newest1, newest2))
            tscs.addSSTable(s);
        when(cfs.getLiveSSTables()).thenReturn(Set.of(oldest1, oldest2, blocked1, blocked2, newest1, newest2));
        when(cfs.getTracker().getCompacting()).thenReturn(Set.of(blocked1));
        stubTrackerRefusing(cfs, Set.of(blocked1));

        AbstractCompactionTask task = onlyTask(tscs.getNextBackgroundTasksAt(NOW, 0));

        assertTrue(task instanceof FreezeCompactionTask);
        assertEquals(Set.of(oldest1, oldest2), task.transaction.originals());
        assertEquals(3, tscs.getEstimatedRemainingTasks());          // all three windows are pending work
    }

    /**
     * A refused freeze used to set the round's "attempted" flag before calling tryModify, and that flag
     * gated the split branch - so one freeze candidate the tracker kept refusing silently stopped ALL
     * split-refreeze work on the table for as long as it stayed stuck. Split is lower priority than
     * freeze (plan D7); lower priority is not "cancelled by a freeze that did not happen".
     * <p>
     * The refusal here is the plain tryModify race (spec section 8), not the compacting filter: the
     * point is the control flow after a refusal, whatever caused it.
     */
    @Test
    public void splitIsNotSuppressedByABlockedFreeze()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        AbstractCompactionTask delegateTask = mock(AbstractCompactionTask.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of(delegateTask));
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);

        SSTableReader f1 = sstableAt(NOW - 20 * HOUR);
        SSTableReader f2 = sstableAt(NOW - 20 * HOUR + 60_000);
        SSTableReader spanning = sstableSpanning(NOW - 11 * HOUR, NOW - 10 * HOUR);
        for (SSTableReader s : List.of(f1, f2, spanning))
            tscs.addSSTable(s);
        when(cfs.getLiveSSTables()).thenReturn(Set.of(f1, f2, spanning));
        stubTrackerRefusing(cfs, Set.of(f1, f2));                    // only the freeze window is refused

        AbstractCompactionTask task = onlyTask(tscs.getNextBackgroundTasksAt(NOW, 0));

        assertTrue("a refused freeze must not cancel the split branch", task instanceof SplitRefreezeCompactionTask);
        assertEquals(Set.of(spanning), task.transaction.originals());
    }

    /**
     * The single easiest thing to get wrong in the availability filter, and the most silent.
     * <p>
     * {@link TimeSeriesCompactionStrategy#recordCompletedRewrite} chains rewrites by comparing the shape
     * a rewrite was handed against the shape the previous rewrite left behind; a mismatch means
     * something outside compaction moved the window, so the chain - and every strike scored on it - is
     * discarded. If the "before" shape were taken from the eligible subset while the "after" shape is
     * recomputed from the whole window, then any sstable starting or stopping compaction between
     * hand-out and completion would break the chain although nothing about the window changed. Nothing
     * would ever accumulate two strikes, the guard would never park anything, and the freeze/split
     * livelock it exists to bound would be back - with no symptom other than compaction never stopping.
     * <p>
     * Here the window never changes and the freeze achieves nothing (T2-I3's JBOD shape), but a member
     * becomes compacting between the first hand-out and the second. The window must still be parked.
     */
    @Test
    public void guardSignatureIsComputedOnTheUnfilteredWindow()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        AbstractCompactionTask delegateTask = mock(AbstractCompactionTask.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of(delegateTask));
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        stubTracker(cfs);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);

        SSTableReader a = sstableAt(NOW - 10 * HOUR);
        SSTableReader b = sstableAt(NOW - 10 * HOUR + 60_000);
        SSTableReader c = sstableAt(NOW - 10 * HOUR + 120_000);
        setWindow(tscs, cfs, Set.of(a, b, c));

        // Strike 1: the whole window is available, the freeze commits, and the window is unchanged.
        AbstractCompactionTask first = onlyTask(tscs.getNextBackgroundTasksAt(NOW, 0));
        assertTrue(first instanceof FreezeCompactionTask);
        assertEquals(Set.of(a, b, c), first.transaction.originals());
        complete(first);

        // Now b is busy elsewhere. The window is exactly as it was - 3 sstables, none of them spanning.
        when(cfs.getTracker().getCompacting()).thenReturn(Set.of(b));

        AbstractCompactionTask second = onlyTask(tscs.getNextBackgroundTasksAt(NOW, 0));
        assertTrue(second instanceof FreezeCompactionTask);
        assertEquals("only the available members may be marked", Set.of(a, c), second.transaction.originals());
        complete(second);

        // Strike 2 landed on the same chain, so the window is parked - keyed, as always, on the shape of
        // the whole window and not on whatever subset happened to be free.
        assertTrue("the chain must survive a member becoming compacting mid-flight",
                   tscs.isParked(oldWindow(), Set.of(a, b, c)));
        assertEquals(List.of(delegateTask), List.copyOf(tscs.getNextBackgroundTasksAt(NOW, 0)));
    }

    /**
     * The eligibility filter, observed at the only place it is allowed to have an effect: the set handed
     * to the tracker. In production tryModify would simply refuse a set containing a compacting sstable,
     * so an unfiltered freeze of this window is a round that accomplishes nothing.
     */
    @Test
    public void freezeHandsTheTrackerOnlyTheAvailableMembersOfTheWindow()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        when(delegate.getEstimatedRemainingTasks()).thenReturn(0);
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);

        SSTableReader a = sstableAt(NOW - 10 * HOUR);
        SSTableReader b = sstableAt(NOW - 10 * HOUR + 60_000);
        SSTableReader c = sstableAt(NOW - 10 * HOUR + 120_000);
        for (SSTableReader s : List.of(a, b, c))
            tscs.addSSTable(s);
        when(cfs.getLiveSSTables()).thenReturn(Set.of(a, b, c));
        when(cfs.getTracker().getCompacting()).thenReturn(Set.of(b));
        stubTrackerRefusing(cfs, Set.of(b));

        AbstractCompactionTask task = onlyTask(tscs.getNextBackgroundTasksAt(NOW, 0));

        assertTrue(task instanceof FreezeCompactionTask);
        assertEquals(Set.of(a, c), task.transaction.originals());
        // One window, counted once: the filter changed what may be compacted, not what needs freezing.
        assertEquals(1, tscs.getEstimatedRemainingTasks());
    }

    /**
     * getEstimatedRemainingTasks() is how the CompactionStrategyManager ranks this table against every
     * other one, and the delegate's contribution to it is a field UCS only writes inside its own
     * getNextCompactionPick. TSCS returns before the delegate on every expired/freeze/split round, so on
     * exactly the busy rounds - the ones where the ranking decides whether this table gets a compaction
     * thread at all - the delegate's term was whatever it had been when the delegate last ran. The
     * freeze and split backlogs were fixed for this in T2-M8; this term was not.
     * <p>
     * The delegate here is stubbed to behave like UCS does: the estimate moves only when
     * getNextCompactionPick runs. Reverting the fix leaves the assertion reading the stale 0.
     */
    @Test
    public void estimatedRemainingTasksIsRefreshedOnRoundsThatReturnEarly()
    {
        AtomicInteger delegateEstimate = new AtomicInteger(0);
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        when(delegate.getEstimatedRemainingTasks()).thenAnswer(invocation -> delegateEstimate.get());
        when(delegate.getNextCompactionPick(anyLong())).thenAnswer(invocation -> {
            delegateEstimate.set(7);                                 // UCS's own refresh point, modelled
            return null;
        });
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        stubTracker(cfs);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);

        SSTableReader a = sstableAt(NOW - 10 * HOUR);
        SSTableReader b = sstableAt(NOW - 10 * HOUR + 60_000);
        setWindow(tscs, cfs, Set.of(a, b));

        AbstractCompactionTask task = onlyTask(tscs.getNextBackgroundTasksAt(NOW, 0));
        assertTrue(task instanceof FreezeCompactionTask);
        // The round returned before the delegate, which is the whole point: it must still have refreshed.
        verify(delegate, Mockito.never()).getNextBackgroundTasks(anyLong());
        assertEquals(7 + 1, tscs.getEstimatedRemainingTasks());      // delegate 7 + freeze backlog 1
    }
}
