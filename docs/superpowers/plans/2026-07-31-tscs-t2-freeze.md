<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# TSCS T2 (동결 상태 기계 + 동결 컴팩션 + 이벤트 훅) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 닫힌 창을 창당 단일 SSTable로 수렴시키는 TSCS 2단계. 무상태 창 상태 기계(CURRENT/CLOSING/FREEZING/FROZEN/EXPIRED), far-future 가드(`max_future_window`), 동결 전용 `FreezeCompactionTask`(커밋 후 `WindowFrozenListener` 발화), 그리고 T1 인계 4건(far-future 가드, 닫힌 창 TTL 공백, min timestamp 배관, getMaximalTasks/getUserDefinedTask 목 커버리지)의 완전 흡수 — 스펙 §3/§5/§6/§8/§11.

**Architecture:** 상태는 어디에도 저장하지 않는다 — 매 컴팩션 라운드에 SSTable의 min/max timestamp에서 재유도한다(스펙 §3, TWCS 무상태 원칙). `windows()`(max timestamp 기준 창 분류, T1)에 min timestamp 배관을 더해 FROZEN 판정("창 경계 안에 완전히 포함된 단일 SSTable")을 가능하게 하고, `getNextBackgroundTasksAt`의 만료 브랜치와 위임 폴스루 **사이**에 동결 브랜치를 꽂는다(우선순위: 만료 > 동결 > 위임). 동결 태스크는 `UnifiedCompactionTask`가 아니라 **평범한 `CompactionTask` 기반**이다 — UCS 태스크는 샤드 분할 라이터로 다중 산출물을 내지만, `CompactionTask`의 `DefaultCompactionWriter`는 라이터를 교체하지 않아 "창당 1 SSTable"이 공짜로 성립한다(CompactionTask.java:400-406). 리스너 레지스트리는 `LocalSessions`의 정적 `CopyOnWriteArraySet` 패턴(repair/consistent/LocalSessions.java:131, 1174-1193)을 새 패키지 `org.apache.cassandra.db.compaction.timeseries`에 미러링한다.

**스코프 주의(전 태스크 공통):** CSM은 전략 인스턴스를 리페어 상태 4계층 × 디스크 N개로 쪼갠다. 따라서 창 상태·"창당 1 SSTable"·FROZEN 판정·동결 이벤트는 전부 **CSM 인스턴스 슬라이스 단위**이지 테이블 전체 단위가 아니다. 분류기 javadoc·스펙 §11 갱신에 이 사실을 명시한다.

**Tech Stack:** Java 21, 기존 컴팩션 프레임워크(CompactionTask/CompactionController/LifecycleTransaction), Mockito(T1의 주입-시각 하네스 확장), SchemaLoader(e2e), NoSpamLogger(far-future 경고 스로틀).

## Global Constraints

- 신규 의존성 금지. `src/gen-java/`, `lib/`, 문법 파일 수정 금지. **UCS/TWCS 업스트림 파일 수정 금지**(위임·미러만 — `CompactionTask`/`AbstractCompactionTask` 등 프레임워크 본체도 오버라이드로만 확장).
- 모든 신규 `.java` 파일에 ASF 라이선스 헤더(기존 파일 1–17행과 동일 블록) 필수.
- 전략·태스크 클래스는 `org.apache.cassandra.db.compaction` 패키지(짧은 이름 해석 규칙). 리스너 훅만 스펙 §5가 지정한 새 패키지 `org.apache.cassandra.db.compaction.timeseries`.
- `validateOptions` 정적 계약 유지: **소비하지 않은 옵션만** 반환. 새 옵션 `max_future_window`도 같은 규칙으로 소비·strip.
- CSM은 리페어 상태 4계층 × 디스크 N개로 전략 인스턴스를 쪼갠다: **인스턴스는 자신이 받은 SSTable만 본다**. 창 상태는 매 호출 파생, 전 테이블 가정 금지, `addSSTable`/`removeSSTable`은 중복 통지에 멱등.
- `getEstimatedRemainingTasks()`는 CSM의 스케줄링 우선순위 — 동결 백로그를 반영하되(안 하면 다른 테이블 뒤에서 기아) 유휴 시 0 반환.
- 오버플로 안전 비교 필수: sstable 유래 타임스탬프(적대적 입력 가능)에는 절대 기간을 더하지 않는다 — TimeSeriesCompactionStrategyOptions.java:80-90의 subtraction-form javadoc 규율을 모든 신규 술어가 따르고, javadoc으로 근거를 남긴다.
- **동결 태스크의 산출물은 창당(인스턴스 슬라이스 기준) 정확히 1개** — 전량 만료로 창이 소멸하는 경우(산출 0개, 이벤트 미발화)만 예외. `shouldReduceScopeForSpace()`의 입력 축소(가장 큰 입력 탈락)는 이 계약을 깨므로 반드시 꺼서 결정적으로 만든다.
- 리스너 예외는 컴팩션 결과에 절대 영향을 주지 않는다(스펙 §5 격리 원칙) — 리스너별 try/catch + 로깅.
- TWCS의 함정 회피: `getMaximalTasks`는 빈 경우 `null`이 아니라 빈 리스트 반환.
- 컴파일: `ant build-test -Dno-checkstyle=true -Dant.gen-doc.skip=true -Drat.skip=true 2>&1 | .build/sh/ant-log-summary.py -`
- 테스트: `.build/sh/ai-ci-test --reuse <FQCN>` (**--reuse는 컴파일 안 함 — 반드시 위 컴파일 선행**), 결과는 `build/test/output/TEST-<FQCN>.xml`의 `failures`/`errors`로 확인.
- CI 이미지는 Java 21 (`eclipse-temurin:21-jdk`) — 로컬도 Java 21로 빌드.
- 커밋 트레일러 2줄 필수:
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`
  `Claude-Session: https://claude.ai/code/session_01Jruk9rM1N82xGmXD41RfmE`

## File Structure

- Modify (Task 1): `src/java/org/apache/cassandra/db/compaction/TimeSeriesCompactionStrategyOptions.java`, `test/unit/org/apache/cassandra/db/compaction/TimeSeriesCompactionStrategyOptionsTest.java`
- Modify (Task 2·3): `src/java/org/apache/cassandra/db/compaction/TimeSeriesCompactionStrategy.java`, `test/unit/org/apache/cassandra/db/compaction/TimeSeriesCompactionStrategyTest.java`
- Create (Task 3): `src/java/org/apache/cassandra/db/compaction/timeseries/WindowFrozenListener.java`, `src/java/org/apache/cassandra/db/compaction/timeseries/WindowFrozenListeners.java`, `src/java/org/apache/cassandra/db/compaction/FreezeCompactionTask.java`, `test/unit/org/apache/cassandra/db/compaction/timeseries/WindowFrozenListenersTest.java`
- Modify (Task 4): `test/unit/org/apache/cassandra/db/compaction/TimeSeriesCompactionStrategyE2ETest.java`
- Modify (Task 5): `.gitlab-ci.yml`, `docs/superpowers/specs/2026-07-31-timeseries-compaction-design.md`(§11), `README.md`(기능 표), `CHANGES.txt`, `CLAUDE.md`(충돌 지점)

---

### Task 1: far-future 가드 옵션 + 창 술어 (`max_future_window`, `isCurrentWindow`, `isFarFutureWindow`)

far-future 가드는 동결 선택 **이전에** 착지해야 한다(스펙 §11: 쓰레기 타임스탬프가 창을 동결로 오판하지 않게). 이 태스크는 옵션·술어만 넣고, 배선은 Task 2가 한다.

**Files:**
- Modify: `src/java/org/apache/cassandra/db/compaction/TimeSeriesCompactionStrategyOptions.java`
- Modify: `test/unit/org/apache/cassandra/db/compaction/TimeSeriesCompactionStrategyOptionsTest.java`

**Interfaces:**
- Consumes: 기존 `parseDuration`/`DURATION` 정규식(`<positive int><m|h|d>`, :149-169), `isActiveWindow`의 subtraction-form javadoc 규율(:80-90)
- Produces (Task 2·3이 사용):
  - `public static final String MAX_FUTURE_WINDOW = "max_future_window"` / `static final String DEFAULT_MAX_FUTURE_WINDOW = "1d"`
  - `public final long maxFutureWindowMillis;`
  - `boolean isCurrentWindow(long windowStartMillis, long nowMillis)` — `windowEnd > now` (CURRENT vs CLOSING 판별)
  - `boolean isFarFutureWindow(long windowStartMillis, long nowMillis)` — `windowStart > now + maxFutureWindow`

- [ ] **Step 1: 실패하는 테스트 추가** — `TimeSeriesCompactionStrategyOptionsTest`에:

```java
    @Test
    public void maxFutureWindowDefaultsAndParses()
    {
        assertEquals(24 * 3_600_000L, new TimeSeriesCompactionStrategyOptions(options()).maxFutureWindowMillis);   // 기본 1d
        assertEquals(2 * 3_600_000L,
                     new TimeSeriesCompactionStrategyOptions(options("max_future_window", "2h")).maxFutureWindowMillis);
    }

    @Test
    public void currentWindowBoundary()
    {
        TimeSeriesCompactionStrategyOptions opts =
            new TimeSeriesCompactionStrategyOptions(options("window_size", "1h", "freeze_after", "2h"));
        long now = 1_700_000_000_000L;
        long currentStart = opts.windowStartFor(now);
        assertTrue(opts.isCurrentWindow(currentStart, now));
        // 직전 창: CURRENT는 아니지만 freeze_after 유예 안이므로 여전히 활성(CLOSING 몫)
        assertFalse(opts.isCurrentWindow(currentStart - opts.windowSizeMillis, now));
        assertTrue(opts.isActiveWindow(currentStart - opts.windowSizeMillis, now));
        // 창 시작 시각 자신(windowEnd == now + windowSize)은 CURRENT
        assertTrue(opts.isCurrentWindow(currentStart, currentStart));
    }

    @Test
    public void farFutureClassificationIsOverflowSafe()
    {
        TimeSeriesCompactionStrategyOptions opts = new TimeSeriesCompactionStrategyOptions(options());   // max_future_window 기본 1d
        long now = 1_700_000_000_000L;
        long day = 24 * 3_600_000L;
        assertFalse(opts.isFarFutureWindow(opts.windowStartFor(now), now));
        assertFalse(opts.isFarFutureWindow(opts.windowStartFor(now + day - 3_600_000L), now));   // 한도 이내 미래
        assertTrue(opts.isFarFutureWindow(opts.windowStartFor(now + 2 * day), now));
        // 쓰레기 타임스탬프의 창(Long.MAX_VALUE 근처)도 반드시 far-future 판정 — 오버플로로 뒤집히면 안 된다
        assertTrue(opts.isFarFutureWindow(opts.windowStartFor(Long.MAX_VALUE - 775_000L), now));
    }

    @Test
    public void validateRejectsBadMaxFutureWindow()
    {
        for (String bad : new String[]{ "0h", "-1d", "1w", "abc", "" })
            assertThatThrownBy(() -> TimeSeriesCompactionStrategyOptions.validateOptions(
                                   options("max_future_window", bad), new HashMap<>(options("max_future_window", bad))))
            .as("max_future_window=" + bad)
            .isInstanceOf(ConfigurationException.class);
    }

    @Test
    public void maxFutureWindowIsConsumedAndStripped()
    {
        Map<String, String> map = options("max_future_window", "2d", "scaling_parameters", "T4");
        Map<String, String> unchecked = TimeSeriesCompactionStrategyOptions.validateOptions(map, new HashMap<>(map));
        assertFalse(unchecked.containsKey("max_future_window"));
        assertTrue(unchecked.containsKey("scaling_parameters"));     // UCS 몫은 남긴다
        TimeSeriesCompactionStrategyOptions opts = new TimeSeriesCompactionStrategyOptions(map);
        assertFalse(opts.delegateOptions(map).containsKey("max_future_window"));
    }
```

- [ ] **Step 2: 컴파일해 실패 확인** (필드/메서드 부재)

- [ ] **Step 3: 구현** — `TimeSeriesCompactionStrategyOptions.java`에 추가:

키/기본값 상수(기존 상수 블록에):

```java
    public static final String MAX_FUTURE_WINDOW = "max_future_window";

    static final String DEFAULT_MAX_FUTURE_WINDOW = "1d";
```

필드 + 생성자 파싱(기존 `timestampResolution` 초기화 옆에):

```java
    public final long maxFutureWindowMillis;
```

```java
        Pair<TimeUnit, Integer> future = parseDuration(MAX_FUTURE_WINDOW, options.getOrDefault(MAX_FUTURE_WINDOW, DEFAULT_MAX_FUTURE_WINDOW));
        this.maxFutureWindowMillis = TimeUnit.MILLISECONDS.convert(future.right, future.left);
```

술어 2개(`isExpiredWindow` 아래):

```java
    /** See {@link #isActiveWindow} for why sstable-derived operands never have durations added to them. */
    public boolean isCurrentWindow(long windowStartMillis, long nowMillis)
    {
        return windowStartMillis > nowMillis - windowSizeMillis;
    }

    /**
     * Far-future guard (design spec section 8): windows whose start lies more than {@code max_future_window}
     * past the wall clock are treated as garbage/misconfigured-writer input - excluded from the UCS delegate
     * and from freeze/frozen judgment, and logged at WARN - so bad timestamps can neither spawn unbounded
     * "current" windows nor trick a window into freezing.
     * <p>
     * Overflow discipline mirrors {@link #isActiveWindow} from the other side: the adversarial, sstable-derived
     * operand ({@code windowStartMillis}, possibly near {@code Long.MAX_VALUE}) stands alone on the left; the
     * arithmetic happens on {@code nowMillis + maxFutureWindowMillis}, whose operands are a real clock value
     * and a config-bounded duration (the duration parser caps at {@code Integer.MAX_VALUE} days, ~1.9e17 ms),
     * so the right-hand sum cannot wrap for any real clock value.
     */
    public boolean isFarFutureWindow(long windowStartMillis, long nowMillis)
    {
        return windowStartMillis > nowMillis + maxFutureWindowMillis;
    }
```

`delegateOptions`와 `validateOptions`의 strip 목록에 `MAX_FUTURE_WINDOW` 추가, `validateOptions` 본문에 파싱 1줄 추가(파싱 실패 = ConfigurationException):

```java
        parseDuration(MAX_FUTURE_WINDOW, options.getOrDefault(MAX_FUTURE_WINDOW, DEFAULT_MAX_FUTURE_WINDOW));
```

```java
        uncheckedOptions.remove(MAX_FUTURE_WINDOW);
```

```java
        copy.remove(MAX_FUTURE_WINDOW);
```

- [ ] **Step 4: 컴파일 + 테스트 통과** — `.build/sh/ai-ci-test --reuse org.apache.cassandra.db.compaction.TimeSeriesCompactionStrategyOptionsTest`, XML `failures="0" errors="0"` (기존 11 + 신규 5 = 16 tests). 기존 테스트(특히 `validateConsumesOwnKeysOnly`, `delegateOptionsStripOwnKeys`)가 여전히 그린인지 확인 — 새 키가 그 테스트들의 단언을 깨지 않는다(그들은 자기 키만 단언).

- [ ] **Step 5: 커밋** — `TSCS T2: max_future_window option and far-future/current window predicates` + 트레일러

---

### Task 2: 창 상태 분류기 + far-future 배선 + min timestamp 배관 + 목 커버리지 보강

**Files:**
- Modify: `src/java/org/apache/cassandra/db/compaction/TimeSeriesCompactionStrategy.java`
- Modify: `test/unit/org/apache/cassandra/db/compaction/TimeSeriesCompactionStrategyTest.java`

**Interfaces:**
- Consumes: Task 1 전체; `SSTableReader.getMinTimestamp()`(SSTableReader.java:1313, StatsMetadata에 항상 존재 — 버전 게이팅 없음); NoSpamLogger(`NoSpamLogger.log(Logger, Level, long, TimeUnit, String, Object...)`); 기존 하네스의 주입-시각 오버로드 패턴
- Produces (Task 3·4가 사용):
  - `public enum WindowState { CURRENT, CLOSING, FREEZING, FROZEN, EXPIRED }` — `TimeSeriesCompactionStrategy` 중첩 enum
  - `long minTimestampMillis(SSTableReader)` — 기존 `maxTimestampMillis`(:168-171)의 쌍둥이 (package-private)
  - `@VisibleForTesting WindowState classify(long windowStartMillis, Set<SSTableReader> windowSSTables, long nowMillis)`
  - `@VisibleForTesting List<AbstractCompactionTask> getMaximalTasksAt(long nowMillis, long gcBefore, boolean splitOutput)` — 주입-시각 오버로드 (기존 `getMaximalTasks`가 위임)
  - `syncDelegate`가 far-future sstable을 위임에서 제외 (+ NoSpamLogger WARN)
  - 테스트 헬퍼 `sstableSpanning(long minMillis, long maxMillis)`, `stubTracker(ColumnFamilyStore)`

- [ ] **Step 1: 실패하는 테스트 추가** — `TimeSeriesCompactionStrategyTest`에. 먼저 헬퍼 2개(기존 `sstableAt` 옆에; import 추가: `org.apache.cassandra.db.lifecycle.LifecycleTransaction`, `java.util.HashSet`, `static org.junit.Assert.assertNotNull`, `static org.junit.Assert.assertNull`, `static org.mockito.ArgumentMatchers.anyCollection`, `static org.mockito.ArgumentMatchers.any`):

```java
    /** min·max를 각각 지정하는 상태 기계용 확장판 (기존 sstableAt(max)는 min = max − 1ms 고정) */
    static SSTableReader sstableSpanning(long minTimestampMillis, long maxTimestampMillis)
    {
        SSTableReader sstable = mock(SSTableReader.class, Mockito.RETURNS_DEEP_STUBS);
        when(sstable.getMaxTimestamp()).thenReturn(maxTimestampMillis * 1000);   // timestampResolution 기본 MICROSECONDS
        when(sstable.getMinTimestamp()).thenReturn(minTimestampMillis * 1000);
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
```

분류기 테스트(상태 기계 — 스펙 §3의 5개 상태 전부):

```java
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
```

far-future 배선 테스트(스펙 §8 — 위임 제외 양방향):

```java
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
    public void farFutureSSTablesArePrunedFromDelegateIfAlreadyThere()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        TimeSeriesCompactionStrategy tscs = strategy(delegate);

        SSTableReader farFuture = sstableAt(NOW + 3L * 24 * HOUR);
        tscs.addSSTable(farFuture);
        when(delegate.getSSTables()).thenReturn(Set.of(farFuture));  // 가드 도입 전에 위임에 들어가 있던 상황

        tscs.getNextBackgroundTasksAt(NOW, 0);

        verify(delegate).removeSSTables(Mockito.argThat(iterable ->
            com.google.common.collect.Sets.newHashSet(iterable).contains(farFuture)));
    }
```

T1 인계: `getMaximalTasks`/`getUserDefinedTask` 목 수준 커버리지(§11 — 기존엔 E2E만 존재):

```java
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
```

- [ ] **Step 2: 컴파일해 실패 확인** (`WindowState`/`classify`/`getMaximalTasksAt` 부재)

- [ ] **Step 3: 구현** — `TimeSeriesCompactionStrategy.java`:

(a) import 추가: `java.util.concurrent.TimeUnit`(이미 있음), `org.apache.cassandra.utils.NoSpamLogger`.

(b) `maxTimestampMillis` 옆에 쌍둥이 + 분류기(스펙 §3; FROZEN 판정에 min·max 둘 다 필요 — T1 인계):

```java
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
     * frozen - but freeze selection ({@link #nextFreezeCandidate}) will not pick a single-sstable window:
     * rewriting one spanning sstable cannot fix containment before T3's flush-split lands, and trying would
     * recompact it forever.
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
```

(c) `syncDelegate` 교체 — far-future 제외 + WARN(스로틀; 이 코드베이스의 NoSpamLogger 관례는 CompactionLogger:343 참조):

```java
    private synchronized void syncDelegate(long nowMillis)
    {
        Set<SSTableReader> active = new HashSet<>();
        for (SSTableReader sstable : sstables)
        {
            long windowStart = tsOptions.windowStartFor(maxTimestampMillis(sstable));
            if (tsOptions.isFarFutureWindow(windowStart, nowMillis))
            {
                // Garbage/misconfigured-writer timestamps: keep them out of both the UCS delegate and the
                // freeze machinery, and complain (throttled) so the operator investigates (design spec section 8).
                NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.MINUTES,
                                 "{} has sstable(s) with max timestamp beyond now + {}ms (e.g. {} in window {}); " +
                                 "excluding from compaction - check writer clocks or USING TIMESTAMP inputs",
                                 cfs.getTableName(), tsOptions.maxFutureWindowMillis, sstable, windowStart);
                continue;
            }
            if (tsOptions.isActiveWindow(windowStart, nowMillis))
                active.add(sstable);
        }

        Set<SSTableReader> inDelegate = new HashSet<>(delegate.getSSTables());
        Set<SSTableReader> toRemove = new HashSet<>(inDelegate);
        toRemove.removeAll(active);
        Set<SSTableReader> toAdd = new HashSet<>(active);
        toAdd.removeAll(inDelegate);
        if (!toRemove.isEmpty())
            delegate.removeSSTables(toRemove);
        if (!toAdd.isEmpty())
            delegate.addSSTables(toAdd);
    }
```

(d) `getMaximalTasks`를 주입-시각 오버로드로 분리(기존 본문 그대로, `nowMillis`만 파라미터화 — 기존 목 커버리지 부재의 원인이 실시계 의존이었다):

```java
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
```

- [ ] **Step 4: 컴파일 + 테스트 통과** — Options 16 + Strategy 18(기존 9 + 신규 9), 전부 `failures="0" errors="0"`. 기존 9개가 전부 그대로 그린인지 특히 확인(딥스텁 far-future 배제·`getMaximalTasksAt` 리팩토링이 회귀를 만들지 않았는지).

- [ ] **Step 5: 커밋** — `TSCS T2: window state classifier, far-future guard wiring, min-timestamp plumbing` + 트레일러

---

### Task 3: `FreezeCompactionTask` + `WindowFrozenListener` 훅 + 동결 선택 배선

**Files:**
- Create: `src/java/org/apache/cassandra/db/compaction/timeseries/WindowFrozenListener.java`
- Create: `src/java/org/apache/cassandra/db/compaction/timeseries/WindowFrozenListeners.java`
- Create: `src/java/org/apache/cassandra/db/compaction/FreezeCompactionTask.java`
- Create: `test/unit/org/apache/cassandra/db/compaction/timeseries/WindowFrozenListenersTest.java`
- Modify: `src/java/org/apache/cassandra/db/compaction/TimeSeriesCompactionStrategy.java` (동결 브랜치 + 백로그 + caveat javadoc 삭제)
- Modify: `test/unit/org/apache/cassandra/db/compaction/TimeSeriesCompactionStrategyTest.java` (추가)

**Interfaces:**
- Consumes: Task 1·2 전체; `CompactionTask(ColumnFamilyStore, ILifecycleTransaction, long)`(:92), `shouldReduceScopeForSpace()`(:158-161, 기본 true — 저공간 시 가장 큰 입력을 조용히 탈락시킴), `finish(AbstractCompactionPipeline)`(:387-390 — runMayThrow:310의 "point of no return", 커밋 후 산출 sstable 반환), `DefaultCompactionWriter`(라이터 비교체 → 산출 ≤1), TWCS의 `previousCandidate` 가드(:100-106), LocalSessions 리스너 패턴(:131, :1174-1193)
- Produces (Task 4·5가 사용):
  - `public interface WindowFrozenListener { void onWindowFrozen(TableMetadata table, long windowStartMillis, SSTableReader frozen); }` — **스펙 §5와 자구 일치**
  - `WindowFrozenListeners.registerListener/unregisterListener/unsafeClearListeners/fire`
  - `public FreezeCompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, long gcBefore, long windowStartMillis)`
  - `@VisibleForTesting Map.Entry<Long, Set<SSTableReader>> nextFreezeCandidate(long nowMillis)` — 가장 오래된 FREEZING 창(≥2 sstable), 부수효과로 `freezeBacklog` 갱신

- [ ] **Step 1: 실패하는 테스트 작성** — 새 파일 `WindowFrozenListenersTest.java` (ASF 헤더 필수):

```java
package org.apache.cassandra.db.compaction.timeseries;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class WindowFrozenListenersTest
{
    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();   // TableMetadata/SSTableReader 목의 클래스 초기화 안전망
    }

    @After
    public void clear()
    {
        WindowFrozenListeners.unsafeClearListeners();
    }

    @Test
    public void listenerExceptionsAreIsolatedAndDoNotPropagate()
    {
        AtomicInteger fired = new AtomicInteger();
        WindowFrozenListeners.registerListener((table, windowStart, frozen) -> { throw new RuntimeException("boom"); });
        WindowFrozenListeners.registerListener((table, windowStart, frozen) -> fired.incrementAndGet());

        WindowFrozenListeners.fire(mock(TableMetadata.class), 42L, mock(SSTableReader.class));

        assertEquals(1, fired.get());   // 예외 리스너 "뒤"의 리스너도 호출되고, 예외는 호출자(컴팩션)로 새지 않는다
    }

    @Test
    public void unregisterAndClearStopDelivery()
    {
        AtomicInteger fired = new AtomicInteger();
        WindowFrozenListener listener = (table, windowStart, frozen) -> fired.incrementAndGet();
        WindowFrozenListeners.registerListener(listener);
        WindowFrozenListeners.fire(mock(TableMetadata.class), 1L, mock(SSTableReader.class));
        WindowFrozenListeners.unregisterListener(listener);
        WindowFrozenListeners.fire(mock(TableMetadata.class), 2L, mock(SSTableReader.class));
        assertEquals(1, fired.get());
    }

    @Test
    public void fireDeliversArgumentsUnchanged()
    {
        TableMetadata table = mock(TableMetadata.class);
        SSTableReader frozen = mock(SSTableReader.class);
        List<Object> seen = new ArrayList<>();
        WindowFrozenListeners.registerListener((t, w, f) -> { seen.add(t); seen.add(w); seen.add(f); });

        WindowFrozenListeners.fire(table, 42L, frozen);

        assertEquals(List.of(table, 42L, frozen), seen);
    }
}
```

그리고 `TimeSeriesCompactionStrategyTest`에 동결 선택 테스트(전부 `stubTracker` + 명시적 `getLiveSSTables` 스텁 — 딥스텁 목 Set에 대한 `retainAll`은 전부를 걸러버린다):

```java
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

        tscs.addSSTable(sstableAt(NOW - 10 * HOUR));                 // 단일 + 포함(min = max − 1ms) = FROZEN
        Collection<AbstractCompactionTask> tasks = tscs.getNextBackgroundTasksAt(NOW, 0);

        assertEquals(List.of(), List.copyOf(tasks));
        verify(cfs.getTracker(), Mockito.never()).tryModify(anyCollection(), any(OperationType.class));
    }

    @Test
    public void spanningSingleSSTableIsNeverFreezeSelected()
    {
        // FREEZING로 분류돼도(포함 실패) 단일 sstable은 재작성해 봐야 포함이 안 고쳐진다(T3 스플릿 전) →
        // 선택 대상이 아니어야 무한 재컴팩션 루프가 없다
        TimeSeriesCompactionStrategy tscs = strategy(mock(UnifiedCompactionStrategy.class));
        tscs.addSSTable(sstableSpanning(NOW - 11 * HOUR, NOW - 10 * HOUR));

        assertNull(tscs.nextFreezeCandidate(NOW));
    }

    @Test
    public void farFutureWindowNeverCountsTowardFreeze()
    {
        // far-future 가드는 분류기 경로에서도 참조된다(스펙 §8): far-future 창은 sstable이 몇 개든 동결 후보가 아니다
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
```

- [ ] **Step 2: 컴파일해 실패 확인** (신규 클래스/`nextFreezeCandidate` 부재)

- [ ] **Step 3: 구현**

`WindowFrozenListener.java` (ASF 헤더 필수) — **스펙 §5의 인터페이스와 자구 일치**:

```java
package org.apache.cassandra.db.compaction.timeseries;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Notification hook for {@link org.apache.cassandra.db.compaction.TimeSeriesCompactionStrategy}: fired every
 * time a window is frozen (or re-frozen after late data) into a single sstable. Design spec section 5.
 * <p>
 * The event is a latency optimisation, not the sole trigger: consumers must be able to fall back to scanning
 * for "frozen windows without downstream output" themselves (e.g. the tiering re-encoder's watermark scan),
 * because events can be lost across restarts. Events are scoped to one strategy-instance slice (the
 * CompactionStrategyManager splits instances per repair status and per disk).
 */
public interface WindowFrozenListener
{
    /** 창이 단일 SSTable로 동결(또는 재동결)될 때마다 호출. 멱등해야 한다. */
    void onWindowFrozen(TableMetadata table, long windowStartMillis, SSTableReader frozen);
}
```

`WindowFrozenListeners.java` (ASF 헤더 필수) — LocalSessions의 레지스트리 패턴 미러 + 리스너별 격리:

```java
package org.apache.cassandra.db.compaction.timeseries;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * Static registry for {@link WindowFrozenListener}s, mirroring the LocalSessions listener pattern
 * (repair/consistent/LocalSessions.java). No default listeners in v1 - the tiering re-encoder is the first
 * consumer. Listener failures are logged and never affect the compaction result (design spec section 5).
 */
public final class WindowFrozenListeners
{
    private static final Logger logger = LoggerFactory.getLogger(WindowFrozenListeners.class);

    private static final Set<WindowFrozenListener> listeners = new CopyOnWriteArraySet<>();

    private WindowFrozenListeners()
    {
    }

    public static void registerListener(WindowFrozenListener listener)
    {
        listeners.add(listener);
    }

    public static void unregisterListener(WindowFrozenListener listener)
    {
        listeners.remove(listener);
    }

    @VisibleForTesting
    public static void unsafeClearListeners()
    {
        listeners.clear();
    }

    /** Delivers to every listener; a throwing listener is logged and skipped, the rest still run. */
    public static void fire(TableMetadata table, long windowStartMillis, SSTableReader frozen)
    {
        for (WindowFrozenListener listener : listeners)
        {
            try
            {
                listener.onWindowFrozen(table, windowStartMillis, frozen);
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                logger.error("WindowFrozenListener {} failed for {}.{} window {}",
                             listener, table.keyspace, table.name, windowStartMillis, t);
            }
        }
    }
}
```

`FreezeCompactionTask.java` (ASF 헤더 필수) — 평범한 `CompactionTask` 기반(UnifiedCompactionTask 금지 — 샤드 라이터가 다중 산출물을 낸다):

```java
package org.apache.cassandra.db.compaction;

import java.util.Collection;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.timeseries.WindowFrozenListeners;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * Freezes one closed time window of {@link TimeSeriesCompactionStrategy}: a major compaction of every sstable
 * in the window (of this strategy-instance slice) down to a single sstable, after which the window classifies
 * FROZEN. Runs a real {@link CompactionController} with the caller's gcBefore, so TTL/tombstone data in the
 * closed window is purged here - this is what structurally closes T1's "closed windows need retention to
 * reclaim TTL'd data" gap. On failure the standard compaction transaction rolls back and the window simply
 * classifies FREEZING again next round (design spec section 8).
 */
public class FreezeCompactionTask extends CompactionTask
{
    private final long windowStartMillis;

    public FreezeCompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, long gcBefore, long windowStartMillis)
    {
        super(cfs, txn, gcBefore);
        this.windowStartMillis = windowStartMillis;
    }

    /**
     * A freeze must be deterministic: silently dropping the largest input on low disk (the CompactionTask
     * default) breaks the whole-window-to-one-sstable contract. Better to fail whole and retry next round.
     */
    @Override
    protected boolean shouldReduceScopeForSpace()
    {
        return false;
    }

    /**
     * Fires the listeners strictly post-commit: {@code super.finish} is the "point of no return" in
     * {@link CompactionTask#runMayThrow} - once it returns, the single output sstable is durably committed.
     * Zero outputs means the whole window was expired data and no longer exists - no event (there is nothing
     * to hand to a consumer). More than one output cannot happen with {@code DefaultCompactionWriter}, which
     * never switches writers; log rather than half-fire if that invariant is ever broken upstream.
     */
    @Override
    protected Collection<SSTableReader> finish(AbstractCompactionPipeline pipeline)
    {
        Collection<SSTableReader> newSStables = super.finish(pipeline);
        if (newSStables.size() == 1)
            WindowFrozenListeners.fire(cfs.metadata(), windowStartMillis, newSStables.iterator().next());
        else if (newSStables.isEmpty())
            logger.debug("Freeze of window {} in {} produced no sstable (window fully expired); no event fired",
                         windowStartMillis, cfs.getTableName());
        else
            logger.warn("Freeze of window {} in {} unexpectedly produced {} sstables; no event fired",
                        windowStartMillis, cfs.getTableName(), newSStables.size());
        return newSStables;
    }
}
```

`TimeSeriesCompactionStrategy.java` 수정:

(a) 필드 추가(`lastExpiredSelection` 옆):

```java
    // number of FREEZING windows (>= 2 sstables) seen on the most recent background round, for
    // getEstimatedRemainingTasks() - freezes starve behind other tables' work if the CSM cannot see them.
    private volatile int freezeBacklog;
    // the freeze candidate that failed tryModify on the previous round: cross-round version of TWCS's
    // previousCandidate guard (:100-106) - warn when the same candidate is stuck two rounds running,
    // but keep retrying (unlike TWCS's intra-call loop, skipping here would never retry at all).
    private volatile Set<SSTableReader> previousFreezeCandidate = Set.of();
```

(b) 동결 후보 선택(`windows()` 아래; TreeMap이라 오래된 창부터 — 스펙 §3 "오래된 창 우선"):

```java
    /**
     * The oldest FREEZING window with at least two sstables, or null. Single-sstable FREEZING windows
     * (a spanning sstable failing containment) are not candidates: rewriting one sstable cannot fix
     * containment before T3's flush-split, and selecting it would recompact it every round forever.
     * Side effect: refreshes {@link #freezeBacklog} with the number of eligible windows.
     */
    @VisibleForTesting
    synchronized Map.Entry<Long, Set<SSTableReader>> nextFreezeCandidate(long nowMillis)
    {
        Map.Entry<Long, Set<SSTableReader>> oldest = null;
        int backlog = 0;
        for (Map.Entry<Long, Set<SSTableReader>> window : windows().entrySet())
        {
            if (tsOptions.isFarFutureWindow(window.getKey(), nowMillis))
                continue;                                 // far-future guard: never judged for freezing (spec section 8)
            if (window.getValue().size() < 2)
                continue;
            if (classify(window.getKey(), window.getValue(), nowMillis) != WindowState.FREEZING)
                continue;
            backlog++;
            if (oldest == null)
                oldest = window;
        }
        freezeBacklog = backlog;
        return oldest;
    }
```

(c) `getNextBackgroundTasksAt` — 만료 브랜치와 `return delegate.getNextBackgroundTasks(gcBefore);` **사이**에 동결 브랜치 삽입(우선순위 만료 > 동결 > 위임; 라운드당 동결 태스크 최대 1개):

```java
        Map.Entry<Long, Set<SSTableReader>> freeze = nextFreezeCandidate(nowMillis);
        if (freeze != null)
        {
            // Same zombie filter as the expired branch: only live, non-suspect sstables.
            Set<SSTableReader> toFreeze = new HashSet<>(AbstractCompactionStrategy.filterSuspectSSTables(freeze.getValue()));
            toFreeze.retainAll(cfs.getLiveSSTables());
            if (toFreeze.size() > 1)                      // a single survivor is already "frozen enough"; self-heals next round
            {
                LifecycleTransaction txn = cfs.getTracker().tryModify(toFreeze, OperationType.COMPACTION);
                if (txn != null)
                {
                    previousFreezeCandidate = Set.of();
                    logger.debug("Freezing window {} of {}: {} sstables -> 1", freeze.getKey(), cfs.getTableName(), toFreeze.size());
                    return List.of(new FreezeCompactionTask(cfs, txn, gcBefore, freeze.getKey()));
                }
                // Refused (e.g. a delegate compaction started while the window was CLOSING is still running):
                // skip this round, fall through to the delegate, retry next round (spec section 8).
                if (toFreeze.equals(previousFreezeCandidate))
                    logger.warn("Could not acquire references for freezing sstables {} which is not a problem per se," +
                                " unless it happens frequently, in which case it must be reported. Will retry later.",
                                toFreeze);
                else
                    logger.debug("Unable to mark window {} of {} for freezing; will retry next round",
                                 freeze.getKey(), cfs.getTableName());
                previousFreezeCandidate = toFreeze;
            }
        }
```

(d) `getEstimatedRemainingTasks` 교체:

```java
    @Override
    public int getEstimatedRemainingTasks()
    {
        return delegate.getEstimatedRemainingTasks()
               + (lastExpiredSelection.isEmpty() ? 0 : 1)
               + freezeBacklog;
    }
```

(e) **T1 caveat javadoc 삭제** — 클래스 javadoc(:54-58)의 `<b>Caveat (this increment):</b> ...` 문단 전체를 제거하고, 앞 문장 "Freezing closed windows to a single sstable and late-data isolation arrive in later increments (design spec section 10)."를 다음으로 교체:

```
 * Closed windows are frozen to a single sstable per window (per strategy-instance slice) by
 * {@link FreezeCompactionTask}, which fires {@link org.apache.cassandra.db.compaction.timeseries.WindowFrozenListener}
 * post-commit and, by running a real compaction controller, also reclaims TTL/tombstone data in closed
 * windows without requiring {@code retention}. Late-data isolation (flush/streaming window splitting)
 * arrives in T3 (design spec section 10).
```

- [ ] **Step 4: 컴파일 + 테스트 통과** — `.build/sh/ai-ci-test --reuse`로 세 클래스: Options 16, Strategy 26(18 + 신규 8), `org.apache.cassandra.db.compaction.timeseries.WindowFrozenListenersTest` 3. 전부 `failures="0" errors="0"`.

- [ ] **Step 5: 커밋** — `TSCS T2: freeze compaction task and WindowFrozenListener hook` + 트레일러

---

### Task 4: SchemaLoader E2E — 동결→이벤트→지각 재동결, TTL 공백의 구조적 해소

**Files:**
- Modify: `test/unit/org/apache/cassandra/db/compaction/TimeSeriesCompactionStrategyE2ETest.java`

**Interfaces:**
- Consumes: Task 3 전체; 기존 E2E 픽스처(prepareServer/createKeyspace, `timestamp_resolution=MILLISECONDS` + 1m 창 관례, `CSM.getCompactionStrategyFor`, `ActiveCompactionsTracker.NOOP`); `TableMetadata.Builder.gcGraceSeconds(int)`(TTLExpiryTest:86 관례)
- Produces: 리플렉션 2-인자 생성자 경로에서의 동결/재동결/이벤트/TTL-회수 증명

- [ ] **Step 1: 실패하는 테스트 추가** — import 추가(`java.util.concurrent.CopyOnWriteArrayList`, `org.apache.cassandra.db.compaction.timeseries.WindowFrozenListener`, `org.apache.cassandra.db.compaction.timeseries.WindowFrozenListeners`, `org.apache.cassandra.schema.TableMetadata`), `defineSchema`에 gcGrace 0 테이블 추가:

```java
    private static final String CF_GCGRACE0 = "StandardGcGrace0";
```

```java
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_GCGRACE0).gcGraceSeconds(0));
```

기록 리스너 + 테스트 2건:

```java
    /** 발화 기록용 리스너 — 멱등성(재동결 시 정확히 1회 추가 발화) 검증에 호출 횟수를 센다 */
    private static final class RecordingListener implements WindowFrozenListener
    {
        final List<Long> windowStarts = new CopyOnWriteArrayList<>();
        final List<SSTableReader> frozen = new CopyOnWriteArrayList<>();

        @Override
        public void onWindowFrozen(TableMetadata table, long windowStartMillis, SSTableReader sstable)
        {
            windowStarts.add(windowStartMillis);
            frozen.add(sstable);
        }
    }

    @Test
    public void testFreezeThenLateDataRefreeze()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);

        // 1분 창 하나에 완전히 포함되는(min·max 동일 창) 두 sstable: 1시간 전의 창 시작으로 정렬한 기준시각
        long windowSizeMillis = 60_000L;
        long base = ((System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1)) / windowSizeMillis) * windowSizeMillis;
        for (int i = 0; i < 2; i++)
        {
            new RowUpdateBuilder(cfs.metadata(), base + 1000 + i, Util.dk("frozen-" + i).getKey())
                .clustering("column")
                .add("val", value).build().applyUnsafe();
            Util.flush(cfs);
        }
        assertEquals(2, cfs.getLiveSSTables().size());

        // retention 미지정: 만료 브랜치가 이 창을 먼저 통삭제하지 않게 해서 동결 경로만 검증한다
        cfs.setCompactionParameters(ImmutableMap.of("class", "TimeSeriesCompactionStrategy",
                                                    "timestamp_resolution", "MILLISECONDS",
                                                    "window_size", "1m",
                                                    "freeze_after", "1m"));

        RecordingListener listener = new RecordingListener();
        WindowFrozenListeners.registerListener(listener);
        try
        {
            TimeSeriesCompactionStrategy tscs = (TimeSeriesCompactionStrategy)
                cfs.getCompactionStrategyManager().getCompactionStrategyFor(cfs.getLiveSSTables().iterator().next());

            // 1) 동결: 닫힌 창의 sstable 2개 → FreezeCompactionTask 1개 → 단일 sstable + 이벤트 정확히 1회
            Collection<AbstractCompactionTask> tasks = tscs.getNextBackgroundTasks(nowInSeconds());
            AbstractCompactionTask task = Iterables.getOnlyElement(tasks, null);
            assertNotNull(task);
            assertTrue(task instanceof FreezeCompactionTask);
            assertEquals(2, Iterables.size(task.transaction.originals()));
            task.execute(ActiveCompactionsTracker.NOOP);

            assertEquals(1, cfs.getLiveSSTables().size());
            assertEquals(1, listener.windowStarts.size());
            assertEquals(base, (long) listener.windowStarts.get(0));
            assertEquals(cfs.getLiveSSTables().iterator().next(), listener.frozen.get(0));

            // 2) FROZEN 창은 재선택되지 않고(무상태 판정) 이벤트도 다시 발화하지 않는다
            Collection<AbstractCompactionTask> idle = tscs.getNextBackgroundTasks(nowInSeconds());
            for (AbstractCompactionTask t : idle)                    // 계약 위반 시에도 txn은 정리하고 실패
                t.transaction.abort();
            assertTrue(idle.isEmpty());
            assertEquals(1, listener.windowStarts.size());

            // 3) 지각 데이터: 같은 창에 늦은 쓰기 → FROZEN이 FREEZING으로 되돌아가 재동결 + 이벤트 재발화 (스펙 §4)
            new RowUpdateBuilder(cfs.metadata(), base + 5000, Util.dk("late").getKey())
                .clustering("column")
                .add("val", value).build().applyUnsafe();
            Util.flush(cfs);
            assertEquals(2, cfs.getLiveSSTables().size());

            Collection<AbstractCompactionTask> refreeze = tscs.getNextBackgroundTasks(nowInSeconds());
            AbstractCompactionTask refreezeTask = Iterables.getOnlyElement(refreeze, null);
            assertNotNull(refreezeTask);
            assertTrue(refreezeTask instanceof FreezeCompactionTask);
            refreezeTask.execute(ActiveCompactionsTracker.NOOP);

            assertEquals(1, cfs.getLiveSSTables().size());
            assertEquals(2, listener.windowStarts.size());           // 재동결 = 정확히 1회 추가 발화
            assertEquals(base, (long) listener.windowStarts.get(1));
        }
        finally
        {
            WindowFrozenListeners.unsafeClearListeners();
        }
    }

    /**
     * T1 caveat("닫힌 창의 TTL 회수는 retention 필요")의 구조적 해소 증명: retention 없이도 동결 컴팩션이
     * 실제 CompactionController + gcBefore로 닫힌 창의 TTL 데이터를 회수한다. 창 전체가 만료면 산출물이
     * 0개라 창 자체가 소멸하며, 그때는 이벤트가 발화하지 않는다(소비자에게 넘길 sstable이 없다).
     */
    @Test
    public void testFreezeReclaimsTTLDataInClosedWindowWithoutRetention() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_GCGRACE0);   // gcGraceSeconds(0)
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);
        long windowSizeMillis = 60_000L;
        long base = ((System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1)) / windowSizeMillis) * windowSizeMillis;
        for (int i = 0; i < 2; i++)
        {
            new RowUpdateBuilder(cfs.metadata(), base + 1000 + i, 1 /* TTL 1s */, Util.dk("ttl-" + i).getKey())
                .clustering("column")
                .add("val", value).build().applyUnsafe();
            Util.flush(cfs);
        }
        assertEquals(2, cfs.getLiveSSTables().size());

        cfs.setCompactionParameters(ImmutableMap.of("class", "TimeSeriesCompactionStrategy",
                                                    "timestamp_resolution", "MILLISECONDS",
                                                    "window_size", "1m",
                                                    "freeze_after", "1m"));
        Thread.sleep(2000);   // TTL(1s) 경과: gcGrace 0이라 gcBefore=now가 두 sstable을 전부 만료로 판정

        RecordingListener listener = new RecordingListener();
        WindowFrozenListeners.registerListener(listener);
        try
        {
            TimeSeriesCompactionStrategy tscs = (TimeSeriesCompactionStrategy)
                cfs.getCompactionStrategyManager().getCompactionStrategyFor(cfs.getLiveSSTables().iterator().next());

            Collection<AbstractCompactionTask> tasks = tscs.getNextBackgroundTasks(nowInSeconds());
            AbstractCompactionTask task = Iterables.getOnlyElement(tasks, null);
            assertNotNull(task);
            assertTrue(task instanceof FreezeCompactionTask);
            task.execute(ActiveCompactionsTracker.NOOP);

            assertTrue(cfs.getLiveSSTables().isEmpty());             // retention 없이 회수 — caveat 해소
            assertEquals(0, listener.windowStarts.size());           // 창 소멸 = 이벤트 없음
        }
        finally
        {
            WindowFrozenListeners.unsafeClearListeners();
        }
    }
```

- [ ] **Step 2: 컴파일 + 실행** — `.build/sh/ai-ci-test --reuse org.apache.cassandra.db.compaction.TimeSeriesCompactionStrategyE2ETest` (기존 2 + 신규 2 = 4 tests), `failures="0" errors="0"`. 주의: `SchemaLoader.standardCFMD(...).gcGraceSeconds(0)`가 이 트리의 빌더 API와 정확히 일치하는지 구현 시 TTLExpiryTest:86을 열어 확인(불일치 시 그 파일의 관례를 그대로 이식).

- [ ] **Step 3: 커밋** — `TSCS T2: freeze/re-freeze and TTL-reclaim end-to-end tests` + 트레일러

---

### Task 5: CI 배선 + 스펙 §11 갱신 + 문서 + 푸시

**Files:**
- Modify: `.gitlab-ci.yml`, `docs/superpowers/specs/2026-07-31-timeseries-compaction-design.md`, `README.md`, `CHANGES.txt`, `CLAUDE.md`

**Interfaces:**
- Consumes: Task 1–4 전체
- Produces: 배포 가능한 T2 (신규 테스트 클래스 전부 CI 배선 — 미배선 클래스 금지)

- [ ] **Step 1: CI 배선** — `.gitlab-ci.yml`의 `timeseries-tests`에서 TSCS 블록을 다음으로 교체(신규 클래스는 `WindowFrozenListenersTest` 하나 — Options/Strategy/E2E는 이미 배선돼 있고 클래스명이 안 바뀌므로 추가 없이 커버된다):

```yaml
    # time-series compaction strategy (TSCS T1 window classification + T2 freeze state machine)
    - ant testsome -Dtest.name=org.apache.cassandra.db.compaction.TimeSeriesCompactionStrategyOptionsTest
    - ant testsome -Dtest.name=org.apache.cassandra.db.compaction.TimeSeriesCompactionStrategyTest
    - ant testsome -Dtest.name=org.apache.cassandra.db.compaction.TimeSeriesCompactionStrategyE2ETest
    - ant testsome -Dtest.name=org.apache.cassandra.db.compaction.timeseries.WindowFrozenListenersTest
```

- [ ] **Step 2: 스펙 §11 갱신** — `docs/superpowers/specs/2026-07-31-timeseries-compaction-design.md` §11 끝의 "T2가 반드시 인수할 것" 목록 아래에 완료 노트를 추가(기존 텍스트는 이력으로 보존):

```markdown
### T2 구현 완료 노트 (2026-07-31)

T2 완료: 무상태 창 상태 분류기(CURRENT/CLOSING/FREEZING/FROZEN/EXPIRED, min·max 타임스탬프 유도),
`FreezeCompactionTask`(창→단일 sstable, `shouldReduceScopeForSpace` 차단, 커밋 후 `WindowFrozenListener`
발화), far-future 가드(`max_future_window`, 기본 1d, NoSpamLogger WARN). T1 인계 4건 전부 흡수:
far-future 가드(§8) 구현, 닫힌 창 TTL 공백 구조 해소(caveat javadoc/문서 경고 제거 — retention 불필요),
min timestamp 배관(`minTimestampMillis`), `getMaximalTasks`/`getUserDefinedTask` 목 커버리지.

스코프 주의: CSM이 전략 인스턴스를 리페어 상태×디스크로 쪼개므로 "창당 1 sstable"·FROZEN 판정·동결
이벤트는 **인스턴스 슬라이스 단위**다(테이블 전체 아님). 리스너 소비자는 같은 창에 대해 슬라이스별
이벤트를 여러 번 받을 수 있다 — 멱등 계약(§5)이 이를 흡수한다.

T3가 인수할 것:
- `createSSTableMultiWriter` 미오버라이드(M3) — flush가 UCS 샤드 분할을 잃는 지점이자 창 경계
  스플릿(§4 불변식)이 들어갈 자리.
- 창 걸침 단일 sstable: FREEZING로 분류되지만(포함 실패) 스플릿 없이는 재작성이 무의미해 동결
  선택에서 제외 — 동결·이벤트 없이 잔류한다. flush/스트리밍 스플릿이 구조적으로 해소.
- `ALTER window_size` 후 구경계 FROZEN 인정(§8): 현재는 보수적으로 "미동결(재작성·이벤트 없음)"로만
  처리 — 구경계 정합 허용은 스플릿과 함께 재검토.
- `getMaximalTasks`(수동 메이저)는 창당 1 태스크로 창 불변식은 지키지만 동결 이벤트를 발화하지
  않는다 — 계층화 연동 관점에서 재검토.
- 동결 동시성 스로틀 설정화(§3 "노드당 동시 1개(설정 가능)"): 현재는 인스턴스당 라운드당 1개 고정.
- jvm-dtest(3노드 리페어/스트리밍 창 편입)와 스케일 벤치(§9)는 T3 이후 일괄.
```

- [ ] **Step 3: 문서** —

`README.md` 기능 표의 TSCS 행(14행)을 교체 — T1 caveat 문구가 "매칭되는 문서 경고"다(doc/timeseries에는 TSCS 문서 없음 — 확인 완료):

```markdown
| **시계열 컴팩션 (TSCS)** | `TimeSeriesCompactionStrategy` — 창 정렬 + 창 내부 UCS 위임 + retention 창 통삭제 + 닫힌 창 동결(창당 1 SSTable, `WindowFrozenListener` 이벤트 훅, far-future 가드 `max_future_window`, 닫힌 창 TTL 회수에 retention 불필요) (T3 지각격리 예정) | [설계 스펙](docs/superpowers/specs/2026-07-31-timeseries-compaction-design.md) |
```

`CHANGES.txt` 6.0.0 절 최상단에 추가:

```
 * Add TSCS T2 window freeze: closed windows compact to a single sstable per window (per strategy-instance slice), WindowFrozenListener post-commit event hook, far-future timestamp guard (max_future_window, default 1d), TTL reclamation in closed windows without retention
```

같은 절의 T1 라인에서 캐비앳 괄호 ` (T1: closed-window TTL reclamation requires 'retention' until freeze lands)`를 삭제.

`CLAUDE.md` 업스트림 충돌 지점 목록의 `db/compaction/TimeSeriesCompactionStrategy*.java (UCS delegation)`을 `db/compaction/TimeSeries*.java + FreezeCompactionTask.java + db/compaction/timeseries/ (UCS delegation, freeze hook)`으로 확장.

- [ ] **Step 4: 최종 검증** — 컴파일 후 신규·수정 테스트 4클래스 전부 재실행 그린 + 회귀로 `TimeWindowCompactionStrategyTest`, `UnifiedCompactionStrategyTest` 재실행 그린, 마지막으로 `.build/sh/ai-build` BUILD SUCCESSFUL(checkstyle 포함).

- [ ] **Step 5: 커밋 + 푸시** — `TSCS T2: CI wiring, spec handover, docs` + 트레일러; `git push origin master && git push origin master:6.0.0`
