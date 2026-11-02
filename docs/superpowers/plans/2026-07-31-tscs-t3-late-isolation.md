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

# TSCS T3: 지각 격리 (창 경계 스플릿 + 국소 재동결) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
> **Quota note (2026-07-31):** premium 모델 주간 한도 소진 상태로 계획됨 — Task 1·3(판단 집약)은 컨트롤러 인라인 실행 권장, Task 2·4는 haiku/sonnet 가능.

**Goal:** flush/스트리밍 산출 SSTable이 시간 창 경계를 걸치지 않게 로우 단위로 분할하고, 이미 존재하는 걸침(spanning) SSTable을 국소 재동결로 창 정합화한다 — spec §4의 "모든 SSTable은 정확히 한 창" 불변식 완성.

**Architecture:** `SSTableMultiWriter` 구현체 `TimeWindowSplittingMultiWriter`가 파티션별 `UnfilteredRowIterator`를 **셀 write-timestamp 축**으로 창별 서브 이터레이터로 나눠 창당 하나의 내부 라이터에 기록한다(업스트림 선례 없음 — 전 라이터가 파티션 경계 스위칭뿐임을 정찰로 확인). TSCS가 `createSSTableMultiWriter`를 오버라이드하면 flush(Flushing.java:242)와 스트리밍(RangeAwareSSTableWriter:77,99) 모두 이 훅을 탄다(둘 다 `cfs.createSSTableMultiWriter` 경유 — 정찰 확인). 레거시 걸침 SSTable은 동결 선택이 단일-걸침 창을 감지해 분할 재작성 태스크로 보낸다.

**Tech Stack:** Java 21, 기존 TSCS(T1/T2) 코드, SSTableMultiWriter/SimpleSSTableMultiWriter, RangeTombstoneBoundaryMarker API.

## Global Constraints

- Java 21 (CI `eclipse-temurin:21-jdk`); `base.version` 6.0.0 유지.
- `src/gen-java/`, `lib/`, CQL 문법, UCS/TWCS 업스트림 파일 절대 수정 금지.
- 신규 파일 ASF 17줄 라이선스 헤더 필수.
- overflow-safe subtraction-form 시간 비교 (TimeSeriesCompactionStrategyOptions javadoc :80-95 규율).
- 커밋 트레일러: `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>` + `Claude-Session: https://claude.ai/code/session_01Jruk9rM1N82xGmXD41RfmE`
- 검증 명령: 증분 컴파일 `ant build-test -Dno-checkstyle=true -Dant.gen-doc.skip=true -Drat.skip=true 2>&1 | .build/sh/ant-log-summary.py -`; 테스트 `.build/sh/ai-ci-test --reuse <FQCN>` 후 `build/test/output/TEST-*.xml`의 failures/errors 속성 확인; 커밋 전 실제 checkstyle `ant checkstyle checkstyle-test 2>&1 | tail -5`.
- 신규 테스트 클래스는 반드시 `.gitlab-ci.yml` timeseries-tests에 배선 (Task 4).
- 이벤트/동결 시맨틱은 T2 그대로: 커밋 후 발화만, 리스너 소비자는 큐잉 (spec §11 (e)).

## 규범 설계 결정 (정찰 근거, 전 태스크 공통)

**D1. 분할 축 = 셀 write-timestamp (클러스터링 값 아님).** T1/T2의 창 분류가 `SSTableReader.getMin/MaxTimestamp()`(write ts) 기준이므로 분할도 같은 축이어야 "창 완전 포함" 불변식이 성립한다. 지각 백필은 `USING TIMESTAMP <이벤트시각>`으로 들어올 때만 과거 창으로 격리된다(서버 타임스탬프 백필은 현재 창 소속이 맞다 — 문서화).

**D2. Unfiltered 라우팅 규칙.**
- `Row`: `max(primaryKeyLivenessInfo.timestamp(), row.deletion().time().markedForDeleteAt(), 모든 cell.timestamp())` 중 유효한(≠ Long.MIN_VALUE/NO_TIMESTAMP) 최댓값의 창.
- `RangeTombstoneBoundMarker`(open 또는 close 단독): `deletionTime().markedForDeleteAt()`의 창. open/close 짝은 같은 deletion time → 같은 창으로 자동 동행.
- `RangeTombstoneBoundaryMarker`(close+open 결합): 두 deletion time의 창이 다르면 `createCorrespondingCloseMarker()`/`createCorrespondingOpenMarker()`로 분해해 각자의 창으로 라우팅.
- 각 출력은 원본 클러스터링 순서의 부분수열 → 정렬 불변식 자동 유지.

**D3 (구현 중 개정). 파티션 헤더는 자기 창으로 1회 라우팅 — 복제 금지.** `partitionLevelDeletion`과 `staticRow`는 **자신의 max write-timestamp가 속한 창의 출력에만** 기록한다. 원안(전 창 복제)은 Task 2 테스트에서 결함이 실증됐다: 복제된 과거 삭제 타임스탬프가 새 창 SSTable의 min 메타데이터를 오염시켜 그 SSTable이 영구히 "걸침"으로 분류되고, Task 3의 분할 재작성과 무한 루프를 이룬다. 1회 라우팅의 안전 근거: ① 읽기는 파티션의 전 SSTable을 병합하므로 삭제는 어느 창에 있든 전역 적용된다. ② 창 통삭제는 오래된 창 우선이므로, 헤더가 든 창이 드롭되는 시점엔 그 삭제가 가릴 수 있는 더 오래된 창들은 이미 드롭된 후다(헤더 창 자신의 데이터는 함께 드롭). ③ gcGrace 이후의 지각 되살아남은 일반 Cassandra 시맨틱과 동일 — 문서화로 처리.

**D4. 창 폭 결정은 flush 시각의 옵션 스냅샷.** ALTER 중 flush 경합은 "그 flush는 구 옵션" — 재작성하지 않는다(§8 ALTER 규칙과 일치).

**D5. far-future 로우**는 별도 출력(창 = far-future 로우의 자기 창)으로 그대로 기록하되 분할은 수행(가드는 컴팩션 선택에서 이미 격리 — T2). 분할기가 미래 창을 특별 취급하지 않는다(단순성).

**D6. UCS 샤딩 상실 허용.** TSCS flush는 창별 `SimpleSSTableMultiWriter`(디스크 위치는 기존 인자 그대로)로 쓴다. UCS ShardedMultiWriter의 토큰 샤딩과의 합성은 비목표(대형 flush 희귀) — spec §11에 후속 기록.

**D7. 국소 재동결 = 분할 재작성.** 닫힌 창에서 단일-걸침 SSTable(T2가 선택 제외하던 케이스)을 감지하면 `SplitRefreezeCompactionTask`(단일 입력 → 창별 다중 출력)를 발급한다. 산출물 각각이 자기 창에 완전 포함되므로 다음 라운드에 FROZEN/FREEZING 정상 판정 → 통상 동결 경로가 이어받는다. 리스너는 이 태스크에서 발화하지 않는다(동결 완결은 후속 라운드의 FreezeCompactionTask 몫 — 이벤트 중복·조기 발화 방지).

## File Structure

- Create: `src/java/org/apache/cassandra/db/compaction/timeseries/TimeWindowSplittingMultiWriter.java` (핵심 분할 라이터)
- Create: `src/java/org/apache/cassandra/db/compaction/timeseries/WindowRoutingIterator.java` (파티션 1개의 Unfiltered를 창별로 나누는 유틸 — 라이터에서 분리해 단위 테스트 가능하게)
- Create: `src/java/org/apache/cassandra/db/compaction/SplitRefreezeCompactionTask.java` (걸침 1개 → 창별 출력 재작성)
- Modify: `src/java/org/apache/cassandra/db/compaction/TimeSeriesCompactionStrategy.java` (createSSTableMultiWriter 오버라이드 + 걸침 감지 분기)
- Test: `test/unit/org/apache/cassandra/db/compaction/timeseries/WindowRoutingIteratorTest.java`, `TimeWindowSplittingMultiWriterTest.java`(E2E-lite), 기존 `TimeSeriesCompactionStrategyTest`/`E2ETest` 확장

### Task 1: WindowRoutingIterator (분할 규칙의 단위 구현) — 컨트롤러 인라인 권장

**Interfaces (Produces):**
```java
package org.apache.cassandra.db.compaction.timeseries;
/** 한 파티션의 UnfilteredRowIterator를 write-timestamp 창별 서브 이터레이터 목록으로 나눈다. */
public final class WindowRoutingIterator
{
    /** @return 창 시작(ms) → 그 창의 Unfiltered들(원본 클러스터링 순서 유지). 파티션 삭제·스태틱 로우는 호출측(라이터)이 복제한다. */
    public static NavigableMap<Long, List<Unfiltered>> route(UnfilteredRowIterator partition,
                                                             LongUnaryOperator windowStartOfMillis,
                                                             TimeUnit tableResolution)
    // D2 라우팅 규칙 그대로; boundary marker 분해 포함.
    /** row/marker의 라우팅 타임스탬프(밀리초) 계산 — D2. @VisibleForTesting */
    static long routingMillis(Unfiltered u, TimeUnit tableResolution)
}
```

- [ ] 테스트 먼저 (`WindowRoutingIteratorTest`, CQLTester 불필요 — RowUpdateBuilder/UnfilteredRowIterators 목 구성): ① 단일 창 로우들 → 맵 크기 1, 순서 보존 ② 두 창 걸침 로우들 → 각 창 정확 라우팅 ③ 멀티셀 로우(서로 다른 셀 ts) → max 규칙 ④ row deletion만 있는 로우 ⑤ RT open/close 짝 동행 ⑥ boundary marker 창 분기 시 분해(각 출력에 open/close 대응물) ⑦ far-future 로우 자기 창 ⑧ 경계 정확성: ts가 정확히 창 경계면 다음 창(§ windowStartFor floor 규칙과 일치)
- [ ] 컴파일 실패 확인 → 구현 → 테스트 그린
- [ ] 커밋 `TSCS T3: window routing for intra-partition splits`

### Task 2: TimeWindowSplittingMultiWriter + createSSTableMultiWriter 오버라이드

**Interfaces (Consumes):** Task 1의 `WindowRoutingIterator.route`. **(Produces):** `TimeSeriesCompactionStrategy.createSSTableMultiWriter(...)` 오버라이드 — 시그니처는 AbstractCompactionStrategy(:558)와 동일.

- 구현 골자: `append(UnfilteredRowIterator partition)`에서 `route(...)` 결과의 각 창에 대해 지연 생성한 창별 `SimpleSSTableMultiWriter`(동일 descriptor 디렉토리, 신규 generation — `cfs.newSSTableDescriptor`)에 `UnfilteredRowIterators`로 재조립한 서브 파티션(파티션 키·삭제·스태틱 복제 = D3)을 append. `finish/finished/abort`는 전 내부 라이터에 위임(부분 실패 시 전체 abort — 원자성은 상위 LifecycleTransaction 소관).
- 오버라이드 가드: 옵션에 window_size 없으면(비TSCS 경로 방어) super로 폴백.
- [ ] 테스트 먼저 (`TimeWindowSplittingMultiWriterTest`, SchemaLoader): ① 두 창 걸침 memtable flush(USING TIMESTAMP 과거+현재) → live SSTable 2개, 각각 min/max가 자기 창 완전 포함 ② 단일 창 flush → 1개(오버헤드 없음) ③ flush 후 데이터 무결성: SELECT 전 로우 일치 + 파티션 삭제 생존 ④ 스트리밍 경로는 RangeAwareSSTableWriter가 같은 훅 경유 — 단위로는 `cfs.createSSTableMultiWriter` 직접 호출로 대변(3노드 dtest는 비목표, spec §11 기록)
- [ ] 기존 `TimeSeriesCompactionStrategyE2ETest` 회귀 (동결 E2E가 걸침 없이도 그대로 통과해야)
- [ ] 커밋 `TSCS T3: window-splitting flush and streaming writer`

### Task 3: SplitRefreezeCompactionTask (레거시 걸침 국소 재동결) — 컨트롤러 인라인 권장

**Interfaces (Consumes):** T2의 `classify`/`nextFreezeCandidate`(:286-310), `FreezeCompactionTask` 패턴.

- `nextFreezeCandidate` 확장: 닫힌 창에서 `size()==1 && !contained`(걸침 단일) 감지 시 별도 후보로 반환 — 우선순위는 통상 동결보다 낮게(오래된 창 우선 규칙 내에서). `getNextBackgroundTasksAt` 분기에서 `SplitRefreezeCompactionTask(cfs, txn(그 1개), gcBefore)` 발급, 라운드당 1개 제한 공유.
- 태스크 구현: `CompactionTask` 상속, `getCompactionAwareWriter` 오버라이드로 창별 스위칭 라이터(내부에서 Task 1 route 재사용, CompactionAwareWriter의 파티션 단위 계약과 다르므로 `realAppend`에서 직접 창별 writer 관리 — Task 2 라이터와 최대 공유). `shouldReduceScopeForSpace()` false. **리스너 발화 없음** (D7).
- [ ] 테스트 먼저 (Mockito + E2E 각 1): ① 목: 걸침 단일 SSTable 창이 SplitRefreeze 후보로 선택되고 통상 동결·만료와 우선순위 충돌 없음 ② E2E: 걸침 SSTable 수동 구성(비TSCS로 flush 후 ALTER→TSCS) → 백그라운드 2라운드: 1라운드 분할(창별 2개 산출) → 2라운드 각 창 동결/FROZEN + 리스너 발화는 동결 라운드에만
- [ ] 커밋 `TSCS T3: split-refreeze for legacy spanning sstables`

### Task 4: 문서·CI·스펙·푸시 (haiku 가능)

- [ ] `.gitlab-ci.yml`: `WindowRoutingIteratorTest`, `TimeWindowSplittingMultiWriterTest` 두 줄 추가 (기존 idiom)
- [ ] spec §11에 T3 완료 노트: 커밋 범위·테스트 수·D1~D7 요약·비목표(3노드 dtest, UCS 샤딩 합성 — 후속), T2 인계 (b)(c)(d)(g) 소화 현황 명기; §4의 "구현 시 확인" 문구를 확정 내용으로 갱신
- [ ] README TSCS 행: "T3 지각격리 예정" → 완료 문구로; CHANGES.txt 한 줄
- [ ] 전체 검증: 4개 TSCS 테스트 클래스 + 신규 2개 + TWCS/UCS 회귀 + `ant checkstyle checkstyle-test` + `.build/sh/ai-build`
- [ ] `git push origin master && git push origin master:6.0.0`

## Self-Review 노트

- spec §4 커버리지: flush 스플릿(Task 2), 스트리밍(같은 훅 — Task 2 ④), FROZEN 창 지각 유입 재동결(T2가 이미 상태 재유도로 처리, 신규는 걸침 케이스의 Task 3), 리페어 유입(스트리밍 경로와 동일 훅).
- T2 인계 소화: (a) createSSTableMultiWriter → Task 2. (b) 걸침 국소 재동결 → Task 3. (c) ALTER 구경계 → D4로 완화(재작성 없음 유지; 걸침이 되면 Task 3가 자연 처리 — spec §11에 이 해석 명기). (d) 동시성 설정화 → 이번에도 비목표, spec 유지. (g) 테스트 공백 2건 → Task 3 E2E가 재동결 산출물 창 포함(=내용 검증의 구조적 등가물)을 assert; gap-round 리셋 핀은 Task 1~3 범위 밖 — spec (g)에 잔존 명기.
- 리스크: RT boundary 분해와 스태틱/파티션 삭제 복제가 정확성 핵심 — Task 1을 가장 두껍게 테스트(8케이스)하고 컨트롤러 인라인으로 실행하는 이유.
