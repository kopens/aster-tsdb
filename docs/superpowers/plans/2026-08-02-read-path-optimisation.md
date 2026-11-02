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

# 읽기 경로 최적화 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 컬럼 저장 memtable의 읽기 지연 회귀(0.88 → 7.5 ms)를 없애고, 계층화 테이블의 `LIMIT` 질의가 필요한 청크만 디코드하게 한다.

**Architecture:** 세 변경 모두 읽기 경로 한정 — 쓰기·flush·인코딩 경로는 건드리지 않는다. ①은 `TimeSeriesColumnarPartition`의 스냅샷 캐시를 증분화하고, ②는 `TimeSeriesMemtable.rowIterator`가 샤드-파티션의 **클러스터링** 경계로 건너뛰게 하고, ③은 청크 병합 이터레이터를 창 단위 지연 디코드로 바꾼다.

**Tech Stack:** Java 21, `.build/sh/ai-*` 래퍼, JUnit 4, 차등 테스트 + 뮤테이션 검증 + `@VisibleForTesting` 카운터.

## Global Constraints

- 빌드 `.build/sh/ai-build`, 테스트 `.build/sh/ai-ci-test --reuse <FQCN>`만. 전체 스위트 금지.
- **기존 테스트를 수정하지 않는다** — 실패하면 구현이 틀린 것이다. 대상: memtable 32건, 계층화 회귀(TransparentReadTest 22 · ChunkReadSupportTest · ChunkMergeIteratorTest · ColdWindowChunkFlushTest 9).
- 결과가 오늘과 **바이트 단위로 동일**해야 한다 — 이 계획의 모든 변경은 "일을 덜 하는" 것이지 "다른 답을 내는" 것이 아니다.
- 새 테스트는 반드시 뮤테이션 검증을 거친다(결함을 넣어 빨간불 확인, `\cp -f`로 원복 후 diff 확인 — `cp`는 `-i` 별칭).
- 성능 주장에는 벽시계가 아니라 **카운터 단언**을 쓴다(전체 재구축 횟수, 디코드된 창 수).
- 구현을 전부 끝낸 뒤 테스트를 몰아서 1사이클로 돌린다(사이클당 3~4분).

---

## 근거 (실측)

| 문제 | 증거 |
| --- | --- |
| ① 스냅샷 전체 재구축 | 쓰기마다 `snapshot = null` → 다음 읽기가 O(파티션) 재조립. 운영 실측: 읽기 지연 trie 0.88 ms → columnar **7.5 ms**. `window_size 1d`면 샤드-파티션이 태그당 ~86k행까지 자람 |
| ② 샤드 전수 순회 | `rowIterator`가 모든 창 샤드를 열고 slices 필터에 맡김 |
| ③ 청크 선디코드 | 범위 없는 `LIMIT 1000`이 파티션의 **모든** 청크를 디코드(벤치마크 0.30×, 3.3배 느림). LIMIT는 디코드 뒤에 적용됨 — 벤치마크 문서에 미구현으로 명기돼 있음 |

## Task A: 증분 스냅샷 (`TimeSeriesColumnarPartition`)

**Files:** Modify `src/java/org/apache/cassandra/db/memtable/TimeSeriesColumnarPartition.java` · Test `TimeSeriesMemtableDifferentialTest`(케이스 추가) + 재구축 카운터 perf 가드

**설계**: 스냅샷과 함께 **그 시점의 행 수**를 기억한다. 읽기 시 스냅샷 이후의 변화가 **순수 append뿐**이면(기존 행 승격 없음, 톰스톤·static 변경 없음, 지연 정렬 미발생) `[snapshotCount..size)` 구간만 물질화해 캐시와 병합한다. **그 외 모든 in-place 변경은 전체 무효화** — 정확성이 우선이다. 변화가 없으면 오늘처럼 캐시를 무할당 반환.

- [ ] 구현 (DESC 클러스터링의 comparator 순서 유지 포함)
- [ ] 차등 케이스 추가: 쓰기/읽기 교차 시퀀스, 승격 후, 행 삭제 후, 역순 도착(정렬 경로), DESC 테이블 — 전부 `skiplist`와 결과 동일
- [ ] perf 가드: N append × N read 에서 전체 재구축 횟수 ≪ 읽기 횟수 (카운터 단언)
- [ ] 뮤테이션: "승격이 무효화한다" 검사를 제거 → 승격 차등 케이스가 빨개져야 함

## Task B: 샤드 클러스터링 경계 가지치기 (`TimeSeriesMemtable`)

**⚠️ 함정 — 쓰기 창으로 가지치기하면 안 된다.** 샤드는 **쓰기 시각** 기준인데 slices는 **클러스터링** 제약이다. 백필 행은 옛 클러스터링을 달고 현재 쓰기 창 샤드에 산다 — 쓰기 창으로 걸러내면 조용히 누락된다. 오늘 프로덕션 진단에서 정확히 이 혼동을 겪었다.

**설계**: 샤드-파티션별로 실제 존재하는 클러스터링 min/max를 유지(append 시 갱신)하고, slices와 교차하지 않는 샤드-파티션만 건너뛴다. 경계를 모르면 **건너뛰지 않는다**(fail open).

- [ ] 구현 (columnar 계층은 클러스터링 배열에서, object 계층은 apply 시 추적)
- [ ] 백필 케이스: 지금 쓰되 클러스터링은 수 창 과거 → 옛 범위 슬라이스 조회가 반드시 찾아야 함
- [ ] 뮤테이션: 쓰기 창 기준 가지치기로 바꿈 → 백필 케이스가 빨개져야 함

## Task C: 창 단위 지연 청크 디코드 (`tiering` 패키지)

**설계**: 특수화가 아니라 **지연화**다. 청크 창은 클러스터링상 서로소이므로(커버리지 보장), 청크 쪽 이터레이터가 **당겨질 때 한 창씩** 가져와 디코드한다 — 질의 방향 순서로(DESC면 최신 창부터). 그러면 상류 `DataLimits`가 LIMIT 충족 시 당기기를 멈추고 나머지 창은 **가져오지도 디코드하지도 않는다**. 정렬 순서 외에 따질 건전성 조건이 없다: 지연화는 결과를 바꾸면 안 된다.

- DESC에서 모든 핫 행(콜드 경계 위)이 모든 청크 행보다 먼저 온다 — 핫만으로 LIMIT가 차면 청크는 아예 안 건드린다
- slices 밖 창은 fetch 자체를 안 하는지 확인(이미 있을 수 있음 — 가정 말고 검증)
- 구조상 불가능하면(병합이 물질화를 요구하는 지점이 있으면) 파일·행을 지목해 **중단·보고**

- [ ] 구현
- [ ] 정확성: DESC LIMIT n / ASC LIMIT n / 전체 스캔 / slice+LIMIT — 비계층 테이블과 행 단위 동일
- [ ] 지연 증명: 디코드된 창 카운터 — 창 10개 파티션에서 DESC LIMIT 소량이 1~2창만, 전체 스캔은 10창
- [ ] static 전용 읽기: 최대 1창 디코드가 목표 — 불가능하면 정직하게 보고
- [ ] 뮤테이션: 방향 정렬을 항상 오름차순으로 → DESC LIMIT 케이스 빨간불; 카운터를 디코드 없이 증가 → 지연 증명 빨간불

## 검증·롤아웃

1. 두 워크트리 병합 → memtable 32건 + 계층화 회귀 전체 + 신규 테스트 일괄 실행
2. jar 빌드 → 41번 배포(drain → stop.sh → 교체 → start.sh → oom -1000 확인)
3. **실측 판정**: 부하 중 `Local read latency`가 1 ms 수준으로 복귀하는지(①), 캐너리 테이블에서 DESC LIMIT 질의의 창 디코드 수(③)
4. 미달이면 미달로 기록하고 원인 분석 — 기대치에 숫자를 맞추지 않는다
5. 문서: `timeseries-memtable.md`·`tiering-benchmark.md`의 해당 절 갱신, 상시 모니터 계속

## 범위 밖

- `put()` 이중 순회(소소함), `integral`/`time_weighted_average` 집계 비용(별건), 스트리밍·repair 경로
