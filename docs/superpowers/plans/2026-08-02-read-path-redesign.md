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

# 읽기 경로 재설계 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** [스펙](../specs/2026-08-02-read-path-redesign.md)의 4단계 구현 — 0단계(격리 해제)는 오늘, 1~4단계(스트리밍 읽기)는 소크 체계를 갖춘 뒤.

**Architecture:** 0단계는 순수 제거(스냅샷 보유 코드 삭제 → v2 이전 동작 복원). 1~2단계는 append-only 슬롯 + supersede 플래그 위에서 배열을 직접 걷는 `StreamingRowIterator`. 3단계는 소크형 힙 게이트, 4단계는 6시간 프로덕션 관찰.

**Tech Stack:** Java 21, `.build/sh/ai-*`, JUnit 4, jamm/allocator 카운터, `executeNet` 페이징.

## Global Constraints

- 빌드 `.build/sh/ai-build`, 테스트 `.build/sh/ai-ci-test --reuse <FQCN>`만. 전체 스위트 금지.
- **읽기 경로는 아무것도 보유하지 않는다** (OOM 제약). "상한 있는 캐시"도 금지 — 스펙 §0 제약 ③.
- **fail-open**: 판단 불가 시 최적화 포기, 예외 금지 (페이징 장애 제약 ①).
- 기존 테스트는 **제거된 기능의 테스트를 지우는 경우 외에** 수정 금지. 지울 때는 커밋 메시지에 사유 명기.
- offheap(`offheap_objects`) + `executeNet` 페이징 테스트는 모든 단계의 게이트.
- 성능 주장은 카운터 단언(조립 행 수), 벽시계 금지.
- 배포 절차: 검증 → drain → stop.sh → lib-backup 백업 → 교체 → start.sh → `oom_score_adj` -1000 확인.
- 41번 배포 후 **6시간 힙 관찰 전에 "완료" 선언 금지** (v2 는 6시간 뒤 죽었다).

---

## Task 0: master 격리 해제 — 스냅샷 보유 제거 (오늘)

**Files:**
- Modify: `src/java/org/apache/cassandra/db/memtable/TimeSeriesColumnarPartition.java`
- Modify: `test/unit/org/apache/cassandra/db/memtable/TimeSeriesMemtableReadPathTest.java` (보유 기능 전용 perf 가드 1건 삭제)

**정확한 제거 범위** (2026-08-02 20:07 코드 기준 행 번호):

| 위치 | 내용 | 조치 |
| --- | --- | --- |
| 182, 184-185 | `lastSnapshot`, `lastSnapshotSize` 필드 | 삭제 |
| 188-194 | `changedBeyondAppend` 필드+주석 | 삭제 |
| 197, 795, 985-987 | `fullSnapshotBuilds` 카운터+게터 | 삭제 |
| 265, 278, 478, 507, 622 | `changedBeyondAppend = true` 표시 5곳 | 삭제 |
| 722-724 | 스냅샷 빌드 후 보유 저장 | 삭제 (빌드 결과는 기존 `snapshot` volatile 캐시에만) |
| 747-756 | 읽기 뷰 선택(`base == null \|\| changedBeyondAppend ...`) | v2 이전으로: `snapshot` null 이면 전체 빌드 |
| 767~ | `extendSnapshot(...)` 메서드 전체 | 삭제 |

**유지(제거 금지):** `mayContainRowsIn` + min/max 클러스터링 추적(가지치기용), `intersectsBounds`·`pruneFailedOpen`(TimeSeriesMemtable), offheap·페이징 테스트 전부.

**삭제할 테스트:** `interleavedAppendsAndReadsExtendTheSnapshotIncrementally` — 제거되는 보유 기능 자체의 perf 가드라 기능과 함께 지운다. "구현에 맞춰 테스트 수정" 이 아니라 "기능 제거에 따른 테스트 제거"이며 커밋 메시지에 명기한다.

- [ ] Step 1: 위 표대로 제거, 빌드
- [ ] Step 2: 배터리 — memtable 6클래스(ReadPathTest는 perf 가드 제외 9건) + Offheap 3건 + 계층화 회귀(TransparentReadTest·ColdWindowChunkFlushTest), XML 직접 확인
- [ ] Step 3: 뮤테이션 1건 — 가지치기 비교를 뒤집어 backfill 케이스 빨간불 확인 (가지치기가 여전히 보호되는지), `\cp -f` 원복+diff
- [ ] Step 4: 커밋·푸시(3원격), jar 빌드
- [ ] Step 5: 41번 배포(절차 준수), 기동 후 ERROR·폴백 0 확인, **30분 힙 표본 2개**가 톱니인지 확인 → 격리 해제 선언, 메모리(`readpath-build-heap-leak`) 갱신

## Task 1: append-only 슬롯 + supersede 플래그 (쓰기 경로)

**Files:** Modify `TimeSeriesColumnarPartition.java` · Test: 차등 케이스 추가

스펙 §2.2. in-place 수정 2곳을 대체한다:
- 동일 클러스터링 재기록(`mergeIntoSlot`의 값 덮어쓰기) → **새 슬롯 append + 옛 슬롯 `superseded`**(volatile boolean[] 또는 long 비트맵, 0→1 단방향)
- 부분 UPDATE 승격 → overflow(`ConcurrentSkipListMap`으로 교체) 삽입 + 옛 슬롯 `superseded`
- superseded 비율 > 50% 시 그 파티션만 객체 계층 강등 (fail-open 원리)
- 읽기는 아직 스냅샷 전체 빌드(Task 0 상태) — 빌드 시 superseded 슬롯 건너뜀

- [ ] 차등 케이스: 재기록 폭주(같은 셀 100회), 승격 반복, 재기록+승격 혼합, DESC — skiplist와 동일
- [ ] 뮤테이션: supersede 플래그를 안 세움 → 재기록 케이스에서 중복 행으로 빨간불
- [ ] 배터리 + 커밋 (**배포는 하지 않음** — Task 2와 함께)

## Task 2: StreamingRowIterator (읽기 경로)

**Files:** Create `db/memtable/TimeSeriesStreamingIterator.java` · Modify `TimeSeriesColumnarPartition.java` (snapshot 제거, unfilteredIterator가 스트리밍 반환) · Test: 조립 카운터 + 기존 전부

스펙 §2.1. 열 때 consolidate+size 캡처, slices 이진 탐색, 당길 때 행 조립, overflow 2-way 병합, 톰스톤·static 객체 경로 병합, superseded 스킵, double-check(플래그 재확인). reversed 지원.

- [ ] `@VisibleForTesting rowsAssembled` 카운터 — slices 읽기가 구간 행 수만 조립하는지 단언
- [ ] 게이트: 차등 전부 + offheap 3건 + 페이징 E2E + 조립 카운터
- [ ] 뮤테이션: double-check 제거 → 승격 경합 케이스 빨간불; 이진 탐색 경계 ±1 → slice 케이스 빨간불

## Task 3: 소크형 힙 게이트

**Files:** Create `test/long/org/apache/cassandra/db/memtable/TimeSeriesMemtableHeapSoakTest.java` · Modify CI(야간 배치)

- 파티션 500개 × 쓰기·읽기 지속 교차(≥10분), 주기적으로 allocator `owns()`+jamm deep-size 표본
- **단언: 후반 1/3 표본의 선형 회귀 기울기 ≈ 0 (평탄)**. 상승 추세면 실패
- 검증: Task 0 이전(v2) 코드로 돌리면 이 테스트가 **빨간불이어야 한다** — 소급 뮤테이션으로 게이트 자체를 증명

## Task 4: 배포 + 6시간 관찰

- [ ] 41번 배포(절차), 모니터 힙 필드 6시간: 톱니 유지·상승 추세 없음·예외 0·flush 적체 0
- [ ] 읽기 지연 실측(부하 중 tablehistograms) — 기준선(Task 0 상태) 대비 기록, 미달이면 미달로
- [ ] 문서: `timeseries-memtable.md` 읽기 절 갱신, 재설계 스펙에 실측 기입, README 링크 확인

## Self-Review

- 스펙 커버리지: §1→Task 0, §2.1→Task 2, §2.2→Task 1, §2.3 범위 밖 유지, §3 게이트→각 태스크+Task 3, §4 순서 일치 ✓
- 타입 일관성: `superseded`(Task 1 정의, Task 2 소비), `rowsAssembled`(Task 2), 소크 단언(Task 3) ✓
- 플레이스홀더: Task 1·2는 정확 코드 대신 제약+뮤테이션 기준 명시(참조 구현이 저장소 안에 있고, v2의 교훈대로 구현자가 코드를 읽고 쓰는 편이 안전) ✓
