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

# SP3: 투명 읽기 (청크 자동 병합 SELECT) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.
> **Quota note:** 프리미엄 모델 소진 상태 — 컨트롤러 인라인 실행 기준으로 작성(세부 코드는 구현 시 트리 확인). 독립 리뷰는 8/3 쿼터 리셋 후 T2/T3 리뷰와 일괄.

**Goal:** 계층화 테이블의 원본 SELECT가 핫 로우와 `__chunks` 디코드 로우를 자동 병합해 돌려준다 — 앱은 압축의 존재를 모른다 (spec §3.3/§3.3.1).

**Architecture:** `SelectStatement`의 결과 처리 직전, PartitionIterator를 `ChunkMergePartitionIterator`로 감싼다(집계 전 주입 — 갭필의 ResultSet 후처리로는 집계 질의가 투명해지지 않음, 정찰 확정). 파티션(태그)별로 요청 타임스탬프 범위와 겹치는 청크를 사용자 CL로 조회·디코드해 합성 Row 스트림을 만들고, 핫 파티션과 클러스터링 순 2-way 병합. 동일 타임스탬프 충돌은 셀 writetime 비교로 해소되는데, 합성 로우 writetime = 청크 `max_row_writetime` ≤ 스윕 maxWt < 미스윕 핫 로우 writetime 이므로 **업스트림 병합 시맨틱이 곧 rows-win 규칙**이다(재인코더와 왕복 일관).

## Global Constraints

- Java 21; `src/gen-java/`·`lib/`·CQL 문법 수정 금지; ASF 17줄 헤더; overflow-safe 시간 비교.
- 데이터 경로 청크 조회는 `QueryProcessor.process(query, cl, ...)` — **사용자 질의와 같은 CL** (SP2 불변식 B 확장).
- 발동 안 하는 질의(정책 없음/범위 안 겹침/비슬라이스)는 기존 경로 그대로 — 오버헤드 0 원칙.
- 검증: 증분 `ant build-test -Dno-checkstyle=true -Dant.gen-doc.skip=true -Drat.skip=true 2>&1 | .build/sh/ant-log-summary.py -`; 테스트 `.build/sh/ai-ci-test --reuse <FQCN>` + TEST-*.xml 확인; 커밋 전 `ant checkstyle checkstyle-test` **PIPESTATUS로 종료코드 직접 확인**(`var` 금지·import 순서 함정, ai-build exit-0 미스터리 미해결).
- 커밋 트레일러: `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>` + `Claude-Session: https://claude.ai/code/session_01Jruk9rM1N82xGmXD41RfmE`
- 신규 테스트 클래스는 `.gitlab-ci.yml` 배선 필수 (마감 태스크).

## 규범 결정 (spec §3.3.1 + 추가)

- **R1 병합 지점**: `processResults`로 들어가기 전의 PartitionIterator 래핑. 비페이징 execute / executeInternal 두 경로 필수, 페이징 경로는 Task 3에서 결합점 확정(페이저가 페이지마다 슬라이스를 좁히므로 "페이지 슬라이스 범위 내 청크만 주입"이면 무상태 정합 — 결합 불가 판명 시 v1은 비페이징+집계 완전 지원, 대용량 비집계 페이징은 명확한 에러+힌트).
- **R2 발동 조건**: TieringPolicy 존재(스키마 버전 키 캐시) ∧ 단일/IN 파티션 질의(getPartitionKeys :794) ∧ ClusteringIndexSliceFilter ∧ 요청 범위 하한 < (now − hot_window + 여유). 컬럼 서브셋 SELECT(value 미포함)나 writetime()/ttl() 선택 시: v1은 병합 수행하되 문서화(합성 writetime = max_row_writetime 근사; ttl 없음).
- **R3 합성 로우**: `BTreeRow.singleCellRow(Clustering(ts), BufferCell.live(valueCol, maxRowWritetimeMicros, decompose(v)))`. 클러스터링 타입은 canonicalSchema가 보장(timestamp). DESC 질의는 슬라이스 역순 — 병합 이터레이터가 isReverseOrder에 맞춰 역순 생성.
- **R4 청크 조회**: 파티션별 `SELECT window_start, codec, samples, max_row_writetime, payload FROM <ks>.<table>__chunks WHERE tag=? AND window_start >= ? AND window_start <= ?` (하한 = floor(rangeStart) − chunk_window 여유, 상한 = rangeEnd), 사용자 CL. 디코드는 `ChunkCodecs.cursor`. 손상 청크는 WARN+스킵(읽기 가용성 우선, SP2 태그 격리와 동일 철학) — 단 스킵 사실을 ClientWarn으로 표면화.
- **R5 LIMIT**: 증명 완료(spec §3.3.1) — 추가 조치 불요, 기존 trim이 병합 후 적용. 단 DataLimits가 CQL 로우 수 기준인 경로 확인 테스트 필수.
- **R6 죽은 태그 청크 만료(SP2 인계 최우선)**: TieredStorageService의 콜드 만료 열거를 베이스 DISTINCT에서 **청크 테이블 열거**로 전환(`SELECT DISTINCT tag FROM __chunks` + 태그별 `window_start < cutoff` 삭제). 베이스 로우가 전무한 태그의 청크도 만료된다.

## File Structure

- Create: `src/java/org/apache/cassandra/db/timeseries/tiering/ChunkReadSupport.java` — 정책 캐시 + 청크 조회/디코드 → 합성 Row 스트림 (서버 의존 최소화로 단위 테스트 가능하게 조회부와 디코드부 분리)
- Create: `src/java/org/apache/cassandra/db/timeseries/tiering/ChunkMergePartitionIterator.java` — PartitionIterator 데코레이터 + 파티션별 RowIterator 2-way 병합
- Modify: `src/java/org/apache/cassandra/cql3/statements/SelectStatement.java` — 발동 판정 + 래핑 (갭필 배선과 같은 최소 디프 스타일; CLAUDE.md 충돌 목록에 이미 있음)
- Modify: `src/java/org/apache/cassandra/db/timeseries/tiering/TieredStorageService.java` — R6 만료 열거 전환
- Test: `test/unit/.../tiering/ChunkMergeIteratorTest.java`(병합 단위), `TransparentReadTest.java`(CQLTester E2E), 기존 `TieredStorageServiceTest` 확장(R6)

### Task 1: ChunkReadSupport (디코드 → 합성 로우, 단위 검증)
- decode부: `static List<Row> rowsFromChunk(TableMetadata, ByteBuffer payload, byte codec, long maxRowWritetime, long startMs, long endMsExcl, boolean reversed)` — 커서 순회, 범위 필터, R3 합성. 테스트: Gorilla/Chimp 페이로드 왕복, 범위 경계(포함/제외), reversed 순서, 손상 페이로드 IAE 전파.
- [ ] 테스트 → red → 구현 → green → 커밋 `SP3: chunk decode to synthetic rows`

### Task 2: ChunkMergePartitionIterator (병합 단위)
- RowIterator 2-way 병합: 클러스터링 비교(메타데이터 comparator, reversed 대응), 동일 클러스터링 → 셀 writetime 큰 쪽(=핫) 선택. 파티션 키 정렬 유지(청크는 핫 이터레이터의 파티션에만 붙음 — 핫에 없는 태그의 파티션은 v1 비지원··· **아니다**: 핫 로우가 0인 태그(전부 청크화)도 반환해야 투명 — 요청 파티션 키 목록 기준으로 핫 이터레이터에 없는 파티션을 합성 삽입. 단일 파티션 질의가 주력이므로 "요청 키 집합" 파라미터로 처리).
- 테스트(서버리스, Util.UnfilteredSource 유사 구성이 아닌 filtered RowIterator 목): 핫만/청크만/교차/동일 ts 충돌 핫 승리/DESC/핫에 없는 파티션 합성.
- [ ] 테스트 → red → 구현 → green → 커밋 `SP3: coordinator merge iterator`

### Task 3: SelectStatement 배선 + 페이징 결합
- 발동 판정(R2) 헬퍼 → execute(비페이징)/executeInternal 래핑 → **페이징 경로 조사·결합**(pager.fetchPage 반환 이터레이터 래핑 + 페이지 슬라이스 범위 전달; AggregationQueryPager 경유 확인). 불가 시 v1 폴백(R1)과 에러+힌트.
- 회귀: 비계층 테이블 쿼리 경로 무영향(발동 0), 기존 SelectTest/AggregationTest 스모크.
- [ ] 구현 → CQLTester 스모크 → 커밋 `SP3: transparent read wiring in SelectStatement`

### Task 4: R6 죽은 태그 만료 전환
- TieredStorageService 만료 열거를 청크 테이블 기준으로. TieredStorageServiceTest에 dead-tag 케이스(베이스 로우 전무 태그의 콜드 청크가 만료됨) 추가 — SP2 원장의 구조적 공백 해소.
- [ ] 테스트 → 구현 → green → 커밋 `SP3: chunk-table-driven cold expiry (dead tags)`

### Task 5: E2E 투명성 매트릭스 (TransparentReadTest, CQLTester)
- 시나리오: 적재(2창) → runOnce(재인코딩) → ① 전범위 SELECT = 적재값 전부 ② 콜드만 ③ 핫만(발동 안 함 확인 — 청크 조회 카운터/추적) ④ 지각 로우(스윕 후 삽입) 병합 우선 ⑤ LIMIT/DESC ⑥ 집계(avg/count/time_bucket GROUP BY) 핫·콜드 걸침 정확 ⑦ gap-fill 결합 ⑧ 페이징(결합 성공 시) ⑨ 손상 청크 스킵+경고. 도커 IT에 투명 읽기 섹션 추가(§는 마감 태스크).
- [ ] 작성 → green → 커밋 `SP3: transparent read end-to-end matrix`

### Task 6: 마감 — 문서·CI·푸시
- tiered-storage.md 개편(명시 조회 → 투명 읽기; 제한: writetime 근사·페이징 스코프·손상 스킵 경고), README/CHANGES, spec §8/§3.3.1 완료 노트, `.gitlab-ci.yml` 신규 클래스, 도커 IT 섹션, 전체 검증, `git push origin master && git push origin master:6.0.0`.

## Self-Review 노트
- spec §3.3 커버: 판정(R2)·병합(R3/R4)·집계(병합 지점이 집계 앞이라 자동)·gap-fill 결합(⑦)·비대상 질의 에러+힌트(R1 폴백). §8 SP3 인계: 죽은 태그(R6=Task 4), 명시 조회 문서 대체(Task 6), 멀티노드 공백은 잔존 명시.
- 리스크 1순위: 페이징 결합(Task 3) — 실패해도 v1 폴백이 정의돼 있어 BLOCKED 없음. 2순위: 합성 Row와 Selection(writetime()/ttl()) 상호작용 — R2에 문서화 경로.
