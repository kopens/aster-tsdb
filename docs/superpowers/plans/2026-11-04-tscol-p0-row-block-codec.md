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

# tscol P0 — 행 블록 코덱 (정확 왕복) 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 한 파티션의 Cassandra `Row` 묶음(클러스터링 = timestamp 하나)을 청크 코덱 v4 페이로드로 인코딩하고, 디코드하면
원래 `Row`와 `equals`가 성립하는 코덱 `RowBlockCodec`을 만든다.

**Architecture:** v4의 바이트 레이아웃은 바꾸지 않는다. 셀 writetime·TTL·local deletion time·셀 톰스톤·행 liveness를
v4 컨테이너 안의 **예약 이름 컬럼**(이름이 `\0`으로 시작)으로 함께 싣는다. 보통의 데이터에서 이 컬럼들은 v4의
CONSTANT/ALL_NULL로 O(1)이 된다. 인코딩할 수 없는 행(행 삭제, 비-frozen 복합 셀, 카운터)이 하나라도 있으면
`encode`가 `null`을 돌려주고, 호출자(P1의 형식 writer)가 그 파티션을 행 블록으로 폴백한다.

**Tech Stack:** Java 21, Cassandra 6.0 `db.rows`(`Row`, `Cell`, `LivenessInfo`, `BTreeRow`, `BufferCell`), 청크 코덱 v4
(`ChunkV4Codec`, `ColumnarChunkCodec`, `ColumnarCursor`), JUnit 4 + `quicktheories`(Cassandra가 이미 쓰는 속성 테스트).

**Spec:** [docs/superpowers/specs/2026-11-04-columnar-frozen-sstable-design.md](../specs/2026-11-04-columnar-frozen-sstable-design.md) §4.2

**이 계획 다음:** P1 `tscol` 형식(writer/reader/scanner, 차등 하네스), P2 TSCS `frozen_format`, P3 분산 검증, P4 성능,
P5 옛 계층화 제거 + `nodetool untier`, P6 41 적용 — 각각 별도 계획. P0이 끝나야 P1 계획을 쓴다(인터페이스가 여기서 정해짐).

## Global Constraints

- 빌드 산출물 `build/apache-cassandra-6.0.0.jar`, `base.version` 6.0.0, 패키지 이름 변경 금지.
- v4 바이트 레이아웃·결정성 규칙(chunk-format-v4.md §5) 변경 금지 — 이 계획은 v4 위의 매핑만 정의한다.
- 새 테스트 클래스는 `.build/sh/ci-timeseries-tests.sh`의 `TESTS=(...)`에 추가(안 하면 CI가 안 돈다).
- 빌드·테스트는 컨테이너에서: `.build/sh/ai-in-container '.build/sh/ai-ci-test <FQCN>'`. 동시에 두 개 돌리지 않는다.
- 커밋 제목 `[기능]`/`[정리]` 등 한국어 태그. 사용자에게 보이는 변화가 없으므로 Changelog 트레일러 없음.

## Review Focus

1. **행 marker만 있는 행** (`INSERT`로 키만 넣음, 모든 컬럼 null) — 디코드에 liveness가 살아 있어야 한다. → Task 1 테스트에 포함.
2. **UPDATE로만 만들어진 행**(primary key liveness가 `EMPTY`, 셀만 있음) — 디코드가 liveness를 만들어 내면 안 된다. → Task 1.
3. **같은 행 안에서 셀마다 writetime이 다른 경우**(늦은 컬럼 UPDATE) — 셀별 타임스탬프 보존. → Task 2.
4. **만료된(TTL 지난) 셀과 셀 톰스톤** — `localDeletionTime`·TTL이 정확히 보존돼야 이후 컴팩션 purge가 같은 결정을 한다. → Task 3.
5. **DESC 클러스터링 테이블** — v4 타임스탬프 축은 엄격 증가만 받는다. 인코딩은 오름차순, 디코드는 테이블 순서로. → Task 1.

---

### Task 1: `RowBlockCodec` — live 행의 값과 liveness 정확 왕복

**Files:**
- Create: `src/java/org/apache/cassandra/db/timeseries/RowBlockCodec.java`
- Test: `test/unit/org/apache/cassandra/db/timeseries/RowBlockCodecTest.java`
- Modify: `.build/sh/ci-timeseries-tests.sh` (TESTS에 `org.apache.cassandra.db.timeseries.RowBlockCodecTest` 추가)

**Interfaces:**
- Produces:
  - `public static ByteBuffer RowBlockCodec.encode(TableMetadata metadata, List<Row> rows)` — `rows`는 테이블 클러스터링 순서,
    static 행 제외, 하나 이상. 인코딩할 수 없는 행이 있으면 `null`.
  - `public static List<Row> RowBlockCodec.decode(TableMetadata metadata, ByteBuffer payload, ColumnFilter columns)` —
    테이블 클러스터링 순서로 돌려줌; `columns`가 `null`이면 전 컬럼.
  - `public static boolean RowBlockCodec.supports(TableMetadata metadata)` — 클러스터링이 timestamp 하나이고 일반 컬럼이
    전부 단일 셀(frozen 포함, 카운터 아님)일 때 `true`. (`ColdBoundary.hasTimestampClustering`과 같은 판정 + 셀 종류.)
  - 예약 컬럼 이름 상수: `ROW_TS = "\0row.ts"`, `ROW_TTL = "\0row.ttl"`, `ROW_LDT = "\0row.ldt"`.

- [ ] **Step 1: 실패하는 테스트 작성** — `RowBlockCodecTest`:
  - `liveRowsRoundTripExactly`: 테이블 `(tag text, ts timestamp, v double, n int, s text, b boolean, m frozen<map<text,text>>,
    PRIMARY KEY (tag, ts))`. 행 3개: 모든 컬럼 값 있음 / `n`·`s` null / 키만(`INSERT (tag, ts)`, 모든 컬럼 null). 모두 같은
    writetime 1000, 1001, 1002. `assertEquals(rows, decode(metadata, encode(metadata, rows), null))`.
  - `updateOnlyRowsKeepEmptyLiveness`: liveness `EMPTY`, 셀 `v`만 있는 행 → 디코드 행의 `primaryKeyLivenessInfo().isEmpty()`.
  - `descendingTablesRoundTripInTableOrder`: `WITH CLUSTERING ORDER BY (ts DESC)`, 행을 DESC 순서로 넣고 같은 순서로 돌아옴.
  - `projectionDecodesOnlyRequestedColumns`: `ColumnFilter`가 `v`만 → 디코드 행에 `v` 셀만, liveness는 그대로.
  - 행 생성은 `BTreeRow.unsortedBuilder()` + `BufferCell.live(...)` + `LivenessInfo.create(ts, nowInSec)`. 테이블은
    `TableMetadata.builder` 또는 `CreateTableStatement.parse(..).build()`로 만들고 CQLTester는 쓰지 않는다(단위 테스트).

- [ ] **Step 2: 실패 확인**

  Run: `.build/sh/ai-in-container '.build/sh/ai-ci-test org.apache.cassandra.db.timeseries.RowBlockCodecTest'`
  Expected: 컴파일 실패(`RowBlockCodec` 없음).

- [ ] **Step 3: 구현** — `RowBlockCodec`:
  - 타임스탬프 축 = 각 행 클러스터링의 ms(`TimestampType.compose`), 테이블이 DESC면 행 목록을 뒤집어 오름차순으로.
  - 값 컬럼: 이름 = 컬럼 이름 그대로, `ChunkColumnTypes.typeCodeFor`/`statOrderFor`(지금은 tiering 패키지 — 그대로 import),
    값 = 셀 값 또는 `null`.
  - `ROW_TS`(INT64): liveness가 비어 있지 않으면 그 timestamp, 비어 있으면 `null`. `ROW_TTL`/`ROW_LDT`(INT32/INT64):
    expiring liveness일 때만, 아니면 `null`.
  - `\0` 접두 이름은 사용자 컬럼과 충돌할 수 없다(CQL 식별자에 NUL 불가) — 클래스 주석에 근거를 적는다.
  - 디코드: `ColumnarChunkCodec.cursor(payload, projection ∪ 예약 컬럼)`로 읽어 `BTreeRow`를 조립, DESC면 뒤집어 반환.

- [ ] **Step 4: 통과 확인** — 같은 명령, Expected: `tests="4" failures="0" errors="0"`.

- [ ] **Step 5: 커밋** — `git commit -m "[기능] tscol P0: 행 블록 코덱 — live 행 값·liveness 정확 왕복"`

### Task 2: 셀별 writetime

**Files:**
- Modify: `src/java/org/apache/cassandra/db/timeseries/RowBlockCodec.java`
- Test: `test/unit/org/apache/cassandra/db/timeseries/RowBlockCodecTest.java`

**Interfaces:**
- Produces: 예약 컬럼 `"\0wt." + 컬럼이름` (INT64) — 셀 timestamp − 기준 timestamp. 기준 = `ROW_TS`가 있으면 그 값,
  없으면 그 행 셀들의 최솟값을 `"\0row.base"`(INT64)에 기록. 모든 셀이 기준과 같으면 전부 0 → v4 CONSTANT.

- [ ] **Step 1: 실패하는 테스트** — `cellWritetimesSurviveLateColumnUpdates`: liveness 1000, `v`@1000, `n`@5000(늦은 UPDATE),
  `s`@1000. `assertEquals(rows, decode(encode(rows)))`. 그리고 `uniformWritetimesCostNothing`: 1,000행 모두 셀 ts = liveness ts일
  때 인코딩 크기 ≤ 같은 행을 값만으로(`ChunkV4Codec.encode`에 값 컬럼만) 인코딩한 크기 + 64바이트 × 예약 컬럼 수.
- [ ] **Step 2: 실패 확인** — Expected: `cellWritetimesSurviveLateColumnUpdates` FAIL(셀 ts가 liveness ts로 복원됨).
- [ ] **Step 3: 구현** — 위 Interfaces대로. 디코드에서 셀 timestamp = 기준 + delta.
- [ ] **Step 4: 통과 확인** — Expected: 전부 PASS.
- [ ] **Step 5: 커밋** — `[기능] tscol P0: 셀별 writetime 보존`

### Task 3: TTL·만료·셀 톰스톤

**Files:**
- Modify: `src/java/org/apache/cassandra/db/timeseries/RowBlockCodec.java`
- Test: `test/unit/org/apache/cassandra/db/timeseries/RowBlockCodecTest.java`

**Interfaces:**
- Produces: 예약 컬럼 `"\0ttl." + 이름`(INT32), `"\0ldt." + 이름`(INT64, `Cell.localDeletionTime()` 원값),
  `"\0del." + 이름`(BOOLEAN, 셀 톰스톤이면 true). 셀이 live·비만료면 셋 다 `null` → ALL_NULL.

- [ ] **Step 1: 실패하는 테스트**
  - `expiringCellsKeepTtlAndDeletionTime`: `BufferCell.expiring(col, ts, ttl=3600, nowInSec, value)` 섞인 행, expiring liveness 행.
  - `cellTombstonesRoundTrip`: `BufferCell.tombstone(col, ts, nowInSec)` 셀이 있는 행(INSERT ... null이 만드는 모양).
  - `expiredCellsRoundTrip`: TTL이 이미 지난 셀(`localDeletionTime` 과거) — 원래 행과 equals, `cell.isLive(now)` 결과도 같음.
- [ ] **Step 2: 실패 확인** — Expected: 세 테스트 FAIL.
- [ ] **Step 3: 구현** — 위 Interfaces대로. 디코드: `del`이면 `BufferCell.tombstone`, `ttl`이 있으면 `BufferCell.expiring`의
  원값 생성자(`new BufferCell(col, ts, ttl, ldt, value, null)`), 아니면 `live`.
- [ ] **Step 4: 통과 확인**
- [ ] **Step 5: 커밋** — `[기능] tscol P0: TTL·만료·셀 톰스톤 보존`

### Task 4: 인코딩 불가 판정, 결정성, 무작위 속성 테스트

**Files:**
- Modify: `src/java/org/apache/cassandra/db/timeseries/RowBlockCodec.java`
- Test: `test/unit/org/apache/cassandra/db/timeseries/RowBlockCodecTest.java`

**Interfaces:**
- Consumes: Task 1~3의 `encode`/`decode`/`supports`.
- Produces: `encode`가 `null`을 돌려주는 조건 확정 — 행 삭제(`!row.deletion().isLive()`), 복합 삭제
  (`row.hasComplexDeletion()`), 비-frozen 복합 셀, 카운터 셀, `ColumnarChunkCodec.MAX_ROWS` 초과, 같은 ms 클러스터링 중복.

- [ ] **Step 1: 실패하는 테스트**
  - `unencodableRowsReturnNull`: 위 조건마다 한 행씩 섞은 입력 → `assertNull(encode(...))`.
  - `encodingIsDeterministic`: 같은 입력 두 번 → 바이트 동일; 입력 리스트 복사본으로도 동일.
  - `randomRowsRoundTrip` (quicktheories, 1,000 예): 행 수 1~2,000, 컬럼 타입 6종, 각 셀이 live/null/expiring/expired/tombstone
    중 하나, 셀 writetime = 행 ts + {0, 소수 랜덤}, liveness 있음/없음/expiring, ASC/DESC. 성질: `decode(encode(rows)) == rows`.
- [ ] **Step 2: 실패 확인** — Expected: `unencodableRowsReturnNull` FAIL(예외 또는 잘못된 인코딩). 속성 테스트가 반례를 내면
  그 반례를 고정 테스트로 추가하고 Task 1~3으로 돌아가 고친다.
- [ ] **Step 3: 구현** — 판정 추가.
- [ ] **Step 4: 통과 확인** — 전체 클래스 PASS. 그리고 `.build/sh/ai-in-container '.build/sh/ai-build'` 후 checkstyle 통과.
- [ ] **Step 5: 커밋** — `[기능] tscol P0: 인코딩 불가 판정·결정성·무작위 왕복 속성 테스트`

### Task 5: 크기 기준선 기록

**Files:**
- Modify: `docs/superpowers/specs/2026-11-04-columnar-frozen-sstable-design.md` (§4.2 아래 "P0 실측" 표)
- Test: `test/unit/org/apache/cassandra/db/timeseries/RowBlockCodecTest.java`

- [ ] **Step 1: 테스트** — `tagPointShapeOverheadIsSmall`: `tm_tag_point` 모양(값 컬럼 12개, 15분 900행, 같은 writetime, TTL
  없음)에서 `encode` 크기 ≤ 값만 인코딩한 v4 크기 × 1.05.
- [ ] **Step 2: 실행** — PASS가 아니면 예약 컬럼 인코딩(기준값 선택, delta 방식)을 조정. 기준을 바꾸지 않는다.
- [ ] **Step 3: 실측 기록** — 900행·86,400행 두 크기에서 값-only v4 대비 바이트 비율을 스펙 §4.2 아래 표로.
- [ ] **Step 4: 커밋** — `[문서] tscol P0: 행 블록 코덱 크기 실측`
