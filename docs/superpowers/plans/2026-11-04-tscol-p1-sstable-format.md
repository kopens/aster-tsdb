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

# tscol P1 — 컬럼형 SSTable 형식 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** BTI에서 파생한 세 번째 SSTable 형식 `tscol`을 만든다. 파티션 본문을 `RowBlockCodec` 블록으로 쓰고 읽으며, 같은
데이터를 BTI로 쓴 SSTable과 **모든 읽기 결과가 같다.**

**Architecture:** `TscolFormat extends BtiFormat`(이름만 다름 → BTI 컴포넌트·파티션 인덱스·블룸·통계·TOC 재사용). 쓰기는
`SortedTableWriter.append`(최종 메서드)가 통계·블룸·인덱스·SAI 옵저버를 행마다 그대로 돌리고, 본문만 `TscolPartitionWriter`가
버퍼링해 블록으로 쓴다. 읽기는 `TscolTableReader extends BtiTableReader`가 `rowIterator`를 가로채 블록을 디코드하고, 순차 읽기
(컴팩션·리페어·스크럽·검증·인덱스 빌드)는 `SSTableIdentityIterator.create`의 형식 분기로 같은 디코더를 쓴다.

**Tech Stack:** Java 21, Cassandra 6.0 `io.sstable.format.bti`, P0의 `org.apache.cassandra.db.timeseries.RowBlockCodec`.

**Spec:** [2026-11-04-columnar-frozen-sstable-design.md](../specs/2026-11-04-columnar-frozen-sstable-design.md) §4.1, §4.4, §4.6.
코드 지도: 이 계획의 근거가 된 탐색 보고(파일:줄)는 각 Task의 Files에 그대로 옮겼다.

## Global Constraints

- P0 계획의 Global Constraints 전부.
- 새 클래스는 `org.apache.cassandra.io.sstable.format.bti` 패키지에 둔다(`BtiVersion` 등 package-private 재사용). 이름은 `Tscol*`.
- 업스트림 파일 수정은 최소: `BtiFormat`(이름 받는 protected 생성자), `BtiTableReader`/`BtiTableWriter`(서브클래스가 자기 Builder를
  쓰게 하는 protected 훅), `SSTableIdentityIterator`(형식 분기), `CursorCompactor`(형식 검사), 서비스 파일 한 줄. 각 수정 지점에
  `// aster-tsdb (tscol):` 주석 — upstream-merge 스킬의 충돌 목록에도 추가.
- `selected_format`(기본 쓰기 형식)은 건드리지 않는다. `tscol`은 명시적으로 고른 Descriptor로만 쓰인다.

## 본문 레이아웃 (모든 Task가 따르는 규범)

파티션 = BTI와 같은 `key`, `partition deletion`, `static row`(있으면) **다음에** 1바이트 `kind`:
- `kind = 0` (ROWS): 이어서 업스트림 `UnfilteredSerializer` 행들 + end-of-partition — BTI 본문 그대로. 코덱이 거절한 파티션.
- `kind = 1` (COLUMNAR): `vint blockCount`, 블록마다 `long minTs, long maxTs, vint rowCount, vint length`(디렉터리), 이어서 블록 바이트
  (`RowBlockCodec.encode` 결과, 블록 순서 = 오름차순 시간). 블록은 시간 폭 `BLOCK_MILLIS = 900_000`(15분) epoch 정렬로 자른다.
- 행 인덱스(Rows.db) 항목은 쓰지 않는다(`finish()`가 -1 → BTI의 "unindexed" 파티션). 디렉터리가 그 역할.

## Review Focus

1. **static 행만 있는 파티션 / 행이 0개인 파티션** — kind를 쓰되 블록 0개. 읽기가 static을 돌려줘야 한다. → Task 3·5.
2. **범위 톰스톤이 섞인 파티션** — 코덱은 Row만 받으므로 ROWS로 폴백, 읽기 결과가 BTI와 같아야 한다. → Task 2·5.
3. **블록 경계에 걸친 슬라이스, 역순, 여러 슬라이스, `ts IN (...)`** — 디렉터리 가지치기 후에도 행 누락·중복 없음. → Task 3·5.
4. **컬럼 투영 + `writetime()`/`ttl()`** — 투영한 컬럼만 디코드해도 셀 메타데이터가 정확. → Task 5.
5. **컴팩션 입력으로서의 tscol** — 다음 컴팩션(재동결)이 tscol을 읽어 다시 쓸 때 데이터 동일(순차 읽기 경로). → Task 4·5.

---

### Task 1: `TscolFormat` 등록

**Files:**
- Modify: `src/java/org/apache/cassandra/io/sstable/format/bti/BtiFormat.java:115-118` (protected `BtiFormat(String name, Map<String,String> options)`)
- Create: `src/java/org/apache/cassandra/io/sstable/format/bti/TscolFormat.java` (`NAME = "tscol"`, 내부 `TscolFormatFactory`, writer/reader 팩토리는 Task 2·3 클래스로)
- Modify: `src/resources/META-INF/services/org.apache.cassandra.io.sstable.format.SSTableFormat$Factory` (한 줄 추가)
- Test: `test/unit/org/apache/cassandra/io/sstable/format/bti/TscolFormatTest.java`

**Interfaces:**
- Produces: `TscolFormat.NAME`, `static TscolFormat TscolFormat.instance()` (= `DatabaseDescriptor.getSSTableFormats().get("tscol")`).

- [ ] **Step 1: 실패 테스트** — `formatIsRegisteredByName`: `DatabaseDescriptor.getSSTableFormats().get("tscol")`가 `TscolFormat`;
  `Descriptor.fromFile(...-tscol-Data.db)`의 `getFormat()`이 같은 인스턴스; `BtiFormat.isSelected()`가 기본 설정에서 tscol 때문에 바뀌지 않음.
- [ ] **Step 2: 실패 확인** — `.build/sh/ai-in-container '.build/sh/ai-ci-test org.apache.cassandra.io.sstable.format.bti.TscolFormatTest'` → 클래스 없음.
- [ ] **Step 3: 구현** — 위 Files대로. 팩토리는 Task 2·3 전까지 BTI 것을 그대로 반환해도 된다(이 Task는 등록만).
- [ ] **Step 4: 통과 확인**
- [ ] **Step 5: 커밋** — `[기능] tscol P1: SSTable 형식 등록`

### Task 2: 쓰기 — `TscolPartitionWriter`, `TscolTableWriter`

**Files:**
- Create: `src/java/org/apache/cassandra/io/sstable/format/bti/TscolPartitionWriter.java` (`BtiFormatPartitionWriter` 상속: `start`/`addStaticRow`는 부모 그대로 +
  `addUnfiltered`는 버퍼, `RangeTombstoneMarker`가 오면 폴백 표시; `finish`가 kind와 본문을 씀; `reset`/`close`)
- Create: `src/java/org/apache/cassandra/io/sstable/format/bti/TscolTableWriter.java` (`BtiTableWriter` 상속, Builder의 `openPartitionWriter`(BtiTableWriter:357-370)와 `openInternal`(:93)이 Tscol 것을 씀)
- Modify: `BtiTableWriter.java` (서브클래스 훅이 필요한 곳만 protected로)
- Test: `test/unit/org/apache/cassandra/io/sstable/format/bti/TscolSSTableTest.java`

**Interfaces:**
- Consumes: `RowBlockCodec.encode(TableMetadata, List<Row>) -> ByteBuffer|null`.
- Produces: 본문 레이아웃(위 규범). `TscolPartitionWriter.BLOCK_MILLIS = 900_000L`.

- [ ] **Step 1: 실패 테스트** (`TscolSSTableTest`, CQLTester) — `writesColumnarBodiesAndFallsBackForTombstones`: 테이블에 3개 파티션(1Hz 2시간치 /
  범위 톰스톤 포함 / static만), BTI로 flush한 SSTable을 테스트 헬퍼 `rewriteAs(cfs, TscolFormat.instance())`(Task 5에서 공용화, 여기서는 `SSTableRewriter`
  + `cfs.newSSTableDescriptor(dir, format)`로 직접)로 다시 씀 → 결과 파일 이름에 `-tscol-`; 원시 바이트에서 파티션별 kind가 각각 1/0/1.
- [ ] **Step 2: 실패 확인**
- [ ] **Step 3: 구현** — 행 버퍼는 블록 경계(epoch 15분)에서 바로 `encode`하고 인코딩 바이트만 보관(메모리 상한). `encode`가 null이면 그 파티션
  전체를 kind 0으로 다시(버퍼한 행을 부모 `addUnfiltered`로 재생). 블록 디렉터리를 먼저 쓰려면 인코딩 바이트를 모아 두었다가 `finish`에서 쓴다.
- [ ] **Step 4: 통과 확인**
- [ ] **Step 5: 커밋** — `[기능] tscol P1: 컬럼형 본문 쓰기와 행 폴백`

### Task 3: 읽기 — `TscolTableReader`, 컬럼형 행 반복자

**Files:**
- Create: `src/java/org/apache/cassandra/io/sstable/format/bti/TscolTableReader.java` (`BtiTableReader` 상속; `rowIterator(key, …)`(:415-426),
  `rowIterator(FileDataInput, …)`(:428-437) 재정의; `Builder.buildInternal`(:604-607), `cloneAndReplace` 두 개(:464-467, :498-504)가 Tscol Builder 사용)
- Create: `src/java/org/apache/cassandra/io/sstable/format/bti/TscolTableReaderLoadingBuilder.java` (`BtiTableReaderLoadingBuilder` 상속)
- Create: `src/java/org/apache/cassandra/io/sstable/format/bti/ColumnarPartitionIterator.java` (`AbstractUnfilteredRowIterator`: 디렉터리 읽기, `Slices`로 블록 가지치기, `RowBlockCodec.decode(…, columns)`, 슬라이스 필터, 역순)
- Modify: `BtiTableReader.java`, `BtiFormat.java` (Builder·팩토리 훅만)
- Test: `TscolSSTableTest`

**Interfaces:**
- Produces: `ColumnarPartitionIterator.open(SSTableReader, FileDataInput, DecoratedKey, DeletionTime, Row staticRow, Slices, ColumnFilter, boolean reversed)`;
  kind 0이면 `null`(호출자가 BTI 반복자로 위임 — 부모 `rowIterator`는 kind 바이트 뒤 위치에서 시작해야 하므로 kind 0 파티션은 버퍼 없이
  `UnfilteredSerializer`로 직접 읽어 슬라이스 필터를 적용하는 단순 반복자를 쓴다; 폴백 파티션은 드물다).

- [ ] **Step 1: 실패 테스트** — `readsMatchRowFormat`: Task 2의 세 파티션에 대해 BTI SSTable과 tscol SSTable에서 각각 같은 `SinglePartitionReadCommand`
  (전체, 슬라이스 [30m,90m), 블록 경계 걸친 [14m,16m), 역순, `ts IN (3개)`, 컬럼 하나 투영)를 `queryMemtableAndDisk` 없이 SSTable 하나만 읽어
  `Unfiltered` 시퀀스가 같음(`assertEquals`로 행 단위 비교).
- [ ] **Step 2: 실패 확인**
- [ ] **Step 3: 구현** — 위 Interfaces대로.
- [ ] **Step 4: 통과 확인**
- [ ] **Step 5: 커밋** — `[기능] tscol P1: 컬럼형 본문 읽기(슬라이스·역순·투영)`

### Task 4: 순차 읽기 경로와 안전 장치

**Files:**
- Modify: `src/java/org/apache/cassandra/io/sstable/SSTableIdentityIterator.java:63,68,91,96,124,129` (리더가 tscol이면 `ColumnarIdentityIterator`)
- Create: `src/java/org/apache/cassandra/io/sstable/format/bti/ColumnarIdentityIterator.java` (key·deletion·static·kind를 읽고 모든 블록을 디코드해
  순서대로 내보냄; kind 0이면 업스트림 직렬화 그대로; `exhaust()` 시 본문 끝까지 소비)
- Modify: `src/java/org/apache/cassandra/db/compaction/CursorCompactor.java:207` (입력·출력 중 하나라도 BIG이 아니면 미지원)
- Modify: `src/java/org/apache/cassandra/db/streaming/CassandraOutgoingFile.java:208-218` 또는 `CassandraStreamWriter` — tscol은 entire-SSTable
  스트리밍이 아니면 명확한 오류(`tscol sstables can only be streamed whole until P3`) — 다른 형식으로 잘못 파싱되는 대신.
- Test: `TscolSSTableTest`

- [ ] **Step 1: 실패 테스트** — `compactionScrubAndVerifyReadColumnarSSTables`: tscol SSTable 둘을 major compaction → 결과 데이터가 입력 합과 같음
  (BTI 기준과 비교); `nodetool scrub`과 `verify`(테스트에서는 `cfs.scrubAndValidate`/`Verifier`)가 오류 0; SAI 인덱스를 만든 테이블에서 인덱스
  재빌드 후 SAI 조회 결과가 BTI와 같음.
- [ ] **Step 2: 실패 확인**
- [ ] **Step 3: 구현**
- [ ] **Step 4: 통과 확인**
- [ ] **Step 5: 커밋** — `[기능] tscol P1: 순차 읽기(컴팩션·스크럽·검증·인덱스 빌드)와 커서 컴팩션·부분 스트리밍 가드`

### Task 5: 차등 하네스 — BTI와 결과가 완전히 같다

**Files:**
- Create: `test/unit/org/apache/cassandra/io/sstable/format/bti/TscolDifferentialTest.java`
- Create: `test/unit/org/apache/cassandra/io/sstable/format/bti/TscolTestUtil.java` (`rewriteAs(ColumnFamilyStore, SSTableFormat<?,?>)`)
- Modify: `.build/sh/ci-timeseries-tests.sh` (TscolFormatTest, TscolSSTableTest, TscolDifferentialTest 추가)

- [ ] **Step 1: 테스트** — 같은 스키마 두 테이블(A: BTI 유지, B: 쓰기 후 `rewriteAs(tscol)`), `tm_tag_point` 모양 + 무작위 데이터(늦은 컬럼 UPDATE, TTL,
  INSERT null, 행 삭제, 범위 삭제, static, DESC 테이블 한 벌). 질의 목록을 둘 다에 실행해 결과가 같음: 전체/슬라이스/역순/LIMIT·페이징(페이지 크기 7)/
  `writetime()`·`ttl()`/`ALLOW FILTERING` 일반 컬럼 필터/`count(*)`·`avg`·`time_bucket`·gap-fill/파티션 키 없는 레인지 스캔/`DISTINCT`/`ts IN`.
  그리고 B에 늦은 쓰기·삭제를 한 뒤 다시 `rewriteAs` → 결과가 A에 같은 쓰기를 한 것과 같음.
- [ ] **Step 2: 실행** — 실패하면 Task 2~4로 돌아가 고치고, 실패를 일으킨 입력을 고정 테스트로 남긴다.
- [ ] **Step 3: ci-local** — `.build/sh/ci-local` 전체 통과(기존 테스트 회귀 없음).
- [ ] **Step 4: 커밋** — `[정리] tscol P1: BTI 대비 차등 하네스`

(블록 건너뛰기 인덱스 — 값 집합·블룸 — 는 P4 성능 계획에서. 이 계획은 블록 디렉터리의 시간 범위 가지치기까지.)
