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

# SP2: 청크 스토어 + 재인코더 + 보존 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 핫 윈도우를 지난 row-per-sample 데이터를 백그라운드에서 Gorilla/Chimp128 청크로 재인코딩해 섀도 테이블에 저장하고 원본을 안전하게 삭제하는 계층화 저장 (스펙 §3.2: docs/superpowers/specs/2026-07-31-industrial-tiered-storage-design.md).

**Architecture:** 노드-로컬 스케줄 서비스(`TieredStorageService`)가 자기 프라이머리 레인지의 태그를 열거하고, 닫힌 창의 로우를 읽어 `ChunkCodecs.encodeSmallest`(auto 정책)로 인코딩, 섀도 테이블 `ks."<table>__chunks"`에 기록 후 **인코딩된 로우들의 max writetime으로 타임스탬프를 고정한 범위 삭제**를 수행한다(그 후 도착한 지각 로우는 툼스톤보다 새 타임스탬프라 생존 → 다음 틱이 청크와 병합·멱등 재인코딩). 모든 재인코더 읽기/쓰기는 `QueryProcessor.process` + CL(기본 LOCAL_QUORUM) — 로컬 전용 `executeInternal`은 다른 레플리카에만 있는 로우를 놓친 채 분산 삭제해 데이터를 잃을 수 있으므로 금지. 정책은 테이블 `extensions['timeseries_tiering']`(JSON) — CQL로 설정 가능하도록 `TableAttributes`에 4줄 패치.

**Tech Stack:** Java 21, TCM 스키마 API(`Schema.instance.submit`), picocli nodetool, AbstractVirtualTable. 의존성 추가 없음.

## Global Constraints

- 대상 스키마는 **정준형만** (사용자 확정): 파티션 키 1개 + `timestamp` 클러스터링 1개 + `double` 값 컬럼 1개. 그 외 스키마에 정책이 설정되면 명확한 에러 로그 + 스킵 (조용한 오동작 금지).
- **삭제 타임스탬프 불변식**: 원본 로우 범위 삭제는 반드시 `USING TIMESTAMP <이번에 인코딩한 로우들의 max writetime>` — 지각 로우 생존 보장. 이 불변식을 깨는 코드는 어떤 리뷰도 통과할 수 없다.
- 재인코더의 원본 읽기·청크 쓰기·삭제는 전부 `QueryProcessor.process(query, consistencyLevel, values...)` 경유 (기본 `LOCAL_QUORUM`, 정책으로 조정). `executeInternal` 계열은 재인코더 데이터 경로에 금지 (스키마 확인 등 메타 작업은 허용).
- 스키마 변경은 `Schema.instance.submit(SchemaTransformations.addTable(meta, true))` (멱등, CMS 직렬화) — CQLSSTableWriter:715 선례.
- 업스트림 파일 수정 허용 범위: `TableAttributes.java`(extensions 디코딩 ~4줄), `SystemViewsKeyspace.java`(가상 테이블 1줄), `NodetoolCommand.java`(서브커맨드 등록), `CassandraDaemon.java`(셋업 1줄) — 각각 최소 diff, CLAUDE.md 충돌 목록에 추가.
- 컴파일/테스트/트레일러/ai-build 규칙은 이전 계획들과 동일 (`--reuse`는 컴파일 안 함).
- 신규 패키지: `org.apache.cassandra.db.timeseries.tiering`.

## File Structure

- Modify: `src/java/org/apache/cassandra/db/timeseries/GorillaCodec.java` (SampleCursor를 최상위로 추출, 중첩은 extends 유지)
- Create: `src/java/org/apache/cassandra/db/timeseries/SampleCursor.java`
- Modify: `src/java/org/apache/cassandra/db/timeseries/ChunkCodecs.java` (+HEADER_SIZE/MAX_SAMPLES/codecOf/encodeSmallest)
- Modify: `src/java/org/apache/cassandra/cql3/statements/schema/TableAttributes.java` (extensions 디코딩)
- Create: `src/java/org/apache/cassandra/db/timeseries/tiering/TieringPolicy.java` (JSON 파싱/검증)
- Create: `src/java/org/apache/cassandra/db/timeseries/tiering/ChunkTables.java` (섀도 테이블 메타 생성/보장/조회)
- Create: `src/java/org/apache/cassandra/db/timeseries/tiering/TieredStorageService.java` (스캔/인코드/쓰기/삭제/병합 사이클 + 스케줄)
- Create: `src/java/org/apache/cassandra/db/virtual/TimeseriesTieringTable.java` (system_views.timeseries_tiering)
- Create: `src/java/org/apache/cassandra/tools/nodetool/TieringStatus.java`, `Retier.java`
- Tests: `test/unit/org/apache/cassandra/db/timeseries/tiering/{TieringPolicyTest,ChunkTablesTest,TieredStorageServiceTest}.java` (+ ChunkCodecsTest 확장)

## 섀도 테이블 스키마 (규범)

```sql
CREATE TABLE ks."<base>__chunks" (
    tag        <base 파티션 키 타입>,
    window_start timestamp,
    codec      tinyint,        -- 버전 바이트 사본 (진단용; 디코드는 payload가 자체 식별)
    samples    int,
    max_row_writetime bigint,  -- 이 청크에 인코딩된 로우들의 max writetime (병합·삭제 멱등성의 기준)
    payload    blob,
    PRIMARY KEY (tag, window_start)
) WITH compaction = {'class':'UnifiedCompactionStrategy','scaling_parameters':'T4'}
```

## 정책 JSON (extensions['timeseries_tiering'])

```json
{"hot_window":"7d", "chunk_window":"1h", "cold_window":"365d",
 "consistency":"LOCAL_QUORUM", "interval":"5m"}
```
- `hot_window`: 이 시간 안의 로우는 건드리지 않음. `chunk_window`: 청크 창 폭(고정 길이, TSCS `window_size`와 정렬 권장). `cold_window`: 선택 — 지나면 청크 파티션 범위 삭제(윈도우 통째). `interval`: 서비스 틱 주기. (계획 당시 있던 `codec` 필드는 SP4 Task 1.5에서 제거됐다 — Chimp128이 유일한 코덱이며, 정책에 `codec`이 남아 있으면 파서가 거부한다.)
- 파서는 TSCS의 duration 문법(`<int><m|h|d>`) 재사용 수준으로 자체 구현, 검증: `hot_window >= chunk_window`, `cold_window > hot_window`(설정 시), 미지 키 거부.

## 재인코딩 사이클 (규범 알고리즘 — TieredStorageService.runOnce)

```
for each (ks, table) with tiering policy:
  ensure chunk table exists (Schema.submit addTable ignoreIfExists)
  cutoff = now - hot_window (창 경계로 내림)
  for each local primary token range R:
    tags = SELECT DISTINCT tag FROM base WHERE token(tag) > ? AND token(tag) <= ?   (paged, CL)
    for each tag:
      rows = SELECT ts, value, WRITETIME(value) FROM base
             WHERE tag=? AND ts < cutoff ORDER BY ts ASC                             (paged, CL)
      group rows by window_start = floor(ts / chunk_window)
      for each complete window W (창 전체가 cutoff 이전):
        existing = SELECT payload, max_row_writetime FROM chunks WHERE tag=? AND window_start=W
        if existing != null:  # 지각 병합 경로
          merged = decode(existing.payload) ∪ rows(W)   # ts 충돌 시 로우(새 쓰기) 우선
        else: merged = rows(W)
        payload = encodeSmallest(codec policy, merged)   # auto면 양쪽 인코딩 후 작은 쪽
        maxWt = max(existing.max_row_writetime, max(writetime of rows(W)))
        INSERT INTO chunks (tag, window_start, codec, samples, max_row_writetime, payload)
               VALUES (...) USING TIMESTAMP maxWt+1                                  (CL)
        DELETE FROM base USING TIMESTAMP maxWt
               WHERE tag=? AND ts >= W AND ts < W+chunk_window                        (CL)
      # cold 만료
      if cold_window set:
        DELETE FROM chunks WHERE tag=? AND window_start < now - cold_window          (CL, 일반 타임스탬프)
```
- 멱등성: 단계 어디서 끊겨도 재실행 수렴 — 청크 쓰고 삭제 전 죽으면 다음 틱에 같은 로우가 다시 병합(동일 결과), 삭제 후엔 로우가 없어 no-op.
- INSERT의 `USING TIMESTAMP maxWt+1`: 같은 창의 재병합 청크가 항상 이전 청크를 이기도록.
- 인코더 스크래치: 서비스 스레드당 재사용(Chimp 64KB 할당 완화 — 인계 노트).

---

### Task 0: SampleCursor 추출 + ChunkCodecs 보강 (+ encodeSmallest)

**Files:** Create `SampleCursor.java`; Modify `GorillaCodec.java`(중첩 인터페이스 → `extends SampleCursor`로 축소, 기존 참조 소스 호환), `Chimp128Codec.java`(반환 타입을 `SampleCursor`로), `ChunkCodecs.java`; Test `ChunkCodecsTest.java` 확장

**Interfaces (Produces):**
- `public interface SampleCursor { boolean advance(); long timestamp(); double value(); }` (db.timeseries 최상위)
- `ChunkCodecs`: `public static final int HEADER_SIZE = 21; public static final int MAX_SAMPLES = ...;`
  `public static Codec codecOf(ByteBuffer payload)` (버전 바이트 → enum, 미지 IAE)
  `public static ByteBuffer encodeSmallest(long[] ts, double[] values, int count)` (양쪽 인코딩, `remaining()` 작은 쪽; 동률 시 GORILLA)
  `public static ByteBuffer encode(Codec, ...)` 기존 유지
- 테스트: codecOf 라운드트립, encodeSmallest가 상수 데이터→GORILLA / 양자화 워크→CHIMP128 페이로드를 실제로 선택하는지(버전 바이트로 단언), 기존 전 스위트 회귀(BitStream/Gorilla/Chimp128/ChunkCodecs 전부 그린).

- [ ] Step 1 테스트 추가 → Step 2 컴파일 실패 확인 → Step 3 구현 → Step 4 전 코덱 테스트 그린 → Step 5 커밋 `Extract SampleCursor and add codec auto-selection (encodeSmallest)`

### Task 1: TableAttributes extensions 패치 + TieringPolicy

**Files:** Modify `TableAttributes.java`; Create `TieringPolicy.java`; Tests `TieringPolicyTest.java` + `SchemaKeyspaceTest` 스타일 CQL 확인 1건(CQLTester: `ALTER TABLE %s WITH extensions = {'timeseries_tiering': 0x...}` 후 `cfs.metadata().params.extensions` 반영 단언)

**Interfaces (Produces):**
- `TableAttributes.build()`가 `EXTENSIONS` 옵션을 디코딩: 맵 값의 `"0x..."` hex 문자열 → `ByteBuffer` (`ByteBufferUtil.hexToBytes`), 형식 오류 시 `InvalidRequestException`. (~4줄 + validKeywords는 이미 포함)
- `TieringPolicy`: `public static TieringPolicy fromTable(TableMetadata)` (extensions 키 `timeseries_tiering` 없으면 null; JSON 파싱은 `JsonUtils.fromJsonMap`), 필드 `hotWindowMillis/chunkWindowMillis/coldWindowMillis(-1)/codec(auto|gorilla|chimp128)/consistencyLevel/intervalMillis`, `long windowStartFor(long tsMillis)`, `static void validate(String json)` (미지 키·규칙 위반 → `ConfigurationException`), 정준 스키마 검증 `static String canonicalSchemaError(TableMetadata)` (null=적합).
- 테스트: 파싱/기본값/검증 거부(미지 키, hot<chunk, cold<=hot, 잘못된 CL/코덱/duration), CQL 경유 extensions 왕복, 비정준 스키마 에러 문자열.

- [ ] TDD 사이클 → 커밋 `Make table extensions settable via CQL; add tiering policy parsing`

### Task 2: ChunkTables + 재인코딩 사이클 코어 (CQLTester 통합)

**Files:** Create `ChunkTables.java`, `TieredStorageService.java`(스케줄 없이 `runOnce(keyspace, table, nowMillis)` 코어만); Test `TieredStorageServiceTest.java` (CQLTester 기반 — 실제 스키마·실제 쿼리, 단일 노드)

**Interfaces (Produces):**
- `ChunkTables.chunkTableName(String baseTable)` = `baseTable + "__chunks"`; `ensureChunkTable(TableMetadata base)` — 규범 스키마로 `Schema.instance.submit(addTable(..., true))`; `chunkTableMetadata(TableMetadata base)`.
- `TieredStorageService.runOnce(String ks, String table, long nowMillis)` — 규범 사이클 구현(위 의사코드), 반환 `TierRunStats {windowsEncoded, rowsEncoded, lateMerges, chunksExpired, bytesWritten}` (가상 테이블·테스트용). 모든 데이터 경로 `QueryProcessor.process(..., policy CL)`, 삭제 타임스탬프 불변식 준수.
- 핵심 테스트 (CQLTester, 정준 스키마):
  1. `encodeClosedWindowsAndDeleteRows` — 3태그×3창 로우 삽입(과거 ts, 명시 `USING TIMESTAMP`), runOnce 후: 청크 행 존재(samples/codec 정확), 원본의 해당 창 로우 0, 핫 창 로우 보존.
  2. `roundtripThroughChunks` — 청크 payload를 `ChunkCodecs.cursor`로 디코드해 원본 (ts,value) 완전 일치.
  3. `deleteTimestampPreservesLateRows` — runOnce 후 **더 새로운 writetime**으로 같은 창에 지각 로우 삽입 → 로우 생존 단언 → 두 번째 runOnce → 청크에 병합(samples+1, 디코드로 값 확인)되고 로우 삭제.
  4. `idempotentWhenInterrupted` — 청크만 쓰고 삭제는 안 된 상태를 인위 구성(직접 INSERT로 청크 선기록) 후 runOnce → 수렴(중복 없음, 로우 우선 병합).
  5. `coldWindowExpiry` — cold_window 지난 청크 삭제.
  6. `nonCanonicalSchemaSkipsWithError` — 컬럼 2개 테이블에 정책 설정 → 스킵 + 에러 로그(스탯 0).
  7. `autoCodecPicksPerWindow` — 상수 태그와 양자화 태그가 각각 GORILLA/CHIMP128 버전 바이트를 얻는지.

- [ ] TDD 사이클 → 커밋 `Add tiered storage core: chunk tables and the re-encode cycle`

### Task 3: 스케줄러 + 데몬 훅 + 가상 테이블 + nodetool

**Files:** Modify `TieredStorageService.java`(+`instance` 싱글턴, `setup()` — 정책 있는 테이블 스캔·`ScheduledExecutors.optionalTasks.scheduleWithFixedDelay` per interval, 동시 실행 1 가드), `CassandraDaemon.java`(setup 말미 1줄 `TieredStorageService.instance.setup()`), Create `TimeseriesTieringTable.java`(+`SystemViewsKeyspace.java` 1줄 — 컬럼: keyspace, table, hot/chunk/cold window, codec, last_run, windows_encoded, rows_encoded, late_merges, chunks_expired), Create `TieringStatus.java`/`Retier.java`(+`NodetoolCommand.java` 등록; Retier는 `<ks> <table>` 파라미터로 JMX 경유 즉시 runOnce — MBean은 `TieredStorageServiceMBean` 신규, HintsService:178 패턴) ; Tests: 가상 테이블(CQLTester `SELECT * FROM system_views.timeseries_tiering`), nodetool은 AutoRepairStatusTest 패턴(Mockito NodeProbe)

- [ ] TDD 사이클 → 커밋 `Schedule tiered storage, expose status via virtual table and nodetool`

### Task 4: E2E·수렴·문서·CI·푸시

**Files:** docker/integration-test.sh(계층화 섹션: 정책 설정→과거 데이터 삽입→retier→청크 확인→지각 삽입→retier→병합 확인), doc/timeseries/tiered-storage.md(사용법: 정책 설정 CQL, 명시 청크 조회 패턴 — SP3 전까지, 운영: nodetool/가상 테이블, 불변식 설명), README 표 갱신, CHANGES.txt, CLAUDE.md 충돌 목록(4개 업스트림 파일), .gitlab-ci.yml(신규 테스트 3클래스), `.build/sh/ai-build` → 푸시

- [ ] 사이클 → 커밋 2개(테스트·통합 / 문서·CI) → `git push origin master && git push origin master:6.0.0`
