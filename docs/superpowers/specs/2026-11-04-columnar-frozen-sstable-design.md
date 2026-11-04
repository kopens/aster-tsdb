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

# 계층형 저장 재설계 — 동결된 창을 컬럼형 SSTable로 (`tscol`)

2026-11-04. 상태: **승인(사용자: "제대로 된 방향으로", "이전 버전 호환 안 돼도 된다", 기존 데이터는 일회성 이전 도구로).** 대체 대상: [2026-07-31-industrial-tiered-storage-design.md](2026-07-31-industrial-tiered-storage-design.md)의
"청크 스토어 + 재인코더 + 투명 읽기"(그림자 테이블 방식). 근거가 된 버그 이력은
[production-rollout.md](../../../doc/timeseries/production-rollout.md) §3과 r7~r9 커밋.

## 0. 한 줄 요약

청크를 **별도 테이블의 행**이 아니라 **같은 테이블의 SSTable 형식**으로 만든다. TSCS가 닫힌 창을 SSTable 하나로
동결(freeze)할 때 그 출력을 새 SSTable 형식 `tscol`(청크 코덱 기반 컬럼형)로 쓴다. 각 노드가 자기 파일을 표준
컴팩션 트랜잭션으로 원자적으로 바꾸므로 **톰스톤도, 복제본 간 조율도, 읽기 병합 훅도 없다.** 읽기·쓰기·리페어·
스트리밍은 지금의 SSTable 경로를 그대로 탄다.

## 1. 왜 다시 설계하는가

현재 방식: 재인코더가 `hot_window`보다 오래된 행을 읽어 `<table>__chunks`에 청크로 쓰고, 원본 행을
`USING TIMESTAMP max_row_writetime` 범위 톰스톤으로 지운다. 읽기는 `DataResolver`/`DigestResolver`에서 핫 행과
디코드한 청크 행을 합친다.

r7~r9에서 고친(또는 막은) 결함은 거의 전부 이 구조 자체에서 나왔다.

| 결함 계열 | 원인 (구조) |
| --- | --- |
| 늦게 반영된 쓰기가 범위 삭제에 가려져 사라짐 (r9) | 원본을 **톰스톤으로** 지움 — 톰스톤보다 오래된 writetime의 쓰기는 영구히 가려짐 |
| 과거 `USING TIMESTAMP` 쓰기 유실 → 거부 가드 (r4) | 같음 |
| 짧은 TTL 쓰기 후 옛 청크 값 부활 (r8) | 청크가 TTL을 모름, 병합이 "청크 = 바닥값" |
| `INSERT ... null`이 콜드 값을 못 지움 (r9 문서화) | 재인코더가 셀 톰스톤을 읽지 못함 (CQL로 읽음) |
| 삭제는 전부 거부 (§5.1.2) | 톰스톤이 청크를 gc_grace까지만 가림 |
| 일반 컬럼 필터 조회가 청크 행을 거르지 못함 → 거부 (r8) | 필터는 복제본에서, 청크 병합은 코디네이터에서 — 순서가 거꾸로 |
| `chunk_window` 변경 시 청크 겹침 → 거부 (r9) | 청크를 `window_start` 정확 일치로만 찾음, 폭이 청크에 없음 |
| RF>1 콜드 플러시가 옛 값으로 덮어씀 → RF 1만 (r9) | 청크는 복제되는 행인데 원본 제거는 노드 로컬 |
| 레인지 스캔 병합·거부, 커버리지 원장, 콜드 경계, CL 하한 … | 병합이 코디네이터 수준에 얹혀 있음 |
| 근사 `writetime()` | 청크에 셀별 타임스탬프가 없음 |

가드를 계속 덧대면 막을 수는 있지만, 하나 막을 때마다 거부되는 쓰기·조회가 늘고 엔진에 얹힌 특수 경로도
늘어난다. ClickHouse(MergeTree 파트), InfluxDB 3(Parquet), TimescaleDB(청크 단위 압축 교체)는 공통적으로
**압축본을 다 만든 뒤 원본을 파일·청크 단위로 원자적으로 교체**하고, 원본을 삭제 표시로 지우지 않는다.
Cassandra에서 그에 해당하는 단위가 SSTable이고, 원자적 교체 수단이 컴팩션 트랜잭션(`LifecycleTransaction`)이다.

## 2. 목표 · 비목표 · 반목표

**목표**
1. 동결된 창의 저장 크기와 집계 속도가 현재 계층화 이상 (234 기준 디스크 7.1× 절감, 집계 3~6×, `tiering-benchmark.md`).
2. 동결된 데이터에도 **보통 Cassandra 의미가 그대로**: 늦은 쓰기, UPDATE, DELETE, TTL, `writetime()`/`ttl()` 정확값,
   일반 컬럼 필터, SAI, 레인지 스캔, 페이징, LWT — 거부되는 연산이 없어야 한다.
3. 복제·리페어·스트리밍·다이제스트가 형식과 무관하게 맞는다(노드마다 형식이 달라도 같은 답).
4. **이전 버전 호환은 목표가 아니다(사용자 결정 2026-11-04).** 그림자 테이블 계층화(재인코더, 투명 읽기, 가드,
   커버리지 원장, `timeseries_tiering` 정책)는 `tscol` 릴리즈에서 엔진에서 통째로 지운다. 단 데이터 손실은 없어야 하므로
   기존 `__chunks` 데이터는 **일회성 이전 도구**로 옮긴다(4.8).
5. 업스트림 머지 비용이 작다: 업스트림 파일 수정은 형식 등록과 TSCS 출력 형식 선택 정도로 한정.

**비목표**
- 오브젝트 스토리지(S3) 계층, 원격 콜드 계층. (폐쇄망 우선. 형식이 생기면 나중에 얹을 수 있음.)
- 동결되지 않은(활성) 창의 컬럼형 저장 — 그건 `TimeSeriesMemtable`과 일반 BTI의 몫.
- 다운샘플링/연속 집계 — 별도 설계(`continuous-aggregates-design.md`).

**반목표 (기술적으로 돌아가도 실패)**
- 디코드 결과가 원래 행과 **비트 단위로 같지 않은** 것(타임스탬프·TTL·삭제 정보 손실). 리페어가 끝없이 돌고,
  조정(reconciliation)이 틀어진다.
- 동결된 데이터에 대한 쓰기·조회를 다시 거부하는 것.
- 노드 간 조율이나 코디네이터 수준 병합 훅이 다시 필요해지는 것.
- 읽지 못하는 형식으로 다운그레이드가 막히는데 되돌릴 길이 없는 것.

## 3. 다른 제품에서 가져오는 것 (2026-11-04 조사)

조사한 제품 중 **원본을 삭제 표시로 지우는 방식은 없었다.** 전부 변환 단위를 제자리에서 바꾸거나(TimescaleDB,
QuestDB) 불변 파일을 메타데이터 아래에서 바꿔 끼우고(ClickHouse, InfluxDB 3, Druid, VictoriaMetrics, Prometheus 블록),
**복제본마다 물리적으로** 계층화한다(ClickHouse "이동은 복제를 방해하지 않는다", DSE 계층 저장, Scylla S3, Druid/Pinot
세그먼트 사본). 우리처럼 압축본을 쿼럼 CL로 **논리적으로** 복제되는 별도 테이블에 쓰는 곳은 없다.

| 제품 | 단위·위치 | 늦은 쓰기 | 압축본 수정·삭제 | 간격 변경 | 조회 |
| --- | --- | --- | --- | --- | --- |
| TimescaleDB | 청크 통째 제자리 변환 (클라우드: S3) | 옆 행 저장소 → 재압축 | 배치 해제·재압축 (S3 계층은 거부) | 새 청크만 | 투명, segmentby/orderby 필터 |
| InfluxDB 3 | 일 파티션 Parquet, 오브젝트 스토리지 | 새 파일, 조회 시 중복 제거 | 덮어쓰기, 삭제는 컴팩션 때(≤24h) | 불가 | 투명 |
| QuestDB | 파티션 통째 Parquet 변환 | 파티션 재작성 | UPDATE 불가 | 문서 없음 | 투명 |
| ClickHouse | 불변 파트, TTL 이동·재압축 | 새 파트 | 행 마스크 → 병합 때 제거 | TTL은 재적용 | 투명 |
| IoTDB | 시간 파티션별 TsFile, S3까지 계층 | unsequence 공간 → 교차 컴팩션 | `.mods` 범위 기록 → 컴팩션 때 | 첫 기동 때만 | 투명 |
| TDengine | DURATION별 파일 세트, 계층·S3 | STT 파일 → COMPACT | COMPACT 때 제거 | 불가 | 투명 |
| Druid | 시간 청크별 버전 세그먼트 | 새 세그먼트 | 구간 덮어쓰기(원자 교체) | 재색인 | 투명 |

우리 설계에 가져온 것:
- **원자적 교체** (ClickHouse 파트, Druid 버전 세그먼트 "시간 청크 단위로 매끄럽게 넘어감", Kafka 복사 후 메타 커밋)
  → 동결 컴팩션 트랜잭션으로 SSTable 교체 (4.3).
- **늦은 데이터는 옆 영역에 두고 나중에 합친다** (TimescaleDB 행 저장소, IoTDB unsequence, TDengine STT, Mimir OOO 헤드)
  → 늦은 행은 새 SSTable, TSCS 재동결이 합침. 경합 대상이 사라진다 (4.3).
- **간격은 단위마다 고정** (TimescaleDB "새 청크에만", IoTDB·TDengine·InfluxDB "변경 불가")
  → 블록 폭을 SSTable 메타데이터에 기록 (4.1).
- **삭제는 기록해 두고 재작성 때 반영** (IoTDB `.mods`, ClickHouse 행 마스크, InfluxDB 삭제 요청)
  → Cassandra 톰스톤이 그 역할, 재동결 때 gc_grace 규칙으로 정리 (4.5).
- **보존은 단위 통째로** (TimescaleDB `drop_chunks`, QuestDB·VictoriaMetrics 파티션 삭제, ClickHouse `ttl_only_drop_parts`)
  → TSCS retention이 이미 창 통째로 버린다. `cold_window`는 은퇴.
- **필터는 병합 뒤, 블록 메타데이터로 건너뛰기** (TimescaleDB 배치별 minmax·bloom)
  → 복제본 로컬 읽기 경로가 이미 "병합 → RowFilter", 블록 디렉터리에 컬럼별 min/max (4.1, 4.4).
- 경고로 받은 것: 늦은 쓰기가 계속 들어오는 압축 단위의 **재압축 반복**(TimescaleDB #3230) → 재동결 빈도 지표와
  속도 제한이 필요 (6. 리스크).

출처: tigerdata.com/docs (hypercore, data-tiering), github.com/timescale/timescaledb/issues/3230,
docs.influxdata.com/influxdb3 (storage-engine, delete-data, custom-partitions), questdb.com/docs (parquet, storage-policy,
out-of-order-data), clickhouse.com/docs (mergetree, ttl, lightweight-delete), iotdb.apache.org (Tiered Storage, Delete),
docs.tdengine.com (multi-tier), docs.victoriametrics.com, grafana.com/docs/mimir (out-of-order), thanos.io (compact),
druid.apache.org/docs (update, rules), docs.pinot.apache.org (tiered-storage), docs.datastax.com (DSE tiered storage),
scylladb.com (2026.3). 확인 못 한 것: HCD/Astra 컬럼형 계층, TDengine S3 이동 데이터의 쓰기·삭제.

## 4. 설계

### 4.1 형식 `tscol` — BTI에서 파생한 세 번째 SSTable 형식

- `SSTableFormat.Factory`로 등록한다(`META-INF/services/...SSTableFormat$Factory`에 한 줄, 업스트림 BIG/BTI와 같은 방식).
  파일 이름의 형식 이름이 `tscol`이라 한 테이블 안에 BIG/BTI/`tscol` SSTable이 섞여 있어도 각자 자기 리더로 읽힌다.
- **재사용:** BTI의 파티션 인덱스(`Partitions.db`, 트라이), 블룸 필터, `Statistics.db`, TOC, Digest, 압축 정보,
  스크럽/검증 골격. **새로 쓰는 것:** `Data.db`의 파티션 본문 레이아웃과, BTI의 `Rows.db`(행 인덱스) 대신 쓰는
  파티션별 블록 디렉터리.
- **파티션 본문:** 파티션 헤더(키, 파티션 수준 삭제 정보, static 행) + 블록 디렉터리 + 블록들. 블록 하나는 클러스터링
  시간 범위 하나(기본 `block_window` = 15분, SSTable 메타데이터에 기록)이며 블록마다 시작·끝·행 수·컬럼별 min/max를
  디렉터리에 둔다. 블록 폭은 **SSTable마다** 기록되므로 설정을 바꿔도 새로 동결되는 SSTable에만 적용되고, 겹침
  문제가 구조적으로 없다(같은 SSTable 안의 블록은 서로 겹치지 않고, SSTable 간 겹침은 보통 LSM 병합이 처리).
- **블록 건너뛰기 인덱스 (ClickHouse skip index에서):** 블록 디렉터리에 컬럼별 min/max 외에, 카디널리티가 낮은
  컬럼(예: `quality`, `type`)은 값 집합(최대 N개, 넘으면 생략), 텍스트 컬럼은 작은 블룸 필터를 둔다. 읽기에서
  RowFilter의 등호·범위 조건이 블록에 있을 수 없으면 그 블록을 디코드하지 않는다(결과는 같고 비용만 준다 —
  필터 적용 자체는 업스트림 경로가 병합 뒤에 한다). 인덱스가 틀리면 행이 사라지므로, 블록 통계는 v4 §4의
  `StatOrder` 규칙(순서를 모르면 통계 없음)을 그대로 따른다.
- **적용 대상:** 지금 계층화가 받는 모양 그대로 — 클러스터링 컬럼이 timestamp 하나(`ColdBoundary.hasTimestampClustering`),
  일반 컬럼은 원시 타입·frozen 컬렉션. 그 밖의 테이블은 `tscol`을 쓰지 않는다(BTI로 동결).

### 4.2 청크 코덱 v5 — "디코드 = 원래 행" (정확 왕복)

v4는 값만 담고 writetime은 청크당 근사 하나였다. SSTable을 대체하려면 행이 가진 **모든 조정 정보**를 담아야 한다.
v4의 블록 모델·인코딩(ALP, FOR/delta 비트패킹, DICT/RAW, CONSTANT/ALL_NULL, presence 비트맵)을 그대로 쓰고
섹션만 더한다.

| 섹션 | 내용 | 보통의 크기 |
| --- | --- | --- |
| 타임스탬프 축 | 행 클러스터링 (v4와 같음) | delta-of-delta, 작음 |
| 값 섹션 (컬럼별) | v4와 같음 | v4와 같음 |
| **셀 writetime** (컬럼별) | 행 liveness 타임스탬프와의 차이 | 거의 전부 0 → CONSTANT, O(1) |
| **행 liveness** | 행 marker 타임스탬프 / TTL / local deletion time | 단조 증가 → delta, 작음 |
| **셀 TTL·만료** (컬럼별) | TTL, local deletion time | 보통 ALL_NULL 또는 CONSTANT |
| **셀 톰스톤 표시** (컬럼별) | presence와 별개인 "삭제된 셀" 비트맵 + 삭제 시각 | 보통 ALL_NULL |

- 규범: 블록을 디코드한 `Unfiltered` 스트림은 그 블록을 만든 입력 스트림과 `equals`가 성립해야 한다
  (타임스탬프·TTL·deletion time·셀 톰스톤 포함). 이게 다이제스트·리페어·조정이 형식과 무관하게 맞는 근거다.
- 인코딩할 수 없는 것: 범위 톰스톤, 행 삭제, 복합(비-frozen) 셀, 카운터. 이런 것이 있는 **파티션**은 그 파티션만
  블록 대신 "행 블록"(BTI와 같은 행 직렬화)으로 저장한다 — SSTable 전체를 포기하지 않는다. 보통은 동결 시점에
  gc_grace가 지나 이미 정리돼 있으므로 드물고, 드물다는 것은 지표로 확인한다(`tscol_row_fallback_partitions`).
- 결정성(v4 §5 규칙) 유지: 같은 입력 → 같은 바이트. 복제본끼리 같은 데이터를 동결하면 같은 파일이 된다
  (리페어 효율, 디버깅 용이).

### 4.3 쓰는 시점 — TSCS 동결과 재동결

- `FreezeCompactionTask`/`SplitRefreezeCompactionTask`가 출력 SSTable을 만들 때 테이블 옵션이 켜져 있으면 writer를
  `tscol`로 고른다. 옵션은 TSCS 옵션에 둔다: `compaction = {..., 'frozen_format': 'columnar'}` (기본 `row` = 지금과 같음).
  정책 JSON 확장(`timeseries_tiering`)은 쓰지 않는다 — 형식은 컴팩션의 일이다.
- 동결은 이미 "창 하나 → SSTable 하나" 메이저 컴팩션이고 표준 `CompactionController`로 TTL·톰스톤을 정리한다. 출력만
  컬럼형으로 바뀌므로 원자성·실패 시 롤백은 기존 컴팩션 트랜잭션 그대로.
- **늦은 데이터:** 동결된 창에 쓰기가 오면 평소처럼 memtable → 그 창의 새 SSTable. TSCS는 이미 이 경우를
  "FROZEN 창에 SSTable이 더 생김"으로 보고 재동결한다 → 두 SSTable을 병합해 새 `tscol` 하나로. 가드 없음.
- **언제 컬럼형이 되나:** 창이 닫히고 `freeze_after`가 지난 뒤. 지금의 `hot_window` 역할을 `freeze_after`가 맡는다.
  41의 `tm_tag_point`는 `window_size 1d / freeze_after 2d`라 최대 약 3일이 행 형식으로 남는다 → 시범 단계에서
  `freeze_after`를 12h~1d로 낮추는 것을 검토(gc_grace 86400 < freeze_after 조건 유지).
- `ColdWindowChunkFlush`(memtable → 청크 직접 쓰기)는 형식이 생기면 "콜드 창을 flush할 때 바로 `tscol`로 쓰기"로
  바뀐다. 노드 로컬 SSTable이므로 RF와 무관하게 안전하다. 1단계 범위에는 넣지 않는다(4.9).

### 4.4 읽기

- `tscol` 리더는 `SSTableReader`를 구현한다: 파티션 조회(트라이 + 블룸), 슬라이스·역순 반복, `ColumnFilter`
  (요청 컬럼의 섹션만 디코드), 블록 디렉터리의 시간 범위로 블록 건너뛰기, 레인지 스캐너.
- 그 위는 전부 업스트림 경로 그대로: memtable·다른 SSTable과의 병합, RowFilter(일반 컬럼 필터, `ALLOW FILTERING`),
  페이징, short-read protection, 다이제스트, 읽기 복구. **`TransparentReads`, `ChunkMergeUnfilteredIterator`,
  `ChunkRangeMergeIterator`, 커버리지 원장, 콜드 경계, 거부 규칙은 필요 없다.**
- 집계 가속(현재 계층화의 3~6×)은 처음엔 "컬럼만 디코드 + 블록 스킵"에서 나온다. 이후 단계에서 블록 통계로
  `count`/`min`/`max` 같은 집계를 디코드 없이 답하는 빠른 경로를 검토(4.9 이후 과제).

### 4.5 쓰기·삭제·TTL

- 쓰기 경로 변경 없음. 동결된 창에 대한 UPDATE·DELETE·TTL·LWT는 보통 Cassandra 의미로 동작하고, 재동결이 정리한다.
- `TieredWrites` 가드 전부(콜드 삭제 거부, 과거 타임스탬프 거부, 짧은 TTL 거부, 쓰기 CL 하한)는 `tscol` 테이블에는
  적용되지 않는다. 옛 그림자 테이블 데이터가 남은 테이블에는 이전이 끝날 때까지 유지(4.8).

### 4.6 복제 · 리페어 · 스트리밍 · 인덱스

- 노드 로컬. 복제본마다 동결 시점이 달라 한쪽은 BTI, 한쪽은 `tscol`이어도 4.2의 정확 왕복 덕분에 같은 행 →
  같은 다이제스트·머클 트리.
- 스트리밍: entire-SSTable 스트리밍은 같은 형식을 아는 노드끼리면 그대로 파일 전송, 아니면 파티션 스트리밍
  (받는 쪽이 자기 writer로 씀 → 이후 자기 TSCS가 다시 동결). 둘 다 업스트림 경로.
- SAI·2차 인덱스: SAI는 SSTable을 쓸 때 writer의 flush observer로 인덱스를 만든다. `tscol` writer도 행 단위로
  observer를 호출해야 한다(설계 요구). 그러면 인덱스는 형식과 무관하게 동작하고, r8의 "인덱스 조건 거부"도 사라진다.

### 4.7 운영

- 지표·가상 테이블: SSTable별 형식, 컬럼형 비율, 압축률, 행 블록 폴백 수, 동결 소요. 지금의
  `system_views.timeseries_tiering`은 이전이 끝나면 은퇴.
- `nodetool`: 형식 되돌리기 — `tscol`을 BTI로 다시 쓰는 명령(`upgradesstables`처럼 동결 SSTable을 지정 형식으로 재작성).
  옵션을 `row`로 바꾸고 이 명령을 돌리면 `tscol`을 모르는 빌드로 내려갈 수 있다. 이게 4.2 반목표의 "되돌릴 길".

### 4.8 이전 — 일회성 이전 도구 (호환 경로 없음)

41과 2026.11 설치는 `tm_tag_point`(41은 `tm_asset_data_based_second`도)에 그림자 테이블 계층화가 걸려 있고, 12시간이 지난
데이터는 `__chunks`에만 있다(원본 행은 범위 톰스톤으로 삭제됨). 새 엔진은 그 형식을 **읽지 않는다.** 대신:

1. **이전 도구 `nodetool untier <ks> <table>`** (새 빌드에 포함, 이전이 끝나면 다음 릴리즈에서 삭제): `__chunks`를 창 단위로
   v4 커서로 풀어 베이스 테이블에 행으로 다시 쓴다. 타임스탬프는 `max_row_writetime + 1` — 범위 톰스톤(`max_row_writetime`)
   위로 올라오므로 되살아난다. writetime은 원래도 근사값이었으므로 잃는 정보가 없다. 창마다 다시 쓴 뒤 베이스에서 같은
   행 수·값을 읽어 대조하고, 맞는 창만 청크 행을 지운다. 멱등(같은 타임스탬프로 다시 써도 같음)이라 중간에 멈춰도
   다시 돌리면 된다. 속도 제한(초당 행 수)을 둔다.
2. 도구가 끝나면 그 테이블의 `timeseries_tiering` 확장과 그림자 테이블 세 개(`__chunks`, `__chunk_coverage`, `__tags`)를
   지운다(도구가 마지막 단계로 수행, 청크가 비었을 때만). 되살린 행은 TSCS가 평소대로 동결하며 `tscol`이 된다.
3. 새 엔진에는 옛 계층화가 없으므로, **옛 데이터를 가진 노드는 새 빌드로 올리자마자 이 도구를 돌려야** 그 구간이 다시
   보인다(그 전까지는 12시간 이전 데이터가 조회에 없다 — 사라진 게 아니라 `__chunks`에 남아 있음). 플랫폼은 업그레이드
   직후 단계로 도구를 실행한다. 41은 이 절차로 시범 적용한다.

41 `tm_tag_point`는 현재 `__chunks` 0.5GB 규모(하루 약 18만 창)라 초당 10만 행이면 수 시간. 41 부하(초당 1만 쓰기)와 함께
시험한다.

### 4.9 단계

| 단계 | 내용 | 끝났다는 증거 |
| --- | --- | --- |
| P0 | 코덱 v5 (정확 왕복 섹션) | 속성 테스트: 무작위 행 스트림(타임스탬프·TTL·셀 톰스톤·static·null 섞어) → encode/decode `equals`, 결정성, 크기 회귀 |
| P1 | `tscol` writer/reader/scanner (BTI 파생), 행 블록 폴백 | 차등 하네스: 같은 데이터를 BTI와 `tscol`로 각각 쓰고 모든 읽기 모양(슬라이스·역순·컬럼 필터·페이징·필터·집계·gap-fill·레인지 스캔)의 결과가 완전히 같음; scrub/verify |
| P2 | TSCS `frozen_format` 옵션, 동결·재동결 출력 | 늦은 쓰기·삭제·TTL 후 재동결 결과 정확; T2/T3 불변식 유지; 통합 테스트 |
| P3 | 분산 검증 | jvm-dtest: 형식이 섞인 복제본 리페어 0 차이, 스트리밍(파일/파티션), 다이제스트 일치, SAI; 3노드 클러스터 테스트 |
| P4 | 성능 | `tiering-benchmark.md`와 같은 하네스로 디스크·집계·행 조회·동결 비용 비교, `ci-perf` 기준선 |
| P5 | 옛 계층화 제거 + 이전 도구(`nodetool untier`) | 옛 코드·가드·정책·가상 테이블 삭제, 41 데이터 사본으로 untier 대조 0 차이 |
| P6 | 41 적용 | 플랫폼 이미지로 배포 → untier → `tm_tag_point` 동결이 `tscol`로 → 3일 숙성 → 승격 |

이후 과제: 콜드 창 flush 시 `tscol` 직접 쓰기, 블록 통계 집계 빠른 경로.

## 5. 제약

- 빌드 산출물은 `apache-cassandra-6.0.0.jar` 드롭인, 패키지 이름 변경 금지. 업스트림 파일 수정 최소화: 형식
  등록(서비스 파일), TSCS의 출력 writer 선택. 나머지는 `io/sstable/format/tscol/`과 `db/timeseries/` 아래 새 파일.
- 41은 단일 노드·RF 1, 초당 약 1만 쓰기, BTI 형식(커서 컴팩션 불가, production-rollout §3.4). 형식 전환은 테이블 옵션으로
  한 표씩, 기본값은 꺼짐.
- 릴리즈 규칙: 후보 → 41 3일 숙성 → 승격. 형식이 바뀌는 빌드는 플랫폼 이미지로만 배포(docker cp 금지).
- 업그레이드: 지원 바닥(2026.11=r8) 메타데이터·데이터로 기동 시험 필수.

## 6. 리스크

| 리스크 | 대응 |
| --- | --- |
| SSTable 형식 구현 규모(BTI 약 4.4천 줄) | BTI 파생으로 파티션 인덱스·필터·통계 재사용, 새 코드는 본문 레이아웃과 반복자에 집중 |
| 정확 왕복이 깨지는 드문 모양(셀 톰스톤, 행 marker만 있는 행, 만료 셀 경계) | P0 속성 테스트를 입력 모양 생성기로; 디코드 ≠ 입력이면 그 파티션은 행 블록 폴백(손실 대신 크기 손해) |
| 업스트림이 `SSTableReader`/`SSTableFormat` 인터페이스를 바꿈 | upstream-merge 스킬의 위험 커밋 검토에 이 경로 추가; 차등 하네스가 머지마다 돈다 |
| untier 중 쓰기 부하·실패 | 속도 제한, 창 단위 멱등(같은 타임스탬프로 다시 써도 같음), 대조 후에만 청크 삭제 |
| 업그레이드와 untier 사이 12시간 이전 데이터가 조회에 안 보임 | 업그레이드 직후 untier를 같은 절차로 실행, 데이터는 `__chunks`에 남아 손실 없음 |
| `freeze_after` 동안은 행 형식(최대 수 일) — 디스크 이득이 늦게 옴 | `freeze_after` 조정, 이후 콜드 창 flush 직접 `tscol` |
| 늦은 쓰기가 계속 오는 창의 재동결 반복(TimescaleDB #3230과 같은 꼴) | 재동결 횟수 지표, 창별 재동결 최소 간격, 늦은 SSTable이 일정 크기 넘을 때만 재동결 |
| `tscol`을 모르는 빌드로 다운그레이드 | 기본 꺼짐 + 되돌리기 명령(4.7) + 릴리즈 노트 |

## 7. 빌더 재량 (내 판단으로 정한 것과 열어 둔 것)

정한 것(필요하면 바꿀 수 있음): BTI 파생, 옵션 위치(TSCS `frozen_format`), 블록 기본 15분, 행 블록 폴백 단위 = 파티션,
untier 타임스탬프 = `max_row_writetime + 1`, 단계 순서.
열어 둔 것: 블록 디렉터리 직렬화 세부, v5 섹션의 정확한 인코딩 선택, 지표 이름, untier 속도 기본값.
