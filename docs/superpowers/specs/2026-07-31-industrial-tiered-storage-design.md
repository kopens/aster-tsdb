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

# 산업용 계층형 시계열 저장소 (Gorilla 청크 + 보존 정책) — 설계

날짜: 2026-07-31 · 저장소: cassandra-timeseries (Apache Cassandra 6.0.0 포크)
상태: 사용자 승인된 브레인스토밍 결과의 설계 명세

## 1. 목표

산업 현장(센서/태그 데이터)용 시계열 저장을 위해:

1. **태그 기반 압축**: Gorilla식 무손실 인코딩(타임스탬프 delta-of-delta + 값 XOR 비트패킹)으로
   오래된 데이터의 저장량을 5~10배 절감한다. 모든 샘플을 보존한다(손실 압축 아님).
2. **완전 투명 CQL**: 사용자는 지금과 동일한 CQL(시계열 함수 포함)을 쓴다. 내부적으로 데이터가
   청크로 재인코딩되어 있어도 읽기 경로가 투명하게 풀어서 병합한다.
3. **산업 데이터 특성에 맞는 TTL/보존**: 테이블 단위 정책 — 핫 윈도우(row-per-sample) →
   콜드 윈도우(청크) → 윈도우 통째 삭제.
4. **지연 도착 필수 처리**: 엣지 장비 store-and-forward 백필(며칠치 지각 데이터)이 일상이라는
   전제. 청크된 과거 시간대에 뒤늦게 도착한 쓰기도 정확하게 반영되어야 한다.

비목표(이번 범위 제외): 손실 압축(스윙잉도어/데드밴드), 태그(파티션) 단위 보존 정책,
롤업(다운샘플) 자동 생성 — 롤업은 기존 continuous-aggregates 설계안과의 별도 결합 과제.

## 2. 아키텍처 (승인된 접근법 A)

**섀도 청크 테이블 + 코디네이터 병합.** gap-fill을 `SelectStatement`에 배선했던 이 포크의
기존 노선을 확장한다. 온디스크 SSTable 포맷·CQL 문법·Operator는 건드리지 않는다.

```
쓰기(항상):   INSERT → 원본 테이블 (row-per-sample)          ← 지각 쓰기도 동일 경로
백그라운드:   hot_window 를 지난 윈도우를 워터마크 기반으로
             (tag, window) 단위 Gorilla 청크로 재인코딩 → 섀도 테이블에 저장 → 원본 로우 삭제
읽기(투명):   질의 범위가 핫 경계를 넘으면
             청크 디코드 ∪ 남아있는 로우(지각 포함) 병합 (충돌 시 로우 우선) → 기존 집계 파이프라인
보존:        cold_window 를 지난 청크 파티션 통째 삭제
```

**지연 도착의 해법이 이 구조에서 자연스럽게 나온다**: 지각 쓰기는 그냥 일반 로우로 착지한다.
읽기는 항상 "청크 ∪ 로우" 병합이므로 즉시 재인코딩이 필요 없고, 백그라운드 잡이 "지각 로우가
쌓인 윈도우"를 발견하면 기회주의적으로 재인코딩(청크+지각 로우 → 새 청크, 멱등)한다.
memtable+SSTable 병합과 같은 Cassandra 관용구다.

### 기각된 대안

- **커스텀 컴팩션 전략으로 스토리지 레벨 재작성**: 컴팩션은 로우 모델을 보존해야 하므로 같은
  테이블 안에서 청크화 불가. 완전 투명 요구와 양립 불가.
- **명시적 아카이브 테이블(반투명)**: 투명성 요구로 최종안에서는 배제. 단, 서브프로젝트 2의
  중간 산출물로는 유효(투명 읽기 완성 전까지 명시 함수로 콜드 조회 가능).

## 3. 구성요소

### 3.1 Gorilla 코덱 (서브프로젝트 1)

- `db/timeseries/GorillaCodec` (신규 패키지 `org.apache.cassandra.db.timeseries`):
  `(long timestampMillis, double value)` 스트림의 인코더/디코더.
  - 타임스탬프: delta-of-delta, 산업 데이터의 규칙적 주기(1s/5s/...)에서 비트당 비용 최소.
  - 값: 직전 값과 XOR 후 leading/trailing zero 런 인코딩 (Facebook Gorilla 논문 방식).
- 순수 Java, Cassandra 의존 최소(ByteBuffer in/out). 신규 외부 의존성 없음.
- 청크 페이로드에 버전 바이트 + 샘플 수 + first/last timestamp 헤더 포함(검증·프루닝용).
- 테스트: property-based 왕복(무작위 시계열 → encode → decode → 동일), 경계값(NaN,
  ±Infinity, 동일 타임스탬프, 역순 입력 거부), 인코딩 크기 회귀 기준.

### 3.2 청크 스토어 + 재인코더 + 보존 (서브프로젝트 2)

- **섀도 테이블**: 원본 `ks.metrics` 에 대해 내부 테이블 `ks.metrics__chunks`
  `(PK: (원본 파티션 키), 클러스터링: window_start timestamp, 컬럼: payload blob, samples int,
  max_writetime bigint)`. UCS + 윈도우 정렬. 사용자가 직접 만질 필요 없음(자동 생성/삭제).
- **정책 표면**: 테이블 `extensions` 맵(스키마 네이티브, 문법 무수정)에
  `timeseries_tiering = {"hot_window":"7d","cold_window":"365d","chunk_window":"1h"}` 저장.
  구성/상태 조회용 가상 테이블(`system_views.timeseries_tiering`)과 nodetool 명령
  (`nodetool tieringstatus`, `nodetool retier <ks> <table>`) 제공.
- **재인코더 서비스**: 노드별 스케줄 서비스(continuous-aggregates 설계안의 워터마크 개념 재사용).
  자기 소유 프라이머리 레인지의 태그에 대해:
  1. `hot_window` 를 지난 닫힌 윈도우를 스캔 → 청크 생성 → 섀도 테이블에 기록 →
     원본 로우 레인지 딜리트. 각 단계 멱등(재실행 시 같은 청크 덮어쓰기).
  2. 지각 로우 감지: 청크가 존재하는 윈도우에 로우가 남아 있으면(스캔으로 발견) 청크+로우를
     병합해 새 청크로 교체 후 로우 삭제.
- **보존 집행**: `cold_window` 를 지난 청크(및 잔여 로우) 윈도우 통째 삭제. TTL이 아니라
  명시 딜리트 + 윈도우 정렬 컴팩션으로 SSTable 통째 회수를 노린다.
- 이 단계의 독립 가치: 저장 절감 + 보존 자동화. 콜드 데이터는 임시로 명시 조회 함수
  (`expand_chunks`) 또는 섀도 테이블 직접 조회로 접근 가능.

### 3.3 투명 읽기 경로 (서브프로젝트 3, 최난이도)

- `SelectStatement` 에서 질의 시간 범위가 핫 경계(`now - hot_window` 근방의 워터마크)를
  넘는지 판정. 넘지 않으면 기존 경로 그대로(오버헤드 0).
- 넘으면: 파티션+범위 제한 질의에 한해 **코디네이터 측 병합 경로**로 전환 —
  1. 섀도 테이블에서 해당 범위 청크 조회 → 디코드,
  2. 원본 테이블에서 같은 범위 로우 조회(지각 포함),
  3. 타임스탬프 기준 병합(로우 우선),
  4. 기존 집계(시계열 함수 포함)를 코디네이터 측에서 적용.
- 제약(정직하게): 집계 푸시다운이 레플리카 읽기 경로에 있으므로, 핫/콜드에 걸치는 질의는
  코디네이터 집계로 우회한다. 파티션+시간범위 질의(산업 주력 패턴)에서는 유한하고 수용 가능.
  전체 테이블 스캔형 질의는 v1에서 핫 경계 안쪽만 투명 지원, 걸치면 명확한 에러+힌트.
- gap-fill·시계열 함수와의 결합 테스트 필수.

#### 3.3.1 SP3 상세 설계 (2026-07-31 정찰 확정 — 구현 규범)

정찰 근거: `scratchpad/sp3-recon-select.md`, `sp3-recon-restrictions.md` (SelectStatement 파이프라인
file:line 포함). 핵심 결정:

- **병합 지점 = PartitionIterator 레벨(집계 전).** 집계는 `ResultSetBuilder`(process() :1185-1195)
  안에서 일어나므로, 갭필처럼 ResultSet 후처리(:1198)로는 집계 질의가 투명해지지 않는다.
  `query.execute(...)`가 돌려준 PartitionIterator를 **ChunkMergePartitionIterator**로 감싸
  파티션(태그)별로 청크 디코드 로우를 클러스터링 순서로 병합 주입한다. 이후의
  Selection/GroupMaker/트림/갭필은 무수정으로 정상 동작.
- **발동 조건**(전부 만족 시에만; 아니면 기존 경로 오버헤드 0): 테이블에 TieringPolicy 존재
  (TableMetadata는 SelectStatement 필드 :182로 접근, 정책 파싱은 스키마 버전 기준 캐시) ·
  단일/다중 파티션 키 질의(StatementRestrictions.getPartitionKeys :794) · 클러스터링 슬라이스
  필터(ClusteringIndexSliceFilter.requestedSlices()의 start/end로 타임스탬프 범위 추출) ·
  요청 범위가 청크 존재 가능 구간과 겹침.
- **병합 규칙**: 파티션별로 `<table>__chunks`에서 `window_start ∈ [floor(rangeStart)-chunk_window,
  rangeEnd]` 청크를 사용자 질의와 **같은 CL**로 조회(QueryProcessor.process — SP2 불변식 B와
  동일 규율), 디코드 후 요청 범위로 필터, 합성 Row(BTreeRow, value 셀 writetime=청크
  max_row_writetime)로 변환, 핫 로우와 2-way 정렬 병합. **동일 타임스탬프 충돌은 핫 로우
  승리**(재인코더의 rows-win 병합 규칙과 일치 — 지각 수정치가 스윕 전까지 항상 보이는 값과
  동일해야 왕복 일관).
- **LIMIT 정합성 증명(정찰 Q5)**: 스토리지 측 DataLimits는 클러스터링 최소(ASC)/최대(DESC)
  N개를 남긴다. 병합 결과의 상위 N개에 포함되는 핫 로우는 반드시 핫 질의 결과의 상위 N개
  안에 있으므로(부분수열 논증 — 지각 로우 포함) 유실 불가. 최종 사용자 LIMIT은 기존
  `ResultSet.trim(userLimit)`(:1203)이 병합 후 스트림에 적용.
- **페이징**: 페이저가 페이지마다 클러스터링 재개 지점으로 슬라이스를 좁히므로, 병합
  래퍼가 "그 페이지의 요청 슬라이스 범위 안의 청크 로우만" 주입하면 페이지 경계에서
  중복·누락이 없다(청크 로우는 창·범위에서 결정론적으로 재유도 가능 — 상태 불필요).
  래핑 지점은 페이지 fetch 경로 안쪽이어야 하며(pager.fetchPage가 주는 이터레이터),
  구현 계획의 Task 0 정찰로 정확한 결합점(QueryPagers/AggregationQueryPager 경유 시 포함)을
  확정한다. v1에서 결합점이 페이저 내부라 불가하면: 비페이징+집계 질의 완전 지원, 대용량
  비집계 페이징 질의는 명확한 에러+힌트(§3.3 원칙 유지).
- **SP2 인계 항목 소화**: 죽은 태그 청크 만료(§8 최우선 후속)는 SP3 범위에 포함 — 만료
  열거를 청크 테이블 기준으로 전환. tiered-storage.md의 명시 조회 패턴 문서를 투명 읽기
  문서로 대체.

## 4. 서브프로젝트 순서와 이유

| 순서 | 내용 | 독립 가치 | 난이도 |
| --- | --- | --- | --- |
| 1 | Gorilla 코덱 | 검증된 기반, 어디에도 쓸 수 있음 | 하 |
| 2 | 청크 스토어+재인코더+보존 | **저장 5~10배 절감 + 보존 자동화** | 중 |
| 3 | 투명 읽기 경로 | 완전 투명 CQL 완성 | 상 |

1→2→3 순서. 각 서브프로젝트는 자체 spec→plan→구현 사이클. 2까지가 배포 가능한 중간 제품이며,
3이 늦어져도 운영 가치가 유지된다.

## 5. 오류 처리·운영 원칙

- 재인코딩의 모든 단계는 멱등: 청크 쓰기 성공 후 로우 삭제 실패 → 다음 주기에 지각-로우
  경로로 수렴(중복 없음, 로우 우선 병합이므로 정답 유지).
- 청크 디코드 실패(손상): 에러 로그 + 해당 윈도우는 로우 폴백(있으면) + `nodetool retier` 로
  재생성 유도. 질의는 실패시키지 않되 경고를 남긴다.
- 재인코딩은 노드 로컬 프라이머리 레인지만 처리해 클러스터 중복 작업 방지(RF>1 에서는
  청크 테이블 자체가 복제되므로 1개 노드만 쓰면 된다).
- 혼합 버전 클러스터: 구버전 노드는 extensions 를 무시하고 섀도 테이블을 일반 테이블로 취급 —
  기능은 전 노드 업그레이드 후 활성화하도록 문서화.

## 6. 테스트 전략(요약)

- 코덱: property-based 왕복 + 경계값 + 크기 회귀.
- 재인코더: 워터마크 진행·멱등성·지각 로우 수렴·보존 삭제를 단위/수명주기 테스트로.
  장애 주입(청크 쓰기 후 로우 삭제 전 중단) 시나리오 포함.
- 투명 읽기: 핫만/콜드만/걸침 3분면 × 시계열 함수 × gap-fill 조합, 로우 우선 병합 정확성,
  지각 데이터 반영. 3노드 jvm-dtest(CL 조합)와 도커 통합 테스트(릴리스 게이트) 확장.
- 스케일: 기존 scale-test 하네스에 청크화 전/후 저장량·질의 시간 비교 추가.

## 7. 성능 비교 보고서 (필수 산출물, 사용자 요구)

완성 시 기존(비계층) 대비 정량 비교 보고서를 만든다 — 기존 scale-test 하네스
(`docker/scale-test.sh`, 1억 건 데이터셋 재사용 가능)를 확장해 동일 조건에서 측정하고,
`doc/timeseries/tiering-benchmark.md` 로 커밋한다.

| 측정 항목 | 비교 대상 | 기대 |
| --- | --- | --- |
| 저장량 (live bytes, SSTable 수) | row-per-sample vs Gorilla 청크 (동일 1억 건) | 5~10배 절감 |
| 압축률 상세 | 청크 payload bytes / 원본 row bytes, 태그 주기별(1s/10s/불규칙) | 주기 규칙성에 비례 |
| 쓰기 처리량 | 계층화 켬/끔 상태의 적재 rows/s (재인코딩 백그라운드 부하 포함) | 끔 대비 소폭 저하 허용, 수치 명시 |
| 읽기: 핫 구간 | 기존 경로 그대로 | 저하 0 (동일 수치) |
| 읽기: 콜드 구간 | 청크 디코드 경로 vs 같은 데이터 row-per-sample | 디코드 비용 vs IO 절감 실측 |
| 읽기: 걸침 질의 | 코디네이터 병합 경로 | 오버헤드 정량화 |
| 재인코딩 처리량 | 윈도우/초, CPU 사용률 | 운영 계획 근거 |
| 지각 백필 | 청크된 윈도우에 백필 후 재수렴 시간 | 유한·멱등 확인 |

서브프로젝트 2 완료 시 저장량·쓰기·재인코딩 항목을, 서브프로젝트 3 완료 시 읽기 항목을
측정해 보고서를 단계적으로 채운다.

(2026-07-31 사용자 결정: 벤치마크는 전 구현 완료 후 — SP3 투명 읽기까지 — 최종 읽기
경로 기준으로 1회만 측정한다.)

## 8. SP2 구현 완료 노트 (2026-07-31) 및 SP3 인계

SP2는 커밋 `8541e06047..be5500363d`(12커밋)로 완료 — 태스크별 리뷰 5회 + 전체 브랜치
최종 리뷰(APPROVED-WITH-FIXES → 수정 웨이브 → 재리뷰 클린). 두 불변식(지각 데이터
writetime-경계 레인지 딜리트 생존, 전 데이터 경로 QueryProcessor.process@CL 쿼럼 플로어)은
엔드투엔드 검증됨. 구성: `db/timeseries/tiering/`(TieringPolicy·ChunkTables·
TieredStorageService), extensions CQL(TableAttributes hex), 60초 스위퍼+MBean+
system_views 가상 테이블+nodetool(retier/tieringstatus), 도커 통합 게이트 52건.
CI: TieringPolicyTest 27 · TieredStorageServiceTest 15 · TieredStorageMockTest 4 ·
AlterTableExtensionsTest 2. 문서: `doc/timeseries/tiered-storage.md`(제한사항 §6 포함).
정책 상한: `chunk_window ≤ 31d`(파스 거부) + 인코드 전 MAX_SAMPLES 이중 가드(페이징 중
중단, 태그 격리 스킵). DESC 클러스터링 지원(ReversedType unwrap + 전 워크 쿼리 ORDER BY ASC).

SP3가 인수할 것 (최종 리뷰 지시):
- **죽은 태그 청크 만료 공백(최우선 후속)** — 베이스 로우가 전부 사라진 태그는 DISTINCT
  열거에서 빠져 콜드 청크가 영구 잔존. 만료 열거를 청크 테이블 기준으로 전환해 구조적으로
  해결할 것 (tiered-storage.md §6.3).
- 투명 읽기 시 핫 로우+청크 코디네이터 병합 — 명시 조회 패턴(§6) 문서를 대체.
- 오버플로 웨지 태그는 사이클마다 상한+1페이지 IO 재발(로그 있음) — 운영 문서화 유지.
- 멀티노드(3노드) 검증 공백은 여전히 이연 상태.
- 두 번째(병합 카운트) MAX_SAMPLES 가드 전용 테스트 미존재 — SP3 테스트 웨이브에서 보강.

## 9. SP3 구현 완료 노트 (2026-07-31)

SP3는 커밋 `ff982c4621..`(마감 커밋 포함)로 완료 — 쿼터 제약으로 컨트롤러 인라인 실행(계획서
`2026-07-31-sp3-transparent-reads.md`, R1~R6). 테스트: ChunkReadSupportTest 6, ChunkMergeIteratorTest 7,
TransparentReadTest 10(E2E 매트릭스: 전범위/콜드만/포인트/핫콜드 집계/gap-fill densify/지각 우선/
손상 스킵/다중 페이지 에러/LIMIT·DESC/비계층 무영향), TieredStorageServiceTest 16(+dead-tag) — CI 배선.
도커 IT에 투명 조회 검증 추가(병합 count/포인트/집계).

구현: `tiering/ChunkReadSupport`(디코드→합성 로우, writetime=max_row_writetime 근사 — 이 선택이
업스트림 타임스탬프 화해를 곧 rows-win 규칙으로 만든다), `ChunkMergePartitionIterator`(요청 키 순
파티션 워크 + 2-way 병합 + 전부-청크화 파티션 합성), `TransparentReads`(발동 판정·사용자 CL 청크
조회·손상 스킵+ClientWarn·hot-only 강등), SelectStatement 3개 결합점(비페이징/페이징 단일 fetch/내부).
§3.3.1의 LIMIT 증명과 v1 페이징 스코프(다중 페이지 병합 = 에러+힌트) 그대로 구현.

구현 중 확정된 추가 규범:
- **재인코더 내부 바이패스**: 계층화 기계 자신의 베이스 읽기는 `TransparentReads.enterInternalBypass()`
  (ThreadLocal)로 병합을 우회해야 한다 — 우회 없으면 인코딩된 창이 라이브로 재관측되어 멱등성 루프.
  runOnce가 브래킷하며, 물리 상태를 검증하는 테스트도 같은 브래킷을 쓴다(raw() 헬퍼).
- 무경계 슬라이스(Long.MIN/MAX)는 창 산술 전 ±2^62 클램프(언더플로 랩 방지, 테스트로 실증된 버그).
- R6: 콜드 만료 열거를 청크 테이블 기준으로 전환 — §8의 "죽은 태그" 구조 공백 해소.

잔여(후속): 멀티노드(3노드) 검증 공백(기존 이연과 합류); 정책 파싱 per-query(스키마 버전 캐시 미구현,
µs 단위라 낮은 우선순위); 클라이언트 페이징의 무상태 재개 앵커 설계(v2 후보).
