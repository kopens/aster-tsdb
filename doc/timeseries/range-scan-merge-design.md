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

# 설계: 계층화 테이블 레인지 스캔 청크 병합 + 스키마 경합(P1-01) 봉쇄

상태: **설계 (2026-11-02).** 지금은 청크에 닿는 레인지 스캔을 `TransparentReads.guardRangeRead`가
거부합니다([production-rollout.md §3.1](production-rollout.md)). 이 문서는 그 거부를 병합으로 바꾸는
방법과, 재인코더의 스키마 경합을 닫는 방법을 정합니다.

## 1. 레인지 스캔 청크 병합

### 1.1 채택안 — 코디네이터 하위 범위 resolve 에서 정렬 병합 조인

훅은 단일 파티션 병합과 같은 자리인 `DataResolver.resolveInternal`에 둡니다.
`UnfilteredPartitionIterators.merge(results, mergeListener)` **뒤**, `Filter` **앞**이고,
`command`가 `PartitionRangeReadCommand`이면 `TransparentReads.maybeWrapRange(command, merged, cl)`를
부릅니다. `RangeCommandIterator.executeNormal`은 하위 범위마다 `DataResolver`를 하나 만들므로 병합 단위는
하위 범위 하나(`command.dataRange().keyRange()`)입니다.

이 자리여야 하는 이유는 셋입니다.

1. **읽기 복구** — 합성 행이 merge listener 에 닿지 않습니다(단일 파티션과 같은 논거,
   `digestMismatchStillMergesChunksWithoutRepairingThemBack`의 불변식).
2. **LIMIT** — 병합 뒤의 `mergedResultCounter`가 청크 행까지 셉니다. 레플리카는 핫 행만 세어 잘랐으므로
   카운터는 레플리카가 자른 지점 이전이나 그 지점에서 끝나고, 그 뒤의 청크 전용 파티션은 소비되지 않습니다.
   PER PARTITION LIMIT 도 같은 논리입니다.
3. **SRP** — 조인은 청크 전용 키를 내보내기 전에 항상 핫 스트림의 `hasNext()`부터 부르므로
   `ShortReadPartitionsProtection`이 먼저 확장할 기회를 얻습니다.

새 클래스 `ChunkRangeMergeIterator`는 두 토큰 정렬 스트림을 `DecoratedKey.compareTo`로 병합 조인합니다.

- **핫 스트림** — 레플리카들을 병합한 파티션들.
- **청크 키 스트림** — 하위 범위에 대해 지연 페이징하는
  `SELECT <pk>, window_start FROM ks."t__chunks" WHERE token(pk) >= ? [AND token(pk) <= ?]
  AND window_start > ? AND window_start < ? PER PARTITION LIMIT 1 ALLOW FILTERING`.
  하한은 `startMs - lookbackMillis`, 결과마다 `keyRange.contains(dk)`로 경계 포함 여부를 맞춥니다.

| 키 | 처리 |
| --- | --- |
| 핫에만 | 그대로 통과(창 목록 쿼리 0회 — N+1 없음) |
| 양쪽 | 기존 `merge` |
| 청크에만 | `synthetic` 파티션 생성 |

`ChunkMergeUnfilteredIterator`의 `merge`/`synthetic`은 package-private 정적 헬퍼로 뽑아 재사용합니다.
키가 역순으로 오면 `outOfOrder`와 같은 정책으로 쿼리를 실패시킵니다(중복은 조용한 오답).
페이징 재개 시 시작 키에만 `forPaging` 필터가 적용되므로, `maybeWrap`의 "필터 → 시간 범위" 도출을
`ChunkReadBounds.of(filter, metadata)`로 추출해 파티션별로 씁니다.

### 1.2 의미론

- **페이징** — 마지막 행이 청크 전용 파티션 K의 합성 행이어도, 다음 페이지가 `[K, end]`에서 열거로 K를
  다시 찾아 `forPaging` 필터로 이어 줍니다.
- **DISTINCT** — 파티션당 1행이므로 청크 전용 파티션은 창 하나만 디코드합니다.
- **count(*) / 집계** — `AggregationQueryPager`가 같은 경로를 탑니다. 청크 헤더 통계로 세는 최적화는 후속.
- **CL** — 청크 열거와 읽기는 `isQuorumStrength(userCl) ? userCl : policy.consistency`(정책 없으면
  `LOCAL_QUORUM`). 쿼럼 쓰기와 겹쳐야 파티션 전체가 누락되지 않습니다. 대가: Spark `LOCAL_ONE` 스캔도
  청크 쪽은 쿼럼 비용을 냅니다. 미결: 단일 파티션 경로는 사용자 CL 을 씁니다 — 맞출지 결정 필요.
- **실패** — 열거 실패·손상 청크·미지원 코덱은 쿼리 실패. 커버리지 UNKNOWN 이면 무조건 병합.

### 1.3 가드 축소

계속 거부: (a) 정책 옵션 `range_scan: "refuse"`(1단계 기본값) 또는 전역 JMX 스위치 꺼짐,
(b) non-key `RowFilter`·인덱스(SAI 는 청크를 인덱싱하지 않음), (c) `transactional_mode != off`
(`executeAccord`는 `DataResolver`를 우회). 내부 경로는 지금처럼 핫 행만 읽습니다.

### 1.4 기각한 대안

- **레플리카 측 병합** — 레플리카마다 결과가 달라 다이제스트 불일치, 읽기 복구가 합성 행을 베이스에 써서 되살림.
- **`RangeCommands.partitions` 위에서 병합** — 이미 limit 으로 잘리고 필터를 거친 스트림이라 LIMIT·페이징이 깨짐.
- **"청크 DISTINCT + 키별 단일 파티션 읽기" 재작성** — N+1, 레인지 페이저·SRP 의미론 상실.

### 1.5 구현 단계

0. 리팩터(동작 변화 없음): `ChunkReadBounds` 추출, `ChunkMergeUnfilteredIterator` 헬퍼 공개.
1. `ChunkRangeMergeIterator`, `ChunkKeyEnumerator`(prepared 1회, 페이징, 순서 검증).
2. `TransparentReads.maybeWrapRange` + `DataResolver.resolveInternal` range 분기.
3. `TieringPolicy.range_scan`, 전역 스위치, 축소된 `guardRangeRead`, 메트릭 `RANGE_SCAN_MERGED`.
4. 문서, `ci-timeseries-tests.sh` 등록.

### 1.6 시험

- 단위(`TransparentRangeReadTest`) — 같은 데이터의 비계층 쌍둥이 테이블을 오라클로: 전체 스캔·`count(*)`·
  `DISTINCT`·집계·프로젝션·DESC, `LIMIT n`, `PER PARTITION LIMIT k`, fetchSize 1/2/7/100(페이지 경계가 청크
  전용 파티션 안에 걸리는 경우 포함), 토큰 범위 16분할의 합집합 = 전체 스캔, 비용 카운터, 실패 경로, 계속 거부되는 형태.
- jvm-dtest(`TieredRangeScanDistributedTest`, 3노드 RF3) — 코디네이터별 ONE/QUORUM/ALL 결과 = 오라클, 읽기 복구
  역류 없음, SRP+LIMIT, 노드 2개 다운 → Unavailable, Spark 형태 토큰 분할의 합집합.
- docker 통합 — 계층화 후 `count(*)`가 계층화 전 값과 같음("20M 중 500" 회귀).

### 1.7 위험과 롤아웃

위험(큰 순서): 조인 순서 위반, 청크 전용 파티션 안의 페이징 재개, LIMIT×SRP, 비용(Spark 전체 스캔은 모든
청크를 디코드), 필터 우회. 롤아웃: 0단계 리팩터 → 1단계 테이블 옵션 opt-in(기본 refuse) → 2단계 node 41
soak 후 기본 merge → 3단계 필터·DISTINCT/count 최적화. 코디네이터 전용 변경이고 wire 변경이 없습니다.

## 2. 스키마 경합 P1-01

### 2.1 실제 구멍

서버 타임스탬프·맞는 시계라면 검사 뒤의 `ALTER` → 새 컬럼 쓰기의 writetime 은 `maxWt`보다 커서 톰스톤에
덮이지 않습니다. 진짜 구멍은 **이 노드의 TCM 뷰 지연**입니다. `ALTER`가 E 에 커밋되고 다른 코디네이터가
새 컬럼에 w 로 쓴 뒤, 뒤처진 이 노드가 옛 스키마로 창을 읽고 검사도 통과하고, 같은 창의 기존 컬럼이 그 뒤
갱신돼 `maxWt > w`가 되면 레인지 딜리트가 w 를 지웁니다.

### 2.2 채택안 — TCM 펜스 + skew 여유

`regularColumnsChanged` 검사 지점에서:

1. 마지막 펜스로부터 1초가 지났으면 최신 epoch 까지 로그를 받아 오고(`fetchLogAndWait`) `fenceMs = now`.
2. 펜스 뒤 로컬 스키마로 `regularColumnsChanged`를 재검사(컬럼 집합 비교 유지).
3. `maxWt >= fenceMs - skewMarginMs`(기본 2000ms)이면 그 창은 이번 사이클에 건드리지 않음(통계
   `windowsDeferredSchemaFence`).
4. 펜스 실패 → 그 테이블 사이클 중단, 삭제 없음.

논증: 펜스 이전에 커밋된 `ALTER`는 2에서 잡히고, 이후에 커밋된 `ALTER`의 새 컬럼 쓰기는
`w > fenceMs - skew > maxWt`라 덮이지 않습니다. 남는 위험은 skew 초과와 명시적 타임스탬프(가드가 거부).

### 2.3 기각한 대안

컬럼 지목 삭제(CQL 이 클러스터링 범위 컬럼 삭제를 허용하지 않고, 행 마커가 남아 freeze 가 회수하지 못함),
삭제 후 재검증·보상(덮인 셀은 복구 불가), 사이클 중 `ALTER` 거부(클러스터 락 필요, TCM 지연은 그대로),
레플리카 측 epoch 검사(wire 변경, 나중 쓰기가 미리 받은 톰스톤에 덮임).

### 2.4 시험

단위: seam `beforeSourceDeleteForTesting`에서 `ADD COLUMN` + 서버 타임스탬프 쓰기 → 값 생존, 다음 사이클
인코딩; 가짜 `fenceMs`로 창 연기; 펜스 예외 → 삭제 0. jvm-dtest: node1 의 TCM 복제를 막은 채 node2 에서
`ALTER ADD` + 새 컬럼 쓰기 → 기존 컬럼 갱신 → node1 `retier` → 필터 해제 후 새 컬럼 값 보존(수정 전 실패).
