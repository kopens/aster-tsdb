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

# tscol 전환 절차 — 그림자 테이블 계층화에서 컬럼형 동결 SSTable로

설계: [docs/superpowers/specs/2026-11-04-columnar-frozen-sstable-design.md](../../docs/superpowers/specs/2026-11-04-columnar-frozen-sstable-design.md).
이 문서는 운영 노드(41)와 2026.11 설치를 옮기는 순서다.

**이전 도구는 없다.** 그림자 테이블 계층화(재인코더, 투명 읽기, 쓰기 가드, `timeseries_tiering` 정책, 그림자 테이블 관리, `retier`·`tieringstatus`,
가상 테이블)와 `nodetool untier`는 이 릴리즈에서 전부 지워졌다. tscol이 이것을 대체하며, 옛 청크를 되돌려 넣는 경로는 두지 않는다
(2026-11-04 사용자 결정).

## 한눈에

| | 그림자 테이블 계층화 (r8까지) | tscol (이 릴리즈부터) |
| --- | --- | --- |
| 켜는 법 | 테이블 확장 `timeseries_tiering` 정책 | TSCS 옵션 `frozen_format = 'columnar'` |
| 압축되는 때 | `hot_window`(예: 12h) 지나 재인코더가 | 창이 닫히고 `freeze_after` 지나 동결될 때 |
| 압축본 위치 | 옆 테이블 `<table>__chunks` | 같은 테이블의 SSTable(파일 이름 `-tscol-`) |
| 원본 처리 | 범위 톰스톤으로 삭제 | 동결 컴팩션이 파일을 통째로 교체 |
| 동결된 데이터에 쓰기·삭제·TTL·필터·SAI | 일부 거부 | 전부 보통 Cassandra 동작 |
| 보존 | `cold_window` | TSCS `retention`(창 통째로) |

## 청크가 있는 설치에서 벌어지는 일

이 빌드로 올리면:

- **청크로 들어간 창이 조회에서 보이지 않는다.** 재인코더가 원본 행을 범위 톰스톤으로 지운 뒤라 베이스 표에는 그 창의 행이 없고, 청크를 합쳐 주던 투명 읽기는
  없어졌다. 아직 재인코딩되지 않은 창은 그대로 보인다. 오류는 나지 않고 그저 행이 없다.
- **`<table>__chunks` 파일은 디스크에 그대로 남는다.** 지워지지 않고, 새로 쓰이지도 않고, 공간은 계속 차지한다. r8은 이 파일을 읽을 수 있으니 필요하면 r8로 되돌려
  청크를 읽는다. 단 r8은 tscol 파일을 읽지 못하므로, 되돌릴 표에 `frozen_format = 'columnar'`을 켜기 전에 판단한다.
- **`__chunks`, `__chunk_coverage`, `__tags`는 이제 보통 표다.** `SELECT`도 `DROP TABLE`도 평범하게 된다. 베이스 표를 지워도 같이 지워지지 않고, 베이스 표 `TRUNCATE`가
  따라오지도 않고, `DROP` 순서 제한도 없다. 필요 없다고 판단되면 손으로 지운다. 지운 뒤에는 r8로도 그 데이터를 읽을 수 없다.
- **스키마의 `timeseries_tiering` 확장은 남아 있어도 무시된다.** 표는 정상으로 올라오고 값은 아무 일도 하지 않는다. TSCS 옵션 `tiering_aware_freeze`도 받아들이고 무시한다.
  떼고 싶으면 `ALTER TABLE ... WITH extensions = {}`.
- 쿼럼 하한, 콜드 쓰기 거부, 조건부 쓰기 거부, 파티션 키 없는 범위 스캔 거부 같은 옛 가드도 없어졌다. 모든 표가 보통 표처럼 답한다.

## 41 절차 (데이터 초기화 경로 — 2026-11-04 사용자 결정)

41은 tscol 후보가 오른 뒤 플랫폼 데이터를 초기화한다(새 ID 체계로 처음부터 다시 쌓음):

1. core 마이그레이션 변경(V9·V32의 `timeseries_tiering` 정책 → `frozen_format = 'columnar'`)이 먼저 들어간 플랫폼 이미지와 tscol 후보 배포.
2. `DROP KEYSPACE pp` — 그림자 표까지 같이 지워진다.
3. 표마다 `allow_auto_snapshot = true`라 DROP이 스냅샷을 남긴다. 버릴 데이터면 `nodetool clearsnapshot --all -- pp`.
4. 부팅 마이그레이션이 pp를 새로 만든다(TSCS `frozen_format = 'columnar'`). 커버리지 원장·그림자 표는 없다.

## 2026.11 설치(옛 계층화 데이터가 있는 노드) 절차

1. **먼저 판단한다.** 위 "청크가 있는 설치에서 벌어지는 일"대로 청크로 들어간 창은 이 빌드에서 안 보인다. 그 기간의 데이터가 필요한 설치는 올리기 전에
   고객과 합의한다. 지금 이 설치의 청크를 옮겨 주는 도구는 없다.
2. **스냅샷:** `nodetool snapshot -t pre-tscol pp` — 하드링크라 디스크를 거의 쓰지 않는다. `__chunks`도 함께 잡힌다. 숙성이 끝나면 `nodetool clearsnapshot -t pre-tscol pp`.
3. **이미지 배포**(플랫폼 이미지로만). 기동 확인: 노드 UN, ERROR 0, 계층화 표가 올라와 있고 최근 창이 조회됨.
4. **컬럼형 동결 켜기**(플랫폼 core 마이그레이션으로 — 스키마는 플랫폼 소관): 기존 compaction 옵션에 `'frozen_format': 'columnar'` 추가.
   `freeze_after`를 12h~1d로 낮추는 것도 같이 검토(gc_grace 86400 < freeze_after 유지). 이 단계부터는 r8로 그 표를 되돌릴 수 없다.
5. **관찰:** 다음 동결부터 `-tscol-Data.db` 파일이 생긴다(`ls <data>/pp/tm_tag_point-*/*tscol*`). 디스크 사용량, 조회 지연, ERROR, 동결 소요.
6. **3일 숙성 → 승격.**
7. 청크가 더는 필요 없다고 판단되면 `DROP TABLE pp.<table>__chunks`(`__chunk_coverage`, `__tags`도)로 공간을 돌려받는다.

## 플랫폼 쪽 변경

- 새 설치 마이그레이션(V9·V32 등)이 `timeseries_tiering` 정책을 거는 부분 → `frozen_format = 'columnar'`로 바꾼다. 정책만 남기면 오류는 없지만
  표에 압축이 전혀 걸리지 않는다.
- 2026.11 고객 설치 업그레이드: 위 1·2단계를 업그레이드 절차에 넣는다.
- `tm_asset_data_based_second`의 계층화 해제 마이그레이션(V33, hot_window 31d)은 이제 아무 효과가 없다. 그대로 둬도 안전하다.

## 알아둘 것

- 부분 스트리밍: 보내는 쪽은 Data.db 구간을 그대로 보내고, 받는 쪽이 헤더의 형식을 보고 tscol 본문을 풀어 tscol SSTable로 다시 쓴다.
- 커서 컴팩션은 tscol을 읽지도 쓰지도 않는다(자동으로 반복자 파이프라인).
- 포크 기본값 변경: ONE/LOCAL_ONE 로컬 읽기의 메모리 응답 한도를 512행·64KiB에서 20,000행·16MiB로 올렸다
  (`cassandra.data_response_in_memory_max_rows_cl_one`, `..._max_size_cl_one`). 하루치 조회가 옛 한도를 넘으면 페이지를 직렬화했다가
  바로 다시 풀었고, JFR에서 컬럼형 읽기 시간의 절반 이상이 거기 쓰였다. 최악 메모리는 16MiB × `concurrent_reads`(32면 약 512MiB)다.
  힙이 작은 설치는 JVM 옵션으로 낮춘다. QUORUM 등 다른 일관성 수준은 128행·16KiB 그대로다.
