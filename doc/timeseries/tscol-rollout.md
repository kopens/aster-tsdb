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

## 한눈에

| | 그림자 테이블 계층화 (r8까지) | tscol (이 릴리즈부터) |
| --- | --- | --- |
| 켜는 법 | 테이블 확장 `timeseries_tiering` 정책 | TSCS 옵션 `frozen_format = 'columnar'` |
| 압축되는 때 | `hot_window`(예: 12h) 지나 재인코더가 | 창이 닫히고 `freeze_after` 지나 동결될 때 |
| 압축본 위치 | 옆 테이블 `<table>__chunks` | 같은 테이블의 SSTable(파일 이름 `-tscol-`) |
| 원본 처리 | 범위 톰스톤으로 삭제 | 동결 컴팩션이 파일을 통째로 교체 |
| 동결된 데이터에 쓰기·삭제·TTL·필터·SAI | 일부 거부 | 전부 보통 Cassandra 동작 |
| 보존 | `cold_window` | TSCS `retention`(창 통째로) |

## 릴리즈 구성

- **릴리즈 N (이번):** tscol 형식 + TSCS `frozen_format` + `nodetool untier`. 옛 계층화 코드는 **남아 있다** — `untier`가 옮기는
  동안 아직 옮기지 않은 청크가 투명 읽기로 계속 보이게 하려는 것이다.
- **릴리즈 N+1:** 옛 계층화 코드(재인코더, 투명 읽기, 쓰기 가드, `timeseries_tiering` 정책, 그림자 테이블 관리, `retier`·`tieringstatus`,
  가상 테이블)를 지운다. 그 전에 모든 노드에서 `untier`가 끝나 있어야 한다.

## 41 절차 (데이터 초기화 경로 — 2026-11-04 사용자 결정)

41은 tscol 후보가 오른 뒤 플랫폼 데이터를 초기화한다(새 ID 체계로 처음부터 다시 쌓음). 그래서 41에는 `untier`가 필요 없다:

1. core 마이그레이션 변경(V9·V32의 `timeseries_tiering` 정책 → `frozen_format = 'columnar'`)이 먼저 들어간 플랫폼 이미지와 tscol 후보 배포.
   정책을 거는 마이그레이션이 남아 있으면 이 판에서는 옛 계층화가 다시 켜진다.
2. `DROP KEYSPACE pp` — 키스페이스 DROP에는 계층화 가드가 없어 그림자 표까지 같이 지워진다. 표 단위 `DROP TABLE`은 그림자 표가 있으면
   거부되고, `TRUNCATE`는 정책을 남긴다.
3. 표마다 `allow_auto_snapshot = true`라 DROP이 스냅샷을 남긴다. 버릴 데이터면 `nodetool clearsnapshot --all -- pp`.
4. 부팅 마이그레이션이 pp를 새로 만든다(TSCS `frozen_format = 'columnar'`). 커버리지 원장·그림자 표는 없다.

`untier` 리허설은 41 데이터 **사본**으로만 한다(고객판 절차 검증용).

## 2026.11 설치(옛 계층화 데이터가 있는 노드) 절차

1. **릴리즈 N 이미지 배포**(플랫폼 이미지로만). 기동 확인: 노드 UN, ERROR 0, 두 계층화 테이블의 `system_views.timeseries_tiering`이 여전히 보임.
2. **스냅샷:** `nodetool snapshot -t pre-untier pp` — 하드링크라 디스크를 거의 쓰지 않는다. `untier`가 끝나고 숙성이 끝나면
   `nodetool clearsnapshot -t pre-untier pp`.
3. **작은 테이블부터:** `nodetool untier pp tm_asset_data_based_second -r 50000`
   - 출력: `restored N rows from M chunks; shadow tables dropped`. `dropped`가 아니면(청크가 남음) 원인을 로그에서 보고 다시 실행 — 멱등이다.
   - 확인: `SELECT count(*)`로 몇 태그의 하루치가 이전과 같은지, `pp.tm_asset_data_based_second__chunks`가 없어졌는지.
4. **`nodetool untier pp tm_tag_point -r 50000`** — 41 기준 수 시간. 같은 확인.
5. **컬럼형 동결 켜기**(플랫폼 core 마이그레이션으로 — 스키마는 플랫폼 소관): 기존 compaction 옵션에 `'frozen_format': 'columnar'` 추가.
   `freeze_after`를 12h~1d로 낮추는 것도 같이 검토(gc_grace 86400 < freeze_after 유지).
6. **관찰:** 다음 동결부터 `-tscol-Data.db` 파일이 생긴다(`ls <data>/pp/tm_tag_point-*/*tscol*`). 디스크 사용량, 조회 지연, ERROR, 동결 소요.
7. **3일 숙성 → 승격.**

## 플랫폼 쪽 변경

- 새 설치 마이그레이션(V9·V32 등)이 `timeseries_tiering` 정책을 거는 부분 → `frozen_format = 'columnar'`로 바꾼다(릴리즈 N+1부터는
  정책이 무시된다).
- 2026.11 고객 설치 업그레이드: 이미지 교체 직후 3·4단계(`untier`)를 업그레이드 절차에 넣는다.
- `tm_asset_data_based_second`의 계층화 해제 마이그레이션(V33, hot_window 31d)은 `untier`가 정책을 떼고 그림자 테이블까지
  지우므로 2단계(33일 뒤 DROP)가 필요 없어진다. V33은 정책이 없으면 no-op이라 그대로 둬도 안전하다.

## 알아둘 것

- 되살린 행에는 TTL이 없다(청크에도 없었다). 보존은 TSCS `retention`이 창 단위로 한다.
- 되살린 행의 writetime은 `max_row_writetime + 1`(청크가 갖고 있던 근사값). 원래 정확한 셀별 writetime은 청크화 때 이미 없어졌다.
- tscol SSTable은 **통째 스트리밍만** 된다(부분 스트리밍은 명확한 오류). 41은 단일 노드라 해당 없음. 다중 노드 지원은 P3.
- 커서 컴팩션은 tscol을 읽지도 쓰지도 않는다(자동으로 반복자 파이프라인).
- 포크 기본값 변경: ONE/LOCAL_ONE 로컬 읽기의 메모리 응답 한도를 512행·64KiB에서 20,000행·16MiB로 올렸다
  (`cassandra.data_response_in_memory_max_rows_cl_one`, `..._max_size_cl_one`). 하루치 조회가 옛 한도를 넘으면 페이지를 직렬화했다가
  바로 다시 풀었고, JFR에서 컬럼형 읽기 시간의 절반 이상이 거기 쓰였다. 최악 메모리는 16MiB × `concurrent_reads`(32면 약 512MiB)다.
  힙이 작은 설치는 JVM 옵션으로 낮춘다. QUORUM 등 다른 일관성 수준은 128행·16KiB 그대로다.
