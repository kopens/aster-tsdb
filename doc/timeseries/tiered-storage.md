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

# 계층형 저장 (Tiered Storage): 청크 스토어 + 백그라운드 재인코더

시계열 테이블의 오래된(닫힌) 행을 백그라운드에서 **컬럼 지향 청크**로 압축해 섀도 테이블
`<테이블>__chunks`로 옮기고, 원본 행은 삭제하는 서버 내장 계층화 엔진입니다. 최근 데이터(핫 구간)는
행 단위로 그대로 남아 쓰기·조회 모두 기존과 동일하고, 오래된 데이터는 행당 수 바이트 수준으로
압축된 청크로 보관됩니다. 청크 1개는 한 창의 타임스탬프 축 하나에 **일반 컬럼 전부**를 컬럼별 독립
섹션으로 담습니다(§3.1.2). 페이로드 포맷은 **청크 포맷 v4** 하나이며, 바이트 레이아웃·블록
인코딩·결정성 규칙은 [chunk-format-v4.md](chunk-format-v4.md)가 규범입니다. `double` 컬럼에 쓰이는
ALP의 압축 특성은 [bake-off 결과](codec-bakeoff.md)를 참고하세요.

> **투명 읽기(SP3) 포함**: 베이스 테이블 `SELECT`가 핫 로우와 청크 디코드 로우를 **자동 병합**해
> 돌려줍니다 — 애플리케이션은 압축의 존재를 모릅니다. 파티션(태그) + 시간 범위/포인트 질의,
> 집계(`avg`/`count`/`time_bucket` GROUP BY), gap-fill, `LIMIT`/`ORDER BY DESC` 모두 핫·콜드에
> 걸쳐 동작합니다. `__chunks` 직접 조회(아래 §3)는 이제 운영·디버그 용도입니다.
> 페이징도 그대로 동작합니다 — 한 페이지를 넘는 병합 질의는 (예전과 달리) 실패하지 않고, `LIMIT`과
> short-read protection이 병합된 결과에 적용되어 모든 행을 돌려줍니다.
> 제한: ① 디코드 로우의 `writetime(value)`은 청크의 `max_row_writetime` 근사값입니다.
> ② 손상 청크는 경고와 함께 건너뛰고 나머지 데이터를 제공합니다 — 반면 제거된 포맷 버전
> (v1/v2/v3)의 청크는 건너뛰지 않고 쿼리를 실패시킵니다(§2.2).

## 1. 대상 스키마 — 시간으로 클러스터링된 아무 테이블

계층화는 **시간축이 하나인 시계열 테이블이면 형태를 가리지 않습니다.** 지원되지 않는 형태에 정책을
걸면 60초 스위프마다 사유를 밝힌 ERROR 로그를 남기고 건너뜁니다.

**지원:**

| 요소 | 조건 |
| --- | --- |
| 컴팩션 | **`TimeSeriesCompactionStrategy`(또는 그 하위 클래스)** — 아래 거부 표 첫 줄 |
| 파티션 키 | **개수 무관** — 복합 키(`PRIMARY KEY ((asset_id, date, hour), ts)`) 가능. 청크 테이블이 전체 파티션 키를 그대로(이름·타입·순서) 미러링합니다 |
| 클러스터링 | **정확히 1개**, 타입 `timestamp` (`ASC`/`DESC` 무관) |
| 일반 컬럼 | **개수·타입 무관** — `text`/`int`/`bigint`/`double`/`boolean`/`blob`/`uuid`/`frozen<...>` 등 |
| static 컬럼 | **개수·타입 무관** (비frozen 컬렉션도 가능). static 셀은 청크화 대상이 아니며, 재인코더의 클러스터링 레인지 딜리트가 static을 건드리지 않으므로 그대로 보존됩니다 |
| 보조 인덱스 | **static 컬럼에 걸린 인덱스만** 무방 (예: static `asset_id`의 SAI) — static 인덱스 엔트리는 `Clustering.STATIC_CLUSTERING`에 있어 재인코더가 지우는 클러스터링 레인지 밖입니다 |

**거부 (각각 사유를 명시한 ERROR 로그):**

| 형태 | 사유 |
| --- | --- |
| 컴팩션이 **TSCS가 아닌** 테이블(UCS·STCS·LCS 등) | 재인코더는 인코딩한 행을 레인지 톰스톤으로 지웁니다. 그 톰스톤과 가려진 행을 한 번에 합쳐 지우는 것은 TSCS의 동결뿐입니다(`tiering_aware_freeze`는 청크가 창을 다 덮으면 곧바로 동결합니다). 다른 전략에서는 가려진 행이 디스크에 남아, 티어 구간을 읽을 때마다 읽고 버리게 됩니다 — 2026-09-24 41번에서 티어 구간 읽기가 타임아웃 난 바로 그 상태입니다. 모양과 무관하게 먼저 검사하며, `ALTER TABLE ... WITH compaction = {'class': 'TimeSeriesCompactionStrategy', ...}` 한 번으로 풀립니다 |
| `counter` 컬럼 | 재인코더는 행을 삭제 후 재삽입하는데, 삭제된 카운터는 영구히 다시 쓸 수 없습니다 — 정합성 정지 조건이지 한계가 아닙니다 |
| 비frozen 컬렉션 **일반** 컬럼 | 멀티셀 값(셀·타임스탬프가 원소별로 존재)은 청크의 행 단위 불투명 바이트로 표현할 수 없습니다. `frozen<...>`으로 감싸면 지원됩니다 |
| 클러스터링이 0개·2개 이상이거나 `timestamp`가 아닌 경우 | 청크가 인코딩할 시간축이 없습니다 |
| **static이 아닌 컬럼**에 걸린 보조 인덱스(SAI 포함) | 인덱스 엔트리는 베이스 **행 단위**입니다. 재인코딩된 행이 삭제되면 엔트리도 사라져 인덱스 질의가 `hot_window`보다 오래된 데이터를 조용히 누락합니다. 일반 컬럼뿐 아니라 **클러스터링 컬럼**(`CREATE INDEX ON t(ts)`는 CQL상 허용됩니다)과 **복합 파티션 키의 구성 컬럼**도 마찬가지입니다 |
| 이 테이블 위의 **머티리얼라이즈드 뷰** | 레인지 딜리트가 뷰까지 전파되지만 투명 읽기는 **베이스 테이블만** 복원하므로, 뷰는 `hot_window` 이전 이력을 아무 에러 없이 영구히 잃습니다 |
| 청크 테이블 예약어와 겹치는 파티션 키 이름 | `window_start`/`codec`/`samples`/`max_row_writetime`/`payload` — 미러링 시 같은 이름이 두 번 선언됩니다 |
| `transactional_mode`이 `off`가 아닌 테이블 | Accord 트랜잭션 읽기(`TxnNamedRead.performLocalKeyRead`)는 `ReadCommand`를 로컬에서 그대로 실행하며 투명 읽기 병합을 **거치지 않습니다**. Accord를 켜는 모든 값(`off` 이외 전부, `mixed_reads`는 평범한 SERIAL 읽기까지 그리로 보냅니다)이 해당됩니다 |

거부는 **그 테이블의 계층화 전체**를 멈추며, 콜드 청크 만료도 함께 멈춥니다(§1.3). `transactional_mode`
줄은 특히 함정이 많으니 §1.2를 반드시 읽으십시오.

아래는 이 문서 전체에서 쓰는 실 운영 테이블입니다 — static 7개 + 일반 컬럼 8개(혼합 타입, frozen 맵
포함) + `DESC` 클러스터링. [docker/integration-test.sh](../../docker/integration-test.sh)가 릴리스
게이트에서 이 형태 그대로 계층화를 검증합니다.

```sql
CREATE TABLE pp.tm_tag_point (
    tag_id     text,
    timestamp  timestamp,
    area_id    text static, asset_id text static, line_id text static,
    opc_id     text static, site_id  text static, tag_name text static, type text static,
    attribute     frozen<map<text,text>>,        -- 항상 {} → CONSTANT
    error_code    int,                           -- 항상 0 → CONSTANT
    latency       int,                           -- 고엔트로피 작은 정수 → 블록 비트패킹
    quality       int,                           -- 항상 192 → CONSTANT
    value         text,                          -- 판독값의 문자열 사본 (사전/raw)
    value_boolean boolean,                       -- type=boolean 태그에서만 채워짐 (1비트 팩)
    value_numeric double,                        -- type이 숫자형일 때만 채워짐 (ALP)
    PRIMARY KEY (tag_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
```

**어느 값 컬럼이 판독값을 담는지는 static `type`이 정합니다.** `type=boolean`이면 `value_boolean`,
`type`이 숫자형(`long`/`double` 등)이면 `value_numeric`에 들어가고, 쓰이지 않는 쪽은 `null`입니다.
(`type=string`은 확인하지 못했습니다 — 두 타입 컬럼 모두 null일 것으로 **추정**됩니다.)

`type`이 **static**, 즉 태그 단위로 고정된 속성이라는 점이 여기서 그대로 이득이 됩니다. 청크 1개는
태그 1개 × 창 1개이므로, 한 청크 안에서 각 값 컬럼은 **전부 채워져 있거나 전부 비어 있거나** 둘 중
하나이지 섞이지 않습니다. 즉 어느 청크든 깨끗하게 한쪽 경우에 떨어집니다 — 쓰이는 쪽은 전용 코덱
(ALP / 1비트 팩)을 타고, 쓰이지 않는 쪽은 ALL_NULL로 **0바이트**입니다.

`value`(text)는 그 판독값의 **문자열 사본**입니다 — 같은 값이 타입 컬럼에 한 번, 텍스트로 또 한 번
저장됩니다(`value_numeric = 20.76` ↔ `value = '20.76'`). 조금 놀랍지만 실제 스키마의 성질이며,
아래 §3.3처럼 청크에서 값을 꺼낼 때 어느 쪽을 읽을지 결정하는 근거가 됩니다.

`DESC` 클러스터링은 산업 현장의 기본 관용구입니다(최신 데이터부터 읽는 조회가 압도적으로 많음).
투명 읽기의 경계 산술이 오름차순을 가정하면 **콜드 행 0개를 에러 없이** 돌려주는 버그가 되므로,
양쪽 방향 바운드(`timestamp <`/`>`)와 양쪽 정렬(`ORDER BY timestamp ASC`/기본 DESC)이 모두 통합
테스트에 고정돼 있습니다.

static은 청크화되지 않습니다 — 재인코더의 레인지 딜리트는 클러스터링 구간만 지우고
`Clustering.STATIC_CLUSTERING`을 건드리지 않으므로, 그 태그의 클러스터링 행이 **전부** 청크로
옮겨진 뒤에도 static은 베이스 테이블에 그대로 남습니다:

```sql
-- 창 전체가 청크로 옮겨진 뒤 (원본 행은 물리적으로 없음)
SELECT count(*) FROM pp.tm_tag_point
 WHERE tag_id='TAG-001' AND timestamp >= '2026-07-01 00:00:00+0000'
                        AND timestamp <  '2026-07-01 01:00:00+0000';   -- 투명 읽기로 원래 행 수

-- static은 계층화와 무관하게 그대로
SELECT site_id, tag_name, type FROM pp.tm_tag_point WHERE tag_id='TAG-001' LIMIT 1;
```

> 클러스터링 행이 하나도 없는(= 전부 청크로 갔고 청크도 지워진) 파티션에 `LIMIT`·클러스터링 제한
> 없이 `SELECT count(*)`를 하면 **1**이 나옵니다 — static만 있는 행이 남아 있기 때문입니다. 계층화
> 여부를 세어서 판단할 때는 반드시 클러스터링 범위를 함께 거세요.

재인코더는 **일반 컬럼 전부**를 청크 1개에 담습니다 — 창의 타임스탬프 축을 한 번만 저장하고, 컬럼마다
독립 섹션에 그 컬럼의 **직렬화 바이트 그대로** 넣습니다. `null` 셀은 `null`로 그대로 왕복하며(기본값으로
바뀌지 않습니다), 어떤 타입이든 담깁니다 (아래 §3.1.2 코덱 표). 일반 컬럼이 **하나도 없는** 테이블은
수용은 되지만 실제로는 아무것도 인코딩되지 않습니다 — 셀 writetime이 존재하지 않아 원본 삭제에 쓸
타임스탬프가 없기 때문이며, 이 경우 그 사실을 밝힌 WARN을 남깁니다(§5.1).

### 1.1 `default_time_to_live`와 `hot_window`

베이스 테이블에 `default_time_to_live`가 있고 `hot_window >= TTL`이면 **재인코더가 데이터를 볼 기회가
없습니다** (TTL이 먼저 지웁니다) — 계층화를 켜 두고도 아무것도 압축되지 않습니다. 이 조합은 거부되지
않고(행 단위 `USING TTL`이 테이블 기본값과 다를 수 있으므로) 두 값을 함께 밝힌 WARN을 남깁니다.
`hot_window`를 TTL보다 짧게 잡거나 `default_time_to_live`를 올리십시오.

> **⚠️ TTL은 청크화되면서 사라집니다.** 재인코더는 셀의 `WRITETIME`만 읽고 `TTL`은 읽지 않으며, 청크
> 포맷에도 TTL 자리가 없습니다. 복원된 행은 TTL 없는 셀로 돌아오므로, **`default_time_to_live`(또는
> 행별 `USING TTL`)로 데이터를 만료시키던 테이블은 청크로 옮겨진 순간 그 데이터가 영구 보존됩니다.**
> 청크화된 데이터의 유일한 보존 장치는 `cold_window`입니다 — TTL에 의존하고 있었다면 그와 같은 기간을
> `cold_window`에 반드시 설정하십시오. (TTL을 청크에 실어 나르는 것은 포맷 변경이 필요해 이월돼
> 있습니다.)

청크로 옮겨진 데이터에는 베이스 TTL이 더 이상 적용되지 않습니다 — `cold_window`가 유일한 보존
장치이며, 이것이 "압축해서 보존 기간을 늘린다"의 메커니즘입니다.

### 1.2 `transactional_mode`은 방어벽이지 해결책이 아닙니다

§1의 거부 표 마지막 줄(Accord를 켜는 `transactional_mode`)은 **이미 계층화된 테이블을 고쳐 주지
않습니다.** 운영자가 알아야 할 두 가지:

- **DDL 시점 게이트가 없습니다.** 스키마 적합성 판정(`TieringPolicy.unsupportedSchemaError`)은 재인코딩
  사이클에서만 호출되고 `ALTER TABLE` 검증 경로에는 걸려 있지 않습니다. 계층화된 테이블에
  `ALTER TABLE … WITH transactional_mode='full'`을 실행하면 **`ALTER`는 성공합니다.** 실패는 문장이
  아니라 다음 계층화 사이클의 ERROR 로그로만 나타나므로, 문장이 막아 줄 것이라 기대하지 마십시오.
- **그때까지 쓰인 청크는 Accord 읽기에 계속 보이지 않습니다.** 계층화가 멈춘다고 이미 청크로 옮겨진
  데이터가 돌아오지는 않습니다 — 그 데이터의 원본 행은 이미 삭제됐고, 트랜잭션 읽기는 청크를 병합하지
  않으므로 **`hot_window`보다 오래된 이력이 통째로 빠진 결과**를 조용히 돌려줍니다. 같은 행을
  평범한(비 Accord) `SELECT`로 읽으면 정상적으로 병합돼 나옵니다 — 같은 데이터가 읽는 경로에 따라
  다르게 보인다는 뜻입니다.

Accord **쓰기**는 안전합니다. `CQL3CasRequest.createWriteFragments`를 거쳐 콜드 불변성 가드(§5.1.2)에
그대로 걸리므로, 콜드 구간을 지우는 트랜잭션 쓰기는 거부됩니다. 구멍은 읽기 쪽 하나뿐이지만, 그것이
바로 계층화가 존재하는 이유인 보장입니다.

> **운영 규칙: 한 번이라도 계층화된 적이 있는 테이블에는 트랜잭션 모드를 켜지 마십시오.** 되돌리는
> 방법은 `transactional_mode = 'off'`로 되돌려 계층화를 재개시키는 것뿐이며, 그 사이 Accord 읽기가
> 돌려준 불완전한 결과는 어디에도 기록되지 않습니다.

### 1.3 거부는 콜드 청크 만료도 함께 멈춥니다

§1의 표에 있는 **어떤 사유로든** 사이클이 거부되면 — 그리고 정책 JSON 파싱이 실패해도(§6.4) —
`runOnce`는 그 자리에서 반환합니다. `cold_window` 만료는 같은 함수의 **마지막** 단계이므로 **함께
건너뜁니다.**

데이터를 지키는 쪽으로 실패하는 것이라 유실은 없지만, 겉으로는 **보존이 고장 난 것처럼 보입니다**:
`chunks_expired`가 0에 머무르고, 청크 테이블은 `cold_window`가 지난 창을 계속 들고 있으며, 디스크는
계속 자랍니다. 보존이 전진하지 않을 때 용량부터 의심하지 말고 그 테이블의 ERROR 로그와
`tieringstatus`를 먼저 보십시오 — 원인은 거의 항상 스키마·정책 거부입니다.

## 2. 정책 설정 — `timeseries_tiering` 테이블 확장

정책은 테이블의 `extensions` 맵에 `timeseries_tiering` 키로 저장된 JSON 문서입니다. JSON을
**그대로 문자열로** 넣으면 됩니다 — CQL 한 줄이면 끝입니다:

```sql
ALTER TABLE pp.tm_tag_point WITH extensions = {
  'timeseries_tiering': '{"hot_window":"2d","chunk_window":"1d","cold_window":"3650d","interval":"1h"}'
};
```

`extensions`는 스키마상 blob 맵이라 원래는 hex 리터럴만 받지만, 이 포크는 평문 문자열을 UTF-8
바이트로 저장한다(`TableAttributes.parseExtensionValue`). `0x`로 시작하는 값만 hex 블롭으로
해석하므로 기존 hex 표기(`0x7b22...`)도 그대로 유효하다.

정책 해제는 `extensions = {}`로 맵을 비우면 됩니다 — **새 인코딩이 멈출 뿐, 이미 청크에 들어간
데이터는 계속 병합되어 보입니다** (§4.5). 설정된 값은
`SELECT extensions FROM system_schema.tables WHERE keyspace_name=? AND table_name=?`로 확인합니다.

### 2.1 필드

| 키 | 필수 | 기본값 | 의미 |
| --- | --- | --- | --- |
| `hot_window` | **필수** | — | 이 나이보다 젊은 행은 건드리지 않음 (행 단위 핫 구간). `chunk_window` 이상이어야 함 |
| `chunk_window` | | `1h` | 재인코딩 창의 고정 길이. 청크 1개 = 태그 1개 × 창 1개 (epoch 정렬) |
| `cold_window` | | 없음 | 지정 시, 이보다 오래된 청크는 통째로 삭제(보존 기한). `hot_window`보다 커야 함 |
| `consistency` | | `LOCAL_QUORUM` | 재인코더의 모든 읽기/쓰기/삭제에 쓰는 CL. **쿼럼 계열만 허용** — [5.3](#53-cl-쿼럼-하한) 참고 |
| `interval` | | `5m` | 이 테이블의 재인코딩 주기 (전역 스위프는 60초마다 돌며, interval이 지난 테이블만 실행) |

기간 값의 문법은 `<양의 정수><m|h|d>` (분/시/일)입니다. 알 수 없는 키, 규칙 위반(JSON 오류,
`hot_window < chunk_window`, `cold_window <= hot_window`, 쿼럼 미만 CL 등)은
`ALTER TABLE`/실행 시점에 거부됩니다.

### 2.2 코덱

고를 것이 없습니다. 컬럼 타입이 인코딩을 결정하고(§3.1.2), `double` 컬럼의 값 코덱은
**ALP/ALP-RD 하나뿐**입니다 — 양자화된 워크/주기 신호(소수점이 잘린 실제 산업 센서값)에서
0.75~1.4 B/샘플이며, 값이 전혀 변하지 않는 컬럼은 코덱을 타기 전에 CONSTANT 플래그가 행 수와
무관하게 O(1) 바이트로 처리합니다. 페이로드에는 버전 바이트가 있어 디코딩은 자동입니다.

> `codec` 정책 필드는 존재하지 않습니다. 정책 JSON에 `codec` 키가 들어 있으면 `ALTER TABLE`이
> 그 키를 지목해 거부하므로, 조용히 무시되는 일은 없습니다. 코덱 선택의 근거는
> [코덱 bake-off](codec-bakeoff.md) 참고.

> **페이로드 포맷은 v4 하나입니다** ([chunk-format-v4.md](chunk-format-v4.md)). 제거된 포맷
> (v1 Gorilla, v2 Chimp128, v3 컬럼 지향)의 페이로드를 만나면 `SELECT`는 조용히 건너뛰지 않고
> `UnsupportedChunkFormatException`으로 **실패합니다** — 손상 청크와 달리 전 청크에 해당하는
> 계통적 문제라, 건너뛰면 쿼리가 성공하면서 과거 데이터만 빠진 결과를 계속 돌려주게 됩니다.
> 그런 청크 테이블은 `DROP`하고 계층화를 다시 돌리는 것 외에 방법이 없습니다 — 인코딩 시점에
> 베이스 행이 이미 삭제됐으므로 **그 데이터는 복구되지 않습니다.** (v3→v4 변환 도구는 의도적으로
> 만들지 않았습니다 — [chunk-format-v4.md §10](chunk-format-v4.md).)

## 3. 청크 직접 조회 — 운영·디버그 용도

> SP3 투명 읽기가 켜진 지금은 베이스 테이블 `SELECT`가 청크를 자동 병합하므로 아래 패턴은
> 애플리케이션에 **불필요**합니다. 청크 자체를 점검할 때(압축률 확인, 손상 진단 등)만 사용하세요.

재인코딩된 창의 행은 베이스 테이블에서 물리적으로 **삭제**되지만, 투명 읽기 이전 관점에서 보면 핫 구간만
반환합니다. 오래된 데이터는 섀도 테이블을 직접 조회합니다.

### 3.1 섀도 테이블 스키마

`<베이스 테이블>__chunks`는 정책 첫 실행 시 자동 생성됩니다 (UCS `T4` 컴팩션):

| 컬럼 | 타입 | 의미 |
| --- | --- | --- |
| *(베이스 파티션 키 전체)* | 동일 | 베이스 테이블의 파티션 키 컬럼 **전부**를 이름·타입·순서 그대로 복제 (예: `tag_id text`, 또는 복합 키면 `(asset_id text, date text, hour int)` 세 컬럼 모두) |
| `window_start` | `timestamp` | 청크가 덮는 창의 시작 (클러스터링 키; 창 길이 = `chunk_window`) |
| `codec` | `tinyint` | 페이로드의 **첫 바이트(포맷 버전)를 그대로 복사한 값**. 현재 쓰이는 값은 `4`(청크 포맷 v4)뿐입니다 — 인코딩 후 페이로드에서 읽어 넣으므로 항상 실제 저장된 포맷을 가리킵니다. `1`·`2`·`3`은 제거된 포맷이며, 그런 페이로드를 읽으면 `UnsupportedChunkFormatException`이 전파됩니다(§2.2) |
| `samples` | `int` | 청크에 인코딩된 **행 수** (값 개수가 아닙니다 — 행 1개가 컬럼 N개를 가집니다) |
| `max_row_writetime` | `bigint` | 청크에 포함된 원본 행들의 **모든 컬럼**을 통틀어 최대 writetime (원본 삭제 타임스탬프이자 지각 병합 판정 기준) |
| `payload` | `blob` | 창 1개의 인코딩 결과: 공유 타임스탬프 축 + 베이스 테이블 **일반 컬럼별 독립 섹션** |

#### 3.1.1 커버리지 원장 `<베이스 테이블>__chunk_coverage`

청크 테이블과 **함께** 자동 생성되는 작은 테이블입니다. 읽기 경로가 "이 테이블의 콜드 데이터가
어디까지 뻗어 있는가"를 매 쿼리마다 전체 스캔 없이 알기 위한 유일한 근거입니다 (§4.5).

| 컬럼 | 타입 | 의미 |
| --- | --- | --- |
| `scope` | `text` | 항상 `'chunks'` (원장은 파티션 하나) |
| `node` | `text` | 청크를 쓴 노드 (클러스터링). 노드별 행을 두고 읽을 때 min/max로 합산하므로, 뒤처진 노드가 다른 노드의 주장을 **좁힐 수 없습니다** |
| `min_window_start` | `timestamp` | 그 노드가 인코딩한 가장 오래된 창 |
| `max_window_start` | `timestamp` | 그 노드가 인코딩할 수 있었던 가장 늦은 창의 상한 (사이클 cutoff) |
| `max_chunk_window` | `bigint` | 그 노드가 청크를 쓸 때 쓴 **역대 최대** `chunk_window` (밀리초) |

원장은 청크를 **쓰기 전에** 넓혀집니다. 그래야 어느 순간에도 원장이 청크 테이블보다 좁지 않고,
중간에 죽어도 커버리지가 실제보다 **넓게만** 남습니다(불필요한 청크 조회 한 번의 비용이지, 틀린
답이 아닙니다). `cold_window` 만료로 청크가 지워져도 원장은 **줄이지 않습니다** — 보수적인 방향
입니다. `min_window_start`는 기록·표시용이며 병합 판단에는 쓰지 않습니다(만료·백필로 인해 잠깐
실제보다 높을 수 있고, 그 값으로 병합을 건너뛰면 다시 데이터를 숨기게 됩니다).

#### 3.1.2 컬럼 타입별 인코딩

타입 코드는 **직렬화된 바이트를 어떤 방식으로 압축할지**만 고릅니다 — 바이트 자체는 바꾸지 않으므로
디코딩 결과는 원본 셀과 바이트 단위로 동일합니다. 블록 단위 인코딩(FOR/델타 비트패킹, ALP/ALP-RD,
사전 등)과 통계·presence의 세부는 [chunk-format-v4.md](chunk-format-v4.md)가 규범이고, 여기서는
타입 → 타입 코드 매핑만 요약합니다:

| 베이스 컬럼 타입 | 청크 타입 코드 |
| --- | --- |
| `double` | `DOUBLE` — 블록별 ALP 또는 ALP-RD |
| `boolean` | `BOOLEAN` — 1비트 팩 |
| `int` | `INT32` — 블록별 FOR/델타 비트패킹 |
| `date` | `DATE32` — 부호 없는 일수 (v4에서 `INT32`와 분리: 통계 비교 순서가 다릅니다) |
| `bigint`, `timestamp`, `time` | `INT64` (정규화 없음: 각각 원값·epoch millis·자정 이후 나노초) |
| `text`, `varchar`, `ascii` | `TEXT` — 사전 / 길이 접두 raw |
| **그 외 전부** | `OPAQUE` — **불투명 바이트**(직렬화 그대로) + 사전 — `blob`, `uuid`, `timeuuid`, `decimal`, `varint`, `inet`, `smallint`, `tinyint`, `float`, `duration`, frozen 컬렉션·UDT·튜플 |

`smallint`(2바이트)·`tinyint`(1바이트)·`float`(4바이트)를 굳이 불투명으로 두는 이유는, 고정폭 코드가
자기 폭으로 다시 직렬화하기 때문에(그리고 `boolean` 코드는 값을 0/1 비트로 접기 때문에) 폭이 다르면
왕복이 조용히 깨지기 때문입니다.

값이 전부 같은 컬럼은 디렉토리에 **한 번만** 저장되고 데이터 섹션이 0바이트가 되며(CONSTANT), 전부
`null`인 컬럼은 아무것도 저장하지 않습니다(ALL_NULL) — 행 수와 무관하게 O(1)입니다.

### 3.2 조회 CQL

```sql
-- [a, b) 구간의 콜드 데이터: 구간에 걸친 창들을 window_start 범위로 가져온다.
-- 첫 창은 a를 chunk_window 경계로 내림한 값부터 시작해야 a 직전 경계에 걸친 청크를 놓치지 않는다.
SELECT window_start, codec, samples, payload
FROM   pp.tm_tag_point__chunks
WHERE  tag_id = 'TAG-001'
  AND  window_start >= '2026-07-01 00:00:00+0000'   -- floor(a, chunk_window)
  AND  window_start <  '2026-07-08 00:00:00+0000';  -- b
```

청크 테이블은 베이스 테이블의 **파티션 키만** 미러링하므로 `tm_tag_point`의 static 7개는 여기
없습니다 — static은 애초에 청크화 대상이 아니고 베이스 테이블에 남아 있습니다(§1).

### 3.3 페이로드 디코딩 (JVM 클라이언트)

`payload`는 이 포크의 jar에 있는 컬럼 지향 디코더로 읽습니다. 두 번째 인자는 **프로젝션**입니다 —
`null`이면 전체 컬럼, 집합을 주면 그 컬럼들의 데이터 섹션만 디코딩하고 나머지는 건너뜁니다:

```java
import org.apache.cassandra.db.timeseries.ColumnarChunkCodec;
import org.apache.cassandra.db.timeseries.ColumnarCursor;

ColumnarCursor cursor = ColumnarChunkCodec.cursor(payload, Set.of("value", "latency"));
while (cursor.advance())
{
    long ts = cursor.timestamp();                   // epoch millis
    ByteBuffer v = cursor.getBytes("value");        // null = 그 행에서 null인 셀
    ByteBuffer l = cursor.getBytes("latency");
    if (v != null)
        handle(ts, UTF8Type.instance.compose(v),    // value는 text — 숫자로 쓰려면 직접 파싱
                   l == null ? null : Int32Type.instance.compose(l));
}
```

프로젝션을 `{"value", "latency"}`로 좁히면 나머지 6개 컬럼의 데이터 섹션은 디코드하지 않습니다.
수치형 태그(static `type`이 `int`/`long`/`float`/`double`)를 읽을 때는 `value_numeric`을 프로젝션에
넣고 `DoubleType.instance.compose(...)`로 복원하면 됩니다.

`value`는 **`text`**라는 점에 주의하세요 — 숫자처럼 보여도 바이트는 UTF-8 문자열이므로, 숫자로
다루려면 클라이언트에서 직접 파싱해야 합니다. 계층화는 어느 컬럼이든 바이트 그대로 왕복시킬 뿐,
타입을 바꿔 주지 않습니다.

`getBytes`가 돌려주는 것은 **베이스 컬럼 타입의 직렬화 바이트 그대로**이므로, 그 컬럼의
`AbstractType.compose(...)`로 그대로 복원하면 됩니다. `hasColumn`은 그 컬럼이 이 청크(그리고 프로젝션)에
있는지, `isNull`은 현재 행에서 값이 없는지를 알려줍니다 — 청크가 쓰인 뒤 `ALTER TABLE ADD`된 컬럼은
없는 컬럼으로, `DROP`된 컬럼은 남아 있는 채로 보일 수 있습니다.

창 경계에 걸친 요청이라면 디코딩 후 `timestamp`로 한 번 더 필터하세요. 집계 대시보드용이라면
`samples`·`window_start`만으로도 창 단위 카운트/커버리지 확인이 가능합니다.

## 4. 운영

### 4.1 nodetool

```bash
nodetool retier <keyspace> <table>   # 지금 즉시 한 사이클 실행 (동기, interval 무시)
nodetool tieringstatus               # 정책이 있는 모든 테이블의 상태 표
```

- `retier`는 해당 테이블의 실행이 이미 진행 중이면(스위프든 다른 retier든) 오류로 알려줍니다.
- `retier`는 **건너뛴 태그가 하나라도 있으면 실패(0이 아닌 종료 코드)** 합니다. 타임아웃·복제본
  부족·읽을 수 없는 기존 청크 등으로 어떤 태그를 끝내지 못했다는 것은 "요청한 일을 다 하지 못했다"는
  뜻이고, 그 태그의 원본 행은 손대지 않았으므로 유실은 없지만 테이블은 cutoff까지 계층화되지
  **않았습니다**. 원인을 로그에서 확인하고 고친 뒤 다시 실행하세요. (백그라운드 스위프는 실패하지
  않고 WARN을 남긴 뒤 다음 틱에 재시도합니다.)
- `tieringstatus` 출력: Keyspace, Table, Interval (ms), Last Run At, Windows Encoded,
  Rows Encoded, Late Merges, Chunks Expired, **Tags Skipped**.

### 4.2 가상 테이블

같은 데이터를 CQL로: 정책 필드 + 마지막 완료 실행의 통계입니다.

```sql
SELECT * FROM system_views.timeseries_tiering;
-- keyspace_name, table_name, hot_window_ms, chunk_window_ms, cold_window_ms(-1=미설정),
-- interval_ms, last_run_at(-1=실행 전), windows_encoded, rows_encoded,
-- late_merges, chunks_expired, tags_skipped
```

`tags_skipped`가 0이 아니면 **마지막 완료 사이클이 cutoff까지 다 인코딩하지 못했다**는 뜻입니다 —
다른 카운터가 시사하는 만큼 계층화되어 있지 않습니다.

### 4.3 MBean

JMX `org.apache.cassandra.db:type=TieredStorage` — `retier(keyspace, table)`,
`statusRows()` (nodetool 두 명령의 백엔드).

### 4.4 알아둘 것

- 통계는 **노드 로컬·인메모리**이며 마지막 완료 실행 기준입니다. 재시작하면 초기화됩니다.
- 다중 노드에서는 각 노드가 **자기 primary 토큰 레인지의 태그만** 재인코딩해 작업을 분할합니다.
- 전역 스위프는 60초 주기 1개이며, 테이블별 `interval`이 실제 실행 빈도를 결정합니다.

### 4.5 정책을 바꾸면 이미 인코딩된 데이터는 어떻게 되나

**규칙 한 줄: 정책 변경은 앞으로의 인코딩만 바꾸며, 이미 청크에 들어간 데이터의 가시성에는 아무
영향이 없습니다.**

재인코딩된 창의 원본 행은 **삭제**되므로 청크가 유일한 사본입니다. 따라서 투명 읽기가 "청크를
병합할지"를 판단할 때 물어야 하는 질문은 오직 **"이 쿼리 구간 아래에 실제로 청크가 존재하는가"**
뿐이며, "지금 정책이 이 구간을 콜드라고 부르는가"가 아닙니다. 후자로 판단하면 아래 세 가지 평범한
운영 행위가 각각 이미 인코딩된 데이터를 **조용히 숨깁니다**.

| 운영 행위 | 잘못된(정책 기반) 동작 | 현재 동작 |
| --- | --- | --- |
| `hot_window`를 늘림 (예: `1h` → `24h`) | 새 핫 구간 안에 들어온 과거 데이터는 "핫"으로 간주되어 병합을 건너뜀 → 원본 행은 이미 지워졌으므로 **빈 결과** | 실제 커버리지 기준으로 판단하므로 그대로 반환됩니다 |
| `timeseries_tiering` 확장 제거 | 정책이 없으니 병합 자체를 하지 않음 → **모든 콜드 이력이 사라짐** | 청크 테이블에 내용이 있는 한 계속 병합하고, 그 구간의 삭제도 계속 거부합니다(§5.1.2). 확장 제거는 **새 인코딩을 멈출 뿐**입니다 |
| `chunk_window`를 줄임 (예: `24h` → `1h`) | 병합이 현재 창 하나만큼만 과거를 조회 → `window_start`가 더 앞에 있는 **넓은 레거시 청크를 못 찾음** | 기록된 **역대 최대 `chunk_window`** 만큼 되돌아보므로 찾습니다 |

정책 JSON이 깨져 파싱에 실패하는 경우도 같습니다 — 오타는 "데이터를 숨기라"는 지시가 아니므로,
확장이 없는 것과 동일하게 커버리지 기준으로 병합합니다.

콜드 데이터를 **실제로 없애는** 방법은 두 가지뿐입니다: 정책의 `cold_window`(권장), 또는 청크
테이블을 명시적으로 `DROP TABLE` 하는 것. 후자는 되돌릴 수 없습니다(원본 행은 이미 없습니다).

> 성능: 빠른 경로는 그대로 있습니다. 쿼리 구간이 커버리지 상단보다 위면 청크 테이블을 **아예 읽지
> 않습니다**. 커버리지는 테이블당 캐시되며 최대 60초마다 갱신되고, 청크 테이블이 아직 없는
> 테이블(= 정책을 걸었지만 첫 사이클 전, 그리고 모든 비계층화 테이블)은 캐시 조회 한 번으로
> 끝납니다.

> 병합이 필요한 질의에서도 디코드는 창 단위로 게으릅니다: 창 목록은 payload 없이 나열해 두고,
> 방출 순서대로(DESC면 최신 창부터) 한 창씩 payload를 페치·디코드하므로 `LIMIT`이 차면 나머지
> 창은 읽지 않습니다. 청크 **하나**의 디코드는 즉시(eager) 전체를 캡처합니다 — 손상·미지원 포맷을
> 행을 방출하기 **전에** 판정하기 위해서입니다. 행 **조립**은 당겨질 때 한 행씩 일어납니다.

> 청크 읽기가 **실패**하면(타임아웃, 복제본 부족, 톰스톤 과다) 쿼리는 핫 행만 담아 성공하는 대신
> **실패합니다**. 원본 행이 삭제된 구간을 "성공했지만 콜드가 빠진" 결과로 돌려주는 것은 느린 것이
> 아니라 틀린 것이기 때문입니다.

### 4.6 파킹된 컴팩션 창은 그 시간대의 컴팩션을 멈춥니다 (계층화는 멈추지 않습니다)

시계열 컴팩션 전략(TSCS)은 **진척을 낼 수 없는 창을 파킹(park)** 합니다 — 프리즈↔스플릿이 서로를
되돌리며 무한히 반복하는 것을 끊기 위한 장치입니다. 파킹된 창은 프리즈·스플릿 후보 선정에서 의도적으로
제외되므로 백로그와 `getEstimatedRemainingTasks()`에서도 빠지고, **할 일이 없는 테이블과 겉모습이
똑같습니다.**

증상은 **그 시간대의 sstable이 하나로 합쳐지지 않고 그대로 남는 것**입니다 — 그 범위를 읽을 때
read amplification이 내려가지 않고, 디스크도 회수되지 않습니다. 다른 창은 정상 진행하므로 "테이블
전체가 멈춤"이 아니라 **특정 시간대만 영원히 컴팩션되지 않음**입니다.

> **계층화(청크 압축)는 이것과 무관하게 계속 돕니다.** 재인코더는 프리즈 이벤트가 아니라 자체
> 스케줄러(60초 주기 sweep)로 돕니다 — `db/timeseries/` 어디에도 `WindowFrozenListener` 참조가
> 없습니다. 파킹된 창의 행도 `hot_window`를 지나면 평소대로 청크로 옮겨집니다.
>
> `WindowFrozenListener` 자체는 TSCS가 제공하는 확장점이지만 **현재 운영 코드에서 등록하는 곳이
> 없습니다**(테스트만 등록합니다). 즉 지금은 프리즈 뒤에 매달린 후속 처리가 존재하지 않습니다.
> 나중에 무언가를 이 훅에 걸 경우, 파킹된 창에서는 그 훅이 영원히 발화하지 않는다는 점을
> 설계에 반영해야 합니다.

확인 경로는 **JMX 하나뿐입니다.** 테이블 자신의 MBean
`org.apache.cassandra.db:type=Tables,keyspace=<ks>,table=<table>`의 두 속성입니다:

| 속성 | 내용 |
| --- | --- |
| `ParkedTimeSeriesWindows` | 창 시작(epoch millis) → 그 창이 파킹된 채로 물고 있는 sstable 목록 |
| `FarFutureTimeSeriesSSTables` | `max_future_window` 밖이라 모든 자동 경로에서 제외된 sstable 목록 |

둘 다 **비어 있는 것이 정상**이며, 시계열 컴팩션을 쓰지 않는 테이블에서는 항상 비어 있습니다.

> **`nodetool` 서브커맨드는 없습니다.** 두 값은 `ColumnFamilyStoreMBean`의 속성일 뿐 nodetool로 노출되지
> 않으므로, 감시하려면 JMX 속성을 직접 폴링해야 합니다. 그 외의 유일한 신호는 파킹되는 순간 남는 WARN
> 한 줄이며(해결 방법도 그 WARN에 적혀 있습니다), 그 뒤로는 조용합니다. 파킹 해제는 그 창에 변화가
> 생기면 자동으로 이뤄집니다.

## 5. 불변식 — 왜 데이터가 유실되지 않는가

### 5.1 레인지 딜리트의 writetime 규칙 (지각 데이터 생존 원리)

한 창을 재인코딩한 뒤 원본 행을 지우는 레인지 딜리트는 항상
`DELETE ... USING TIMESTAMP <maxWt>` — **그 사이클이 청크에 인코딩한 행들의 최대 writetime** —
으로 발행됩니다. 사이클이 창을 읽은 **이후** 도착한 지각(late) 행은 정의상 더 새로운 writetime을
가지므로 이 톰스톤보다 새롭고, 따라서 살아남습니다. 다음 사이클이 그 행을 발견해 기존 청크와
**병합 재인코딩**하고(`samples` 증가, `late_merges` 카운트), 그때의 새 maxWt로 다시 지웁니다.
즉 "인코딩 안 된 행이 지워지는" 시점은 존재하지 않습니다.

`WRITETIME`은 **컬럼 단위**이므로, 여기서 말하는 최대치는 그 창에서 읽은 **모든 행 × 모든 일반 컬럼**의
셀 writetime을 통틀어 취한 값입니다(교체 대상 청크의 `max_row_writetime`도 함께 고려). 한 컬럼만
보고 지우면 다른 컬럼이 더 나중에 갱신된 행이 반쯤 남습니다.

> **불변식의 내재적 한계**: 이 논증은 "나중에 쓰인 행은 더 큰 writetime을 가진다"에 기대고 있습니다.
> 클라이언트가 `USING TIMESTAMP`로 타임스탬프를 직접 지정하거나, 코디네이터 간 시계가 어긋난 경우,
> 사이클이 창을 읽은 **뒤** 도착한 쓰기가 `maxWt` 이하의 타임스탬프를 가질 수 있고 그러면 톰스톤이
> 그 쓰기를 지웁니다. 계층화 대상 테이블에는 서버 타임스탬프를 쓰고, 과거 타임스탬프를 지정한
> 백필은 `hot_window` 안에서만 하십시오.

**셀 writetime이 하나도 없는 창은 통째로 건드리지 않습니다.** 일반 컬럼이 전부 `null`인 행(키만 넣은
`INSERT`, 셀이 전부 삭제·TTL 만료된 행)만 있는 창에는 안전하게 쓸 톰스톤 타임스탬프가 존재하지
않기 때문입니다 — 인코딩도, 삭제도 하지 않고 다음 사이클에 다시 시도하며, 실행 끝에 그 사실을 요약한
WARN을 남깁니다. 반대로 **일부** 컬럼만 `null`인 행은 정상이며 그대로 인코딩되고, 같은 창에 writetime을
가진 행이 하나라도 있으면 전부-`null` 행도 (존재 자체를 잃지 않도록) 함께 인코딩됩니다.

### 5.1.1 지각 행 병합은 **컬럼 단위**입니다

이미 청크에 들어간 타임스탬프에 대해 베이스 행이 다시 나타나면
(`UPDATE pp.tm_tag_point SET latency = 431 WHERE tag_id = 'TAG-001' AND timestamp = ?`),
그 행이 **실제로 셀을 가진 컬럼만** 청크 값을 덮어씁니다. 나머지 컬럼(`value`, `quality`, …)은
청크에 있던 값을 그대로
유지합니다 — CQL의 셀 단위 last-write-wins와 같은 규칙이며, 행 단위로 통째 교체하면 `UPDATE`가
언급하지 않은 컬럼이 전부 지워집니다. (같은 이유로 콜드 구간의 **삭제**는 병합으로 표현할 수 없어
아예 거부됩니다 — §5.1.2.)

### 5.1.2 콜드 데이터는 **불변**입니다 (삭제는 거부됩니다)

**콜드 경계보다 오래된 클러스터링을 톰스톤으로 지우는 쓰기는 거부됩니다.** 그 경계는 읽기 경로가
청크를 병합하는 경계와 **같은 값**입니다(§4.5): 정책이 걸려 있으면 `now - hot_window`와 **실제 청크
커버리지 상단** 중 더 나중 쪽, 정책이 없으면 커버리지 상단.

| 거부되는 쓰기 | 예 |
| --- | --- |
| 파티션 전체 삭제 | `DELETE FROM pp.tm_tag_point WHERE tag_id = ?` (클러스터링 경계가 없으므로 반드시 콜드 구간을 덮습니다) |
| 레인지 삭제 | `DELETE FROM pp.tm_tag_point WHERE tag_id = ? AND timestamp >= ? AND timestamp < ?` |
| 행 삭제 | `DELETE FROM pp.tm_tag_point WHERE tag_id = ? AND timestamp = ?` |
| 셀 삭제 | `DELETE latency FROM pp.tm_tag_point WHERE ...`, `UPDATE pp.tm_tag_point SET latency = null WHERE ...`, `INSERT ... VALUES (..., null)` |

static 컬럼(`site_id`, `tag_name`, …)에 대한 쓰기·삭제는 이 규칙 **밖**입니다 — 청크화 대상이 아니라
콜드 경계 판정에 걸리지 않으므로 언제든 갱신·삭제할 수 있습니다. 반대로 파티션 전체 `DELETE`는 static까지
지우려는 것이더라도 클러스터링 경계가 없어 거부되므로, static만 지우려면 `DELETE site_id FROM ...`처럼
컬럼을 지목하십시오.

**허용되는 것**: 핫 윈도 안에 완전히 들어가는 삭제는 평소와 똑같이 동작합니다. 그리고 콜드 클러스터링에
**실제 값을 쓰는 것**(지각 `UPDATE pp.tm_tag_point SET latency = 7 ...`, 과거 시각으로의 `INSERT`)도
그대로 허용됩니다 —
다음 사이클이 컬럼 단위로 청크에 병합합니다(§5.1.1). 거부되는 것은 콜드 데이터를 **지우는** 쓰기뿐입니다.

**왜 문서화가 아니라 거부인가.** 청크화된 창의 베이스 행은 이미 삭제됐고 청크가 유일한 사본입니다.
그 창에 톰스톤을 쓰면 투명 읽기의 병합에서 청크 행을 가려주긴 하지만, 그것은 `gc_grace_seconds`가
톰스톤을 수거할 때까지뿐입니다. 그 뒤에는 지운 데이터가 **조용히, 영구히 되살아납니다** — 타이머 달린
데이터 부활이라 문서로 넘길 수 없습니다. 콜드 데이터를 지우는 지원되는 방법은 `cold_window` 만료이며,
파티션을 통째로 없애야 한다면 베이스 테이블과 `<테이블>__chunks`를 **함께** 지우십시오.

재인코더 자신의 레인지 딜리트는 예외입니다 — 방금 청크에 복사한 행만 지우며, 계층화 내부 경로임을
알리는 같은 스레드 로컬 플래그로 이 검사를 우회합니다.

> **경계는 정책이 아니라 커버리지에 걸려 있습니다.** 읽기가 청크에서 되살려 주는 행은 쓰기가 지우기를
> 거부해야 하는 행이며, 이 두 판정은 하나의 함수(`ColdBoundary.coldBelowMs`)에서 나옵니다. 따라서:
>
> - **확장을 제거해도 이미 인코딩된 데이터는 계속 보호됩니다.** 예전에는 정책이 없으면 검사 자체를
>   건너뛰어, 확장을 지운 뒤의 콜드 `DELETE`가 통과했습니다. 그 톰스톤은 `gc_grace_seconds`가 수거할
>   때까지만 청크를 가리고, 그 뒤에는 지운 데이터가 조용히 되살아납니다. 확장 제거는 **새 인코딩을
>   멈출 뿐**이지 콜드 데이터를 지울 수 있게 만드는 스위치가 아닙니다. 정책 JSON이 깨진 경우도 같습니다.
> - **정책을 막 걸어서 재인코더가 아직 한 번도 돌지 않은** 테이블에서도 `now - hot_window`보다 오래된
>   데이터의 삭제는 거부됩니다(경계가 둘 중 **더 나중** 쪽이므로). 의도된 동작입니다 — 과거 데이터를
>   정리해야 한다면 정책을 걸기 **전에** 하십시오.
> - **커버리지를 확인할 수 없으면 거부합니다.** 청크 테이블은 있는데 원장(§3.1.1)을 읽을 수 없으면
>   청크가 어디까지 뻗어 있는지 알 수 없으므로, 그 테이블의 톰스톤 쓰기는 모두 거부됩니다. 잘못 거부된
>   쓰기는 즉시 눈에 보이는 오류지만, 잘못 허용된 쓰기는 `gc_grace_seconds` 타이머가 달린 조용한 데이터
>   부활입니다. (읽기 경로가 같은 상황에서 "무조건 병합"으로 기우는 것과 같은 방향의 선택입니다.)
>
> 비용: 판정은 읽기 경로와 **같은 테이블별 캐시**를 씁니다. 청크 테이블이 없는 테이블(= 거의 모든
> 테이블)은 캐시 조회 한 번으로 끝나며, 뮤테이션마다 청크 테이블을 읽지 않습니다.

> **LWT(`IF ...`) 조건은 청크 데이터를 보지 못합니다.** CAS 선행 읽기는 투명 읽기 병합을 의도적으로
> 우회합니다 — 조건은 베이스 행에 대한 compare-and-set이고, Paxos는 자기가 소유하지 않는 데이터(다른
> 테이블의 blob인 청크)를 직렬화할 수 없기 때문입니다. 따라서 청크화된 행은 `IF`에게 **없는 행**으로
> 보이고 `IF EXISTS`는 적용되지 않습니다(아무것도 쓰지 않으므로 안전합니다). 조건이 통과하는 경우
> (`IF col = null` 등)에는 위의 불변성 검사가 그대로 걸립니다.
>
> 같은 이유로 **`INSERT ... IF NOT EXISTS`는 청크화된 클러스터링에서 성공합니다** — 선행 읽기가 행을
> 보지 못하므로 `[applied] = true`가 되고 실제로 값이 쓰입니다. 데이터가 유실되지는 않습니다(콜드
> 클러스터링에 실제 값을 쓰는 것은 설계상 허용되며, 이어지는 `SELECT`는 청크의 나머지 컬럼을 그 "새"
> 행에 다시 병합해 줍니다). 다만 "없을 때만 쓴다"는 의도대로 동작하지는 않으므로, 과거 구간에
> `IF NOT EXISTS`로 중복을 막으려는 설계는 피하십시오.

> **`CONSISTENCY SERIAL` SELECT와 `IF`는 같은 행을 다르게 봅니다.** SERIAL `SELECT`는 CAS 선행 읽기가
> 아니라 선형화 읽기이므로 위의 우회 대상이 아니며, 청크 데이터를 **병합해서** 돌려줍니다. 그 자체는
> 안전합니다(재인코딩은 값을 보존하므로 어떤 SERIAL 리더도 값이 되돌아가는 것을 볼 수 없습니다).
> 하지만 흔한 read-then-CAS 관용구가 깨집니다: `SELECT ... CONSISTENCY SERIAL`이 청크에서 온 값을
> 돌려주더라도, 뒤따르는 `UPDATE ... IF col = <그 값>`은 `IF`가 그 행을 보지 못하므로 결코 매치되지
> 않습니다.

### 5.1.3 복원 타임스탬프 = `max_row_writetime + 1`

투명 읽기가 청크에서 복원하는 셀의 writetime은 `max_row_writetime`이 **아니라 그보다 1μs 큰 값**입니다.
선택이 아니라 **강제**입니다: 재인코더는 원본 행을 `USING TIMESTAMP maxWt`로 지우고 `max_row_writetime`은
같은 `maxWt`인데, Cassandra의 삭제 판정은 `timestamp <= markedForDeleteAt`이므로 `maxWt`로 복원하면
**재인코더 자신의 톰스톤이 복원된 행을 전부 가려버립니다**. 톰스톤이 지운 행을 복원하려면 그 톰스톤보다
뒤여야 합니다.

- 사용자 톰스톤은 창을 청크화한 사이클보다 뒤이므로 여전히 복원본을 가립니다 → 삭제는 병합에서 정상
  동작합니다(다만 §5.1.2대로 애초에 거부됩니다).
- 대가: 톰스톤을 넘어 살아남는 지각 행은 writetime `> maxWt`, 즉 `>= maxWt + 1`이므로, **정확히
  `maxWt + 1`에 쓰인 지각 행은 복원본과 동률**이 됩니다. Cassandra는 셀 동률을 값 비교로 깨므로, 값이
  더 작은 지각 수정이 청크의 옛 값에 질 수 있습니다. 클라이언트가 `USING TIMESTAMP`를 직접 지정할 때만
  도달 가능하고(서버 마이크로초 타임스탬프면 사실상 불가능), 명시적 타임스탬프가 평범한 Cassandra에서도
  이미 갖는 위험과 같은 종류입니다.

### 5.2 청크 INSERT 타임스탬프 규칙

청크 upsert는 `USING TIMESTAMP max(maxWt + 1, 기존_청크_writetime + 1)`로 씁니다. 항상
(1) 방금 인코딩한 모든 원본 행보다 뒤, (2) 교체 대상인 기존 청크 행보다 뒤가 보장되어, 크래시 후
재실행이 기존 청크와 **같은 타임스탬프로 충돌**해 행의 셀들이 이전/이후 실행 사이에 찢어지는
(column-level tie-break) 경우를 차단합니다. 덕분에 "청크 쓰고 삭제 전에 죽는" 어떤 시점에서도
재실행은 안전하게 수렴합니다(멱등).

### 5.3 CL 쿼럼 하한

`consistency`는 `QUORUM`/`LOCAL_QUORUM`/`EACH_QUORUM`/`ALL`만 허용됩니다. `ONE` 같은 약한 CL을
허용하면: 재인코더가 직전 사이클의 청크를 **못 본 채**(복제 지연) 새 행만으로 청크를 만들어 쓰고,
원본 레인지 딜리트는 그대로 나가므로 — 이전 청크에만 있던 샘플이 **조용히, 영구히** 유실됩니다.
쿼럼 읽기는 쿼럼 쓰기와 반드시 겹치므로 이 경로가 봉쇄됩니다.

## 6. 제한사항 (현 단계에서 이연된 항목)

1. **cold 만료 경계**: 만료 기준이 `window_start < now - cold_window`이므로, 삭제되는 마지막 창의
   후반부 샘플은 `cold_window`보다 최대 `chunk_window` 하나만큼 덜 오래됐는데도 함께 삭제될 수
   있습니다. `cold_window`를 정할 때 `chunk_window`만큼의 여유를 두세요.
2. **서버측 write timestamp 가정**: 설계는 행 writetime이 서버가 찍는 현재 시각이라는 전제 위에
   있습니다. 클라이언트가 `USING TIMESTAMP`로 **이미 스윕된 maxWt 이하의** writetime을 지정해 행을
   넣으면, 그 행은 기존 레인지 톰스톤보다 오래된 것으로 취급되어 재인코딩 없이 사라질 수 있습니다.
   계층화 대상 테이블에는 클라이언트 지정 타임스탬프를 쓰지 마세요.
3. ~~**베이스 행이 전부 사라진 태그의 청크 만료**~~ — **해결됨.** `cold_window` 만료는 베이스
   테이블이 아니라 **청크 테이블**에서 태그를 열거하므로, 모든 행이 재인코딩·삭제된 뒤 새 쓰기가
   없는 태그의 콜드 청크도 정상 만료됩니다. (인코딩 쪽 태그 열거는 여전히 베이스 테이블
   `DISTINCT` 스캔이지만, 인코딩할 행이 없는 태그에는 할 일도 없습니다.)
4. **잘못된 정책 JSON**: 파싱에 실패하는 정책이 걸린 테이블은 고칠 때까지 60초 스위프마다 ERROR
   로그를 남깁니다 (조용히 무시되어 잊히는 것보다 시끄러운 쪽을 선택).
5. **동명 테이블 DROP + CREATE**: 실행 통계가 `keyspace.table` 이름 키의 인메모리 맵이라, 같은
   이름으로 다시 만든 테이블은 첫 실행 전까지 이전 테이블의 통계가 가상 테이블/`tieringstatus`에
   잠깐 보입니다.

## 7. 테스트

- `org.apache.cassandra.db.timeseries.tiering.TieringPolicyTest` — 정책 파싱/검증 규칙
- `org.apache.cassandra.db.timeseries.tiering.TieredStorageServiceTest` — 재인코딩 사이클 전체
  (인코딩·삭제, 핫 구간 보존, 지각 병합, 멱등 수렴, cold 만료, 스위프 격리, 재진입 가드,
  가상 테이블)
- `org.apache.cassandra.db.timeseries.ChunkV4CodecTest` 등 `db/timeseries`의 포맷 테스트 —
  레이아웃 골든 벡터, 바이트 결정성(JIT 티어 간 동일성 `encoderIsDeterministicAcrossJitTiers`
  포함), 통계 건전성, 블록 독립 디코드
- `org.apache.cassandra.tools.nodetool.mock.TieredStorageMockTest` — nodetool 두 명령의 JMX
  패스스루/오류 표면화
- `org.apache.cassandra.distributed.test.timeseries.TieredStorageDistributedTest` ·
  `ChunkTableSchemaPropagationTest` — 3노드 jvm-dtest (프라이머리 레인지 분할, 코디네이터 독립
  투명 읽기, 스키마 전파, 노드 재시작)
- [docker/integration-test.sh](../../docker/integration-test.sh) — 실제 이미지에서 정책 설정 →
  retier → 청크 검증 → 지각 병합 → 상태 표까지 (릴리스 게이트),
  [docker/cluster-test.sh](../../docker/cluster-test.sh) — 3노드 실컨테이너 검증
