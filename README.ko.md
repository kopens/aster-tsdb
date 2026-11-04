<p align="center"><img src="doc/brand/banner.png" alt="Aster TSDB - Aster Timeseries Database" width="900"></p>

[English](README.md) · [한국어](README.ko.md)

# Aster Timeseries Database

**산업 현장의 시계열 워크로드에 최적화된 데이터베이스.** CQL 함수 21종, gap-fill, 닫힌 창을
컬럼형 SSTable로 동결하는 시간 창 컴팩션.

**Apache Cassandra 호환.** 같은 CQL, 같은 드라이버, 같은 운영 도구, 같은 온디스크 포맷입니다 —
기존 6.0 데이터를 그대로 읽습니다. 돌고 있는 클러스터에 설치하는 것은 jar 하나이고, 새 기능은
전부 옵트인입니다.

| 일반 Cassandra 6.0.0 대비 | 일반 Cassandra | Aster TSDB |
| --- | --- | --- |
| 시계열 연산 | 애플리케이션이 원시 행을 끌어와 계산 | **서버에서 끝난다** |
| 압축·보존 | 애플리케이션 몫 | **테이블 정책** |

## TimescaleDB · InfluxDB 와 비교하면

| | TimescaleDB | InfluxDB 3 Core (OSS) | **Aster TSDB** |
| --- | --- | --- | --- |
| 기반 | PostgreSQL 확장 | 전용 엔진 | Cassandra 호환 엔진 |
| 질의 언어 | SQL | SQL / InfluxQL | CQL |
| 버킷팅 | `time_bucket` | `date_bin_gapfill` | `time_bucket` / `time_bucket_gapfill` |
| Gap-fill (`locf`/`interpolate`) | 있음 | 있음 | 있음 |
| 시계열 함수 | Toolkit | SQL 내장 | **21종, Timescale 어휘** |
| 압축 | 있음 | 있음 | 있음 — **컬럼형 `tscol`** |
| 보존 | 정책 | 정책 | **윈도우 통째 드롭(TSCS)** |
| 전문 검색 | PostgreSQL FTS | 제한적 | SAI `LIKE` + 분석기 |
| **수평 확장 (오픈소스판)** | 2.13 이후 multi-node 폐기 — 직접 만드는 샤딩 | 단일 노드 | **노드를 더해 늘린다** |
| **고가용성·복제 (오픈소스판)** | PostgreSQL 복제 | **상용 전용** | **포함 · 멀티 DC** |
| 기존 Cassandra 에 도입 | — | — | **jar 하나 교체** |

앞의 여덟 행은 대체로 동등하고, 그게 요점입니다 — **이걸 고른다고 시계열 기능을 포기하지
않습니다.** 갈리는 곳은 마지막 세 행입니다. Timescale 은 수평 확장을 접었습니다(2.13 이후
multi-node 폐기, 확장은 직접 만드는 샤딩,
[폐기 공지](https://github.com/timescale/timescaledb/blob/main/docs/MultiNodeDeprecation.md)).
InfluxDB 3 Core 는 단일 노드이고 클러스터링·HA·읽기 복제는 상용판 몫입니다
([InfluxDB 3 Core](https://www.influxdata.com/products/influxdb/),
[Enterprise 클러스터링](https://docs.influxdata.com/influxdb3/enterprise/admin/clustering/)).

여기서는 클러스터링과 복제가 «등급» 이 아닙니다. 이건 Cassandra 입니다 — 노드를 더해 늘리고,
랙과 데이터센터에 걸쳐 복제하고, 한 대를 잃어도 살아 있으며, 그중 어느 것도 라이선스 뒤에
있지 않습니다.

> 경쟁 제품 열은 각 제품의 «구조» 를 적고 출처를 달았습니다. 판단 전에 각자의 최신 문서를
> 확인하십시오. Aster 열의 수치는 우리 실측이고, 그 벤치마크는 이 저장소 안에 있습니다.

## 어떤 워크로드를 위한 것인가

산업 현장의 데이터는 모양이 정해져 있습니다: 태그마다 초 단위로 끝없이 쌓이고, 규정상 몇 년치를
보관해야 하며, 통신이 끊겼던 엣지 장비가 며칠치를 한꺼번에 밀어 넣고, 조회는 거의 항상
"이 태그의 이 기간"입니다. 범용 Cassandra 도 이 부하를 감당합니다 — 다만 버킷팅·압축·보존·집계가
전부 애플리케이션 몫으로 남습니다. Aster 는 그것을 **데이터베이스 안으로** 가져옵니다.
연산은 서버에서 끝나고, 오래된 윈도우는 스스로 압축·만료되며, 압축된 과거 데이터도 평범한
`SELECT` 로 그대로 읽힙니다.

Spark 연동은 짝이 되는 프로젝트
[cassandra-spark-connector](https://dev.kopens.io/common/cassandra-spark-connector)(Spark 4.1.2)로 제공됩니다.

## ✨ 구현 기능

| 기능 | 내용 | 상세 |
| --- | --- | --- |
| **시계열 CQL 함수 21종** | `time_bucket`, `first`/`last`, `delta`/`rate`/`derivative`, 리셋 보정 `counter_delta`/`counter_rate`, `percentile`, `time_weighted_average`, `integral`, `variance`/`stddev`, `histogram`, `approx_count_distinct`, 이변량 `corr`/`covar_*`/`regr_*` | [사용법 §2~9](#시계열-cql-사용법) |
| **Gap-fill** | `GROUP BY time_bucket_gapfill(width, ts, start, finish)` — 빈 버킷 실체화 + `locf()`/`interpolate()` 채움 정책 | [사용법 §3](#3-빈-구간-채우기-time_bucket_gapfill) |
| **풀텍스트 검색** | SAI `LIKE` + `index_analyzer`(ngram/standard/cjk/keyword + JSON) — 단어 중간 조각·공백 걸침·한글까지 진짜 부분문자열 매치, ALLOW FILTERING 불필요 | [fulltext-search.md](doc/timeseries/fulltext-search.md) |
| **시계열 컴팩션 (TSCS)** | `TimeSeriesCompactionStrategy` — 창 정렬 + 창 내부 UCS 위임 + retention 창 통삭제 + 닫힌 창 동결(창당 1 SSTable, `WindowFrozenListener` 이벤트 훅, far-future 가드 `max_future_window`, **동결 시점에** 이미 만료된 TTL 데이터는 retention 없이 회수 — 동결 이후 만료되는 데이터는 `retention` 필요) + 지각 격리(flush/스트리밍 창 경계 스플릿 — 백필이 과거 창에 국소 편입, 레거시 걸침 SSTable 자동 분할) + **전용 memtable**(테이블별 옵트인 — 행을 쓰기 시점에 TSCS 창으로 배정해 flush 라우팅·64 MiB 파티션 상한 제거, 원시 배열 컬럼 저장으로 행당 힙 **5.5×↓** 실측) | [timeseries-compaction.md](doc/timeseries/timeseries-compaction.md) · [timeseries-memtable.md](doc/timeseries/timeseries-memtable.md) · [설계 스펙](docs/superpowers/specs/2026-07-31-timeseries-compaction-design.md) |
| **컬럼 지향 청크 코덱 (chunk format v4)** | 창 1개 = 공유 타임스탬프 축 + 일반 컬럼별 독립 섹션의 무손실 압축, 모든 블록이 독립 디코드·랜덤 접근. `double`은 ALP/ALP-RD(유일한 double 코덱), 정수·시각 계열은 FOR/델타 비트팩, `boolean`은 1비트팩, `text`·불투명 바이트는 사전(DICT)/RAW. 값이 일정한 컬럼은 CONSTANT, 전부 null인 컬럼은 ALL_NULL로 O(1) 처리. `tscol` SSTable 안의 블록 형식이다 | [포맷 규격](doc/timeseries/chunk-format-v4.md) · [코덱 실측 비교](doc/timeseries/codec-bakeoff.md) |
| **컬럼형 동결 SSTable (`tscol`)** | BTI에서 파생한 세 번째 SSTable 형식. TSCS 옵션 `frozen_format = 'columnar'`이면 닫힌 창을 `tscol` SSTable 하나로 동결하고, 파티션 본문은 값과 셀별 writetime·TTL·삭제 시각·셀 톰스톤까지 **정확히 왕복**하는 청크 코덱 블록이라 컬럼형 복제본도 행 형식과 같은 답·다이제스트·리페어를 낸다. 동결된 데이터의 늦은 쓰기·수정·삭제·TTL·일반 컬럼 필터·SAI는 보통 Cassandra 동작 — 거부도 옆 톰스톤도 없고 창이 재동결될 뿐 | [설계](docs/superpowers/specs/2026-11-04-columnar-frozen-sstable-design.md) · [전환 절차](doc/timeseries/tscol-rollout.md) |
| **테스트 인프라** | 도커 통합 테스트(릴리스 게이트), 3노드 클러스터 테스트, 1억 건 스케일 하네스, jvm-dtest, JMH 성능 회귀 게이트, GC 비교(ZGC vs G1) | [보고서들](doc/timeseries/) |
| **배포/CI** | Testcontainers 호환 도커 이미지, GitLab CI(빌드→테스트→이미지→통합 게이트→릴리스), 태그 릴리스 자동화 | [.gitlab-ci.yml](.gitlab-ci.yml) |

## 🎯 핵심 — 무엇이 좋아지나 (일반 Cassandra 6.0.0 대비)

**1. 서버에서 끝나는 시계열 연산.** 버킷팅·집계·보간·회귀를 CQL 한 줄로 처리합니다. 애플리케이션이 원시 데이터를 끌어와 계산하던 왕복이 사라집니다.

```sql
-- 시간별 평균 + 빈 구간 자동 채움 — 업스트림에서는 앱이 100k행을 받아 직접 계산해야 하는 작업
SELECT time_bucket_gapfill(1h, timestamp, '2026-07-01', '2026-07-02'), locf(avg(latency))
FROM pp.tm_tag_point
WHERE tag_id='TAG-001' AND timestamp >= '2026-07-01' AND timestamp < '2026-07-02'
GROUP BY tag_id, time_bucket_gapfill(1h, timestamp, '2026-07-01', '2026-07-02')
ORDER BY timestamp ASC;
```

**2. 오래된 데이터는 자동 압축, 조회는 그대로.** `frozen_format = 'columnar'`이면 TSCS가 닫힌 창을 컬럼형 `tscol` SSTable 하나로 동결합니다. 테이블은 그대로입니다 — `SELECT`, 늦은 쓰기, 삭제, TTL, 필터, SAI 모두 보통 Cassandra로 동작합니다.

**3. 얼마나 줄어드는지는 데이터에 달렸습니다.** `tm_tag_point` 형태 2,000만 건, TTL 3650일(호스트 234)에서 `tscol`은 34.7 MB로, 행 형식 237.8 MB보다 약 6.8배 작았습니다. 이 형태는 값이 일정하거나 전부 null인 컬럼이 많습니다. 컬럼이 적고 값이 무작위인 `double`은 약 2배만 줄어듭니다. 제거된 그림자 테이블 청크 스토어와 비교하면 `tscol`은 약 7% 크고, 태그 하나의 하루치 조회는 13~23%, 집계는 약 2배 느립니다. 대신 동결된 데이터가 보통 Cassandra처럼 동작합니다([측정값](doc/timeseries/tiering-benchmark.md#tscol과-r8-청크-비교-2026-11-04-호스트-234)). 도입 전 자기 테이블 형태로 재보십시오.

**4. 시계열에 맞는 컴팩션·보존.** 시간 창 단위로 SSTable을 정렬·동결(창당 1개)하고, 보존 만료 창은 컴팩션 없이 통째 삭제합니다. 엣지 장비의 지각 백필도 자기 시간대로 자동 격리됩니다.

**5. 로그·이벤트 본문 검색.** SAI `LIKE` + `index_analyzer`로 한글 포함 부분문자열 검색이 `ALLOW FILTERING` 없이 동작합니다.

## 시작하기

```bash
# 1. 순정 jar 자리에 넣는다
cp apache-cassandra-6.0.0.jar $CASSANDRA_HOME/lib/

# 2. 이게 설치의 전부다. 기존 데이터, 기존 CQL, 기존 운영 도구 그대로.
#    nodetool version 도 여전히 6.0.0 이다 — 릴리즈 버전 자체가 상류 것이다.
```

## 📖 문서

| 문서 | 내용 |
| --- | --- |
| **[사용 예제 (examples.md)](doc/timeseries/examples.md)** | 아래 "시계열 CQL 사용법"의 원본 예제 모음 (영문) |
| [시계열 함수 설계 (timeseries-functions-design.md)](doc/timeseries/timeseries-functions-design.md) | 각 함수의 시그니처·의미론(semantics), 분산 환경에서의 정확성, 코드 위치 |
| [Gap-Fill 설계 (gapfill-design.md)](doc/timeseries/gapfill-design.md) | `time_bucket_gapfill`의 CQL 문법, 보간 규칙, 가드레일 |
| [Continuous Aggregates 설계 (continuous-aggregates-design.md)](doc/timeseries/continuous-aggregates-design.md) | 시간 버킷 롤업(연속 집계) 설계안 — 진행 중 |
| **[풀텍스트 검색 (fulltext-search.md)](doc/timeseries/fulltext-search.md)** | SAI `LIKE` + `index_analyzer` — 로그/메시지 본문 부분문자열 검색 (한글 포함) |
| [프로덕션 투입 체크리스트 (production-rollout.md)](doc/timeseries/production-rollout.md) | 역사 기록 — 제거된 그림자 테이블 계층화의 확인 목록. `tscol` 전환은 [tscol-rollout.md](doc/timeseries/tscol-rollout.md) 참고 |
| [계층화 벤치마크 (tiering-benchmark.md)](doc/timeseries/tiering-benchmark.md) | 역사 기록 — 제거된 그림자 테이블 청크 스토어의 운영 형태 2,000만 건 전/후 실측 (호스트 234, chunk v4) |
| **[운영 튜닝 가이드 (operations-tuning.md)](doc/timeseries/operations-tuning.md)** | 장기 보존(10년) 전환 실전 가이드 — 용량 산수, 적용 순서, 튜닝값과 근거, 점검 목록. 제거된 그림자 테이블 계층화 기준으로 쓰여 청크 테이블 부분은 역사 기록이다 |
| **[시계열 컴팩션 (timeseries-compaction.md)](doc/timeseries/timeseries-compaction.md)** | `TimeSeriesCompactionStrategy` — 창 크기·동결·`retention` 설정, 창의 일생, 지각 데이터 격리, 파킹된 창의 두 원인과 진단법, TTL이 아니라 `retention`이 만료를 담당하는 이유, **운영 노드 실측** |
| **[시계열 전용 Memtable (timeseries-memtable.md)](doc/timeseries/timeseries-memtable.md)** | `TimeSeriesMemtable` — 켜는 법(yaml 설정 키 + `ALTER TABLE`, 두 단계를 틀리기 쉬운 이유), 지원/미지원 스키마와 폴백 동작, 파킹 원인 제거·행당 힙 5.5× 실측·**스트리밍 읽기**(슬라이스 이진 탐색, 필요한 행만 조립, 보유 0), 확인 절차 |
| **[운영 투입 보고서 2026-08-02 (prod-ops-report-2026-08-02.md)](doc/timeseries/prod-ops-report-2026-08-02.md)** | 실 노드 12시간 기록 — 배포 5회, 사고 2건의 전말과 재발 방지, 판단이 뒤집힌 것들, 실측 절차 |
| **[운영 TSCS 설정과 파킹 진단 (prod-tscs-settings.md)](doc/timeseries/prod-tscs-settings.md)** | 75개 테이블의 현재 설정과 근거, 파킹된 창의 두 원인을 가르는 진단 절차, 24k rows/s 유입 중 실측값 |
| [계층형 저장 (tiered-storage.md)](doc/timeseries/tiered-storage.md) | 역사 기록 — 제거된 `timeseries_tiering` 정책과 청크 재인코더 |
| **[tscol 전환 절차 (tscol-rollout.md)](doc/timeseries/tscol-rollout.md)** | 컬럼형 동결 SSTable로 옮기기 — 옛 청크 스토어가 있는 설치가 잃는 것과 남는 것, 전환 순서 |
| **[압축 설명 (compression.md)](doc/timeseries/compression.md)** | 컬럼별로 무엇이 왜 얼마나 줄어드는가 — 두 압축 층의 관계, 타입별 인코딩과 행당 비용, 절감폭의 컬럼별 분해(8컬럼 중 4개가 0바이트), 내 테이블 추정 규칙과 실측 방법 |
| **[청크 포맷 v4 (chunk-format-v4.md)](doc/timeseries/chunk-format-v4.md)** | 유일한 청크 포맷의 **와이어 포맷 규격** — 헤더·디렉토리·블록 테이블·presence 4모드·타입별 블록 인코딩(ALP 포함), 결정성 규칙, 크기 한계 (v1~v3는 제거된 포맷 — 읽으면 `UnsupportedChunkFormatException`) |
| [코덱 bake-off (codec-bakeoff.md)](doc/timeseries/codec-bakeoff.md) | double 코덱 실측 비교 — **ALP/ALP-RD 단일화** 결론과 분포별 B/값 (Gorilla·Chimp128 대비) |
| [통합 테스트 보고서](doc/timeseries/integration-test-report.md) | 실제 컨테이너에서 실행한 각 검증의 CQL·결과·소요 시간 |
| [스케일 테스트 보고서 (1억 건)](doc/timeseries/scale-test-report.md) | 1억 행 용량 검증 — 적재·집계의 스캔 행 수 선형성 (구형 호스트 기록) |
| **[읽기/쓰기 처리량 벤치마크 (rw-throughput-benchmark.md)](doc/timeseries/rw-throughput-benchmark.md)** | 초당 처리량 실측 — 적재 **233k rows/s**(호스트 234) · 쓰기 경로 424k rows/s·청크 인코딩 684k rows/s·청크 풀스캔 740µs/3,600행(호스트 237, JMH) · 패턴별 읽기 ops/s는 v4 재측정 대기 |
| **[Memtable 쓰기 튜닝 기록 (memtable-write-tuning.md)](doc/timeseries/memtable-write-tuning.md)** | 쓰기 경로 최적화 기록 — DESC가 끄는 두 고속 경로, 설정 튜닝의 한계, 코드 수정 3라운드(min 가드·역순 long 스토어·꼬리 인덱스), ALTER 순서 함정 |
| [SP4 계획 (sp4-plan.md)](doc/timeseries/sp4-plan.md) | SP4 로드맵 — Compressed Query·vectorized 집계 커널·SIMD 등의 삽입 지점·마일스톤·검증 게이트 (진행 상태는 문서 참고) |
| [GC 비교: ZGC generational vs G1](doc/timeseries/gc-comparison.md) | 같은 1억 건 데이터로 두 GC의 쿼리 시간·쓰기 처리량 비교 (원자료) |
| **[아티클: 시계열 DB에서 G1GC vs Generational ZGC](doc/timeseries/g1gc-vs-zgc-article.md)** | 위 측정을 정리한 성능 비교 아티클 (환경·방법·해석·권장 설정) |

전체 문서 디렉터리: [doc/timeseries/](doc/timeseries/)

---

# 시계열 CQL 사용법

별도 설치나 UDF 등록 없이 `cqlsh`에서 바로 쓸 수 있는 네이티브 함수입니다. 아래 예제는 모두 실행 가능한 CQL입니다.

## 함수 레퍼런스

**인자 순서가 중요합니다.** 대부분의 시계열 집계는 `(값, 타임스탬프)` 순서입니다.

| 함수 | 시그니처 | 반환 | 설명 |
| --- | --- | --- | --- |
| `time_bucket` | `time_bucket(duration, ts [, origin])` | `timestamp` | 버킷 시작 시각 (다운샘플링용 스칼라) |
| `time_bucket_gapfill` | `time_bucket_gapfill(width, ts, start, finish)` | `timestamp` | 빈 버킷까지 생성하는 `GROUP BY` 셀렉터 |
| `locf` | `locf(집계)` | 인자와 동일 | 빈 버킷을 직전 값으로 채움 (LOCF) |
| `interpolate` | `interpolate(집계)` | `double` | 빈 버킷을 앞뒤 값의 선형 보간으로 채움 |
| `first` / `last` | `first(value, ts)` / `last(value, ts)` | `value`의 타입 | 시각 기준 최초/최종 값 |
| `delta` | `delta(value, ts)` | `double` | 마지막 − 첫 샘플 |
| `rate` | `rate(value, ts)` | `double` | `delta` ÷ 경과 초 (양 끝점 기준) |
| `derivative` | `derivative(value, ts)` | `double` | 최소제곱 회귀 기울기 (초당) |
| `counter_delta` / `counter_rate` | `counter_delta(value, ts)` / `counter_rate(value, ts)` | `double` | 리셋을 보정한 카운터 증가량 / 초당 증가율 |
| `percentile` | `percentile(value, q)` — `q`는 `[0,1]` | `double` | 정확한 연속 백분위 (선형 보간) |
| `time_weighted_average` | `time_weighted_average(value, ts)` | `double` | 시간 가중 평균 |
| `integral` | `integral(value, ts)` | `double` | 곡선 아래 면적 (value·초) |
| `variance` / `stddev` | `variance(value)` / `stddev(value)` | `double` | 표본 분산 / 표준편차 |
| `corr` / `covar_pop` / `covar_samp` | `corr(y, x)` 등 | `double` | 상관계수 / 모공분산 / 표본공분산 |
| `regr_slope` / `regr_intercept` / `regr_r2` | `regr_slope(y, x)` 등 | `double` | y의 x에 대한 선형 회귀 |
| `histogram` | `histogram(value, min, max, nbuckets)` | `list<bigint>` | 등간격 히스토그램 (길이 `nbuckets+2`) |
| `approx_count_distinct` | `approx_count_distinct(value)` | `bigint` | HyperLogLog 근사 고유값 개수 |

## 1. 스키마와 샘플 데이터

아래 예제는 모두 산업 현장의 실제 태그 테이블 `tm_tag_point` 위에서 돕니다 — 태그당 파티션 하나, 시간으로 클러스터링, **최신 데이터가 앞**(`DESC`). 컴팩션은 Aster 의 시계열 전용 전략 `TimeSeriesCompactionStrategy`(TSCS)를 씁니다 — SSTable을 시간 창으로 정렬하고, 닫힌 창은 창당 1 SSTable로 동결하며, 보존기간이 지난 창은 컴팩션 없이 통째 삭제합니다. 현재 창 내부의 컴팩션 선택은 UCS 컨트롤러에 위임되므로 UCS의 쓰기 최적 특성은 그대로 유지됩니다.

```sql
CREATE KEYSPACE IF NOT EXISTS pp
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE pp;

CREATE TABLE tm_tag_point (
    tag_id     text,                              -- 파티션 키: 태그 하나 = 파티션 하나
    timestamp  timestamp,
    area_id    text static, asset_id text static, line_id text static,
    opc_id     text static, site_id  text static, tag_name text static,
    type       text static,                       -- 'boolean' | 'long' | 'double' | ...
    attribute  frozen<map<text,text>>,
    error_code int,
    latency    int,                               -- 수집 지연, 항상 존재
    quality    int,
    value      text,                              -- 판독값의 문자열 사본
    value_boolean boolean,                        -- type='boolean'일 때 채워짐
    value_numeric double,                         -- type이 숫자형일 때 채워짐
    PRIMARY KEY (tag_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
   AND compaction = {'class': 'TimeSeriesCompactionStrategy',
                     'window_size': '1d',          -- 시간 창 폭
                     'freeze_after': '2h',         -- 창이 닫히고 이 시간 후 창당 1 SSTable로 동결
                     'scaling_parameters': 'T4',   -- 현재 창 내부는 UCS 위임 (쓰기 최적 4-way)
                     'retention': '62d'}           -- 창 상한이 62일을 지나면 통째 삭제
   AND default_time_to_live = 5356800;   -- 62일 (retention과 병행 시 먼저 도래하는 쪽 적용)

-- static은 태그당 한 번만 씁니다 (샘플마다가 아니라).
INSERT INTO tm_tag_point (tag_id, area_id, asset_id, line_id, opc_id, site_id, tag_name, type)
     VALUES ('TAG-001', 'A1', 'AS1', 'L1', 'OPC1', 'S1', 'boiler.temp', 'double');

INSERT INTO tm_tag_point (tag_id, timestamp, attribute, error_code, latency, quality, value, value_numeric)
     VALUES ('TAG-001', '2024-01-01 09:05:00+0000', {}, 0,  17, 192, '20.1', 20.1);
INSERT INTO tm_tag_point (tag_id, timestamp, attribute, error_code, latency, quality, value, value_numeric)
     VALUES ('TAG-001', '2024-01-01 09:35:00+0000', {}, 0, 431, 192, '20.8', 20.8);
INSERT INTO tm_tag_point (tag_id, timestamp, attribute, error_code, latency, quality, value, value_numeric)
     VALUES ('TAG-001', '2024-01-01 10:15:00+0000', {}, 0,   3, 192, '21.4', 21.4);
INSERT INTO tm_tag_point (tag_id, timestamp, attribute, error_code, latency, quality, value, value_numeric)
     VALUES ('TAG-001', '2024-01-01 10:45:00+0000', {}, 0, 902, 192, '22.0', 22.0);
```

### 1.0 어느 컬럼을 집계할 수 있나 — 이 스키마 최대의 함정

**`value`는 `text`라서 수치 집계가 불가능합니다.** `avg(value)`·`percentile(value, 0.95)`·`delta(value, timestamp)`는 전부 거부됩니다(수치 타입만 허용: `tinyint`/`smallint`/`int`/`bigint`/`varint`/`float`/`double`/`decimal`/`counter`). 거부보다 위험한 건 **통과하는 쪽**입니다 — `min(value)`/`max(value)`/`count(value)`는 `text`에도 동작하지만 **사전순**으로 비교하므로, `'9.1'`과 `'20.76'` 중 `max`는 `'9.1'`입니다.

산술이 가능한 컬럼은 `latency`(`int`, 항상 존재 — 예제·스모크 테스트의 기본값), `value_numeric`(`double`, 수치형 태그에서만), 그리고 상수인 `error_code`·`quality`입니다. 예외는 `first`/`last`/`approx_count_distinct` — 타입을 가리지 않으므로 `first(value, timestamp)`는 `text`를 그대로 돌려줍니다.

### 1.1 TSCS 컴팩션 옵션 요약

| 옵션 | 설명 |
| --- | --- |
| `window_size` | 시간 창 폭 (`<정수><m\|h\|d>`) |
| `freeze_after` | 창이 닫힌 뒤 이 시간이 지나면 동결 대상 — 지각 데이터 유예 기간 |
| `scaling_parameters`, `target_sstable_size` | 현재 창 내부 UCS 위임 파라미터 (UCS 문법 그대로: `T4` = 쓰기 최적 4-way tiered 등) |
| `retention` | 선택 — 창 상한이 `now - retention`을 지나면 SSTable 통째 삭제 (`window_size + freeze_after` 이상) |
| `max_future_window` | 미래 타임스탬프 가드(기본 `1d`) — 오입력이 창을 오염시키지 않게 격리 |
| `frozen_format` | `row`(기본) 또는 `columnar` — 닫힌 창을 `tscol` SSTable로 동결 (§12) |

동결(창당 1 SSTable) 시점에 이미 만료된 TTL 데이터는 `retention` 없이도 회수되고, 파티션+시간범위 조회의 읽기 증폭이 최소화됩니다. 다만 창이 SSTable 1개로 줄면 다시는 동결 후보가 되지 않으므로, **동결 이후에 만료되는 데이터는 `retention`을 설정해야 회수됩니다.** 지각(백필) 데이터는 flush 시 창 경계에서 분리되어 자기 창에 국소 편입됩니다. 상세: [§11 시계열 컴팩션 설정](#11-시계열-컴팩션tscs-설정)

## 2. 버킷팅 · 다운샘플링 — `time_bucket`

### 2.1 각 행에 버킷 붙이기 (스칼라로 사용)

```sql
SELECT timestamp, time_bucket(1h, timestamp) AS bucket, latency, value
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001';
```

기간 인자는 **따옴표 없는 CQL duration 리터럴**(`1h`)입니다 — `time_bucket('1h', ts)`처럼 문자열로 주면 시그니처가 맞지 않아 실패합니다. 반면 origin/start/finish는 `timestamp`라서 따옴표를 씁니다.

### 2.2 `GROUP BY`로 고정 간격 다운샘플링

```sql
-- 시간별 평균 / 최소 / 최대 / 개수 (수집 지연)
SELECT time_bucket(1h, timestamp) AS bucket,
       avg(latency), min(latency), max(latency), count(latency)
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
GROUP  BY tag_id, time_bucket(1h, timestamp);

-- 판독값 자체 (수치형 태그)
SELECT time_bucket(1h, timestamp) AS bucket, avg(value_numeric)
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
GROUP  BY tag_id, time_bucket(1h, timestamp);

-- 다른 간격: 5분, 1일
SELECT time_bucket(5m, timestamp) AS bucket, avg(latency) FROM tm_tag_point
  WHERE tag_id = 'TAG-001' GROUP BY tag_id, time_bucket(5m, timestamp);

SELECT time_bucket(1d, timestamp) AS bucket, avg(latency) FROM tm_tag_point
  WHERE tag_id = 'TAG-001' GROUP BY tag_id, time_bucket(1d, timestamp);
```

`time_bucket`은 `GROUP BY`의 **마지막 요소**(파티션 키 컬럼 뒤)로 와야 그룹핑이 읽기 경로로 내려갑니다. `DESC` 테이블에서도 그대로 동작하며, 버킷이 최신순으로 나올 뿐입니다. `int` 컬럼의 `avg`는 `int`로 절삭된다는 업스트림 규칙이 그대로 적용됩니다.

### 2.3 기준점(origin)을 옮긴 버킷

```sql
-- 30분 밀린 1시간 버킷: [08:30, 09:30), [09:30, 10:30), ...
SELECT time_bucket(1h, timestamp, '2024-01-01 00:30:00+0000') AS bucket, avg(latency)
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
GROUP  BY tag_id, time_bucket(1h, timestamp, '2024-01-01 00:30:00+0000');
```

## 3. 빈 구간 채우기 — `time_bucket_gapfill`

일반 `time_bucket`은 **데이터가 있는 버킷만** 반환합니다. `time_bucket_gapfill`은 `[start, finish)` 범위의 **모든** 버킷에 대해 행을 만들어 주므로, 대시보드가 끊김 없는 시간축을 얻습니다. 데이터가 없는 버킷의 집계값은 기본적으로 null입니다.

> **⚠️ `DESC` 테이블에서는 `ORDER BY timestamp ASC`가 필수입니다.** gap-fill의 densify는 버킷이 **오름차순**으로 도착한다고 가정하는데, 이를 강제하는 검사가 없습니다. `DESC` 클러스터링 테이블에서 정렬 없이 실행하면 행이 최신순으로 들어와 채움이 거꾸로 적용되며 **에러도 나지 않습니다**. `ORDER BY timestamp ASC`를 붙이면 `DESC` 선언과 `ASC` 요청이 상쇄되어 읽기 자체가 오름차순이 되고, 이것이 densify가 원하는 형태입니다. 이 조합은 아직 테스트로 덮여 있지 않습니다 — [gapfill-design.md §4](doc/timeseries/gapfill-design.md) 참고.

```sql
SELECT time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000'),
       avg(latency)
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
  AND  timestamp >= '2024-01-01 00:00:00+0000' AND timestamp < '2024-01-02 00:00:00+0000'
GROUP  BY tag_id, time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000')
ORDER  BY timestamp ASC;
```

`WHERE timestamp >= <start>`는 장식이 아닙니다 — 스캔되는 행 중 gap-fill `start`보다 **오래된** 것이 하나라도 있으면 *"The floor function starting time is greater than the provided time"*으로 실패합니다.

### 3.1 `locf()` — 직전 값 이어가기

집계를 `locf(...)`로 감싸면 빈 버킷이 null 대신 **직전 비어있지 않은 버킷의 값**을 그대로 이어받습니다(last-observation-carried-forward).

```sql
SELECT time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000'),
       locf(avg(latency))   -- 빈 버킷은 직전 시간의 평균을 반복
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
  AND  timestamp >= '2024-01-01 00:00:00+0000' AND timestamp < '2024-01-02 00:00:00+0000'
GROUP  BY tag_id, time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000')
ORDER  BY timestamp ASC;
```

`locf`는 실제 데이터가 있는 행에는 아무 영향이 없습니다(인자를 그대로 반환). 첫 실제 값보다 앞선 버킷은 이어받을 값이 없으므로 null로 남습니다.

### 3.2 `interpolate()` — 선형 보간

빈 버킷을 앞뒤 값 사이에서 선형 보간하려면 `interpolate(...)`를 씁니다(결과 타입은 `double`).

```sql
SELECT time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000'),
       interpolate(avg(value_numeric))   -- 빈 버킷은 양옆 값 사이를 직선으로 채움
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
  AND  timestamp >= '2024-01-01 00:00:00+0000' AND timestamp < '2024-01-02 00:00:00+0000'
GROUP  BY tag_id, time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000')
ORDER  BY timestamp ASC;
```

첫 실제 값 이전 / 마지막 실제 값 이후의 빈 버킷은 보간할 대상이 없으므로 null로 남습니다.

### 3.3 여러 태그

태그별로 독립적으로 채워집니다. 파티션 키를 `SELECT`와 `GROUP BY` 양쪽에 포함하세요.

```sql
SELECT tag_id, time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000'),
       avg(latency)
FROM   tm_tag_point WHERE tag_id IN ('TAG-001', 'TAG-002')
  AND  timestamp >= '2024-01-01 00:00:00+0000' AND timestamp < '2024-01-02 00:00:00+0000'
GROUP  BY tag_id, time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000')
ORDER  BY timestamp ASC;
```

`IN` + `ORDER BY`는 파티션을 **가로지르는** 후처리 정렬을 걸기 때문에 결과가 태그별이 아니라 버킷 기준 전역 순서로 나옵니다. 이 스키마에서는 태그 하나씩 도는 쪽이 다루기 쉽습니다.

### 3.4 제약 사항

- 폭(width)은 **고정 길이**여야 합니다(월 단위 성분 불가).
- 버킷 컬럼과 `locf(...)`/`interpolate(...)`에 **별칭(alias)을 붙이지 마세요** — 결과 메타데이터의 함수 이름으로 찾기 때문에, 별칭을 붙이면 gap-fill이 조용히 아무 일도 하지 않습니다.
- 채울 집계는 **수치 컬럼**이어야 합니다 — `latency` 또는 `value_numeric`, `value`(text)는 불가.
- 버킷 범위를 가로지르는 페이징은 피하세요.
- 범위 ÷ 폭이 **1,000,000 버킷**을 넘으면 쿼리가 거부됩니다.

## 4. 최초/최종 값 — `first`, `last`

`first`/`last`는 값 타입을 가리지 않으므로 `text`인 `value`에도 그대로 쓸 수 있습니다 — 이 컬럼으로 서버에서 할 수 있는 몇 안 되는 일 중 하나입니다.

```sql
-- 태그의 최초/최종 판독값 (text 그대로 반환)
SELECT first(value, timestamp) AS first_reading,
       last(value, timestamp)  AS last_reading
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001';
```

시간별 OHLC(시가/고가/저가/종가) 캔들 — 여기서는 **수치 컬럼**을 써야 합니다. `text`에 `max`/`min`을 걸면 사전순 비교가 됩니다:

```sql
SELECT time_bucket(1h, timestamp) AS bucket,
       first(value_numeric, timestamp) AS open,
       max(value_numeric)              AS high,
       min(value_numeric)              AS low,
       last(value_numeric, timestamp)  AS close
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
GROUP  BY tag_id, time_bucket(1h, timestamp);
```

`first`/`last`는 **삽입 순서가 아니라 타임스탬프 인자 기준**으로 정렬하므로, 순서가 뒤바뀐 쓰기가 있어도, 그리고 `DESC` 클러스터링이어도 시가/종가가 정확합니다.

## 5. 변화량 — `delta`, `rate`, `derivative`

```sql
SELECT time_bucket(1h, timestamp) AS bucket,
       delta(value_numeric, timestamp)      AS change,
       rate(value_numeric, timestamp)       AS per_second,
       derivative(value_numeric, timestamp) AS slope_per_second
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
GROUP  BY tag_id, time_bucket(1h, timestamp);
```

- `delta` = 버킷 내 마지막 샘플 − 첫 샘플
- `rate` = `delta` ÷ 경과 초 (양 끝점 기준 변화율)
- `derivative` = 최소제곱 회귀 기울기. 모든 점을 사용하므로 시계열이 비선형일 때 `rate`와 값이 달라집니다.

두 번째 인자는 `timestamp` 또는 `bigint`(epoch 밀리초)여야 합니다 — `int`·`timeuuid` 시각 컬럼은 거부됩니다. 수치형이 아닌 태그라면 `value_numeric` 대신 `latency`를 넣으세요.

### 5.1 리셋 보정 처리량 — `counter_rate`

`counter_delta`/`counter_rate`는 **수치 컬럼**이면 되고 CQL `counter` 타입을 요구하지 않습니다 — 단조 증가하는 `int`/`bigint` 게이지면 충분하며, 테스트가 덮고 있는 것도 이 형태입니다. 리셋 가능성이 있으면 `rate()` 대신 이쪽을 쓰세요. `rate()`는 리셋을 큰 음수 계단으로 오해합니다.

```sql
CREATE TABLE tag_counters (
    tag_id text, timestamp timestamp, total bigint,
    PRIMARY KEY (tag_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

-- 분당 초당 이벤트 수 (리셋 보정)
SELECT time_bucket(1m, timestamp) AS minute, counter_rate(total, timestamp) AS per_sec
FROM   tag_counters
WHERE  tag_id = 'TAG-001'
GROUP  BY tag_id, time_bucket(1m, timestamp);
```

`tm_tag_point`에는 단조 증가 컬럼이 없어 별도 테이블을 씁니다. CQL `counter` **타입**을 쓴 테이블은 늘 행 형식으로 동결된다는 점도 함께 고려하세요 — `tscol`에 담을 수 있는 쪽은 `bigint` 게이지입니다.

## 6. 백분위 · SLO — `percentile`

```sql
-- 분당 p50 / p95 / p99 지연시간
SELECT time_bucket(1m, ts) AS minute,
       percentile(latency_ms, 0.50) AS p50,
       percentile(latency_ms, 0.95) AS p95,
       percentile(latency_ms, 0.99) AS p99
FROM   latencies
WHERE  service = 'checkout'
GROUP  BY service, time_bucket(1m, ts);

-- 전체 구간 중앙값
SELECT percentile(value, 0.5) AS median FROM metrics WHERE series = 'cpu';
```

`percentile`은 인접 값 사이를 선형 보간하는 **정확한** 연속 백분위입니다(`q`는 0~1). 그룹의 값을 메모리에 유지하므로, 무제한 스캔보다는 크기가 제한된 다운샘플 버킷에 적합합니다.

## 7. 분포 · 산포 · 카디널리티

```sql
-- 시간 가중 평균: 각 값이 유효했던 시간만큼 가중.
-- 샘플 간격이 불규칙할 때는 avg() 대신 이것을 쓰세요.
SELECT time_bucket(1h, ts) AS bucket, time_weighted_average(value, ts) AS twa
FROM   metrics WHERE series = 'cpu' GROUP BY series, time_bucket(1h, ts);

-- 곡선 아래 면적 (value·초). 예: 전력(W) → 에너지(J)
SELECT time_bucket(1h, ts) AS bucket, integral(value, ts) AS area
FROM   metrics WHERE series = 'cpu' GROUP BY series, time_bucket(1h, ts);

-- 버킷별 산포
SELECT time_bucket(1h, ts) AS bucket, variance(value) AS var, stddev(value) AS sd
FROM   metrics WHERE series = 'cpu' GROUP BY series, time_bucket(1h, ts);

-- [0, 1000) ms를 10개 등간격 버킷으로 나눈 히스토그램.
-- 결과 리스트: [ <0ms, bucket1, .. bucket10, >=1000ms ]
SELECT histogram(latency_ms, 0, 1000, 10) AS dist
FROM   latencies WHERE service = 'checkout';

-- 분당 고유 클라이언트 IP 근사 개수 (HyperLogLog, 메모리 사용량 고정)
SELECT time_bucket(1m, ts) AS minute, approx_count_distinct(client_ip) AS unique_ips
FROM   requests WHERE service = 'api' GROUP BY service, time_bucket(1m, ts);
```

## 8. 이변량 통계 · 회귀

두 컬럼 사이의 관계를 서버에서 바로 계산합니다. 인자 순서는 `(y, x)` — y가 종속변수입니다.

```sql
-- 온도와 전력 사용량의 상관관계 (시간별)
SELECT time_bucket(1h, ts)          AS bucket,
       corr(power, temperature)     AS r,
       covar_samp(power, temperature) AS cov,
       regr_slope(power, temperature) AS slope,      -- 1도당 전력 증가량
       regr_intercept(power, temperature) AS intercept,
       regr_r2(power, temperature)  AS r_squared
FROM   sensors
WHERE  site = 'plant1'
GROUP  BY site, time_bucket(1h, ts);
```

## 9. 대시보드 종합 쿼리

버킷팅, OHLC, 변화량, 백분위를 한 번에:

```sql
SELECT time_bucket(1h, ts)     AS bucket,
       count(value)            AS samples,
       first(value, ts)        AS open,
       last(value, ts)         AS close,
       min(value)              AS low,
       max(value)              AS high,
       avg(value)              AS mean,
       delta(value, ts)        AS change,
       rate(value, ts)         AS per_second,
       percentile(value, 0.95) AS p95
FROM   metrics
WHERE  series = 'cpu'
  AND  ts >= '2024-01-01 00:00:00+0000'
  AND  ts <  '2024-01-02 00:00:00+0000'
GROUP  BY series, time_bucket(1h, ts);
```

## 10. 풀텍스트 검색 — SAI `LIKE` + `index_analyzer`

로그·이벤트 메시지 본문을 시계열 조회 패턴 안에서 검색합니다. `ngram` 분석기가 진짜 부분문자열 매치를 제공합니다(단어 중간 조각, 공백 걸침, 한글 전부 지원). 자세한 내용: [fulltext-search.md](doc/timeseries/fulltext-search.md)

```sql
CREATE TABLE logs (
    device text, ts timestamp, msg text,
    PRIMARY KEY (device, ts)
) WITH CLUSTERING ORDER BY (ts ASC);

CREATE INDEX logs_msg_idx ON logs(msg) USING 'sai'
  WITH OPTIONS = { 'index_analyzer': 'ngram' };

-- 장비 1대 · 1시간 구간에서 본문 검색 (ALLOW FILTERING 불필요)
SELECT ts, msg FROM logs
 WHERE device = 'pump-01'
   AND ts >= '2026-07-31 00:00' AND ts < '2026-07-31 01:00'
   AND msg LIKE '%타임아웃%';

-- 단어 중간 조각도 매치: '%imeou%' 가 "timeout" 을 찾음
-- 접두/접미/완전일치: 'connection%', '%9042', LIKE 'connection refused'
-- 다중 조각 AND: msg LIKE '%connection%' AND msg LIKE '%refused%'

-- 시계열 함수와 조합: 5분 버킷별 에러 건수
SELECT time_bucket(5m, ts), count(*) FROM logs
 WHERE device='pump-01' AND ts >= ? AND ts < ? AND msg LIKE '%timeout%'
 GROUP BY device, time_bucket(5m, ts);
```

동작 원리: 값 전체를 2~3글자 n-gram으로 색인(재현율) → 그램 교집합으로 후보 추출 → **원문에 LIKE 패턴 재적용**(정밀도). 색인은 원본 컬럼의 수 배 크기가 되므로 로그성 테이블에 선별 적용하세요. 2글자 미만 조각은 명시적 에러로 거부됩니다. `=`는 완전일치 의미를 유지합니다.

## 11. 시계열 컴팩션(TSCS) 설정

TWCS의 시간 정렬·통삭제와 UCS의 창 내부 컴팩션을 결합한 전용 전략입니다. 테이블 생성(또는 ALTER) 시 지정합니다:

```sql
ALTER TABLE pp.tm_tag_point WITH compaction = {
  'class': 'TimeSeriesCompactionStrategy',
  'window_size': '1d',           -- 시간 창 폭
  'freeze_after': '2h',          -- 창이 닫히고 이 시간이 지나면 동결(창당 1 SSTable로 수렴)
  'scaling_parameters': 'T4',    -- 현재 창 내부는 UCS에 위임 (UCS 문법 그대로)
  'target_sstable_size': '256MiB',
  'retention': '62d',            -- 선택: 창 상한이 now-62d를 지나면 컴팩션 없이 통째 삭제
  'max_future_window': '1d'      -- 선택: 미래 타임스탬프 가드 (기본 1d)
};
```

- 닫힌 창은 자동으로 **창당 1 SSTable**로 동결되어 읽기 증폭이 최소화되고, 동결 시점에 이미 만료된 TTL 데이터는 retention 없이 회수됩니다. 동결 이후에 만료되는 데이터의 회수는 `retention`이 담당합니다.
- 지각(백필) 데이터는 flush/스트리밍 시 창 경계에서 분리되어 **자기 창에 국소 편입**됩니다 — 현재 창 컴팩션을 오염시키지 않습니다.
- 상세: [설계 스펙](docs/superpowers/specs/2026-07-31-timeseries-compaction-design.md)

### 설정 유의사항

| 규칙 | 어기면 생기는 일 |
| --- | --- |
| `retention`은 **`window_size + freeze_after` 이상**이어야 합니다 | `ALTER` 시점에 명시적 에러로 거부됩니다. 아직 쓰이고 동결되는 중인 창이 삭제되지 않도록 하는 하한입니다 |
| **동결된 창은 다시 동결되지 않습니다** | 창이 SSTable 1개로 줄면 동결 후보에서 빠지므로, 그 **이후에** 만료되는 데이터는 동결로 회수되지 않습니다. TTL이 `freeze_after`보다 길면 `retention`이 필요합니다 — `default_time_to_live`만으로는 디스크에 남습니다 |
| 실제 보존 기간은 **`retention` + `window_size`** | 삭제 조건이 `windowStart <= now - retention - window_size`라, 창 안의 가장 최근 행은 적어둔 값보다 대략 창 하나만큼 더 살아남습니다. 놀라지 말고 미리 감안하십시오 |
| `timestamp_resolution`은 쓰기 측과 일치해야 합니다 | `USING TIMESTAMP` 값을 벽시계 시각으로 읽는 기준입니다. 쓰기가 정말 밀리초를 쓰는 게 아니라면 기본값 `MICROSECONDS`를 두십시오 — 틀리면 모든 행이 1000배 떨어진 창으로 분류돼, 데이터가 먼 미래로 사라지는 것처럼 보입니다 |
| `max_future_window`(기본 `1d`)는 필터가 아니라 가드입니다 | 이를 넘는 타임스탬프의 행은 **파킹**됩니다 — 컴팩션·동결·retention에서 전부 제외되고, 잘못된 시계가 지속되는 동안 flush마다 SSTable이 하나씩 쌓입니다. 테이블 MBean의 `FarFutureTimeSeriesSSTables`가 비어 있어야 정상이며, 아니라면 쓰기 측 시계를 고친 뒤 user-defined 또는 maximal 컴팩션으로 다시 씁니다 |

`window_size` 고르기: 너무 작으면 긴 보존 기간에서 SSTable이 수천 개가 되고, 너무 크면 동결과 retention의 단위가 거칠어집니다(창 하나보다 작게는 못 지웁니다). 창 1일 + 보존 62일이면 테이블당 62개로 무난합니다.

## 12. 컬럼형 동결 SSTable (`tscol`)

TSCS 테이블에 `frozen_format`을 넣으면 닫힌 창이 `tscol` SSTable 하나로 동결됩니다. 파티션 본문은 청크 코덱 블록입니다([chunk-format-v4.md](doc/timeseries/chunk-format-v4.md)):

```sql
ALTER TABLE pp.tm_tag_point WITH compaction = {
  'class': 'TimeSeriesCompactionStrategy',
  'window_size': '1d',
  'freeze_after': '2h',
  'frozen_format': 'columnar'    -- row(기본) 또는 columnar
};
```

다음 동결부터 `-tscol-Data.db` 파일이 생깁니다. 이미 행 형식으로 동결된 창은 한 번 변환되고 그 뒤로는 그대로 둡니다.

| 알아둘 것 | 내용 |
| --- | --- |
| 지원 스키마 | `timestamp` 클러스터링 컬럼 하나, 단일 셀이고 카운터가 아닌 일반 컬럼. 그 밖의 테이블은 옵션과 상관없이 행 형식으로 동결됩니다 |
| 동결된 데이터도 보통 데이터 | 늦은 쓰기·수정·삭제·TTL·일반 컬럼 필터·SAI가 평소대로 동작합니다. 거부되는 것이 없고 창이 재동결될 뿐이며, 복제본은 행 형식과 같은 답·다이제스트·리페어를 냅니다 |
| 보존 | TSCS `retention`이 창을 통째로 지웁니다(§11) |
| 옛 청크 스토어에서 옮길 때 | 이전 도구는 없습니다. [tscol-rollout.md](doc/timeseries/tscol-rollout.md)를 먼저 읽으십시오 |

설계: [컬럼형 동결 SSTable 스펙](docs/superpowers/specs/2026-11-04-columnar-frozen-sstable-design.md).

---

## 13. 운영 팁

- **항상 파티션을 지정하세요**(`WHERE series = ...`) 그리고 시간 범위도 함께. 시계열 스캔은 `ts`로 정렬된 단일 파티션 안에서 가장 저렴합니다.
- **파티션 크기를 제한하세요.** 고빈도 시리즈라면 파티션 키에 굵은 시간 버킷을 넣어 무한정 커지는 파티션을 막습니다. 예: `PRIMARY KEY ((series, day), ts)`.
- **TSCS 컴팩션**(`TimeSeriesCompactionStrategy`)을 쓰세요 — 창 정렬·동결·통삭제가 시계열에 맞게 자동화되고, 현재 창 내부는 UCS(`scaling_parameters: 'T4'`)에 위임됩니다. 보존은 `retention`(창 통삭제) 또는 `default_time_to_live`로 지정하면 됩니다.
- `time_bucket(interval, ts)`는 `GROUP BY`의 마지막 요소(파티션 키 컬럼들 뒤)여야 그룹핑이 읽기 경로로 푸시다운됩니다.

### 13.1 권장 조합 — TSCS · `tscol` · TimeSeriesMemtable

세 기능은 같은 **시간 창**을 기준으로 맞물립니다.

| 테이블 성격 | 조합 | 얻는 것 |
| --- | --- | --- |
| 모든 시계열 테이블 (기본) | **TSCS** | 창 단위 SSTable 분리, 닫힌 창의 동결(창당 1 SSTable), `retention`에 의한 창 통삭제. 혼자 써도 충분히 이득입니다 |
| 오래 보관하면서 공간을 줄일 테이블 | **TSCS + `frozen_format = 'columnar'`** | 닫힌 창을 컬럼형 `tscol` SSTable로 동결하고 SELECT는 그대로(§12) |
| 유입이 많은 테이블 | **TSCS + `TimeSeriesMemtable`** | 쓰기 시점에 행을 창으로 배정하고 원시 배열 컬럼에 저장 |

- **`TimeSeriesMemtable`은 두 단계로 켭니다**: ① 모든 노드의 `cassandra.yaml`에 `memtable.configurations` 키 추가 후 재시작, ② `ALTER TABLE ... WITH memtable = '<키>'`. 키가 없는 노드는 ERROR 로그 한 줄만 남기고 **조용히 기본 memtable로 폴백**하므로, 노드마다 실제로 켜졌는지 확인하십시오 — [timeseries-memtable.md §2, §5](doc/timeseries/timeseries-memtable.md).

### 13.2 운영할 때

- **TSCS 이상 신호 두 가지**: 테이블 MBean의 `ParkedTimeSeriesWindows`(진척 못 낸 창)와 `FarFutureTimeSeriesSSTables`(미래 타임스탬프로 파킹된 SSTable)는 **비어 있어야 정상**입니다. 전용 nodetool 명령이 없어 `nodetool sjk mx`로 읽습니다 — [prod-tscs-settings.md §2, §3](doc/timeseries/prod-tscs-settings.md).
- **Spark로 읽을 때**는 [aster-tsdb-spark-connector](http://dev.kopens.io/common/aster-tsdb-spark-connector) `4.1.4-r1` 이상을 쓰십시오.

---

## 빌드

요구 사항: **Java 21**, Ant 1.10 이상(테스트 실행 시 ant-junit 포함). `modules/accord`는 git 서브모듈이므로 `git submodule update --init`이 필요합니다.

```bash
.build/sh/ai-build     # clean + jar + checkstyle -> build/apache-cassandra-6.0.0.jar
```

빌드 산출물은 항상 `apache-cassandra-6.0.0.jar`입니다(`base.version`이 6.0.0으로 고정되어 있습니다).

> `ant`이 PATH에 없는 머신에서는 `ai-build`가 **아무것도 빌드하지 않고** `BUILD SUCCESSFUL`을 출력합니다 — 로그 요약기가 빈 입력에 그렇게 찍습니다. 그런 환경에서는 CI 컨테이너로 빌드하십시오: `.build/sh/ai-build-image` 후 `.build/sh/ai-in-container '<명령>'`, 게이트 전체는 `.build/sh/ci-local`. 확인은 로그가 아니라 jar의 타임스탬프로 하십시오.

## 검증

`.build/sh/ci-local`이 릴리스 게이트의 스테이지를 같은 순서로 로컬에서 돕니다 — jar+checkstyle → Aster 테스트 클래스 → 도커 이미지 → 통합 테스트.

```bash
.build/sh/ci-local                  # 3노드 클러스터 테스트까지 포함하려면 --with-cluster
.build/sh/ci-local --stage image    # 단일 스테이지: jar | tests | image | integration | cluster
```

## 통합 테스트 (릴리스 게이트)

유닛 테스트는 함수를 프로세스 안에서 검증하지만, [docker/integration-test.sh](docker/integration-test.sh)는 **실제 이미지를 띄워** 스키마 생성부터 읽기 경로·집계·네이티브 프로토콜까지 통과하는 시계열 CQL 결과를 손으로 계산한 값과 대조합니다(프로세스 재시작 포함).

```bash
docker build -t aster-tsdb:6.0.0 -f docker/Dockerfile .
./docker/integration-test.sh aster-tsdb:6.0.0     # CONTAINER_RUNTIME=podman 도 지원
```

실행하면 항목·CQL·결과가 그대로 출력되고, `build/timeseries-it-report.html`(+ 같은 내용의 `.md`)에 보고서가 생성됩니다. **실행 결과 예시: [통합 테스트 보고서](doc/timeseries/integration-test-report.md)** — 각 검증의 CQL·응답·소요 시간이 그대로 들어 있습니다.

CI에서는 태그를 밀면 `docker-image → docker-integration-test → docker-image-publish + release` 순서로 자동 실행되며, **이 테스트가 통과해야만** 이미지 배포와 릴리스가 진행됩니다. 기본 브랜치에서는 이미지 빌드 비용 때문에 수동(manual) 실행입니다.

### 클러스터 테스트 (3노드)

[docker/cluster-test.sh](docker/cluster-test.sh)는 도커 네트워크 위에 실제 컨테이너 3개를 RF=3으로 띄워 단일 노드가 닿지 못하는 것들을 검증합니다: 코디네이터 3개 각각을 통한 집계·gap-fill, 레플리카마다 독립적으로 일어나는 TSCS 동결과 `tscol` 수렴, OS 프로세스 간 실제 repair 스트리밍, 레플리카를 정말 정지시킨 상태의 QUORUM.

```bash
./docker/cluster-test.sh aster-tsdb:6.0.0
```

CI에서는 수동입니다(2G JVM 3개가 공용 러너에 안 들어갈 수 있음). 컴팩션·스트리밍·repair·`tscol`을 건드린 릴리스라면 손으로 한 번 돌리십시오.

### 성능 회귀 게이트

[.build/sh/ci-perf](.build/sh/ci-perf)가 청크 코덱·커서 JMH 클래스를 돌려 기록된 baseline과 비교하고, 임계값을 넘는 회귀에서 실패합니다.

```bash
.build/sh/ci-perf                   # doc/timeseries/perf-baseline.json 과 비교
.build/sh/ci-perf --record          # 기준선을 의도적으로 옮길 때만
```

3회 스윕의 최소값을 취하며(공용 머신에서 1회 스윕은 측정이 되지 못합니다), baseline을 뜬 호스트가 아니면 판정하지 않고 보고만 합니다.

### 스케일 테스트 (1억 건)

[docker/scale-test.sh](docker/scale-test.sh)는 컨테이너 노드에 대량 데이터를 적재하고 각 시계열 쿼리의 **CQL 실행 시간**을 측정합니다. 적재와 쿼리 모두 컨테이너 안에서 cqlsh 번들 파이썬 드라이버로 수행하므로(→ [docker/scale-workload.py](docker/scale-workload.py)) 측정값에 cqlsh 기동 시간이 섞이지 않습니다.

```bash
SCALE_ROWS=100000000 SCALE_SERIES=1000 SCALE_LOADERS=16 SCALE_HEAP=16G \
  ./docker/scale-test.sh aster-tsdb:6.0.0
# 적재된 데이터를 재사용해 쿼리만 다시 재기: SCALE_SKIP_LOAD=1
```

GC를 바꿔 비교할 수도 있습니다 — `SCALE_GC=g1`(기본은 `zgc`, `conf/jvm21-server.options`에 이미 generational ZGC가 켜져 있음), `SCALE_PASSES=2`(웜업 후 측정), `SCALE_WBENCH_ROWS=10000000`(쓰기 벤치). 두 실행 결과를 `docker/gc-compare.py <prefix-a> <prefix-b>`에 넣으면 비교표가 나옵니다 → **[GC 비교 결과](doc/timeseries/gc-comparison.md)**.

결과는 `build/timeseries-scale-report.html`(+ 같은 내용의 `.md`)에 생성됩니다. 용량 검증 기록: **[스케일 테스트 보고서 (1억 건)](doc/timeseries/scale-test-report.md)** — 1억 행 완주와 스캔 행 수 선형성.

주의: 수백만 행 이상을 집계하려면 서버 타임아웃을 올려야 합니다. `read/range_request_timeout`뿐 아니라 **`native_transport_timeout`(기본 12초)** 이 요청 전체를 자르므로 이 값도 함께 올려야 하며, 이 키는 기본 `cassandra.yaml`에 없어서 추가해야 합니다. 스크립트가 이 설정을 대신 해 줍니다.

### 초당 처리량 (ops/s) 벤치마크

위 스케일 테스트가 분석 쿼리 1건의 실행 시간을 잰다면, **초당 몇 건을 처리하는가**는 별도로 측정합니다: 쓰기는 `scale-workload.py load`의 적재 속도(rows/s)가 곧 측정값이고, 읽기는 [docker/rwbench-read.py](docker/rwbench-read.py)(운영 형태 3패턴 — 태그 최신값 / 단건 / 100행 시간창)와 번들 `cassandra-stress`(서버 한계 확인용)로 잽니다. **실행 결과: [읽기/쓰기 처리량 벤치마크](doc/timeseries/rw-throughput-benchmark.md)** — 적재 233k rows/s(호스트 234, 100행 배치), 쓰기 경로 424k rows/s·청크 인코딩 684k rows/s(호스트 237, JMH); 패턴별 읽기 ops/s는 v4 기준 재측정 대기. 재현 명령 전체가 리포트에 있습니다.

## CI 및 릴리스

- 푸시할 때마다 jar를 빌드하고 시계열 테스트 스위트를 실행합니다(`.gitlab-ci.yml`).
- 최신 `main` 빌드의 jar: *CI/CD → Pipelines → build-jar 아티팩트*.
- 태그 푸시(예: `v6.0.0`) 시 jar 다운로드 링크가 포함된 [Release](../../-/releases)가 발행됩니다.

> **빨간 파이프라인을 만나면 CI가 무엇을 말하는지부터 확인하십시오.** 2026-08-07 부터 프로젝트 러너가 offline 이었고, 그 기간의 파이프라인은 잡을 시작조차 못 한 채 `stuck_pending_no_matching_runners` 로 실패했습니다 — 그 빨강은 코드에 대한 진술이 아닙니다. 러너는 복구됐습니다(2026-09-16 실측, 2026-09-06 이후 초록). 그래도 습관은 유지할 값어치가 있습니다 — 버그를 찾으러 가기 전에 `glab ci list`, `glab ci get -p <id>` 로 사유를 먼저 보십시오. 로컬 재현은 `.build/sh/ci-local` 과 `docker/cluster-test.sh` 입니다. [production-rollout.md §6](doc/timeseries/production-rollout.md) 참고.

## 브랜치 및 업스트림 정책

- `main` 이 릴리스 라인이자 기본 브랜치입니다. apache/cassandra의 최신 업스트림 `cassandra-6.0` 브랜치(리모트 `upstream`)와 **항상 머지된 상태로 유지**해야 하며, `build.xml` 의 `aster.upstream.sha` 도 같은 커밋에서 함께 올립니다 — 그 값은 `git merge-base main upstream/cassandra-6.0` 과 같아야 합니다.
- `6.0.0` 은 그 라인의 옛 이름이고 2026-08-30 에서 멈춰 있습니다. 유지보수하지 **않으며**, 여기서 브랜치를 따지 마십시오.
- 자주 충돌하는 지점: `CHANGES.txt`, `debian/changelog`, `modules/accord` 서브모듈 포인터, `cql3/statements/SelectStatement.java`(gap-fill 연결부).

## 개발

빌드/테스트/코드 스타일 규칙은 [CLAUDE.md](CLAUDE.md)와 [AGENTS.md](AGENTS.md)를 참고하세요(전체 테스트 스위트는 몇 시간이 걸리므로 대상 테스트만 실행합니다). 테스트 레이아웃은 [TESTING.md](TESTING.md)에 있습니다. 시계열 테스트 진입점: `org.apache.cassandra.cql3.functions.TimeSeriesFctsTest`, `org.apache.cassandra.db.aggregation.TimeBucketGapFillerTest`.


## 라이선스

Apache License 2.0, 업스트림과 같습니다 — `LICENSE`·`NOTICE` 는 그대로 둡니다.
Apache Cassandra 는 Apache Software Foundation 의 상표입니다. Aster TSDB 는 독립 제품이며
ASF 와 제휴 관계가 없습니다.
