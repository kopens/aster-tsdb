# 카산드라 시계열 데이터베이스에서 G1GC vs Generational ZGC

**Apache Cassandra 6.0.0 기반 시계열 DB에 1억 건을 넣고, 두 가비지 컬렉터로 같은 쿼리를 돌려 비교한 기록**

작성일: 2026-07-30 · 대상: [aster-tsdb](https://dev.kopens.io/common/aster-tsdb) (Apache Cassandra 6.0.0 + 네이티브 시계열 CQL 함수)

> 측정 환경: 구형 24코어 컨테이너(X5670급), v3 청크 시절. GC 간 **상대 비교**와 결론
> (Generational ZGC 채택 — 현행 설정)은 유효하나, 절대 수치는 현재 기준이 아니다.

---

## 요약

| 항목 | Generational ZGC | G1 | 판정 |
| --- | --- | --- | --- |
| 시계열 쿼리 17종 합계 | **275.4 초** | 351.4 초 | G1이 **27.6% 더 오래** 걸림 |
| 단일 파티션 집계 (10만 행) | 227 ~ 437 ms | 301 ~ 657 ms | ZGC 우세 (19~50%) |
| 다중 파티션 집계 (1000만 행) | 33.3 초 | 44.3 초 | ZGC 우세 (33%) |
| 전체 스캔 (1억 행) | 206.0 초 | 261.9 초 | ZGC 우세 (27%) |
| 쓰기 처리량 (1천만 건 적재) | **296,636 rows/s** | 287,793 rows/s | ZGC 3% 우세 |
| 최대 STW 일시정지 | 로그상 미검출(동시 수집) | **459 ms** | ZGC 우세 |

**17개 쿼리 전부에서 ZGC가 빨랐습니다.** 방향이 한 번도 뒤집히지 않았다는 점이 개별 수치보다 중요합니다.

결론부터: **Cassandra 6.0.0 + Java 21 환경의 시계열 워크로드에서는 기본값인 generational ZGC를 그대로 쓰는 것이 맞습니다.** 이 저장소의 `conf/jvm21-server.options`는 이미 ZGC를 켜고 G1 블록을 주석 처리한 상태로 배포되므로, 이 글은 실질적으로 **"굳이 G1으로 되돌리면 무엇을 잃는가"** 에 대한 답입니다.

---

## 1. 왜 이걸 재봤나

시계열 워크로드는 일반적인 OLTP 키-값 조회와 성격이 다릅니다.

- **집계가 기본**입니다. `time_bucket(1h, ts)` + `avg/percentile/first/last` 같은 쿼리가 수만~수백만 행을 훑으면서 셀·행·집계 상태 객체를 대량으로 만들어냅니다. 즉 **할당률(allocation rate)이 매우 높습니다.**
- **대시보드 응답 지연에 민감**합니다. GC 일시정지 한 번이 곧 그래프가 끊기는 시간입니다.
- **읽기 타임아웃이 짧습니다.** Cassandra 기본 `read_request_timeout`은 5초, `range_request_timeout`은 10초입니다. 수백 ms짜리 STW가 여러 번 겹치면 타임아웃 실패로 직결됩니다.

Java 21은 generational ZGC를 실용 단계로 끌어올렸고 Cassandra 6.0은 이를 기본값으로 채택했습니다. 그렇다면 실제로 이 워크로드에서 얼마나 차이가 나는가 — 그것을 실측했습니다.

---

## 2. 테스트 환경

### 하드웨어 / 런타임

| 항목 | 값 |
| --- | --- |
| 호스트 | 24 코어, 62 GB RAM, 리눅스 5.14 |
| 배치 | 단일 노드, Docker 컨테이너 (고정 IP), 데이터는 호스트 디스크에 바인드 마운트 |
| Java | Temurin 21.0.11 (JRE) |
| 힙 | `MAX_HEAP_SIZE=16G`, `HEAP_NEWSIZE=4G` (양쪽 동일) |
| DB | Apache Cassandra 6.0.0 포크 (시계열 CQL 함수 추가), 소스 빌드 |

### 스키마와 데이터

```sql
CREATE TABLE scale.metrics (
    series text,
    ts     timestamp,
    value  double,
    PRIMARY KEY (series, ts)
) WITH CLUSTERING ORDER BY (ts ASC)
   AND compaction = {'class': 'UnifiedCompactionStrategy',
                     'scaling_parameters': 'T4',
                     'target_sstable_size': '1GiB',
                     'expired_sstable_check_frequency_seconds': 600};
```

- **1억 행** = 1,000 파티션 × 100,000 행 (파티션당 약 27.8시간 분량, 1초 간격)
- 값은 결정적 생성: `50 + 40·sin(i/500) + (i mod 7)` — 재현 가능하고 집계 결과가 자명하지 않도록
- 적재 후 live 데이터 **1.47 GB**, SSTable 9개
- 컴팩션은 Cassandra 6.0의 통합 전략 UCS(tiered `T4`)

### GC 설정

두 설정 모두 `conf/jvm21-server.options`에 이미 존재하는 블록을 그대로 사용했습니다. 즉 **임의 튜닝 없이 배포 기본값끼리의 비교**입니다.

**Generational ZGC (배포 기본값)**
```
-XX:+UseZGC
-XX:+ZGenerational
-XX:-UseCompressedOops        # jamm 호환용 워크어라운드 (ZGC 전용)
```

**G1 (주석 해제)**
```
-XX:+UseG1GC
-XX:+ParallelRefProcEnabled
-XX:MaxTenuringThreshold=2
-XX:G1HeapRegionSize=16m
-XX:+UnlockExperimentalVMOptions
-XX:G1NewSizePercent=50
-XX:G1RSetUpdatingPauseTimePercent=5
-XX:MaxGCPauseMillis=300
```
G1 쪽은 **compressed oops를 되살렸습니다.** 위 `-XX:-UseCompressedOops`는 ZGC 전용 워크어라운드이므로, 그대로 두면 G1에 불리하게 작용합니다. 즉 이번 비교는 G1에 유리한 쪽으로 조건을 맞춘 것입니다.

### 서버 타임아웃 (양쪽 동일)

수백만 행 집계는 기본 타임아웃 안에 끝나지 않으므로 다음을 올렸습니다.

```yaml
read_request_timeout:  600000ms
range_request_timeout: 600000ms
request_timeout:       600000ms
native_transport_timeout: 600s     # 기본 12초, cassandra.yaml에 키가 없어 추가해야 함
```

> **실무 함정 하나.** `read/range_request_timeout`만 올리면 1000만 행 이상 집계가 **정확히 12초에서** `ReadTimeout`으로 죽습니다. 원인은 `native_transport_timeout`(기본 12초)이 요청 전체에 거는 상한인데, 이 키는 배포되는 `cassandra.yaml`에 **아예 존재하지 않아** `sed` 치환으로는 잡히지 않고 새로 추가해야 합니다. 대용량 집계를 쓸 계획이라면 반드시 확인하세요.

---

## 3. 측정 방법

```
GC 설정 적용 → 컨테이너 기동 → 웜업 패스(쿼리 17종) → 측정 패스(쿼리 17종) → 쓰기 벤치 1천만 건
```

- **같은 데이터, 같은 노드, 같은 쿼리.** ZGC 측정 후 동일한 데이터 디렉터리를 재사용해 G1을 측정했습니다.
- **웜업 1회 후 측정 1회.** 페이지 캐시와 JIT 상태를 맞추기 위함입니다. 보고하는 수치는 전부 두 번째(웜) 패스입니다.
- **측정 위치는 컨테이너 내부**입니다. cqlsh 기동 비용(약 0.8초)이 섞이지 않도록, cqlsh에 번들된 파이썬 드라이버로 직접 접속해 `session.execute()` 왕복만 계측했습니다.
- 양쪽 모두 `-Xlog:gc`로 JVM GC 로그를 남겨 사후 집계했습니다.

재현 명령:

```bash
# ZGC (기본값)
SCALE_ROWS=100000000 SCALE_SERIES=1000 SCALE_HEAP=16G SCALE_LOADERS=16 \
SCALE_GC=zgc SCALE_PASSES=2 SCALE_WBENCH_ROWS=10000000 \
SCALE_REPORT=build/scale-zgc.html ./docker/scale-test.sh aster-tsdb:6.0.0

# G1 (같은 데이터 재사용)
SCALE_SKIP_LOAD=1 SCALE_GC=g1 SCALE_PASSES=2 SCALE_WBENCH_ROWS=10000000 \
SCALE_REPORT=build/scale-g1.html ./docker/scale-test.sh aster-tsdb:6.0.0

# 비교표 생성
python3 docker/gc-compare.py build/scale-zgc build/scale-g1 > gc-comparison.md
```

---

## 4. 결과 ① 쿼리 지연

| 구분 | 쿼리 | ZGC | G1 | 차이 |
| --- | --- | ---: | ---: | ---: |
| 단일 파티션 (10만 행) | `count(*)` | 227 ms | 301 ms | +32.3% |
| | `time_bucket(1h)` + avg/min/max | 350 ms | 462 ms | +32.2% |
| | `time_bucket(5m)` + avg | 334 ms | 436 ms | +30.5% |
| | `first`/`last`/`delta`/`rate` 시간별 | 375 ms | 499 ms | +32.9% |
| | `derivative` 시간별 | 338 ms | 431 ms | +27.6% |
| | `percentile` p50/p95/p99 | 306 ms | 377 ms | +23.3% |
| | `variance`/`stddev` | 265 ms | 326 ms | +22.7% |
| | `histogram` 20버킷 | 274 ms | 329 ms | +20.2% |
| | `approx_count_distinct` | 280 ms | 333 ms | +18.9% |
| | `integral` + `time_weighted_average` | 287 ms | 346 ms | +20.8% |
| Gap-fill | gapfill 1h + `locf` 전 구간 | 376 ms | 431 ms | +14.7% |
| | gapfill 5m + `interpolate` 6시간 | 91 ms | 99 ms | +8.9% |
| 다중 파티션 | 10 시리즈 시간별 평균 (100만 행) | 3,362 ms | 4,268 ms | +27.0% |
| | 100 시리즈 시간별 평균 (1000만 행) | 33,333 ms | 44,303 ms | +32.9% |
| | 100 시리즈 p95 (1000만 행) | 28,785 ms | 35,878 ms | +24.6% |
| 대시보드 | OHLC + delta + rate + p95 한 쿼리 | 437 ms | 657 ms | +50.2% |
| 전체 스캔 | `count(*)` 1억 행 | 205,983 ms | 261,894 ms | +27.1% |
| | **합계** | **275,404 ms** | **351,370 ms** | **+27.6%** |

`+`는 G1이 더 느리다는 뜻입니다. 뒤집어 말하면 ZGC가 총 소요를 **21.6% 줄였습니다.**

**처리량으로 환산하면:**

| 쿼리 | ZGC | G1 |
| --- | ---: | ---: |
| 전체 스캔 (1억 행) | 485,000 행/초 | 381,800 행/초 |
| 100 시리즈 집계 (1000만 행) | 300,000 행/초 | 225,700 행/초 |
| 단일 파티션 시간별 집계 | 286,000 행/초 | 216,500 행/초 |

읽어낼 점 두 가지입니다.

1. **차이가 쿼리 크기에 비례하지 않습니다.** 10만 행짜리 짧은 집계에서도 20~30% 벌어집니다. 이는 "가끔 오는 큰 일시정지" 때문이 아니라, 집계 경로 전반의 할당·회수 비용 차이가 꾸준히 반영된 결과로 보는 것이 타당합니다.
2. **가장 크게 벌어진 것은 대시보드 종합 쿼리(+50.2%)** 입니다. `first`/`last`/`delta`/`rate`/`percentile`을 한 번에 계산하느라 집계 상태 객체가 가장 많이 생기는 쿼리이고, 정확히 그 지점에서 격차가 최대였습니다.

---

## 5. 결과 ② 쓰기 처리량

1천만 건을 200 파티션에 적재 (16개 로더 프로세스, 파티션 내부 unlogged 배치 100행):

| GC | 소요 | 처리량 |
| --- | ---: | ---: |
| Generational ZGC | 34 초 | **296,636 rows/s** |
| G1 | 35 초 | 287,793 rows/s |

차이는 **3.0%** 로, 읽기 쪽에 비하면 미미합니다. 참고로 1억 건 최초 적재는 ZGC에서 **356초, 280,806 rows/s** 였습니다.

쓰기 경로는 커밋로그 + memtable 삽입이 주도하고 객체 수명이 짧아, 두 컬렉터 모두 young 영역에서 저렴하게 처리합니다. **GC 선택이 크게 갈리는 곳은 쓰기가 아니라 집계 읽기입니다.**

---

## 6. 결과 ③ GC 동작 특성

측정 구간 전체의 JVM GC 로그 집계입니다.

| GC | 성격 | 횟수 | 합계 | 평균 | 최대 |
| --- | --- | ---: | ---: | ---: | ---: |
| G1 | **STW 일시정지** | 31 | 1,477 ms | 47.7 ms | **459 ms** |
| Generational ZGC | 동시 수집 사이클 | 75 | 290,960 ms | 3,879 ms | 18,668 ms |

**두 숫자를 같은 축에서 비교하면 안 됩니다.** G1의 1,477 ms는 애플리케이션이 **멈춘** 시간이고, ZGC의 290,960 ms는 애플리케이션이 **돌아가는 동안 백그라운드로 수집한** 시간입니다. ZGC는 사이클이 길고 잦아도 그 시간 동안 쿼리를 계속 처리합니다.

시계열 운영 관점에서 의미 있는 숫자는 **G1의 최대 일시정지 459 ms** 입니다.

- Cassandra 기본 `read_request_timeout`은 5,000 ms입니다. 459 ms 정지 자체가 타임아웃을 유발하진 않지만, 이미 4초 걸리던 무거운 집계에는 치명적입니다.
- 대시보드 p99 지연에 그대로 얹힙니다. 1초 목표 SLO에서 459 ms는 절반입니다.
- 노드가 멈춘 동안 코디네이터로 들어온 요청은 큐에 쌓이고, 이는 `native_transport_timeout`(기본 12초) 상한을 갉아먹습니다.

---

### 왜 이런 차이가 났을까

여기서부터는 측정이 아니라 해석입니다. 우리가 측정한 것은 결과(지연·처리량·정지 시간)이지 JVM 내부 동작이 아니므로, 아래는 관측과 모순되지 않는 설명이라는 정도로 읽어주십시오.

시계열 집계 쿼리는 **수명이 아주 짧은 객체를 대량으로 쏟아냅니다.** SSTable에서 읽어 올린 셀과 행, 반복자, 집계 상태가 버킷 단위로 만들어졌다가 즉시 버려집니다. 100만 행을 훑는 `time_bucket` 집계 하나가 수백 MB를 할당하고 그중 거의 전부가 곧바로 쓰레기가 되는 구조입니다.

이런 패턴에서 두 컬렉터의 성격 차이가 드러납니다. G1은 회수 작업 상당 부분을 STW 구간에 몰아서 처리하므로, 할당률이 높아질수록 정지가 잦아지거나 길어집니다. 실제로 이번 측정에서 G1은 31회 정지에 최대 459 ms를 기록했습니다. 반면 generational ZGC는 대부분의 작업을 애플리케이션과 **동시에** 수행하고, 세대 분리 덕에 "금방 죽는 객체"를 young 영역에서 저렴하게 회수합니다. 사이클이 75회, 합계 291초로 훨씬 많고 길지만 그동안 쿼리는 계속 처리됩니다.

집계 규모와 무관하게 20~30%의 격차가 일정하게 유지된 점도 이 해석과 맞습니다. 큰 정지가 가끔 끼어드는 것이라면 짧은 쿼리에서는 차이가 희석돼야 하는데 그렇지 않았습니다. 반대로 쓰기 벤치에서 차이가 3%로 줄어든 것도 같은 맥락입니다 — 쓰기는 커밋로그와 memtable이 주도해 힙 압박 자체가 훨씬 덜합니다.

## 7. 그래서 시계열 DB에는 무엇을 써야 하나

**Generational ZGC를 그대로 두십시오.** 이 워크로드에서 G1으로 되돌릴 이유를 찾지 못했습니다. 읽기 집계는 20~30% 느려지고, 쓰기 이득은 없으며(오히려 3% 손해), 수백 ms STW를 새로 떠안게 됩니다.

함께 확인할 설정:

| 설정 | 권장 | 이유 |
| --- | --- | --- |
| `-XX:+UseZGC -XX:+ZGenerational` | 유지 (6.0 기본값) | 위 측정 결과 |
| `-XX:-UseCompressedOops` | ZGC 사용 시 유지 | jamm 기본값 문제 회피용. **G1으로 바꾼다면 반드시 되돌릴 것** |
| `native_transport_timeout` | 대용량 집계 시 상향 | 기본 12초가 요청 전체를 자름. `cassandra.yaml`에 키가 없어 추가 필요 |
| `read/range_request_timeout` | 집계 규모에 맞춰 상향 | 기본 5초/10초 |
| 컴팩션 | UCS, tiered (`T4`) | append-only 수집 패턴에 적합 |
| 파티션 설계 | 파티션당 수만~수십만 행 | 10만 행 파티션 집계가 0.3초대. 무한정 커지는 파티션은 금물 |
| `-XX:+AlwaysPreTouch` | 검토 | 기본 주석 처리. 기동 후 초기 할당 지연 제거 |

---

### 그래도 G1을 고려할 만한 경우

한쪽으로만 몰아가지 않기 위해 반대편도 적어둡니다. 이번 측정은 "24코어·62GB 호스트에 16GB 힙, 집계 중심 시계열"이라는 조건에서의 결과이며, 아래 상황이라면 판단이 달라질 수 있습니다.

- **힙이 작을 때(수 GB 이하).** ZGC는 컬러드 포인터와 로드 배리어를 위해 일정한 상시 비용을 치릅니다. 힙이 작고 라이브셋도 작으면 그 비용을 회수하지 못할 수 있습니다.
- **CPU 여유가 없을 때.** ZGC의 동시 수집은 애플리케이션을 멈추지 않는 대신 CPU를 함께 씁니다. 코어가 빠듯한 노드에서는 동시 수집 스레드가 쿼리 스레드와 경쟁합니다. 이번 호스트는 24코어로 여유가 있었습니다.
- **지연보다 총 처리량만 중요한 배치성 워크로드.** 대시보드 응답이 아니라 야간 롤업만 돌리는 노드라면 수백 ms 정지가 문제되지 않습니다.
- **Java 21 미만.** 비세대 ZGC(JDK 15~20)는 이번에 측정한 generational ZGC와 다릅니다. 세대 분리가 없으면 시계열 집계처럼 단명 객체가 많은 워크로드에서 이점이 크게 줄어듭니다.

정리하면, **집계 읽기가 주력이고 힙이 크며 CPU에 여유가 있는 시계열 노드**가 generational ZGC의 이점이 가장 크게 나타나는 조건이고, 이번 측정 대상이 정확히 그 경우였습니다.

## 8. 한계

정직하게 밝혀둡니다.

1. **GC별 측정 1회**(웜업 후)입니다. 같은 GC로 반복했을 때 쿼리당 약 10% 변동이 있었으므로, **개별 항목의 10% 이내 차이는 노이즈**로 보셔야 합니다. 다만 17개 항목 전부에서 부호가 같으므로 결론의 방향은 견고합니다.
2. **ZGC의 실제 STW 시간은 이 글에 없습니다.** `-Xlog:gc` 레벨에서 ZGC가 남기는 것은 동시 수집 사이클 길이이지 정지 시간이 아닙니다. 진짜 pause 비교가 필요하면 양쪽을 `-Xlog:gc*,safepoint`로 다시 측정해야 합니다(스크립트는 이후 실행부터 이 옵션을 켭니다).
3. **단일 노드**입니다. 복제·힌트·리페어가 도는 다중 노드에서는 GC 부하 양상이 달라질 수 있습니다.
4. **G1을 별도로 튜닝하지 않았습니다.** 배포본에 주석으로 들어 있는 설정 그대로입니다. `MaxGCPauseMillis`를 늘리면 처리량이 다소 오를 여지는 있습니다.
5. 힙 16 GB 기준입니다. 힙이 훨씬 크거나 작으면 결론의 크기(비율)는 달라질 수 있습니다.

---

## 9. 원자료

같은 저장소에 측정 원본이 있습니다.

- [GC 비교 표 (자동 생성)](gc-comparison.md) — 이 글의 근거 데이터
- [스케일 테스트 보고서](scale-test-report.md) — 1억 건 기준 쿼리별 CQL 실행 시간, 실행 CQL과 응답 원문 포함
- [통합 테스트 보고서](integration-test-report.md) — 시계열 함수 32개 검증(기능 정확성)
- 스크립트: [`docker/scale-test.sh`](../../docker/scale-test.sh), [`docker/scale-workload.py`](../../docker/scale-workload.py), [`docker/gc-compare.py`](../../docker/gc-compare.py)

---

## 부록: 측정 중 부딪힌 실무 이슈 3가지

시계열 DB를 컨테이너로 운영할 때 그대로 만나게 될 문제들이라 남겨둡니다.

**① `native_transport_timeout` 12초 상한**
`read/range_request_timeout`을 600초로 올렸는데도 1000만 행 집계가 매번 정확히 12초에서 `ReadTimeout(code=1200)`으로 실패했습니다. 범인은 요청 전체에 걸리는 `native_transport_timeout`(기본 12초)이었고, 이 키는 배포 `cassandra.yaml`에 없어서 새로 추가해야 했습니다.

**② 데이터 디렉터리 재사용 시 노드 IP 고정 필요**
컨테이너를 지우고 같은 데이터로 다시 띄웠더니 노드가 `NORMAL`에 도달하지 못했습니다. 클러스터 메타데이터에 이전 IP가 자기 자신으로 기록돼 있는데, 기본 bridge 네트워크가 다른 IP를 배정하면서 **예전 자기 자신에게 gossip을 시도**하는 상태가 됩니다. 전용 네트워크 + 고정 IP로 해결했습니다.

**③ 컨테이너 이미지에 python3 부재**
`eclipse-temurin:21-jre` 기반 런타임 이미지에는 python3가 없어 번들된 `cqlsh`가 실행되지 않았습니다. Testcontainers 대기 전략이 `cqlsh`를 쓰는 경우가 많으므로, 이미지에 python3를 포함시켜야 합니다.
