# 초당 읽기/쓰기 처리량 벤치마크

[계층화 벤치마크](tiering-benchmark.md)가 **분석 쿼리 1건의 실행 시간**을 재는 것이라면, 이
문서는 **초당 몇 건을 처리하는가(rows/s, ops/s)** 를 다룹니다. 수치는 반드시 측정 호스트와 함께
읽으십시오 — 호스트가 다르면 절대치는 직접 비교할 수 없습니다.

측정 호스트:

- **234** — Xeon Silver 4114T (Skylake-SP, 40 threads, 250 GB RAM) · 도커, CI와 동일 이미지 · 2026-08-04
- **237** — Xeon Silver 4210R (Cascade Lake, AVX-512) · JMH 마이크로벤치 · 2026-08-04

## 현재 수치

| 항목 | 결과 | 호스트 |
|---|---|---|
| **적재** — 운영 형태 `tm_tag_point`, 12 로더 × 100행 unlogged batch ([docker/scale-workload.py](../../docker/scale-workload.py)) | **233,048 rows/s** (20M행 / 86 s) | 234 |
| **재인코딩** — `nodetool retier`, 20M행 전체 | **108k rows/s** (185 s) — 설계 스펙 게이트(50k)의 2.2배 | 234 |
| **쓰기 경로 벤치** (`SCALE_WBENCH_ROWS`, scale-test.sh) | **424k rows/s** | 237 |
| **청크 인코딩** (JMH) — 운영 형태 (상수·null 컬럼 포함 8컬럼) | **684k rows/s** | 237 |
| **청크 인코딩** (JMH) — all-double 최악 형태 | **221k rows/s** | 237 |
| **청크 풀스캔** (JMH) — 3,600행 청크 전체 디코드+행 조립 | **740 µs** ≈ 4.9M rows/s (단일 스레드) | 237 |

JMH 소스: [test/microbench/.../Chunk\*Bench.java](../../test/microbench/org/apache/cassandra/test/microbench/)
(`ChunkEncodeBench`, `ChunkReadBench`, `ChunkBlockDecodeBench`, `ChunkBitUnpackBench`, `ChunkPresenceBench`).

프로파일에서 확인된 사실 두 가지 (호스트 237):

- 청크 스캔 시간에서 **ALP 역변환+비트 언팩 산술은 5~8%뿐**이고 나머지 ~92%는 커서 진행·행
  조립입니다 — 스캔을 더 줄이려면 SIMD가 아니라 행 조립 쪽을 건드려야 한다는 뜻입니다.
- presence 비트맵에서 행 위치를 찾을 때 rank-per-row 대신 running index를 쓰면 **6.7×**
  차이가 납니다 (현재 코드는 running index).

## 일정 속도 쓰기와 동시 조회

위 적재 수치는 최대 속도를 재는 값입니다. 정해진 속도를 조회와 함께 유지하는지는
[docker/rate-test.sh](../../docker/rate-test.sh)가 잽니다. `tm_tag_point` 형태에 `scale-workload.py`의 값 분포를
쓰고, 태그 수만큼을 1Hz로 넣습니다. 창과 `freeze_after`는 5분이라 동결이 부하 중에 일어나고, 끝나면 모든 태그의
행 수와 `latency` 합을 서버가 응답한 쓰기 원장과 대조합니다.

```bash
docker/rate-test.sh <image> [minutes] [rows-per-second]   # 기본 30분, 초당 1만 행
```

분 단위 속도가 목표의 99% 아래로 한 번이라도 내려가거나, 쓰기·읽기 오류, 대조 불일치, ERROR, OOM, 드롭이 있으면
FAIL이고 0이 아닌 값으로 끝납니다. 노드는 조사용으로 남습니다. 끝나면 `docker rm -f cassandra-ts-rate`로 지웁니다.

호스트 234, r9, 힙 16G, ZGC, 노드 CPU 16개 제한, 41 템플릿의 `concurrent_reads` 256,
`native_transport_max_threads` 256, `memtable_heap_space` 2GiB, `read_request_timeout` 10초, 30분 2회, 2026-11-05:

| 항목 | 1회 | 2회 |
|---|---|---|
| 판정 | FAIL (4분째 9,334행/초, 다음 분 10,663행/초) | PASS |
| 쓰기 | 18,010,000행, 오류 0 | 18,010,000행, 오류 0 |
| 대조 (태그 10,000개) | 불일치 0 | 불일치 0 |
| 분 단위 최저 | 9,334행/초 | 9,995행/초 |
| 쓰기 p50 / p99 / 최대 (클라이언트) | 16 / 54 / 291 ms | 16 / 45 / 236 ms |
| 서버 쓰기 지연 | 0.017 ms | 0.017 ms |
| 조회 p99: 최신값 / 10분 범위 / 전체 집계 / 1분 버킷 | 10.6 / 34 / 17 / 19 ms | 9.8 / 44 / 21 / 25 ms |
| 동결 (창 300만 행) | 약 10초, 약 48 → 12 MiB | 약 10초, 약 48 → 12 MiB |
| ERROR, OOM, 드롭 | 0 | 0 |

조회는 합쳐서 초당 약 20건입니다. 값 분포가 불리언 79%라 절대 크기(1,801만 행에 73 MiB)는 41에 옮길 수 없고, 같은
데이터에서 행 형식 대비 약 4.1배 줄어든 비율만 봅니다. 클라이언트 지연에는 Python 부하기 대기가 들어 있는 것으로 봅니다.

1회의 저하는 계측이 없어 원인을 확정하지 못했습니다. 2회에서는 플러시가 약 70초마다 돌아도 속도가 유지됐고,
부하기가 최대 3.3초 밀린 구간은 호스트 부하 78과 겹쳤으며 그때 서버에 걸린 쓰기는 33건이었습니다. 공유 호스트에서
FAIL이 나면 `out/lag.json`, `load.log`, `cpu.log`로 부하기 쪽 지연인지 먼저 봅니다.

41의 장비, 1일 창의 동결 시간, 초당 1만 행을 넘는 한계 속도는 이 측정으로 알 수 없습니다.

## v4 기준 재측정 대기

아래 항목은 **구형 24코어 호스트(X5670급)·v3 청크 시절 측정**만 있고, v4 빌드로는 아직 재측정하지
않았습니다. 서버 자체의 단건 조회 포화점(cassandra-stress)도 현세대 호스트 기준 측정이 없습니다.

| 패턴 (계층화 테이블) | 마지막 측정값 — 구형 호스트, v3 청크 시절 측정, v4 재측정 전 |
|---|---|
| `latest` (`WHERE tag_id=? LIMIT 1`) | 10,702 ops/s |
| `point` (타임스탬프 단건) | 11,324 ops/s |
| `range100` (100행 시간창) | 349k rows/s |

정성적 결론은 유효합니다: **시계열 조회는 넓게 끊어 읽는 쪽이 압도적으로 유리합니다.** 요청 1건의
고정 비용이 지배적이라, 행 기준 처리량은 윈도우 스캔이 단건 조회의 수십 배가 나옵니다.

## 방법 주의점

- 단일 노드 · RF=1 · CL=LOCAL_ONE · 클라이언트가 같은 컨테이너에서 실행 — 복제·쿼럼·네트워크
  왕복 비용이 없는 수치입니다. 3노드 RF=3 쿼럼이라면 쓰기는 대략 1/2~1/3 수준을 예상하는 것이
  안전합니다.
- 적재 rows/s는 **100행 unlogged batch** 기준입니다. 단건 INSERT의 한계는 별도로
  `cassandra-stress`로 재야 합니다.
- 파이썬 드라이버 기반 읽기 하네스([docker/rwbench-read.py](../../docker/rwbench-read.py))는
  워커당 ~1.4k ops/s에서 클라이언트가 먼저 포화합니다 — 서버 한계 확인은 Java 클라이언트
  (`cassandra-stress`)로 하십시오.
- 콜드 노드에 파이썬 드라이버로 동시 접속하면 control connection의 기본 2초 타임아웃이 첫
  시스템/스키마 읽기에서 죽습니다 — cqlsh는 되고 드라이버만 안 되는 모양새면 이것입니다.
  `rwbench-read.py`는 30초로 올려 두었습니다.

## 재현

```bash
docker build -t aster-tsdb:6.0.0 -f docker/Dockerfile .

# 쓰기(운영 형태): 적재가 곧 측정 — rows/s를 출력. 쓰기 경로 벤치는 SCALE_WBENCH_ROWS.
./docker/scale-test.sh aster-tsdb:6.0.0        # SCALE_ROWS/SCALE_SERIES/SCALE_LOADERS 조절

# 계층화·재인코딩(rows/s 포함): 같은 데이터에 이어서
./docker/tiering-bench.sh aster-tsdb:6.0.0

# 읽기 패턴별 ops/s (latest | point | range100):
docker exec <container> python3 /tmp/rwbench-read.py \
  --series 500 --rows-per-series 40000 --workers 12 --inflight 64 \
  --duration 60 --pattern latest

# 서버 한계(표준 stress): tools/bin은 이미지에서 실행권한이 없어 chmod가 필요
docker exec <container> chmod +x /opt/cassandra/tools/bin/cassandra-stress
docker exec <container> sh -c \
  "/opt/cassandra/tools/bin/cassandra-stress write n=20000000 -rate threads=128"
docker exec <container> sh -c \
  "/opt/cassandra/tools/bin/cassandra-stress read duration=60s \
     -pop dist='uniform(1..20000000)' -rate threads=128"

# JMH 마이크로벤치 (청크 인코드/디코드):
ant microbench -Dbenchmark.name=ChunkEncodeBench   # ChunkReadBench 등도 동일
```
