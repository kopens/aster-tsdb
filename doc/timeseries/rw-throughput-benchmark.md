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
