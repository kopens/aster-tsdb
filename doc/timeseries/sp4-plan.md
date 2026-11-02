# SP4: 최신 시계열 기술 적용 — 종결 기록

상태: **종결 (2026-08-04).** SP4의 4기둥(① Compressed Query ② Vectorized 집계 ③ SIMD 디코드
④ 쓰기 최적화 3라운드)은 아래 최종 상태로 끝났다. 출하된 것은 상태 한 줄과 근거 문서 링크만
남기고, **폐기된 것은 이유와 함께** 남긴다(같은 제안이 다시 나올 때 재론하지 않기 위해).
미결 항목은 다음 트랙의 입력이다.

## 출하 (shipped)

- **쓰기 최적화 3라운드** — TimeSeriesMemtable 인제스트 93.6k → **156.4k rows/s**
  (SkipList 베이스라인 145.4k 대비 +7.6%). DESC 고속 경로·역순 long 스토어·셀 조립 경량화·
  꼬리 해시 인덱스, 이후 파티션 락 ReentrantLock화(경합 카운터)·스킵리스트 이중 탐색 제거·
  dataSize 융합. 세부: [memtable-write-tuning.md](memtable-write-tuning.md) ·
  [rw-throughput-benchmark.md](rw-throughput-benchmark.md). (커밋 `6d5f2a3156` · `97cabeac4c`)
- **tiered `latest` 푸시다운 — 73×** (146 → 10,702 ops/s, p50 34 → 1.34 ms). 근본 원인은
  코디네이터의 `DigestResolver.getData()`가 병합 스트림에 LIMIT 카운터를 적용하지 않던 것 —
  계층화 읽기는 `TransparentReads.maybeWrap`이 레플리카의 LIMIT **이후에** 청크 행을 합류시키므로
  업스트림의 "레플리카가 이미 잘랐다" 불변식이 깨진다. `DataResolver`와 같은 위치·같은 인자의
  카운터를 추가(비계층 읽기는 바이트 단위로 기존 동작 유지). 함께: 창 목록 lazy 나열, 하강 분기
  역방향 커서, 소스별 prepared statement 메모이제이션. **검증 규율: 네트워크 경로(`executeNet`)로
  테스트할 것** — 로컬 `execute()`만 green인 수정이 운영 경로에서 무효였던 것이 이 병목이 숨어
  있던 이유다. (커밋 `bfc8000248` · `97cabeac4c`)
- **ALP / ALP-RD double 코덱** — 유일한 double 인코딩. 판정 근거와 실측:
  [codec-bakeoff.md](codec-bakeoff.md). 부수 수정: 기존 경로의 NaN 페이로드 정규화 무손실성 구멍.
- **청크 포맷 v4** — 설계 → 구현 → 배선(`5cbdf914fa`) → v3 내부 삭제(`6f4ce9faa7`). 유일한 청크
  포맷이며 v1/v2/v3는 `UnsupportedChunkFormatException`. 벤치(호스트 234): 저장 **7.1×**, 집계
  질의 **4.0~6.1×** 빠름 — v3의 "저장 절감 대가로 질의 감속" 트레이드오프가 사라졌다.
  [chunk-format-v4.md](chunk-format-v4.md) · [tiering-benchmark.md](tiering-benchmark.md).
- **SIMD 디코드 — 측정 후 보류 확정** — 사전 등록 임계값 실측으로 종결: ALP+언팩은 청크 스캔의
  5~8%(임계값 15% 미달), 지배 비용은 행 조립 ~92%. 스칼라 이득 2건(RLE 워드 전개, running-rank
  규칙 — 6.7×)은 반영. 재론 트리거는 컬럼나 직접 집계 경로의 신설.
  [simd-decode-design.md](simd-decode-design.md).
- **TSCS 컴팩션 위생** (델리게이트는 유지 — 제거는 아래 미결 게이트 뒤): 라운드당 스냅샷 1회
  (5회 순회·TreeMap 3회 재구축 제거) · UCS에는 있고 TSCS에 없던 compacting/EARLY 필터를
  **선정 자격에만** 적용(분류·무진행 시그니처는 비필터 창 유지 — 아니면 가드 체인이 오탐) ·
  head-of-line 블로킹 수정(compacting에 막힌 동결이 이후 동결·split을 굶기지 않음) · 거부 WARN
  스로틀 · 델리게이트 백로그 항의 조기 반환 라운드 갱신. (커밋 `97cabeac4c`)

## 폐기 (되돌리지 말 것, 이유와 함께)

- **집계 푸시다운 / 청크 메타 집계 / 벡터화 집계 커널** — 전부 같은 벽에 막힌다: 이 코드베이스에도
  업스트림에도 **부분 집계 상태를 결과로 내보내는 경로가 없다.** 집계는 코디네이터가 행을 병합한 뒤
  행 단위로 접는다. 통계를 완벽히 만들어도 표준 집계 기계를 우회하는 특수 경로가 필요하고, 그건
  LIMIT·페이징·gap-fill·읽기수리와 동시에 얽힌다. 사용자 결정으로 폐기. v4 통계가 **가지치기
  전용**인 이유이기도 하다([chunk-format-v4.md §1](chunk-format-v4.md)).
- **`__chunks` 스키마 확장(존맵 컬럼)** — 사용자 결정으로 폐기. 이득이 좁은 구간 조회 2× 정도로
  작고, 운영 섀도 테이블에 자동 ALTER를 태우는 리스크에 비해 남는 게 없다.
- **v3 payload 트레일러 통계** — payload 바이트가 바뀌어 `chunkUnchanged`가 전부 깨지고 콜드 전체
  재인코딩 폭풍이 난다. 게다가 위 집계 벽 때문에 쓸 데가 없다. (v3 자체가 이후 v4로 대체·삭제되어
  이 방향은 이중으로 닫혔다.)
- **UCS 소스 복사** — UCS 1,008줄 + 부속 1,269줄. 업스트림 머지 부담이 영구화되고, 우리가 안 쓰는
  복잡도(토큰 샤딩, 다자릿수 밀도 레벨)를 떠안는다. TSCS는 상속이 아니라 **위임**이므로
  (`extends AbstractCompactionStrategy`, UCS는 필드) UCS를 안 고치고 경계만 바꿀 수 있다.

## 미결 — 다음 트랙의 입력

- **TSCS 델리게이트 제거의 운영 게이트**: 제거 근거는 비용이 아니라
  "`SplitRefreezeCompactionTask`가 `WindowFrozenListener`를 발화하지 않아 델리게이트 정상 상태에서
  동결 이벤트가 사실상 0건"이라는 가설이다. 판정은 운영 로그의 `grep -c "Split spanning sstable"` —
  0이면 전제가 거짓. 노드 재시작 직후 37분치 로그로는 판별 불가였으므로 **며칠치 로그가 쌓인 뒤
  재측정**해야 한다. 창당 UCS 델리게이트(L난이도, 창 경계를 넘는 병합의 물리적 차단)는 이 판정
  뒤에 재평가.
- **창 목록 페이징**: 서버 쪽이 여전히 5,000행 페이지를 읽어 보낸다 — `windowRowsListed`가 1을
  보고해도 최대 5,000배 과소보고. `latest` 계열에 남은 최대 후보.
- **Continuous aggregates** — 다음 큰 트랙. 기존 DRAFT·watermark 설계
  ([continuous-aggregates-design.md](continuous-aggregates-design.md))가 업계 방향과 일치함을 서베이로
  확인. 롤업과 `chunk_samples`는 릴리스 범위에서 이연.

## 운영 튜닝 제약 (확정 사실)

- `freeze_after` 축소는 `gc_grace_seconds < freeze_after` 규칙에 걸린다 — gc_grace 1d 기준 6h로
  내리려면 gc_grace를 3h 이하로 함께 내려야 하고 `max_hint_window=3h`와 충돌한다. 현실적 첫 걸음은
  12h + gc_grace 6h. UCS 노출 감소폭도 72h→30h ≈ 2.4×지 6-12×가 아니다. 지각 데이터가 다일 단위면
  재동결이 늘어난다 — 지각 분포를 먼저 측정할 것.
- `syncDelegate` 비용 논거는 약하다 — `getLiveSSTables()`는 복사가 아닌 keySet(3,651개에 ~70µs),
  라운드는 타이머가 아니라 flush/완료 시에만. TSCS 자신의 라운드당 작업이 더 크다.

## SP4 이후 후보 (백로그, 서베이 확보분)

- **Continuous aggregates 실행**(위 미결 — 첫 순위)
- 청크 블룸 필터로 콜드 구간 값/텍스트 검색(현재 non-static SAI 거부 제한의 부분 해제)
- memtable→청크 직행 flush 완성(InfluxDB 3 패턴; stage 4의 일반화, write amp 3-4× 제거)
- 적응형 freeze_after(QuestDB dynamic commit lag 패턴; 지각 분포 관측 기반)
- FSST 텍스트 인코딩(`value` 컬럼의 문자열 사본이 최대 섹션 — BtrBlocks/DuckDB 패턴; v4는 버전
  범프 없이 새 `blockEncoding` 코드로 수용 가능)
- 관망: Elf(ALP로 대체됨)·Camel·DeXOR(논문 단계), FCBench(코덱 검증 방법론)
- SSTable 블록 수준 통계 인덱스(TSIndex): 원리는 청크 통계와 동일하되 원본 SSTable 계층에 적용.
  청크 통계가 콜드(대부분)를 커버하므로 후순위 — 비계층화 테이블/핫 윈도우의 집계 스캔이
  프로파일상 실병목으로 확인되면 착수. 주의: 새 SSTable 컴포넌트는 io/sstable 포맷 수술 +
  업스트림 머지 마찰.

## r5 후보 — 다른 포크에서 가져올 것 (datastax/cassandra, 2026-11-02 검토)

[datastax/cassandra](https://github.com/datastax/cassandra) `main`은 **Cassandra 5.0 기반**이고
상당수가 Astra/CNDB 전용(테넌트 센서, 자체 메시징 버전, 벡터·BM25)이라 통째 머지는 하지 않는다.
아래만 골라 가져온다. 1차 판단은 커밋 제목 기준이며, 각 항목은 착수 전에 6.0 코드에 같은 문제가
있는지부터 확인한다(업스트림이 이미 고친 것은 제외). 라이선스는 Apache 2.0.

1. **의존성 CVE 범프** — 첫 순위. 우리 빌드가 DataStax의 CVE 범프 이전 버전을 쓴다:
   snappy-java 1.1.10.4 → 1.1.10.7(CVE-2026-108104/108106), guava 32.0.1 → 33.7.2(CVE-2026-102554),
   netty 4.1.130 → 4.1.139, jackson 2.19.2 → 2.21.8, logback 1.5.18 → 1.5.35, lz4-java 1.11.2 → 1.11.4.
   업스트림 6.0이 고정한 버전이므로 범프마다 전체 게이트를 돌리고, 폐쇄망 반입을 위해 Nexus 미러에
   해당 버전이 있는지 확인한다.
2. **SSTable이 많을 때의 성능** — TSCS는 창마다 SSTable을 만들므로 직접 해당: HCD-468
   (`IntervalTree` 빌드 처리량, add/replace 지원), CNDB-15064(압축 청크 오프셋 블록 캐시 — 오프힙 절감).
3. **6.0에도 있을 법한 버그 수정**: `WaitQueue.Signal.awaitUninterruptibly` 인터럽트 시 무한 대기,
   HCD-585(static 컬럼 일부 조회 시 `Columns$Serializer.encodeBitmap` 예외), HCD-534(MemtablePool
   "Negative released" 크래시), CNDB-19222(청크 캐시 누수·계정 오류).
4. **설계 참고만**: CNDB-18245 "UCS 시간 기반 레벨" — TSCS와 같은 발상. 코드를 가져오지 않고
   TSCS 델리게이트 재평가(위 미결) 때 비교 자료로 쓴다.

함께 r5에서: r4의 TCM 스냅샷 호환 코드(`LegacyAccordLayout`) 삭제 — 노드 41이 r4로 한 번 기동해
메타데이터가 새 형식으로 고쳐 쓰인 것을 확인한 뒤.

## 유지되는 게이트

- 쓰기 ≥152k rows/s 유지(현행 156.4k), 재인코더 ≥50k rows/s(실측 684k — 13.7×),
  집계는 [tiering-benchmark.md](tiering-benchmark.md)의 234 재측정 절이 기준선.
- 읽기 경로 변경은 `executeNet` 경유 테스트 필수(위 `latest` 항목의 교훈), 메모리 변경은
  TimeSeries* 배터리 전체 green.
