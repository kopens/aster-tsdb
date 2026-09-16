# 스케일 테스트 (1억 건) — 용량 검증

**2026-07-29** · 구형 24코어 컨테이너 호스트(X5670급) · 단일 노드 · `scale.metrics`
(계층화 없음, [docker/scale-test.sh](../../docker/scale-test.sh)) — **용량 한계 검증 기록**입니다.
쿼리별 절대 시간은 구형 호스트 측정값이라 이 문서에서 내렸습니다. **현재 기준 성능 수치는
[tiering-benchmark.md](tiering-benchmark.md)** (호스트 234, 20M행, chunk v4)를 보십시오.

이 테스트가 확인했고 지금도 유효한 것:

- **1억 행**(1,000 파티션 × 100,000행)의 적재·조회·풀스캔이 단일 노드에서 전부 완주합니다
  (해당 호스트에서 적재 393 s = 254k rows/s, 1억 행 `count(*)` 풀스캔 238 s).
- **집계 시간은 스캔 행 수에 선형입니다** — 같은 실행에서 10개 시리즈 100만 행 시간별 avg가
  3.6 s, 100개 시리즈 1,000만 행이 36.1 s로 정확히 10배. 표준 벤치(20M행) 대비 5배 규모에서도
  비선형 열화는 없었습니다.
- 21종 시계열 함수·gap-fill 전부가 10만 행 파티션에서 정상 결과를 냈습니다.
- 수백만 행 이상 집계에는 서버 타임아웃 상향이 필요합니다 — `read/range_request_timeout`에 더해
  `native_transport_timeout`(기본 12초)도 올려야 하며, 스크립트가 대신 설정합니다.

재실행:

```bash
SCALE_ROWS=100000000 SCALE_SERIES=1000 SCALE_LOADERS=16 SCALE_HEAP=16G \
  ./docker/scale-test.sh aster-tsdb:6.0.0
```
