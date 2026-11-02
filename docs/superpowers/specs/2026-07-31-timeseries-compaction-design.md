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

# TimeSeriesCompactionStrategy (TSCS) — 설계

날짜: 2026-07-31 · 저장소: cassandra-timeseries (Apache Cassandra 6.0.0 포크)
상태: 사용자 승인된 브레인스토밍 결과 (접근법 A)

## 1. 목표

TWCS의 시간 정렬·통째 만료와 UCS의 정교한 창 내부 컴팩션을 결합한 시계열 전용 전략.
사용자가 선정한 4대 목표:

1. **보존 만료 회수 효율** — 보존기간 지난 창의 SSTable을 컴팩션 없이 통째 삭제.
2. **지각 데이터 격리** — store-and-forward 백필이 현재 창 컴팩션을 오염시키지 않고,
   소속 창에 국소적으로 편입된다.
3. **읽기 성능** — 닫힌 창은 동결(freeze) 컴팩션으로 **창당 SSTable 1개**로 수렴.
   파티션+시간범위 조회의 읽기 증폭 최소화.
4. **계층화 연동** — 창 동결 시 이벤트를 발행해 Gorilla 재인코더(계층화 서브프로젝트 2)의
   트리거가 된다. 결합은 이벤트 훅 수준(느슨한 결합) — 재인코더 부재/실패가 컴팩션을
   막지 않고, TSCS는 단독으로도 완결적 가치를 가진다.

비목표: 컴팩션이 직접 청크를 쓰는 것(관례 위반·복구 복잡성으로 기각), TWCS 개조(기각),
UCS 본체 수정(머지 부담으로 기각).

## 2. 아키텍처 (승인된 접근법 A)

**창 파티셔닝 외피 + 창 내부 UCS 위임.** TSCS는 SSTable을 `max timestamp` 기준 시간 창으로
분류하는 외피 전략이고, 창 내부의 컴팩션 선택은 UCS의 컨트롤러(스케일링 파라미터, 샤딩,
목표 크기)에 위임한다. TWCS가 창 내부를 STCS에 위임한 구조의 UCS 세대 버전.

```
시간축:  ... | W-3 (만료→통삭제) | W-2 (동결: 1 SSTable) | W-1 (동결 대기) | W0 현재 창 (UCS T4) |
지각:    창 밖 데이터는 flush/컴팩션 시 창 경계로 스플릿 → 소속 창의 '지각 버킷'으로 격리
이벤트:  W가 동결될 때마다 WindowFrozenListener.onWindowFrozen(ks, table, windowStart, sstable)
```

### CQL 표면

```sql
CREATE TABLE metrics (...) WITH compaction = {
  'class': 'TimeSeriesCompactionStrategy',
  'window_size': '1h',          -- 창 폭 (고정 길이; 계층화 chunk_window와 정렬 권장)
  'freeze_after': '2h',         -- 창 상한 경과 후 이 시간이 지나면 동결 대상 (지각 유예)
  'scaling_parameters': 'T4',   -- 현재 창 내부 UCS 위임 파라미터 (UCS 문법 그대로)
  'target_sstable_size': '1GiB',-- UCS 위임
  'retention': '30d'            -- 선택: 창 상한이 now-retention 이전이면 통째 삭제
};
```

`retention` 미지정 시 통삭제는 TTL 만료(전 셀 만료 SSTable)에만 의존한다. TTL과 `retention`
병행 시 먼저 도래하는 쪽이 적용된다.

## 3. 창 상태 기계

각 창은 SSTable 집합에서 유도되는 상태를 가진다(별도 영속 메타데이터 없음 — 재시작 안전):

```
CURRENT   : windowEnd > now.  UCS 위임 컴팩션.
CLOSING   : windowEnd <= now < windowEnd + freeze_after.  UCS 위임 유지 (지각 유예).
FREEZING  : now >= windowEnd + freeze_after 이고 창 내 SSTable > 1 (또는 미동결 마크).
            → 동결 컴팩션 태스크: 창 내 전 SSTable을 단일 SSTable로 메이저 컴팩션.
FROZEN    : 창 내 SSTable == 1 이고 동결 컴팩션 산출물.  이벤트 발행 완료 상태.
EXPIRED   : windowEnd <= now - retention (또는 전 셀 TTL 만료).  → 통째 삭제.
```

- 상태는 매 컴팩션 라운드에 SSTable 메타데이터(min/max timestamp)로 재계산 — TWCS와 동일한
  무상태 원칙. FROZEN 판정은 "창 경계 안에 완전히 포함된 단일 SSTable" 사실로 판단하며,
  동결 이벤트의 중복 발행은 리스너가 멱등하게 처리한다(재인코더는 원래 멱등 설계).
- 동결 컴팩션은 노드당 동시 1개(설정 가능)로 스로틀 — 대량 백로그(부트스트랩 직후 등)가
  IO를 삼키지 않게 한다. FREEZING 대상 선택은 오래된 창 우선.

## 4. 지각 데이터 격리

- **flush 스플릿**: memtable flush 산출 SSTable이 창 경계를 걸치면 창 경계에서 분할해 쓴다
  (Cassandra의 flush writer는 분할 지점 지정을 지원 — 구현 시 확인, 불가하면 flush 후
  1회 스플릿 컴팩션으로 대체). 결과: **모든 SSTable은 정확히 한 창에 속한다.** 이 불변식이
  통째 삭제와 창당 1 SSTable을 가능하게 하는 토대다.
- FROZEN 창에 지각 SSTable이 도착하면: 그 창은 FREEZING으로 되돌아가 [동결 SSTable + 지각
  SSTable] → 새 단일 SSTable로 국소 재동결하고 이벤트를 재발행한다(재인코더가 청크를
  갱신). 현재 창 컴팩션과 완전히 독립적인 태스크다.
- 리페어/스트리밍으로 유입되는 SSTable도 같은 규칙(창 분류 → 해당 창 재동결)을 탄다.
  스트리밍 SSTable이 창을 걸치면 수신 후 스플릿한다.

## 5. 이벤트 훅 인터페이스

```java
package org.apache.cassandra.db.compaction.timeseries;

public interface WindowFrozenListener
{
    /** 창이 단일 SSTable로 동결(또는 재동결)될 때마다 호출. 멱등해야 한다. */
    void onWindowFrozen(TableMetadata table, long windowStartMillis, SSTableReader frozen);
}
```

- TSCS는 리스너 레지스트리(정적 등록)를 가진다. 리스너 예외는 로깅만 하고 컴팩션 결과에
  영향을 주지 않는다(격리 원칙). v1 기본 리스너 없음 — 서브프로젝트 2의 재인코더가 첫 소비자.
- 이벤트 유실 대비: 리스너는 폴백으로 "FROZEN인데 청크 없는 창" 스캔을 자체 수행할 수 있어야
  한다(재인코더의 워터마크 스캔이 그 역할). 이벤트는 지연 단축 최적화이지 유일한 트리거가 아니다.

## 6. 만료 회수

- `retention` 지정 시: 창 상한 < now − retention 인 창의 SSTable 전부를 컴팩션 없이 삭제
  (TWCS의 만료 창 삭제와 동일 메커니즘). §4의 "SSTable은 정확히 한 창" 불변식 덕에 안전.
- 주의: 계층화(§계층형 스펙)와 병용 시 `retention`은 **핫 로우의 보존**이다. 재인코더가 이미
  청크화한 창은 레인지 딜리트로 비워지므로 보통 만료 전에 사라진다. retention은 재인코더
  장애 시의 안전망 겸 계층화 미사용 테이블의 1차 보존 장치다.

## 7. UCS 위임 세부

- 창 분류 후, CURRENT/CLOSING 창의 SSTable 집합만 UCS 컨트롤러에 공급해 컴팩션 후보를 받는다.
  UCS의 샤딩/스케일링 로직은 수정 없이 재사용(위임 객체로 UCS 인스턴스 보유 — TWCS가 창 내부
  STCS 인스턴스를 쓰는 패턴).
- FREEZING 동결 컴팩션은 UCS를 거치지 않는 전용 메이저 태스크.
- 업스트림 머지 리스크: UCS Controller 시그니처 변경 시 위임부 1곳만 영향 — CLAUDE.md의
  재발 충돌 지점 목록에 추가한다.

## 8. 오류·엣지 처리

- 동결 컴팩션 실패: 창은 FREEZING으로 남고 다음 라운드 재시도. 부분 산출물은 표준 컴팩션
  트랜잭션 롤백에 맡긴다.
- 시계 역행/이상 타임스탬프(멀리 미래 데이터): 미래 창은 CURRENT 취급로 방치하지 않고
  "far-future 가드"(예: now + 1일 초과 창은 경고 + 즉시 동결 대상 제외)로 로깅 — 오입력이
  창을 무한 생성하지 않게 한다.
- `window_size` 변경(ALTER): 새 창 폭은 새 데이터부터 적용. 기존 동결 SSTable은 재작성하지
  않는다(경계 불일치 창은 FROZEN 판정에서 "구 경계 정합"도 허용).
- 카운터 테이블·비시계열 테이블에 대한 오용: 클러스터링 첫 컬럼이 timestamp 계열이 아니면
  CREATE 시 경고(차단은 하지 않음 — max timestamp 기반이라 동작 자체는 가능).

## 9. 테스트 전략

- 단위: 창 분류(경계 정확성, 걸침 스플릿), 상태 기계 전이, 만료 선택, 동결 후보 선택,
  far-future 가드 — 기존 `CompactionsTest`/`TimeWindowCompactionStrategyTest` 패턴.
- 수명주기: 쓰기→flush→창 닫힘→동결→지각 유입→재동결→만료 통삭제 전 과정을 시뮬레이션
  시간으로 검증. 이벤트 발행 횟수·멱등성 확인(목 리스너).
- jvm-dtest: 3노드에서 리페어 후 유입 SSTable의 창 편입, 스트리밍(부트스트랩) 경로.
- 도커 통합: logs/metrics 테이블에 TSCS 적용 후 기존 통합 테스트 전체 + 만료 통삭제 확인.
- 스케일: 기존 1억 건 하네스에서 UCS 대비 — 만료 회수 시간, 닫힌 창 조회 지연(SSTable 수),
  쓰기 처리량. 결과는 `doc/timeseries/tscs-benchmark.md`.

## 10. 서브프로젝트 분해

| 순서 | 내용 | 독립 가치 |
| --- | --- | --- |
| T1 | 창 분류 + CURRENT 위임 + 만료 통삭제 (동결 없음) | "시간을 아는 UCS" — 만료 효율 즉시 확보 |
| T2 | 동결 상태 기계 + 동결 컴팩션 + 이벤트 훅 | 창당 1 SSTable 읽기 최적 + 계층화 트리거 준비 |
| T3 | 지각 격리(flush/스트리밍 스플릿, 국소 재동결) | 백필 현실 대응 완성 |

각 단계는 자체 plan→구현 사이클. T1만으로도 배포 가치가 있다.

## 11. T1 구현 완료 노트 (2026-07-31) 및 T2/T3 인계

T1은 커밋 `c6bb636..3cd922e8b9`로 완료(최종 리뷰 + 수정 웨이브 포함). 테스트: Options 11, Strategy 9(Mockito), E2E 2(SchemaLoader), 회귀 TWCS 6·UCS 27 — 전부 CI 배선됨.

T2가 반드시 인수할 것:
- **far-future 가드(§8) 미구현** — 극단 미래 타임스탬프 SSTable은 활성도 만료도 아니어서 컴팩션에 영원히 안 보임. FROZEN 판정 전에 반드시 구현(쓰레기 타임스탬프가 창을 동결로 오판하지 않게).
- **닫힌 창 TTL 회수 공백(I2)** — T1은 문서 경고로 처리(retention 필요). T2 동결 컴팩션이 구조적으로 해결하고 경고 문구를 제거할 것. — **T2에서 해소**(아래 완료 노트의 `testFreezeReclaimsTTLDataInClosedWindowWithoutRetention` 참고).
- FROZEN 판정에는 min·max 타임스탬프가 둘 다 필요 — 현재 max만 배관됨.
- `getMaximalTasks`/`getUserDefinedTask`의 목 수준 커버리지 부재(E2E만 존재) — T2에서 상태기계 재작성 시 보강.

T3 훅: `createSSTableMultiWriter` 미오버라이드(M3) — flush가 UCS 샤드 분할을 잃는 지점이자 창 경계 스플릿(§4 불변식)이 들어갈 자리.

### T2 구현 완료 노트 (2026-07-31)

T2는 커밋 `3c69e831d6..ad50003d80`로 완료. 테스트: Options 16, Strategy 26(Mockito), E2E 4(SchemaLoader),
WindowFrozenListenersTest 3 — 전부 CI 배선됨(`.gitlab-ci.yml`).

구현 요약: 무상태 창 상태 분류기(CURRENT/CLOSING/FREEZING/FROZEN/EXPIRED, min·max 타임스탬프 유도 —
T1 인계 항목이던 min 배관 완료), `max_future_window` 옵션(기본 1d)과 far-future/current 창 술어,
`FreezeCompactionTask`(창 내 전 SSTable → 단일 SSTable 메이저 컴팩션), 커밋 후에만 발화하는
`WindowFrozenListener`/`WindowFrozenListeners` 훅(신규 패키지 `db.compaction.timeseries`).

동결 시맨틱(코드로 증명됨, `FreezeCompactionTask`/`WindowFrozenListenersTest`/E2E 참고):
- **커밋 후 발화만**(post-commit-only) — 리스너는 `CompactionTask.finish()`가 `super.finish()`(트랜잭션
  커밋의 "돌아올 수 없는 지점")를 반환한 *이후*에만 호출된다. 산출물 0개(창 전체가 만료 데이터였던
  경우)는 이벤트 없음, 1개는 정확히 1회 발화, 2개 이상(발생해선 안 됨)은 발화하지 않고 WARN 로그만.
  E2E에서 발화 횟수 1(최초 동결)/2(지각 데이터 재동결 — 누적)/0(만료 창 동결로 산출물 없음)으로 증명.
- **단일 산출물은 평범한 `CompactionTask`/`DefaultCompactionWriter`로 보장** — 별도 신규 writer가
  아니라 writer를 스위칭하지 않는 기존 `CompactionTask`를 그대로 상속해 얻는다.
- `shouldReduceScopeForSpace()`를 `false`로 오버라이드 — 디스크 부족 시 가장 큰 입력을 조용히 버리는
  `CompactionTask` 기본 동작은 "창 전체 → 1 SSTable" 계약을 깨므로, 전량 실패 후 다음 라운드 재시도를
  택한다(§8과 일치).

스코프 주의(변경 없음): CSM이 전략 인스턴스를 리페어 상태×디스크로 쪼개므로 "창당 1 sstable"·FROZEN
판정·동결 이벤트는 **인스턴스 슬라이스 단위**다(테이블 전체 아님). 리스너 소비자는 같은 창에 대해
슬라이스별 이벤트를 여러 번 받을 수 있다 — 멱등 계약(§5)이 이를 흡수한다.

T3가 인수할 것(우선순위 순서 아님):
- (a) `createSSTableMultiWriter` flush/스트리밍 창-스플릿 훅 — T1 인계 항목 그대로 미착수, 여전히 보류.
- (b) 닫힌 창 안의 **걸침(SPANNING) SSTable 1개**는 FREEZING으로 분류되지만(포함 실패) 동결 후보
  선택에서 제외된다(선택 조건이 SSTable 수 ≥ 2) — 스플릿 없이 재작성해도 무의미하기 때문. T3의
  flush-스플릿이 구조적으로 걸침 SSTable 발생을 앞으로 불가능하게 만들 것이므로, 이미 존재하는
  레거시 걸침 SSTable을 위한 **국소 재동결 경로**를 T3에서 추가해야 한다.
- (c) `ALTER window_size` 후 구경계 FROZEN 인정(§8) — 현재는 보수적으로 "미동결(재작성·이벤트 없음)"로만
  처리. 구경계 정합 허용은 스플릿과 함께 재검토.
- (d) 동결 동시성 스로틀 설정화(§3 "노드당 동시 1개(설정 가능)") — 현재는 **인스턴스당 라운드당 1개
  고정**이며 설정 노출 없음. 보류.
- (e) **`WindowFrozenListener` 소비자는 반드시 큐잉해야 하며 인라인으로 작업해선 안 된다** — 발화가
  컴팩션 스레드 위에서 실행되므로, 리스너의 예외는 격리되지만(로깅만) 리스너가 블로킹/행(hang)하면
  컴팩션 스레드 자체가 멈춘다. SP2 재인코더 연동 시 반드시 지켜야 할 구속 지침.
- (f) `nextFreezeCandidate`의 far-future 필터는 선결조건 위생(precondition hygiene)이다 — far-future
  SSTable은 애초에 FREEZING으로 분류되지 않으므로 이 필터는 현재 중복(defense-in-depth)이지만,
  **제거하지 말 것**(분류기 변경 시 안전망).
- (g) 인수받지 못한 테스트 공백: `previousFreezeCandidate`의 갭-라운드(창 없는 라운드) 리셋이 핀 고정
  테스트 없음, 재동결 산출물(merge된 SSTable)의 내용·span 어서션 부재(발화 여부만 검증됨).
- 참고(T1 인계에서 이어짐, 여전히 유효): `getMaximalTasks`(수동 메이저 컴팩션)는 만료 창을
  `TimeSeriesCompactionTask`로, 그 외 창을 평범한 `CompactionTask`로 라우팅해 창 불변식은 지키지만
  `FreezeCompactionTask`를 거치지 않으므로 동결 이벤트를 발화하지 않는다 — 계층화 연동 관점에서 재검토.
- jvm-dtest(3노드 리페어/스트리밍 창 편입)와 스케일 벤치(§9)는 T3 이후 일괄.

### T3 구현 완료 노트 (2026-07-31)

T3는 커밋 `e57cbf4f36..8b1fd162ff`(+마감 커밋)로 완료 — 프리미엄 모델 쿼터 소진으로 컨트롤러
인라인 실행(계획서 `2026-07-31-tscs-t3-late-isolation.md`의 규범 결정 D1~D7 참고). 테스트:
WindowRoutingIteratorTest 11, TimeWindowSplittingMultiWriterTest 4(SchemaLoader), Strategy 29(+3),
E2E 5(+1) — 전부 CI 배선됨.

구현 요약:
- **창 경계 스플릿(§4)**: `db/compaction/timeseries/WindowRoutingIterator`(로우/마커를 셀
  write-timestamp 축으로 창별 라우팅 — D1·D2; boundary 마커는 close/open으로 분해) +
  `TimeWindowSplittingMultiWriter`(창별 지연 생성 라이터). `TimeSeriesCompactionStrategy.
  createSSTableMultiWriter` 오버라이드로 **flush와 스트리밍이 같은 훅**을 타므로(RangeAware
  SSTableWriter 경유 확인) 두 경로 모두 "SSTable은 정확히 한 창" 불변식을 만족한다.
- **파티션 헤더는 자기 창으로 1회 라우팅(D3 개정)** — 전 창 복제 원안은 과거 삭제 타임스탬프가
  새 창 SSTable min 메타데이터를 오염시켜 영구 걸침·무한 분할 루프를 유발함을 테스트로 실증하고
  폐기. 안전 근거: 읽기 병합의 전역 적용 + 창 통삭제의 오래된-창-우선 순서.
- **레거시 걸침 국소 재동결(§4/§10)**: `SplitRefreezeCompactionTask` — 안티컴팩션 안무(공유 txn +
  창별 SSTableRewriter)로 걸침 단일 SSTable을 창별 산출물로 재작성. 리스너 미발화(동결 완결은
  후속 라운드의 FreezeCompactionTask 몫). 우선순위 만료 > 동결 > 분할 > 위임, splitBacklog가
  getEstimatedRemainingTasks에 반영.
- T2 인계 소화: (a) 완료, (b) 완료(스플릿+국소 재동결), (c) 구경계는 걸침이 되는 순간 분할 경로가
  자연 처리 — 별도 인정 로직 불필요로 종결, (d) 동시성 설정화는 계속 보류.

후속(비목표로 명시): UCS 토큰 샤딩과 flush 스플릿 합성(D6), 3노드 jvm-dtest(리페어/스트리밍 창
편입 — 기존 멀티노드 검증 공백에 합류), (g) 테스트 공백 2건, `ant testsome` 오타 FQCN 무음 통과의
상류 수정(ai-ci-test 가드는 완료). 스케일 벤치는 SP3 후 일괄(§9).
