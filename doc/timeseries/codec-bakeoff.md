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

# Double 코덱 선정 기록 — ALP / ALP-RD 확정

> **상태 (확정): ALP / ALP-RD가 이 저장소의 유일한 double 코덱입니다.** chunk v4의 블록 인코딩
> `0x20 ALP` / `0x21 ALP_RD`로 구현되어 있습니다 —
> [`AlpBlockCodec`](../../src/java/org/apache/cassandra/db/timeseries/AlpBlockCodec.java)(블록
> 레이아웃·규범)과 [`AlpCodec`](../../src/java/org/apache/cassandra/db/timeseries/AlpCodec.java)
> (계획 함수·값 산술). **Gorilla와 Chimp128은 트리에서 삭제됐습니다** — 그 코덱이 쓰던 청크
> 버전 바이트(v1, v2)는 v3와 함께 제거된 포맷으로 `UnsupportedChunkFormatException`을 받습니다
> ([chunk-format-v4.md §10](chunk-format-v4.md)).
>
> **현재 재현 가능한 실측**은 ALP vs `RAW`(코덱 없는 double 블록이 받는 인코딩) 비교입니다:
> `DoubleBlockCodecTest.sizeAgainstRawOnTheProductionDistributions`가 실행마다 분포별 비율을
> 출력하고 상한을 게이트합니다 — 운영형 소수 2자리 워크 **0.0712 실측**(게이트 0.15),
> integral 워크 게이트 0.15, near-constant 게이트 0.05, full-precision 가우시안 게이트 0.85,
> all-NaN 0.80, random 64비트 0.95.
>
> ```bash
> .build/sh/ai-ci-test org.apache.cassandra.db.timeseries.DoubleBlockCodecTest
> ```

이 문서의 나머지는 그 확정에 이른 **측정 기록**입니다. 비교 상대(Chimp128·Gorilla)와 측정
하네스는 삭제되어 아래 표는 더 이상 재현되지 않지만, 분포별 수치와 구조적 설명은 코덱을 다시
논의하려는 시도가 참조해야 하는 판정 근거로 남습니다.

## ALP vs Chimp128 — 확정 근거 실측 (3,600 값/컬럼 = 1시간 × 1초 간격)

| 분포 | 변형 | ALP B/값 | Chimp128 B/값 | ALP/Chimp |
| --- | --- | ---: | ---: | ---: |
| 운영 `value_numeric` 소수 2자리 워크 | ALP | **0.752** | 1.351 | **0.56×** |
| 운영 `value_numeric` 정수 워크 | ALP | **0.752** | 1.905 | **0.40×** |
| near-constant (997행마다 1회 변화) | ALP | **0.013** | 1.130 | **0.011×** |
| quantized-walk-0.1 | ALP | 0.752 | 1.269 | 0.59× |
| quantized-sine | ALP | 1.377 | 3.179 | 0.43× |
| 소수 3자리 압력 워크 | ALP | 0.753 | 1.330 | 0.57× |
| 단조 증가 정수 카운터 | ALP | 1.627 | 2.320 | 0.70× |
| uniform [0,1) double | ALP-RD | 6.893 | 7.220 | 0.96× |
| random 64비트 패턴 | ALP-RD | 8.002 | 8.251 | 0.97× |
| **full-precision 가우시안 워크** | ALP-RD | 6.627 | **6.532** | **1.015×** |
| **full-precision 사인** | ALP-RD | 7.038 | **6.773** | **1.039×** |

**알려진 손해 — 수용된 트레이드오프**: 굵게 표시한 두 줄 — 소수점이 잘리지 **않은**, 매끄럽게
변하는 double — 에서는 Chimp128이 1.5~3.9% 더 작습니다. 튜닝 실패가 아니라 구조적입니다. Chimp는
각 값을 비슷한 최근 값과 XOR해 **순차 상관**을 먹지만, ALP-RD는 비트 패턴의 정적 분포만 모델링하고
모든 값을 같은 자리에서 자릅니다. ALP-RD 사전을 8개 이상으로 키워도 개선되지 않습니다 — 그런
계열의 하위 ~53 맨티사 비트는 순차 모델 없이는 사실상 비압축이고, 그것이 ALP-RD의 하한
(53 bits/값)을 정합니다.

이 두 형태는 **측정된 운영 분포에는 없습니다** (`docker/scale-workload.py` — 운영 판독값은 전부
십진 양자화되어 있습니다). 순차 상관이 없는 full-precision 데이터에서는 오히려 ALP-RD가 더
작습니다. 이것이 손해를 수용하고 폴백 없이 ALP 단독을 채택한 근거입니다.

측정 외 판정 요소: ALP는 블록 기반이라 v4의 독립 디코드·랜덤 접근 모델과 정합합니다. Chimp의
XOR 체인은 직렬 의존이라 청크 전체 순차 디코드를 강제했고, 그것이 v4가 폐기한 성질입니다
([chunk-format-v4.md §0](chunk-format-v4.md)).

## 앞선 라운드 — Gorilla (v1) vs Chimp128 (v2), 100,000 샘플/패턴, seed 17

| pattern | gorilla (B/sample) | chimp (B/sample) | chimp vs gorilla |
|---|---:|---:|---:|
| constant | 0.250 | 1.250 | **+400% (5.0× 큼)** |
| quantized-walk-0.1 | 4.669 | 1.451 | **−68.9%** |
| quantized-sine | 6.469 | 2.503 | **−61.3%** |
| full-precision-walk | 8.359 | 6.637 | −20.6% |
| random-bits | 8.375 | 8.375 | 0.0% (동일) |

이 라운드가 남긴, 지금도 유효한 두 가지 사실:

- **Chimp가 상수 패턴에서 5× 지는 것은 버그가 아니라 구조입니다.** Chimp128은 최적 참조 샘플을
  항상 7비트 링 인덱스로 지목하므로 정확한 반복 하나가 9비트(분기 1 + 링 7 + 일치 1)인 반면,
  Gorilla는 "직전 값과 동일" 1비트 전용 경로가 있습니다. 링 구조는 *주기적* 재출현(quantized-sine)
  에 최적화된 것이고, 그래서 거기서 크게 이깁니다.
- **상수 계열은 코덱 층에 도달하기 전에 끝나는 문제입니다.** 청크 포맷이 `CONSTANT`(청크 수준
  디렉토리 + v4의 블록 수준 `0x02 CONSTANT`)로 상수를 행 수와 무관하게 O(1) 바이트로 처리하므로,
  Gorilla의 유일한 강점이었던 축은 코덱 선정과 무관해졌습니다. "거의 상수지만 완전한 상수는 아닌"
  계열도 ALP가 흡수합니다 — 위 표의 near-constant 0.013 B/값이 그 측정입니다.
