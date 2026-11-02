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

# 청크 코덱 v2: Chimp128 — 설계

날짜: 2026-07-31 · 상태: 사용자 승인(제안서 → "a" 승인, Chimp128 선행 / SP2 후행)
근거 논문: Liakos, Papakonstantinopoulou, Kotidis. "Chimp: Efficient Lossless Floating Point
Compression for Time Series Databases." PVLDB 15(11), 2022. **클린룸 구현** — 참조 구현
코드를 열람하지 않고 논문 알고리즘 기술(Algorithm 2, Figure 6/8, §4)만 사용한다.

## 1. 목표·판정 기준

- Gorilla(v1)의 값 압축 약점(직전 값 고정 XOR, 5+6비트 윈도우 헤더)을 Chimp128로 대체한
  **청크 버전 2**를 추가한다. 타임스탬프 축(DoD)과 21바이트 헤더는 v1과 동일, 버전 바이트만 2.
- **판정 기준(bake-off)**: 양자화 워크(0.1 스텝)와 양자화 주기 신호에서 Gorilla 대비
  bytes/sample **30% 이상 절감이면 기본 코덱으로 승격**, 미달이면 옵션으로만 유지. 상수
  계열에서 Gorilla(0.253 B/샘플) 대비 퇴행 금지(±10% 이내). 결과는
  `doc/timeseries/codec-bakeoff.md`로 커밋하며 논문 수치가 재현되지 않으면 그대로 보고한다.

## 2. 규범 알고리즘 (논문 §4 요약 — 인코더/디코더 공통)

값 스트림(v1의 XOR 윈도우 인코딩을 대체; 첫 값은 raw 64비트 동일):

**리더 상태**: `ring[128]`(최근 값, 위치 `i % 128`), `indices[2^14]`(키 `rawBits & (2^14-1)` →
마지막 등장 절대 인덱스), `prevLeadCode`(직전 기록의 3비트 leading 코드).

**leading 버킷(3비트)**: 실제 leading zeros를 {0,8,12,16,18,20,22,24}로 **내림** 매핑
(24 초과도 24). 코드↔값 테이블은 인코더/디코더 동일 상수.

각 값 `v` (절대 인덱스 `j`):
1. `key = rawBits(v) & 0x3FFF`; `i = indices[key]`; 후보 유효 조건 `j - i < 128 && i >= 0`.
   유효하면 `cand = ring[i % 128]`, `xorC = raw(v) ^ raw(cand)`, `trailC = ntz(xorC)`
   (xorC==0이면 trailC=64로 취급). 무효면 trailC = 0.
2. **좌분기** — `trailC > 13` (= log₂128 + log₂64):
   - 비트 `0` + **7비트 `i % 128`** 기록 후:
     - `xorC == 0` → 비트 `0`. (끝)
     - 아니면 → 비트 `1` + 3비트 leading 버킷코드(of xorC) + 6비트 `center = 64 − lead − trailC`
       (center는 1..63 범위; 64는 xorC==0 케이스로 불가) + `xorC >>> trailC`의 center비트.
   - 이 분기에서도 `prevLeadCode`를 이번 leading 코드로 갱신(xorC==0이면 갱신 없음 —
     논문 Algorithm 2는 lead를 항상 갱신하나 xor==0의 lead(=64→코드7)는 다음 '10' 판정을
     망치므로, **본 구현은 xor==0일 때 prevLeadCode를 유지**한다. 이 결정은 인코더/디코더가
     동일하게 따르면 정합하며, 명세로 고정한다.)
3. **우분기** — `trailC <= 13`: 후보를 버리고 **직전 값** `prev = ring[(j−1) % 128]`과
   `xorP = raw(v) ^ raw(prev)`, `leadCode = bucket(nlz(xorP))`:
   - 비트 `1` 기록 후:
     - `leadCode == prevLeadCode` → 비트 `0` + `xorP`의 하위 `64 − leadValue` 비트("non-lead").
     - 아니면 → 비트 `1` + 3비트 `leadCode` + `xorP`의 `64 − leadValue` 비트.
   - `prevLeadCode = leadCode` 갱신. (xorP==0이어도 우분기 규칙대로 non-lead 64−lead 비트를
     기록 — 논문 Figure 8 우분기에는 zero 특례가 없다. lead 버킷 최대 24라 최소 40비트.)
4. 기록 후 `ring[j % 128] = raw(v)`, `indices[key] = j`.

**디코더 상태**: 동일한 `ring[128]` + `prevLeadCode` (indices 불필요 — 좌분기의 7비트가
링 위치를 직접 준다). 디코더는 아직 쓰이지 않은 링 위치를 참조하는 손상 페이로드에
`IllegalArgumentException`을 던진다(`j < 128`인 동안 위치 `> j−1` 참조 금지... 링 위치와
절대 인덱스의 대응은 최근 128개 안이므로 "참조 위치의 마지막 기록 절대 인덱스가
`j−128 .. j−1` 범위"임을 디코더가 추적·검증한다 — 슬롯별 마지막 기록 인덱스 128개 유지).

**타임스탬프**: v1과 동일한 DoD 버킷 인코딩을 재사용한다(`GorillaCodec`의
`writeDod`/`readDod`를 package-private로 완화해 공유; 동작 무변경).

## 3. 구성

- `db/timeseries/Chimp128Codec.java` — GorillaCodec와 동일한 공개 API 형태
  (`encode(long[], double[], int)`, `cursor(ByteBuffer)`, `sampleCount/firstTimestamp/lastTimestamp`,
  `VERSION = 2`, `MAX_SAMPLES` 동일, 헤더 동일 21바이트).
- `db/timeseries/ChunkCodecs.java` — 버전 디스패처: `cursor(ByteBuffer)`가 버전 바이트를
  읽어 v1/v2 커서 반환, `SampleCursor` 인터페이스는 GorillaCodec의 것을 공용으로 승격
  (또는 동일 시그니처 유지). SP2는 이 클래스만 소비한다.
- 메모리: 인코더 33KB(링+인덱스), 디코더 ~2KB(링+슬롯 인덱스). 창당 1회 할당.

## 4. 테스트·검증

- Gorilla 수준 동등 스위트: 시드 프로퍼티 왕복(30시드×4패턴), NaN/±Inf/-0.0 비트 정확,
  단조 타임스탬프 거부, 버전/절단/손상(링 미기록 참조) 방어, dod 경계 재사용, 크기 회귀 기준
  (bake-off 결과로 확정).
- bake-off: 동일 데이터로 v1/v2 bytes/sample + 인코드/디코드 처리량(샘플/초) 비교 —
  패턴: 양자화 워크(0.1), 양자화 주기(sin 0.1 반올림), 상수, 풀정밀 워크, 임의 비트.
  JUnit 측정 테스트가 수치를 출력하고, 문서는 그 출력으로 작성.
- ALP는 범위 외(v3 후보로만 기록).

## 5. 구현 완료 노트 (2026-07-31) 및 SP2 인계

커밋 `55c058d..eca361dca8` 완료 (최종 리뷰 APPROVED + 수정 웨이브). 테스트: Chimp128CodecTest 20,
ChunkCodecsTest 4, Gorilla 회귀 18 — CI 배선됨. bake-off 결과(`doc/timeseries/codec-bakeoff.md`):
양자화 워크 -68.9%, 양자화 주기 -61.3% (기준 30% 상회), 상수 5배 손해(링 인덱스 고유 비용) →
**기본 승격 보류, 명시 옵션**. SP2 권고 정책: `chunk_codec: auto` — 창마다 양쪽 인코딩 후 작은 쪽
저장(버전 바이트가 구분자, encode 비용 창당 ms 단위).

SP2가 인수할 것 (최종 리뷰 지시):
- **태스크 0: `SampleCursor` 최상위 추출** (`db/timeseries/SampleCursor.java`) — 현재
  `GorillaCodec.SampleCursor`에 묶여 있어 SP2 전 파일이 GorillaCodec를 임포트하게 됨.
  Gorilla의 중첩 인터페이스는 이를 extends 해 소스 호환 유지.
- `ChunkCodecs`에 `HEADER_SIZE`/`MAX_SAMPLES` 상수 통합 + `Codec codecOf(ByteBuffer)` 추가.
- 인코더 고정비용: encode()마다 64KB(int[2^14]) 할당+fill — 작은 창 다수 재인코딩 시 지배적.
  재사용 인코더 객체 또는 thread-local 스크래치 검토.
- `encode`는 배치 전용(스트리밍 어펜드 없음) — seal-on-flush 설계면 무영향.
- auto 정책 확정 후 `encodeSmallest` 헬퍼 추가(조기 헬퍼로 이중 인코딩을 강제하지 말 것).
