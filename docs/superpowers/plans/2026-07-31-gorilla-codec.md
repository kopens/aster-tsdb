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

# Gorilla 코덱 구현 계획 (계층형 시계열 저장소 서브프로젝트 1)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `(long timestampMillis, double value)` 스트림을 Gorilla 방식(타임스탬프 delta-of-delta + 값 XOR 비트패킹)으로 무손실 인코딩/디코딩하는 순수 Java 코덱.

**Architecture:** 신규 패키지 `org.apache.cassandra.db.timeseries`. `BitWriter`/`BitReader`(비트 스트림 기반) 위에 `GorillaCodec`(고정 헤더 + 비트 스트림, 스트리밍 커서 디코더). Cassandra 의존은 ByteBuffer 뿐 — 후속 서브프로젝트(청크 스토어)가 그대로 소비한다.

**Tech Stack:** Java 21, JUnit 4 (기존 test/unit 관례), 외부 의존성 추가 없음.

## Global Constraints

- 신규 jar/의존성 추가 금지. `src/gen-java/`, `lib/`, CQL 문법 수정 금지 (스펙·CLAUDE.md).
- 모든 신규 `.java` 파일 최상단에 ASF 라이선스 헤더(기존 파일과 동일한 17줄 블록, 예: `src/java/org/apache/cassandra/index/sai/analyzer/AnalyzerOptions.java` 1–17행을 그대로 복사) 필수 — checkstyle이 검사한다.
- 컴파일(반복 주기): `ant build-test -Dno-checkstyle=true -Dant.gen-doc.skip=true -Drat.skip=true 2>&1 | .build/sh/ant-log-summary.py -`
- 테스트 실행: `.build/sh/ai-ci-test --reuse org.apache.cassandra.db.timeseries.<테스트클래스>` — **주의: `--reuse`는 컴파일을 하지 않으므로 반드시 위 컴파일을 먼저 실행**할 것 (이 세션에서 확립된 절차). 결과는 `build/test/output/TEST-<FQCN>.xml`의 `tests/failures/errors` 속성으로 확인.
- 최종 검증: `.build/sh/ai-build` (checkstyle 포함 전체 빌드) 통과.
- 커밋 메시지 끝에 기존 관례대로 Co-Authored-By/Claude-Session 트레일러를 붙인다.

## 인코딩 포맷 v1 (규범 명세 — 인코더/디코더가 모두 따른다)

**헤더 (고정 21바이트, big-endian):**

| 오프셋 | 크기 | 내용 |
| --- | --- | --- |
| 0 | 1 | version = `1` |
| 1 | 4 | sample count `n` (int, `n >= 1`) |
| 5 | 8 | first timestamp (millis, long) |
| 13 | 8 | last timestamp (millis, long) — 디코드 없이 프루닝용 |

**비트 스트림 (헤더 직후):**

- 샘플 0: 값 `v_0`의 raw 비트 64개 (`Double.doubleToRawLongBits`). 타임스탬프는 헤더의 first.
- 샘플 `i>=1` (타임스탬프 → 값 순으로 인터리브):
  - **타임스탬프**: `delta_i = t_i − t_{i−1}`, `dod = delta_i − delta_{i−1}` (`delta_0 = 0` 규약).
    타임스탬프는 **엄격 증가**여야 하며 위반 시 인코더가 `IllegalArgumentException`.
    | dod 범위 | 프리픽스 | 페이로드 |
    | --- | --- | --- |
    | `0` | `0` | 없음 |
    | `[-63, 64]` | `10` | 7비트, `dod+63` |
    | `[-2047, 2048]` | `110` | 12비트, `dod+2047` |
    | `[-65535, 65536]` | `1110` | 17비트, `dod+65535` |
    | 그 외 | `1111` | 64비트 raw |
  - **값**: `xor = rawBits(v_i) ^ rawBits(v_{i−1})`.
    - `xor == 0` → 비트 `0`.
    - 아니면 비트 `1` + 제어 비트:
      - 이전 윈도우가 존재하고 `leading >= prevLeading && trailing >= prevTrailing` → `0` +
        의미비트(`xor >>> prevTrailing`, `64−prevLeading−prevTrailing`비트). 윈도우 유지.
      - 아니면 `1` + leading 5비트(31 초과는 31로 캡) + `(meaningful−1)` 6비트(1..64→0..63,
        고전적 64 오버플로 버그 방지) + 의미비트(`xor >>> trailing`, meaningful비트). 윈도우 갱신.

## File Structure

- Create: `src/java/org/apache/cassandra/db/timeseries/BitWriter.java` — 비트 쓰기(패키지 프라이빗)
- Create: `src/java/org/apache/cassandra/db/timeseries/BitReader.java` — 비트 읽기(패키지 프라이빗)
- Create: `src/java/org/apache/cassandra/db/timeseries/GorillaCodec.java` — 공개 API(인코드/커서/헤더 피크)
- Test: `test/unit/org/apache/cassandra/db/timeseries/BitStreamTest.java`
- Test: `test/unit/org/apache/cassandra/db/timeseries/GorillaCodecTest.java`

---

### Task 1: BitWriter / BitReader

**Files:**
- Create: `src/java/org/apache/cassandra/db/timeseries/BitWriter.java`
- Create: `src/java/org/apache/cassandra/db/timeseries/BitReader.java`
- Test: `test/unit/org/apache/cassandra/db/timeseries/BitStreamTest.java`

**Interfaces:**
- Consumes: 없음 (순수 신규)
- Produces (Task 2가 사용):
  - `BitWriter`: `void writeBit(boolean)`, `void writeBits(long value, int numBits /*1..64*/)`, `int sizeInBytes()`, `void writeTo(ByteBuffer out)`
  - `BitReader`: `BitReader(ByteBuffer buffer /*position=비트스트림 시작*/)`, `boolean readBit()`, `long readBits(int numBits /*1..64*/)`
  - 비트 순서: 첫 비트가 첫 바이트의 MSB (big-endian bit order). `writeBits`는 `value`의 하위 `numBits`비트를 상위 비트부터 기록.

- [ ] **Step 1: 실패하는 테스트 작성** — `test/unit/org/apache/cassandra/db/timeseries/BitStreamTest.java` (ASF 헤더 생략 표기 — 실제 파일에는 필수):

```java
package org.apache.cassandra.db.timeseries;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BitStreamTest
{
    @Test
    public void singleBitsRoundtrip()
    {
        BitWriter writer = new BitWriter();
        boolean[] bits = { true, false, true, true, false, false, true, false, true };
        for (boolean bit : bits)
            writer.writeBit(bit);

        BitReader reader = readerOf(writer);
        for (boolean bit : bits)
            assertEquals(bit, reader.readBit());
    }

    @Test
    public void fullWidthValuesRoundtrip()
    {
        BitWriter writer = new BitWriter();
        long[] values = { 0L, -1L, Long.MIN_VALUE, Long.MAX_VALUE, 0xDEADBEEFCAFEBABEL };
        for (long value : values)
            writer.writeBits(value, 64);

        BitReader reader = readerOf(writer);
        for (long value : values)
            assertEquals(value, reader.readBits(64));
    }

    @Test
    public void unalignedMixedWidthsRoundtrip()
    {
        // widths deliberately misaligned so values straddle byte and word boundaries
        BitWriter writer = new BitWriter();
        int[] widths = { 1, 7, 12, 3, 64, 17, 5, 33, 9, 64, 2 };
        long[] values = new long[widths.length];
        Random random = new Random(42);
        for (int i = 0; i < widths.length; i++)
        {
            values[i] = random.nextLong() & (widths[i] == 64 ? -1L : (1L << widths[i]) - 1);
            writer.writeBits(values[i], widths[i]);
        }

        BitReader reader = readerOf(writer);
        for (int i = 0; i < widths.length; i++)
            assertEquals("width " + widths[i], values[i], reader.readBits(widths[i]));
    }

    @Test
    public void randomizedRoundtripAcrossSeeds()
    {
        for (long seed = 0; seed < 50; seed++)
        {
            Random random = new Random(seed);
            BitWriter writer = new BitWriter();
            List<long[]> written = new ArrayList<>(); // [value, width]
            int operations = 1 + random.nextInt(4000);   // forces capacity growth past 16 words
            for (int i = 0; i < operations; i++)
            {
                int width = 1 + random.nextInt(64);
                long value = random.nextLong() & (width == 64 ? -1L : (1L << width) - 1);
                writer.writeBits(value, width);
                written.add(new long[]{ value, width });
            }

            BitReader reader = readerOf(writer);
            for (long[] entry : written)
                assertEquals("seed " + seed, entry[0], reader.readBits((int) entry[1]));
        }
    }

    @Test
    public void sizeInBytesRoundsUp()
    {
        BitWriter writer = new BitWriter();
        writer.writeBits(0b101, 3);
        assertEquals(1, writer.sizeInBytes());
        writer.writeBits(0, 5);
        assertEquals(1, writer.sizeInBytes());
        writer.writeBit(true);
        assertEquals(2, writer.sizeInBytes());
    }

    private static BitReader readerOf(BitWriter writer)
    {
        ByteBuffer buffer = ByteBuffer.allocate(writer.sizeInBytes());
        writer.writeTo(buffer);
        buffer.flip();
        return new BitReader(buffer);
    }
}
```

- [ ] **Step 2: 실패 확인**

컴파일: `ant build-test -Dno-checkstyle=true -Dant.gen-doc.skip=true -Drat.skip=true 2>&1 | .build/sh/ant-log-summary.py -`
기대: `BitWriter`/`BitReader` 부재로 **컴파일 실패** (그 자체가 red 단계).

- [ ] **Step 3: 구현** — `BitWriter.java`:

```java
package org.apache.cassandra.db.timeseries;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Append-only bit stream writer. The first bit written becomes the most significant bit of the
 * first byte (big-endian bit order). {@code writeBits} appends the low {@code numBits} bits of
 * the value, most significant of those bits first.
 */
final class BitWriter
{
    private long[] words = new long[16];
    private int bitCount;

    void writeBit(boolean bit)
    {
        writeBits(bit ? 1L : 0L, 1);
    }

    void writeBits(long value, int numBits)
    {
        assert numBits >= 1 && numBits <= 64 : numBits;
        ensureCapacity(numBits);
        if (numBits < 64)
            value &= (1L << numBits) - 1;

        int wordIndex = bitCount >>> 6;
        int used = bitCount & 63;
        int space = 64 - used;
        if (numBits <= space)
        {
            words[wordIndex] |= value << (space - numBits);
        }
        else
        {
            int overflow = numBits - space;
            words[wordIndex] |= value >>> overflow;
            words[wordIndex + 1] |= value << (64 - overflow);
        }
        bitCount += numBits;
    }

    int sizeInBytes()
    {
        return (bitCount + 7) >>> 3;
    }

    void writeTo(ByteBuffer out)
    {
        int bytes = sizeInBytes();
        for (int i = 0; i < bytes; i++)
        {
            int word = i >>> 3;
            int shift = 56 - ((i & 7) << 3);
            out.put((byte) (words[word] >>> shift));
        }
    }

    private void ensureCapacity(int numBits)
    {
        int required = (bitCount + numBits + 63) >>> 6;
        if (required > words.length)
            words = Arrays.copyOf(words, Math.max(required, words.length * 2));
    }
}
```

`BitReader.java`:

```java
package org.apache.cassandra.db.timeseries;

import java.nio.ByteBuffer;

/**
 * Bit stream reader mirroring {@link BitWriter}: the most significant bit of the first byte is
 * the first bit. Reading past the underlying buffer's limit throws {@link IndexOutOfBoundsException},
 * which callers treat as a truncated/corrupt payload.
 */
final class BitReader
{
    private final ByteBuffer buffer;
    private final int start;
    private int bitPosition;

    BitReader(ByteBuffer buffer)
    {
        this.buffer = buffer;
        this.start = buffer.position();
    }

    boolean readBit()
    {
        return readBits(1) == 1L;
    }

    long readBits(int numBits)
    {
        assert numBits >= 1 && numBits <= 64 : numBits;
        long result = 0;
        int remaining = numBits;
        while (remaining > 0)
        {
            int byteIndex = bitPosition >>> 3;
            int bitIndex = bitPosition & 7;
            int available = 8 - bitIndex;
            int take = Math.min(available, remaining);
            int current = buffer.get(start + byteIndex) & 0xFF;
            int taken = (current >>> (available - take)) & ((1 << take) - 1);
            result = (result << take) | taken;
            bitPosition += take;
            remaining -= take;
        }
        return result;
    }
}
```

- [ ] **Step 4: 통과 확인**

컴파일(위 명령) 후: `.build/sh/ai-ci-test --reuse org.apache.cassandra.db.timeseries.BitStreamTest`
확인: `build/test/output/TEST-org.apache.cassandra.db.timeseries.BitStreamTest.xml` 에서 `failures="0" errors="0"`.

- [ ] **Step 5: 커밋**

```bash
git add src/java/org/apache/cassandra/db/timeseries/BitWriter.java \
        src/java/org/apache/cassandra/db/timeseries/BitReader.java \
        test/unit/org/apache/cassandra/db/timeseries/BitStreamTest.java
git commit -m "Add big-endian bit stream writer/reader for the gorilla codec"
```

---

### Task 2: GorillaCodec 인코드/디코드 (해피 패스)

**Files:**
- Create: `src/java/org/apache/cassandra/db/timeseries/GorillaCodec.java`
- Test: `test/unit/org/apache/cassandra/db/timeseries/GorillaCodecTest.java`

**Interfaces:**
- Consumes: Task 1의 `BitWriter`/`BitReader` (시그니처 위 참조)
- Produces (서브프로젝트 2와 Task 3·4가 사용):
  - `public static ByteBuffer encode(long[] timestamps, double[] values, int count)` — flip 된 read-ready 버퍼 반환
  - `public interface SampleCursor { boolean advance(); long timestamp(); double value(); }`
  - `public static SampleCursor cursor(ByteBuffer payload)` — payload 는 소비하지 않음(duplicate)
  - `public static int sampleCount(ByteBuffer payload)` / `public static long firstTimestamp(ByteBuffer payload)` / `public static long lastTimestamp(ByteBuffer payload)` — position 기준 상대 피크, 버퍼 불변
  - `public static final byte VERSION = 1;`

- [ ] **Step 1: 실패하는 테스트 작성** — `GorillaCodecTest.java` (첫 테스트 3개만; Task 3·4가 이 파일에 테스트를 추가한다):

```java
package org.apache.cassandra.db.timeseries;

import java.nio.ByteBuffer;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GorillaCodecTest
{
    @Test
    public void roundtripRegularOneSecondSeries()
    {
        int n = 3600;                              // 1시간, 1초 주기
        long[] timestamps = new long[n];
        double[] values = new double[n];
        for (int i = 0; i < n; i++)
        {
            timestamps[i] = 1_700_000_000_000L + i * 1000L;
            values[i] = 20.0 + Math.sin(i / 500.0) * 40.0;
        }

        assertRoundtrip(timestamps, values, n);
    }

    @Test
    public void headerPeeksDoNotConsumeBuffer()
    {
        long[] timestamps = { 1000L, 2000L, 3500L };
        double[] values = { 1.5, 1.5, -2.25 };
        ByteBuffer payload = GorillaCodec.encode(timestamps, values, 3);

        int position = payload.position();
        assertEquals(3, GorillaCodec.sampleCount(payload));
        assertEquals(1000L, GorillaCodec.firstTimestamp(payload));
        assertEquals(3500L, GorillaCodec.lastTimestamp(payload));
        assertEquals(position, payload.position());
    }

    @Test
    public void constantValuesCompressToUnderHalfByteAmortized()
    {
        int n = 3600;
        long[] timestamps = new long[n];
        double[] values = new double[n];
        for (int i = 0; i < n; i++)
        {
            timestamps[i] = i * 1000L;
            values[i] = 42.0;                      // 산업 설정값(setpoint) 패턴
        }

        ByteBuffer payload = GorillaCodec.encode(timestamps, values, n);
        // dod=0(1비트) + xor=0(1비트) = 샘플당 2비트 + 헤더/첫샘플 상수 비용
        assertTrue("bytes=" + payload.remaining(), payload.remaining() <= n / 2);
        assertRoundtrip(timestamps, values, n);
    }

    static void assertRoundtrip(long[] timestamps, double[] values, int count)
    {
        ByteBuffer payload = GorillaCodec.encode(timestamps, values, count);
        GorillaCodec.SampleCursor cursor = GorillaCodec.cursor(payload);
        for (int i = 0; i < count; i++)
        {
            assertTrue("cursor ended early at " + i, cursor.advance());
            assertEquals("timestamp " + i, timestamps[i], cursor.timestamp());
            // NaN 포함 비트 단위 동일성 (== 비교는 NaN에서 깨진다)
            assertEquals("value bits " + i,
                         Double.doubleToRawLongBits(values[i]),
                         Double.doubleToRawLongBits(cursor.value()));
        }
        assertFalse(cursor.advance());
    }
}
```

- [ ] **Step 2: 실패 확인** — 컴파일 명령 실행, `GorillaCodec` 부재로 컴파일 실패 확인.

- [ ] **Step 3: 구현** — `GorillaCodec.java` (포맷 명세 §인코딩 포맷 v1 구현):

```java
package org.apache.cassandra.db.timeseries;

import java.nio.ByteBuffer;

/**
 * Lossless gorilla-style codec for (timestampMillis, double) series: delta-of-delta timestamps
 * and XOR-bitpacked values behind a fixed 21-byte header (version, count, first/last timestamp).
 * See docs/superpowers/plans/2026-07-31-gorilla-codec.md for the normative bit format.
 * Timestamps must be strictly increasing; values roundtrip bit-exactly (NaN payloads, -0.0).
 */
public final class GorillaCodec
{
    public static final byte VERSION = 1;
    static final int HEADER_SIZE = 21;

    private GorillaCodec()
    {
    }

    public interface SampleCursor
    {
        boolean advance();
        long timestamp();
        double value();
    }

    public static ByteBuffer encode(long[] timestamps, double[] values, int count)
    {
        if (count < 1)
            throw new IllegalArgumentException("count must be >= 1, got " + count);
        if (timestamps.length < count || values.length < count)
            throw new IllegalArgumentException("arrays shorter than count " + count);

        BitWriter bits = new BitWriter();
        long previousBits = Double.doubleToRawLongBits(values[0]);
        bits.writeBits(previousBits, 64);

        long previousTimestamp = timestamps[0];
        long previousDelta = 0;
        int windowLeading = -1;
        int windowTrailing = 0;

        for (int i = 1; i < count; i++)
        {
            long timestamp = timestamps[i];
            if (timestamp <= previousTimestamp)
                throw new IllegalArgumentException("timestamps must be strictly increasing: " +
                                                   timestamp + " after " + previousTimestamp);
            long delta = timestamp - previousTimestamp;
            writeDod(bits, delta - previousDelta);
            previousTimestamp = timestamp;
            previousDelta = delta;

            long valueBits = Double.doubleToRawLongBits(values[i]);
            long xor = valueBits ^ previousBits;
            if (xor == 0)
            {
                bits.writeBit(false);
            }
            else
            {
                bits.writeBit(true);
                int leading = Math.min(31, Long.numberOfLeadingZeros(xor));
                int trailing = Long.numberOfTrailingZeros(xor);
                if (windowLeading != -1 && leading >= windowLeading && trailing >= windowTrailing)
                {
                    bits.writeBit(false);
                    bits.writeBits(xor >>> windowTrailing, 64 - windowLeading - windowTrailing);
                }
                else
                {
                    bits.writeBit(true);
                    int meaningful = 64 - leading - trailing;
                    bits.writeBits(leading, 5);
                    bits.writeBits(meaningful - 1, 6);   // 1..64 stored as 0..63
                    bits.writeBits(xor >>> trailing, meaningful);
                    windowLeading = leading;
                    windowTrailing = trailing;
                }
            }
            previousBits = valueBits;
        }

        ByteBuffer out = ByteBuffer.allocate(HEADER_SIZE + bits.sizeInBytes());
        out.put(VERSION);
        out.putInt(count);
        out.putLong(timestamps[0]);
        out.putLong(timestamps[count - 1]);
        bits.writeTo(out);
        out.flip();
        return out;
    }

    public static int sampleCount(ByteBuffer payload)
    {
        return payload.getInt(payload.position() + 1);
    }

    public static long firstTimestamp(ByteBuffer payload)
    {
        return payload.getLong(payload.position() + 5);
    }

    public static long lastTimestamp(ByteBuffer payload)
    {
        return payload.getLong(payload.position() + 13);
    }

    public static SampleCursor cursor(ByteBuffer payload)
    {
        ByteBuffer buffer = payload.duplicate();
        byte version = buffer.get();
        if (version != VERSION)
            throw new IllegalArgumentException("Unsupported gorilla chunk version: " + version);
        int count = buffer.getInt();
        if (count < 1)
            throw new IllegalArgumentException("Corrupt gorilla chunk: count " + count);
        long firstTimestamp = buffer.getLong();
        buffer.getLong();   // last timestamp: header-only metadata
        BitReader bits = new BitReader(buffer);

        return new SampleCursor()
        {
            private int index = -1;
            private long timestamp;
            private long delta;
            private long valueBits;
            private int windowLeading = -1;
            private int windowTrailing;

            @Override
            public boolean advance()
            {
                if (index + 1 >= count)
                    return false;
                index++;
                if (index == 0)
                {
                    timestamp = firstTimestamp;
                    valueBits = bits.readBits(64);
                    return true;
                }

                delta += readDod(bits);
                timestamp += delta;

                if (bits.readBit())
                {
                    if (bits.readBit())
                    {
                        windowLeading = (int) bits.readBits(5);
                        int meaningful = (int) bits.readBits(6) + 1;
                        windowTrailing = 64 - windowLeading - meaningful;
                        valueBits ^= bits.readBits(meaningful) << windowTrailing;
                    }
                    else
                    {
                        int meaningful = 64 - windowLeading - windowTrailing;
                        valueBits ^= bits.readBits(meaningful) << windowTrailing;
                    }
                }
                return true;
            }

            @Override
            public long timestamp()
            {
                return timestamp;
            }

            @Override
            public double value()
            {
                return Double.longBitsToDouble(valueBits);
            }
        };
    }

    private static void writeDod(BitWriter bits, long dod)
    {
        if (dod == 0)
        {
            bits.writeBit(false);
        }
        else if (dod >= -63 && dod <= 64)
        {
            bits.writeBits(0b10, 2);
            bits.writeBits(dod + 63, 7);
        }
        else if (dod >= -2047 && dod <= 2048)
        {
            bits.writeBits(0b110, 3);
            bits.writeBits(dod + 2047, 12);
        }
        else if (dod >= -65535 && dod <= 65536)
        {
            bits.writeBits(0b1110, 4);
            bits.writeBits(dod + 65535, 17);
        }
        else
        {
            bits.writeBits(0b1111, 4);
            bits.writeBits(dod, 64);
        }
    }

    private static long readDod(BitReader bits)
    {
        if (!bits.readBit())
            return 0;
        if (!bits.readBit())
            return bits.readBits(7) - 63;
        if (!bits.readBit())
            return bits.readBits(12) - 2047;
        if (!bits.readBit())
            return bits.readBits(17) - 65535;
        return bits.readBits(64);
    }
}
```

- [ ] **Step 4: 통과 확인**

컴파일 후: `.build/sh/ai-ci-test --reuse org.apache.cassandra.db.timeseries.GorillaCodecTest`
확인: XML에서 `failures="0" errors="0"` (테스트 3개).

- [ ] **Step 5: 커밋**

```bash
git add src/java/org/apache/cassandra/db/timeseries/GorillaCodec.java \
        test/unit/org/apache/cassandra/db/timeseries/GorillaCodecTest.java
git commit -m "Add gorilla codec: delta-of-delta timestamps + XOR-packed values"
```

---

### Task 3: 경계값과 검증 (인코더 거부·디코더 방어)

**Files:**
- Modify: `test/unit/org/apache/cassandra/db/timeseries/GorillaCodecTest.java` (테스트 추가)
- Modify: `src/java/org/apache/cassandra/db/timeseries/GorillaCodec.java` (필요 시에만 — Step 3 참조)

**Interfaces:**
- Consumes: Task 2의 전체 API
- Produces: 추가 API 없음 (계약 강화만)

- [ ] **Step 1: 실패(또는 통과) 테스트 추가** — `GorillaCodecTest`에 다음을 추가:

```java
    @Test
    public void specialValuesRoundtripBitExactly()
    {
        long[] timestamps = { 1L, 2L, 3L, 4L, 5L, 6L, 7L };
        double[] values = { Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY,
                            -0.0, 0.0, Double.MIN_VALUE, Double.MAX_VALUE };
        assertRoundtrip(timestamps, values, values.length);
    }

    @Test
    public void singleSampleRoundtrip()
    {
        assertRoundtrip(new long[]{ 1_700_000_000_000L }, new double[]{ 3.14 }, 1);
    }

    @Test
    public void irregularGapsIncludingLargeOnesRoundtrip()
    {
        // 밀리초·초·시간·여러 날 갭이 섞인 불규칙 시계열 (dod 전 버킷 + 64비트 escape 경유)
        long[] timestamps = { 0L, 1L, 1001L, 1002L, 3_600_000L, 3_600_050L, 90_000_000_000L };
        double[] values = { 1, 2, 3, 4, 5, 6, 7 };
        assertRoundtrip(timestamps, values, timestamps.length);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsOutOfOrderTimestamps()
    {
        GorillaCodec.encode(new long[]{ 1000L, 999L }, new double[]{ 1, 2 }, 2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsDuplicateTimestamps()
    {
        GorillaCodec.encode(new long[]{ 1000L, 1000L }, new double[]{ 1, 2 }, 2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsZeroCount()
    {
        GorillaCodec.encode(new long[0], new double[0], 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsUnknownVersion()
    {
        ByteBuffer payload = GorillaCodec.encode(new long[]{ 1L }, new double[]{ 1.0 }, 1);
        payload.put(payload.position(), (byte) 99);
        GorillaCodec.cursor(payload);
    }

    @Test(expected = RuntimeException.class)
    public void truncatedPayloadFailsInsteadOfReturningGarbage()
    {
        long[] timestamps = new long[100];
        double[] values = new double[100];
        for (int i = 0; i < 100; i++)
        {
            timestamps[i] = i * 1000L;
            values[i] = i * 0.1;
        }
        ByteBuffer payload = GorillaCodec.encode(timestamps, values, 100);
        payload.limit(payload.limit() - 10);   // 뒤 10바이트 절단

        GorillaCodec.SampleCursor cursor = GorillaCodec.cursor(payload);
        while (cursor.advance())
        {
            // 절단 지점에서 IndexOutOfBoundsException(RuntimeException) 이 나야 한다
        }
    }
```

- [ ] **Step 2: 실행해 실제로 무엇이 실패하는지 확인**

컴파일 후 실행. 기대: Task 2 구현이 이미 대부분을 만족해 **대부분 통과**한다.
통과하지 못하는 항목이 있으면 그것이 구현 결함이다 — 전형적 후보는
`truncatedPayload`(BitReader가 절대 인덱스 `buffer.get(start + byteIndex)`을 쓰므로 limit 축소를
무시하고 원 버퍼 용량까지 읽는 경우). 실패 메시지를 그대로 기록해 둘 것.

- [ ] **Step 3: 결함만 최소 수정**

`truncatedPayload`가 통과 못 하면 `BitReader.readBits`의 접근을 limit 검사로 강화:

```java
            int absolute = start + byteIndex;
            if (absolute >= buffer.limit())
                throw new IndexOutOfBoundsException("Truncated bit stream at byte " + byteIndex);
            int current = buffer.get(absolute) & 0xFF;
```

(주의: `ByteBuffer.get(int)`은 limit이 아니라 capacity 기준 검사를 하는 구현이 있으므로 명시
검사가 안전하다.) 다른 테스트가 이미 통과하면 구현 수정 금지 — 테스트만 추가된 커밋이 된다.

- [ ] **Step 4: 전체 통과 확인**

`.build/sh/ai-ci-test --reuse org.apache.cassandra.db.timeseries.GorillaCodecTest` — XML `failures="0" errors="0"`.
`BitStreamTest`도 재실행해 회귀 없음 확인.

- [ ] **Step 5: 커밋**

```bash
git add src/java/org/apache/cassandra/db/timeseries/ test/unit/org/apache/cassandra/db/timeseries/
git commit -m "Harden gorilla codec: special values, ordering rejection, truncation defence"
```

---

### Task 4: 프로퍼티 왕복 + 크기 회귀 기준 + 최종 검증

**Files:**
- Modify: `test/unit/org/apache/cassandra/db/timeseries/GorillaCodecTest.java` (테스트 추가)

**Interfaces:**
- Consumes: Task 2·3의 전체 API
- Produces: 크기 회귀 기준(아래 상수)이 향후 인코딩 변경의 게이트가 된다

- [ ] **Step 1: 프로퍼티/회귀 테스트 추가**:

```java
    @Test
    public void propertyRoundtripAcrossSeedsAndPatterns()
    {
        for (long seed = 0; seed < 30; seed++)
        {
            java.util.Random random = new java.util.Random(seed);
            int n = 1 + random.nextInt(5000);
            long[] timestamps = new long[n];
            double[] values = new double[n];

            long timestamp = Math.abs(random.nextLong() % 4_000_000_000_000L);
            int pattern = (int) (seed % 4);
            double walk = random.nextDouble() * 100;
            for (int i = 0; i < n; i++)
            {
                // 타임스탬프: 규칙(1s) / 지터 / 불규칙 갭을 시드별로 섞는다
                long step;
                switch (pattern)
                {
                    case 0:  step = 1000; break;                                   // 규칙
                    case 1:  step = 995 + random.nextInt(11); break;               // ±5ms 지터
                    case 2:  step = 1 + random.nextInt(10_000_000); break;         // 불규칙
                    default: step = random.nextInt(100) == 0
                                    ? 86_400_000L + random.nextInt(1_000_000)      // 드문 하루+ 갭
                                    : 100; break;
                }
                timestamp += step;
                timestamps[i] = timestamp;

                switch (pattern)
                {
                    case 0:  values[i] = 42.0; break;                              // 상수
                    case 1:  walk += random.nextGaussian(); values[i] = walk; break;
                    case 2:  values[i] = Double.longBitsToDouble(random.nextLong()); break;  // 임의 비트(NaN 포함)
                    default: values[i] = 20 + Math.sin(i / 300.0) * 5; break;
                }
            }

            assertRoundtrip(timestamps, values, n);
        }
    }

    @Test
    public void sizeRegressionBaselines()
    {
        // 회귀 기준: 이 수치를 넘기는 인코딩 변경은 명시적 결정 없이는 금지
        assertTrue("constant: " + bytesPerSample(0), bytesPerSample(0) <= 0.5);   // 설정값 패턴
        // 풀정밀도 가우시안 워크는 매 샘플 가수부가 거의 전부 바뀌는 XOR 압축의 준최악 케이스(실측 ~7.1).
        // 실제 산업 센서(양자화된 값)는 훨씬 잘 압축된다 — 실측 비교는 tiering 벤치마크(스펙 §7)에서.
        assertTrue("walk: " + bytesPerSample(1),     bytesPerSample(1) <= 8.0);   // 풀정밀도 랜덤워크
        assertTrue("random: " + bytesPerSample(2),   bytesPerSample(2) <= 11.0);  // 최악(원본 16B 대비)
    }

    private static double bytesPerSample(int pattern)
    {
        int n = 10_000;
        java.util.Random random = new java.util.Random(7);
        long[] timestamps = new long[n];
        double[] values = new double[n];
        double walk = 50;
        for (int i = 0; i < n; i++)
        {
            timestamps[i] = i * 1000L;
            switch (pattern)
            {
                case 0:  values[i] = 42.0; break;
                case 1:  walk += random.nextGaussian(); values[i] = walk; break;
                default: values[i] = Double.longBitsToDouble(random.nextLong()); break;
            }
        }
        return GorillaCodec.encode(timestamps, values, n).remaining() / (double) n;
    }
```

- [ ] **Step 2: 실행·통과 확인** — 컴파일 후 `.build/sh/ai-ci-test --reuse org.apache.cassandra.db.timeseries.GorillaCodecTest`, XML `failures="0" errors="0"`. 실측 bytes/sample 값을 로그로 확인해 커밋 메시지에 기록.

- [ ] **Step 3: checkstyle 포함 최종 빌드** — `.build/sh/ai-build` 실행, `BUILD SUCCESSFUL` 확인 (신규 파일 라이선스 헤더·임포트 순서 검증 포함).

- [ ] **Step 4: 커밋**

```bash
git add test/unit/org/apache/cassandra/db/timeseries/GorillaCodecTest.java
git commit -m "Add gorilla codec property roundtrip and size regression baselines"
```

- [ ] **Step 5: CI 배선 + 푸시** — `.gitlab-ci.yml`의 `timeseries-tests`에 두 줄 추가:

```yaml
    # tiered storage: gorilla codec
    - ant testsome -Dtest.name=org.apache.cassandra.db.timeseries.BitStreamTest
    - ant testsome -Dtest.name=org.apache.cassandra.db.timeseries.GorillaCodecTest
```

`python3 -c "import yaml; yaml.safe_load(open('.gitlab-ci.yml'))"` 로 검증 후:

```bash
git add .gitlab-ci.yml
git commit -m "CI: run gorilla codec tests"
git push origin master && git push origin master:6.0.0
```
