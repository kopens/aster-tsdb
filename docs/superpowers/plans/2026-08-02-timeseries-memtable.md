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

# 시계열 Memtable Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 창 배정을 flush 시점이 아니라 **쓰기 시점**으로 옮겨, 파티션 크기에 걸린 분할 상한(64 MiB)과 flush 시점 라우팅 비용을 없앤다.

**⚠️ 이 계획이 하지 않는 것 — 먼저 읽을 것**

창 경계 분할 자체는 **이미 동작한다.** `TimeSeriesCompactionStrategy`가 flush writer로 `TimeWindowSplittingMultiWriter`를 만들고(`TimeSeriesCompactionStrategy.java:783`), 2026-08-02 프로덕션 실측에서 메모테이블 하나가 창 경계에 맞춰 SSTable 6개로 쪼개지는 것을 확인했다. **따라서 이 계획의 이득은 "창 정렬 SSTable"이 아니다.**

실제로 더하는 것은 다음 셋뿐이다:

| 항목 | 현재 | 이 계획 이후 |
| --- | --- | --- |
| 64 MiB 파티션 상한 | 넘으면 분할 포기 → 창 걸친 SSTable → **파킹** | **상한 없음** |
| flush 시 라우팅 | 매 행 라우팅 + 파티션별 버퍼링 | 라우팅 자체가 없음 |
| flush 시 힙 스파이크 | 파티션당 최대 64 MiB × 동시 writer | 없음 |

**행당 메모리와 컴팩션 총량은 이 계획으로 거의 변하지 않는다.** 그 이득은 스펙 §8의 3단계(원시 배열 저장)와 4단계(콜드 창 청크 직접 flush)에 있으며, 별도 계획으로 진행한다. 이 계획은 그 둘을 얹을 수 있는 **구조를 만드는 단계**다.

**Architecture:** `TimeSeriesMemtable`은 `AbstractAllocatorMemtable`을 상속하고 내부를 `NavigableMap<Long windowStart, WindowShard>`로 가진다. `put`은 행의 쓰기 타임스탬프로 창을 골라 해당 샤드에 넣는다(버퍼링 없음). 각 샤드는 파티션별 클러스터링 정렬을 유지하므로, flush는 샤드를 그대로 써서 창당 SSTable 하나를 만든다. 질의 읽기 경로는 바꾸지 않는다.

**Tech Stack:** Java 21, Apache Cassandra 6.0 포크, `Memtable` 플러그인 API(`Memtable.Factory`), JUnit 4, jvm-dtest, `.build/sh/ai-*` 래퍼.

## Global Constraints

- 빌드는 `.build/sh/ai-build`, 테스트는 `.build/sh/ai-ci-test <FQCN>` 로만 한다. 절대 `ant`를 직접 부르지 않는다. 전체 스위트를 돌리지 않는다.
- `base.version`은 `6.0.0`을 유지한다. `build/apache-cassandra-6.0.0.jar`가 나와야 한다.
- `src/gen-java/`, `lib/`, CQL 문법(`src/antlr/Cql.g`)은 건드리지 않는다.
- 업스트림 파일 수정은 최소화한다. 이 계획에서 업스트림 파일을 고치는 것은 Task 2의 `WindowRoutingIterator` 한 곳뿐이다.
- 새 파일은 `src/java/org/apache/cassandra/db/memtable/timeseries/` 아래에 둔다.
- 모든 새 파일에 Apache License 2.0 헤더를 붙인다(기존 파일에서 그대로 복사).
- 이 memtable은 **테이블별 옵트인**이다. 기본 memtable을 바꾸지 않는다.
- 창 크기는 테이블의 `TimeSeriesCompactionStrategyOptions.window_size`에서 읽는다. 별도 옵션을 만들지 않는다.
- 스키마 제약은 **TSCS 사용 여부 하나뿐**이다. 창은 셀의 쓰기 타임스탬프로 정해지므로 클러스터링 모양·컬럼 타입과 무관하다. TSCS가 아닌 테이블만 기본 memtable로 폴백한다.
- `writesAreDurable()` = false, `streamFromMemtable()` = false (기본값 유지).
- 최종 완료 기준은 **프로덕션 노드 192.168.0.41에서의 실측**이다(Task 5).

---

## File Structure

| 파일 | 책임 |
| --- | --- |
| `db/memtable/TimeSeriesMemtable.java` | `Memtable` 구현 + 중첩 `Factory` + 스키마 판정 `unsupportedReason` |
| `db/memtable/timeseries/WindowShard.java` | 한 창의 파티션 저장소 |
| `db/memtable/timeseries/WindowGroupedFlushSet.java` | flush 시 `창 → 정렬된 파티션 이터레이터` |
| `db/memtable/timeseries/FlyweightRowView.java` | flush 전용 무할당 `Row` 뷰 (Task 4) |
| `test/unit/org/apache/cassandra/db/memtable/TimeSeriesMemtableSchemaSupportTest.java` | 스키마 판정·폴백 |
| `test/unit/org/apache/cassandra/db/memtable/TimeSeriesMemtableDifferentialTest.java` | 기준 memtable과의 차등 테스트 |
| `test/unit/org/apache/cassandra/db/memtable/TimeSeriesMemtableFlushTest.java` | flush가 창당 SSTable 1개를 내는지 |
| `test/conf/cassandra.yaml` | 테스트용 `timeseries` 설정 키 |
| `test/distributed/.../TimeSeriesMemtableDistributedTest.java` | 3노드: 부트스트랩·스트리밍·재시작 |

### ⚠️ memtable은 클래스명이 아니라 **설정 키**로 선택됩니다

`memtable` 테이블 속성은 **문자열**입니다. CQL에 클래스명을 쓰면
`SyntaxException: Invalid value for property 'memtable'. It should be a string` 이 납니다.

```yaml
# 모든 노드의 cassandra.yaml — 재시작 필요
memtable:
  configurations:
    timeseries:
      class_name: TimeSeriesMemtable
```
```sql
ALTER TABLE ks.tbl WITH memtable = 'timeseries';
```

따라서:

- 짧은 `class_name`은 `org.apache.cassandra.db.memtable.` 아래에서 찾습니다. 그래서 구현 클래스를
  **그 패키지에 직접** 둡니다 — 서브패키지에 두면 운영자가 FQCN을 써야 합니다.
- `MemtableParams`는 그 클래스의 static `factory(Map<String,String>)` 또는 `FACTORY` 필드를 찾고,
  소비되지 않은 파라미터가 남으면 설정을 거부합니다.
- 없는 키는 `getWithFallback`이 기본값으로 떨어뜨리고 ERROR만 남깁니다 — 조용히 폴백하므로
  오타가 어디서도 크게 실패하지 않습니다. 배포 후 반드시 실제 적용 여부를 확인해야 합니다.
- 테스트도 같은 방식입니다 — `test/conf/cassandra.yaml`에 키를 넣고 `WITH memtable = '<key>'`.

---

### Task 1: 스키마 적합성 판정과 팩토리 폴백

가장 먼저 만드는 이유: 이후 모든 태스크가 "이 테이블에 이 memtable을 쓸 수 있는가"에 의존한다. 그리고 이것만으로도 독립 배포 가능하다(폴백만 하므로 동작 변화 없음).

**Files:**
- Create: `src/java/org/apache/cassandra/db/memtable/timeseries/TimeSeriesMemtableFactory.java`
- Test: `test/unit/org/apache/cassandra/db/memtable/timeseries/TimeSeriesMemtableSchemaSupportTest.java`

**Interfaces:**
- Consumes: `org.apache.cassandra.schema.TableMetadata`, `org.apache.cassandra.db.compaction.TimeSeriesCompactionStrategyOptions`
- Produces:
  - `static String TimeSeriesMemtableFactory.unsupportedReason(TableMetadata)` — 지원 가능하면 `null`, 아니면 사람이 읽을 사유 문자열
  - `static Memtable.Factory TimeSeriesMemtableFactory.factory(Map<String,String> options)` — `MemtableParams`가 리플렉션으로 찾는 진입점

- [ ] **Step 1: 실패하는 테스트를 쓴다**

`test/unit/org/apache/cassandra/db/memtable/timeseries/TimeSeriesMemtableSchemaSupportTest.java`:

```java
package org.apache.cassandra.db.memtable.timeseries;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TimeSeriesMemtableSchemaSupportTest extends CQLTester
{
    /** 운영 테이블 모양: 파티션키 1 + timestamp 클러스터링 + static 여럿 + 일반 컬럼 여럿. */
    @Test
    public void acceptsTheProductionTagShape()
    {
        createTable("CREATE TABLE %s (" +
                    "tag_id text, timestamp timestamp," +
                    "type text static, tag_name text static," +
                    "value text, value_numeric double, value_boolean boolean, quality int," +
                    "PRIMARY KEY (tag_id, timestamp)) " +
                    "WITH CLUSTERING ORDER BY (timestamp DESC) " +
                    "AND compaction = {'class':'TimeSeriesCompactionStrategy','window_size':'1d'}");

        assertNull(TimeSeriesMemtableFactory.unsupportedReason(currentTableMetadata()));
    }

    /**
     * counter 는 삭제 후 재삽입이 불가능하다. 창 사이로 행을 옮기는 이 memtable 의
     * flush 는 counter 셀에 대해 안전하지 않으므로 아예 받지 않는다.
     */
    @Test
    public void rejectsCounterTables()
    {
        createTable("CREATE TABLE %s (k text, c timestamp, hits counter, PRIMARY KEY (k, c)) " +
                    "WITH compaction = {'class':'TimeSeriesCompactionStrategy','window_size':'1d'}");

        String reason = TimeSeriesMemtableFactory.unsupportedReason(currentTableMetadata());
        assertNotNull("counter 테이블은 거부되어야 한다", reason);
        assertTrue(reason, reason.toLowerCase().contains("counter"));
    }

    /** 비frozen 컬렉션은 멀티셀이라 행 단위 fast path 가 성립하지 않는다. */
    @Test
    public void rejectsNonFrozenCollections()
    {
        createTable("CREATE TABLE %s (k text, c timestamp, tags map<text,text>, PRIMARY KEY (k, c)) " +
                    "WITH compaction = {'class':'TimeSeriesCompactionStrategy','window_size':'1d'}");

        String reason = TimeSeriesMemtableFactory.unsupportedReason(currentTableMetadata());
        assertNotNull("비frozen 컬렉션 테이블은 거부되어야 한다", reason);
    }

    /** frozen 컬렉션은 단일 셀이므로 받는다 -- 운영 tm_tag_point 의 attribute 가 이 모양이다. */
    @Test
    public void acceptsFrozenCollections()
    {
        createTable("CREATE TABLE %s (k text, c timestamp, attribute frozen<map<text,text>>, " +
                    "PRIMARY KEY (k, c)) " +
                    "WITH compaction = {'class':'TimeSeriesCompactionStrategy','window_size':'1d'}");

        assertNull(TimeSeriesMemtableFactory.unsupportedReason(currentTableMetadata()));
    }

    /** 창 크기를 TSCS 에서 읽으므로, TSCS 가 아니면 창을 정의할 수 없다. */
    @Test
    public void rejectsTablesNotUsingTimeSeriesCompaction()
    {
        createTable("CREATE TABLE %s (k text, c timestamp, v double, PRIMARY KEY (k, c)) " +
                    "WITH compaction = {'class':'UnifiedCompactionStrategy'}");

        String reason = TimeSeriesMemtableFactory.unsupportedReason(currentTableMetadata());
        assertNotNull("TSCS 가 아닌 테이블은 거부되어야 한다", reason);
        assertTrue(reason, reason.contains("TimeSeriesCompactionStrategy"));
    }

    /** 클러스터링이 timestamp 계열이 아니면 창을 계산할 대상이 없다. */
    @Test
    public void rejectsNonTimestampClustering()
    {
        createTable("CREATE TABLE %s (k text, c text, v double, PRIMARY KEY (k, c)) " +
                    "WITH compaction = {'class':'TimeSeriesCompactionStrategy','window_size':'1d'}");

        assertNotNull(TimeSeriesMemtableFactory.unsupportedReason(currentTableMetadata()));
    }
}
```

- [ ] **Step 2: 테스트가 실패하는지 확인한다**

```
.build/sh/ai-build
.build/sh/ai-ci-test --reuse org.apache.cassandra.db.memtable.timeseries.TimeSeriesMemtableSchemaSupportTest
```

Expected: 컴파일 실패 — `TimeSeriesMemtableFactory` 가 없다.

- [ ] **Step 3: 최소 구현을 쓴다**

`src/java/org/apache/cassandra/db/memtable/timeseries/TimeSeriesMemtableFactory.java`:

```java
package org.apache.cassandra.db.memtable.timeseries;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.compaction.TimeSeriesCompactionStrategy;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.SkipListMemtableFactory;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;

/**
 * {@link TimeSeriesMemtable} 의 팩토리이자, 그 memtable 이 다룰 수 있는 스키마의 판정자.
 *
 * <p>판정을 팩토리에 두는 이유는 실패 방식 때문이다. memtable 은 쓰기 경로에 있으므로,
 * 다룰 수 없는 스키마를 만났을 때 던지면 그 테이블에 대한 쓰기가 전부 실패한다.
 * 대신 기본 memtable 로 폴백하고 한 번 경고한다 -- 성능은 못 얻지만 데이터는 정상이다.
 * 계층화의 {@code TieringPolicy} 가 같은 이유로 같은 형태를 취한다.
 */
public class TimeSeriesMemtableFactory implements Memtable.Factory
{
    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesMemtableFactory.class);

    /** MemtableParams 가 리플렉션으로 찾는 진입점. 소비하지 않은 옵션이 남으면 상위가 거부한다. */
    public static Memtable.Factory factory(Map<String, String> options)
    {
        return new TimeSeriesMemtableFactory();
    }

    /**
     * @return 이 memtable 로 다룰 수 없는 이유, 다룰 수 있으면 {@code null}.
     */
    public static String unsupportedReason(TableMetadata metadata)
    {
        if (!TimeSeriesCompactionStrategy.class.getName().equals(metadata.params.compaction.klass().getName()))
            return "table does not use TimeSeriesCompactionStrategy, so there is no window_size to shard by";

        if (metadata.clusteringColumns().size() != 1)
            return "expected exactly one clustering column, found " + metadata.clusteringColumns().size();

        ColumnMetadata clustering = metadata.clusteringColumns().get(0);
        if (!(clustering.type.unwrap() instanceof TimestampType))
            return "clustering column " + clustering.name + " is " + clustering.type.asCQL3Type()
                   + ", not a timestamp";

        for (ColumnMetadata column : metadata.columns())
        {
            if (column.type.isCounter())
                return "counter column " + column.name + " cannot be re-inserted after deletion";
            if (column.type.isCollection() && column.type.isMultiCell())
                return "non-frozen collection column " + column.name + " is multi-cell";
            if (column.type.isUDT() && column.type.isMultiCell())
                return "non-frozen UDT column " + column.name + " is multi-cell";
        }
        return null;
    }

    @Override
    public Memtable create(AtomicReference<CommitLogPosition> commitLogLowerBound,
                           TableMetadataRef metadataRef,
                           Memtable.Owner owner)
    {
        String reason = unsupportedReason(metadataRef.get());
        if (reason != null)
        {
            logger.warn("TimeSeriesMemtable is configured on {}.{} but cannot be used: {}. " +
                        "Falling back to the default memtable -- writes are unaffected, only the " +
                        "window-sharding optimisation is lost.",
                        metadataRef.get().keyspace, metadataRef.get().name, reason);
            return SkipListMemtableFactory.instance.create(commitLogLowerBound, metadataRef, owner);
        }
        return new TimeSeriesMemtable(commitLogLowerBound, metadataRef, owner);
    }

    @Override
    public boolean equals(Object o)
    {
        return o != null && getClass() == o.getClass();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass());
    }
}
```

이 단계에서는 `TimeSeriesMemtable` 이 아직 없으므로, Task 2 가 만들 때까지 `create` 의 마지막 줄은 컴파일되지 않는다. **Task 1 에서는 그 줄도 `SkipListMemtableFactory.instance.create(...)` 로 두고**, Task 2 에서 교체한다. 즉 Task 1 의 산출물은 "판정 로직 + 항상 폴백하는 팩토리"이며, 그것만으로 테스트가 통과한다.

- [ ] **Step 4: 테스트가 통과하는지 확인한다**

```
.build/sh/ai-build
.build/sh/ai-ci-test --reuse org.apache.cassandra.db.memtable.timeseries.TimeSeriesMemtableSchemaSupportTest
```

Expected: 6개 테스트 전부 PASS. `build/test/output/TEST-*.xml` 에서 `failures="0" errors="0"` 이고 `tests="6"` 인지 직접 확인한다 — ant 는 클래스를 못 찾아도 BUILD SUCCESSFUL 을 낸다.

- [ ] **Step 5: 체크스타일과 커밋**

```bash
ant checkstyle checkstyle-test
git add src/java/org/apache/cassandra/db/memtable/timeseries/TimeSeriesMemtableFactory.java \
        test/unit/org/apache/cassandra/db/memtable/timeseries/TimeSeriesMemtableSchemaSupportTest.java
git commit -m "Add schema-support gate for the time-series memtable

The gate lives in the factory and falls back rather than throwing. A memtable
sits on the write path, so refusing a schema there would fail every write to
that table; falling back costs the optimisation and nothing else."
```

---

### Task 2: 창 샤딩 memtable — 저장은 기존 파티션 구조 재사용

**Files:**
- Create: `src/java/org/apache/cassandra/db/memtable/timeseries/TimeSeriesMemtable.java`
- Create: `src/java/org/apache/cassandra/db/memtable/timeseries/WindowShard.java`
- Modify: `src/java/org/apache/cassandra/db/memtable/timeseries/TimeSeriesMemtableFactory.java` (Step 3 의 마지막 줄을 실제 생성으로 교체)
- Test: `test/unit/org/apache/cassandra/db/memtable/timeseries/TimeSeriesMemtableDifferentialTest.java`

**Interfaces:**
- Consumes: `TimeSeriesMemtableFactory.unsupportedReason(TableMetadata)`
- Produces:
  - `class TimeSeriesMemtable extends AbstractAllocatorMemtable`
  - `NavigableMap<Long, WindowShard> TimeSeriesMemtable.shards()` — 테스트가 창 분포를 확인할 때 쓴다
  - `long WindowShard.windowStart()`

**핵심 설계 — `put` 의 창 배정**

`PartitionUpdate` 하나가 여러 창에 걸칠 수 있다. 행마다 그 행의 **최대 쓰기 타임스탬프**로 창을 정하고 해당 샤드에 넣는다. 기준을 최대값으로 잡는 이유는 `TimeSeriesCompactionStrategy` 가 SSTable 을 최대 타임스탬프로 창에 배정하기 때문이다 — 기준이 다르면 flush 결과가 전략이 기대하는 창에 들어가지 않는다.

버퍼링이 없다는 점이 이 설계의 전부다. `WindowRoutingIterator` 가 파티션 전체를 힙에 올려야 했던 이유는 `SSTableWriter` 가 파티션 키를 한 번만 받기 때문인데, memtable 은 가변 맵이라 행 하나씩 O(1) 로 꽂으면 된다.

- [ ] **Step 1: 차등 테스트를 쓴다 (실패하는 상태)**

```java
package org.apache.cassandra.db.memtable.timeseries;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;

import static org.junit.Assert.assertEquals;

/**
 * 기준 구현과 답이 같은지 본다. memtable 결함은 조용한 데이터 손실로 나타나므로,
 * "예외 없이 돌았다" 는 증거가 되지 못한다. 같은 연산 시퀀스를 기본 memtable 과
 * TimeSeriesMemtable 에 각각 넣고 읽기 결과가 완전히 일치하는지 비교한다.
 */
public class TimeSeriesMemtableDifferentialTest extends CQLTester
{
    private static final String TS_MEMTABLE =
        "{'class':'org.apache.cassandra.db.memtable.timeseries.TimeSeriesMemtableFactory'}";
    private static final String TSCS =
        "{'class':'TimeSeriesCompactionStrategy','window_size':'1h','freeze_after':'2h'}";

    /** 창 경계를 넘나드는 쓰기가 기본 memtable 과 같은 결과를 내야 한다. */
    @Test
    public void matchesReferenceAcrossWindowBoundaries() throws Throwable
    {
        List<Object[]> ops = randomOperations(400, 20260801_00_00_00L);
        assertEquals(runAndRead(false, ops), runAndRead(true, ops));
    }

    /** 같은 (파티션, 클러스터링) 을 여러 번 쓰면 마지막 값이 이겨야 한다. */
    @Test
    public void matchesReferenceOnCellOverwrite() throws Throwable
    {
        List<Object[]> ops = new ArrayList<>();
        long base = 1785600000000L;
        for (int i = 0; i < 50; i++)
            ops.add(new Object[]{ "S1", base, (double) i });          // 같은 클러스터링 반복
        assertEquals(runAndRead(false, ops), runAndRead(true, ops));
    }

    /** 시간 역순으로 도착해도 결과가 같아야 한다 (지연 정렬 경로). */
    @Test
    public void matchesReferenceOnOutOfOrderArrival() throws Throwable
    {
        List<Object[]> ops = new ArrayList<>();
        long base = 1785600000000L;
        for (int i = 200; i >= 0; i--)
            ops.add(new Object[]{ "S" + (i % 4), base + i * 60_000L, i * 1.5 });
        assertEquals(runAndRead(false, ops), runAndRead(true, ops));
    }

    private static List<Object[]> randomOperations(int count, long seed)
    {
        Random rng = new Random(seed);
        List<Object[]> ops = new ArrayList<>(count);
        long base = 1785600000000L;                       // 2026-08-02 근처
        for (int i = 0; i < count; i++)
            ops.add(new Object[]{ "S" + rng.nextInt(5),
                                  base + rng.nextInt(6) * 3600_000L + rng.nextInt(3600) * 1000L,
                                  Math.round(rng.nextGaussian() * 1000) / 100.0 });
        return ops;
    }

    /** 지정한 memtable 로 테이블을 만들고 ops 를 적용한 뒤, 전 행을 문자열로 뽑는다. */
    private String runAndRead(boolean timeSeriesMemtable, List<Object[]> ops) throws Throwable
    {
        String memtableClause = timeSeriesMemtable ? " AND memtable = " + TS_MEMTABLE : "";
        createTable("CREATE TABLE %s (series text, ts timestamp, v double, PRIMARY KEY (series, ts)) " +
                    "WITH compaction = " + TSCS + memtableClause);

        for (Object[] op : ops)
            execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?)", op[0], op[1], op[2]);

        StringBuilder sb = new StringBuilder();
        for (String s : new String[]{ "S0", "S1", "S2", "S3", "S4" })
        {
            UntypedResultSet rs = execute("SELECT series, ts, v FROM %s WHERE series = ?", s);
            for (UntypedResultSet.Row row : rs)
                sb.append(row.getString("series")).append('|')
                  .append(row.getTimestamp("ts").getTime()).append('|')
                  .append(row.getDouble("v")).append('\n');
        }
        return sb.toString();
    }
}
```

- [ ] **Step 2: 실패를 확인한다**

```
.build/sh/ai-build
.build/sh/ai-ci-test --reuse org.apache.cassandra.db.memtable.timeseries.TimeSeriesMemtableDifferentialTest
```

Expected: `memtable = {'class': ...TimeSeriesMemtableFactory}` 로 만든 테이블이 기본 memtable 로 폴백되므로 **테스트는 통과한다**(Task 1 상태). 이 시점의 통과는 "폴백이 정상 동작한다" 는 뜻이며, Step 3 이후 실제 구현으로 바뀌었을 때도 계속 통과해야 한다. 통과 사실을 커밋 메시지에 기록한다.

- [ ] **Step 3: `WindowShard` 를 만든다**

```java
package org.apache.cassandra.db.memtable.timeseries;

import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.partitions.AtomicBTreePartition;

/**
 * 한 창에 속하는 파티션들. 저장 구조는 기본 memtable 과 동일한
 * {@link AtomicBTreePartition} 을 그대로 쓴다 -- 이 태스크의 목적은 저장 형식을 바꾸는 것이
 * 아니라 <em>창 단위로 나누는</em> 것이고, 검증된 저장 구조를 유지해야 차등 테스트의
 * 불일치가 곧 샤딩 결함을 뜻하게 된다.
 */
class WindowShard
{
    private final long windowStart;
    final ConcurrentSkipListMap<DecoratedKey, AtomicBTreePartition> partitions =
        new ConcurrentSkipListMap<>();

    WindowShard(long windowStart)
    {
        this.windowStart = windowStart;
    }

    long windowStart()
    {
        return windowStart;
    }
}
```

- [ ] **Step 4: `TimeSeriesMemtable` 을 만든다**

`ShardedSkipListMemtable` 을 참고 구현으로 삼되, 샤딩 축을 토큰이 아니라 창으로 바꾼다. `put` 은 `PartitionUpdate` 의 행을 순회하며 각 행의 최대 쓰기 타임스탬프로 창을 계산하고, 그 창의 샤드에 해당 행만 담은 `PartitionUpdate` 를 적용한다. `partitionIterator` 와 `rowIterator` 는 모든 샤드의 같은 파티션 키를 `UnfilteredRowIterators.merge` 로 합친다.

구현 분량이 커서 여기 전부 싣지 않는다. 다음 두 가지를 반드시 지킨다:

1. **창 계산은 `TimeSeriesCompactionStrategyOptions.windowStartFor(long millis)` 를 재사용한다.** 직접 나눗셈을 쓰지 않는다 — 전략과 어긋나면 flush 결과가 엉뚱한 창에 들어간다.
2. **`put` 은 어떤 경우에도 파티션 전체를 버퍼링하지 않는다.** 행 단위로 대상 샤드에 바로 적용한다.

- [ ] **Step 5: 팩토리를 실제 생성으로 바꾼다**

`TimeSeriesMemtableFactory.create` 의 폴백 아래 줄을 `return new TimeSeriesMemtable(commitLogLowerBound, metadataRef, owner);` 로 교체한다.

- [ ] **Step 6: 차등 테스트가 여전히 통과하는지 확인한다**

```
.build/sh/ai-build
.build/sh/ai-ci-test --reuse org.apache.cassandra.db.memtable.timeseries.TimeSeriesMemtableDifferentialTest
.build/sh/ai-ci-test --reuse org.apache.cassandra.db.memtable.timeseries.TimeSeriesMemtableSchemaSupportTest
```

Expected: 두 클래스 모두 `failures="0" errors="0"`, `tests` 가 0이 아님.

- [ ] **Step 7: 커밋**

```bash
ant checkstyle checkstyle-test
git add src/java/org/apache/cassandra/db/memtable/timeseries/ test/unit/org/apache/cassandra/db/memtable/timeseries/
git commit -m "Shard the time-series memtable by write-timestamp window

Windows are decided when the row is written, not when the memtable flushes.
The routing buffer exists because SSTableWriter takes a partition in one pass
and accepts its key once, so splitting one at flush means holding it on heap.
A memtable is a mutable map and has no such constraint."
```

---

### Task 3: 창당 SSTable 하나로 flush

**Files:**
- Create: `src/java/org/apache/cassandra/db/memtable/timeseries/WindowGroupedFlushSet.java`
- Modify: `src/java/org/apache/cassandra/db/memtable/timeseries/TimeSeriesMemtable.java` (`getFlushSet`)
- Modify: `src/java/org/apache/cassandra/db/compaction/timeseries/WindowRoutingIterator.java` (이미 창별로 그룹된 입력에 대한 무버퍼 경로)
- Test: `test/unit/org/apache/cassandra/db/memtable/timeseries/TimeSeriesMemtableFlushTest.java`

**Interfaces:**
- Consumes: `TimeSeriesMemtable.shards()`
- Produces: `interface WindowGroupedFlushSet` — `NavigableSet<Long> windows()` 와 `Iterator<Partition> partitionsInWindow(long windowStart)`

**설계 요점**

`getFlushSet` 은 파티션을 **토큰 순서**로 내야 하고, 한 SSTable 안에서 행은 **클러스터링 순서**여야 한다. 창은 쓰기 타임스탬프 기준이라 두 순서가 어긋나는데, 창 샤딩 memtable 은 이걸 구조적으로 해결한다: 샤드마다 파티션이 토큰 순서로, 파티션 안의 행이 클러스터링 순서로 이미 정렬돼 있다. 그러므로 **샤드 하나를 통째로 하나의 SSTable 로 쓰면** 두 순서가 모두 만족된다.

`WindowRoutingIterator` 에는 "이미 창별로 그룹돼 있음" 을 알리는 경로를 추가해, 라우팅·버퍼링을 건너뛰게 한다. 기존 경로(레거시 SSTable 분할, 스트리밍 수신)는 그대로 둔다.

- [ ] **Step 1: 실패하는 테스트를 쓴다**

```java
package org.apache.cassandra.db.memtable.timeseries;

import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TimeSeriesMemtableFlushTest extends CQLTester
{
    /**
     * 3개 창에 걸친 데이터를 한 번 flush 하면 SSTable 이 3개 나오고,
     * 각각이 정확히 한 창 안에 들어가야 한다. 이것이 split-refreeze 를 불필요하게 만드는 조건이다.
     */
    @Test
    public void flushEmitsOneSSTablePerWindow() throws Throwable
    {
        createTable("CREATE TABLE %s (series text, ts timestamp, v double, PRIMARY KEY (series, ts)) " +
                    "WITH compaction = {'class':'TimeSeriesCompactionStrategy','window_size':'1h'} " +
                    "AND memtable = {'class':'org.apache.cassandra.db.memtable.timeseries.TimeSeriesMemtableFactory'}");

        long hour = 3600_000L;
        long base = (System.currentTimeMillis() / hour) * hour - 3 * hour;
        for (int w = 0; w < 3; w++)
            for (int i = 0; i < 100; i++)
                executeWithTimestamp((base + w * hour + i * 1000L) * 1000L,
                                     "INSERT INTO %s (series, ts, v) VALUES (?, ?, ?)",
                                     "S" + (i % 3), base + w * hour + i * 1000L, i * 1.0);
        flush();

        Set<SSTableReader> sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        assertEquals("창마다 SSTable 하나가 나와야 한다", 3, sstables.size());

        for (SSTableReader sstable : sstables)
        {
            long minWindow = (sstable.getMinTimestamp() / 1000L / hour) * hour;
            long maxWindow = (sstable.getMaxTimestamp() / 1000L / hour) * hour;
            assertEquals("SSTable " + sstable.getFilename() + " 이 창을 걸친다", minWindow, maxWindow);
        }
    }

    /**
     * 라우팅 버퍼를 1바이트로 줄여도 오버플로 경고가 나오지 않아야 한다 --
     * flush 경로에서 라우팅 자체가 일어나지 않는다는 뜻이다.
     */
    @Test
    public void flushDoesNotRouteAndSoCannotOverflow() throws Throwable
    {
        createTable("CREATE TABLE %s (series text, ts timestamp, v double, PRIMARY KEY (series, ts)) " +
                    "WITH compaction = {'class':'TimeSeriesCompactionStrategy','window_size':'1h'} " +
                    "AND memtable = {'class':'org.apache.cassandra.db.memtable.timeseries.TimeSeriesMemtableFactory'}");

        long saved = org.apache.cassandra.db.compaction.timeseries.WindowRoutingIterator.maxBufferedBytesPerPartition;
        try
        {
            org.apache.cassandra.db.compaction.timeseries.WindowRoutingIterator.maxBufferedBytesPerPartition = 1;
            long hour = 3600_000L;
            long base = (System.currentTimeMillis() / hour) * hour - 2 * hour;
            for (int w = 0; w < 2; w++)
                for (int i = 0; i < 500; i++)
                    executeWithTimestamp((base + w * hour + i * 1000L) * 1000L,
                                         "INSERT INTO %s (series, ts, v) VALUES (?, ?, ?)",
                                         "SAME", base + w * hour + i * 1000L, i * 1.0);
            flush();

            assertEquals("창마다 SSTable 하나", 2, getCurrentColumnFamilyStore().getLiveSSTables().size());
        }
        finally
        {
            org.apache.cassandra.db.compaction.timeseries.WindowRoutingIterator.maxBufferedBytesPerPartition = saved;
        }
    }
}
```

- [ ] **Step 2: 실패를 확인한다**

```
.build/sh/ai-build
.build/sh/ai-ci-test --reuse org.apache.cassandra.db.memtable.timeseries.TimeSeriesMemtableFlushTest
```

Expected: FAIL — 현재 `getFlushSet` 은 창을 구분하지 않으므로 SSTable 이 1개만 나오거나, 라우팅 버퍼 오버플로 경로를 탄다.

- [ ] **Step 3: `WindowGroupedFlushSet` 과 `getFlushSet` 을 구현한다**

`getFlushSet(from, to)` 이 창별로 나뉜 뷰를 반환하고, `Flushing` 이 창마다 writer 를 하나씩 열도록 한다. 기존 `TimeWindowSplittingMultiWriter` 를 재사용하되, 입력이 이미 창별이므로 라우팅을 건너뛰는 생성자를 추가한다.

- [ ] **Step 4: 테스트 통과를 확인한다**

```
.build/sh/ai-build
.build/sh/ai-ci-test --reuse org.apache.cassandra.db.memtable.timeseries.TimeSeriesMemtableFlushTest
.build/sh/ai-ci-test --reuse org.apache.cassandra.db.memtable.timeseries.TimeSeriesMemtableDifferentialTest
.build/sh/ai-ci-test --reuse org.apache.cassandra.db.compaction.timeseries.WindowRoutingIteratorTest
.build/sh/ai-ci-test --reuse org.apache.cassandra.db.compaction.TimeSeriesCompactionStrategyE2ETest
```

Expected: 네 클래스 모두 `failures="0" errors="0"`. 뒤 두 개는 기존 경로가 깨지지 않았는지 보는 회귀 확인이다.

- [ ] **Step 5: 커밋**

```bash
ant checkstyle checkstyle-test
git add src/java/org/apache/cassandra/db/memtable/timeseries/ \
        src/java/org/apache/cassandra/db/compaction/timeseries/WindowRoutingIterator.java \
        test/unit/org/apache/cassandra/db/memtable/timeseries/TimeSeriesMemtableFlushTest.java
git commit -m "Flush one sstable per window from the time-series memtable

A shard already holds its partitions in token order and their rows in
clustering order, so writing a shard straight out satisfies both orderings the
sstable writer needs -- and the split never has to happen."
```

---

### Task 4: flush 전용 무할당 Row 뷰

**Files:**
- Create: `src/java/org/apache/cassandra/db/memtable/timeseries/FlyweightRowView.java`
- Modify: `src/java/org/apache/cassandra/db/memtable/timeseries/TimeSeriesMemtable.java`
- Test: `test/unit/org/apache/cassandra/db/memtable/timeseries/TimeSeriesMemtableFlyweightTest.java`

**Interfaces:**
- Produces: `class FlyweightRowView implements Row` — `void reset(int index)` 로 재배치, `static volatile boolean enabled` (테스트가 끄고 켠다)

**반드시 지킬 제약 (코드로 확인함)**

`SortedTablePartitionWriter` 는 `Unfiltered` 객체 자체는 보관하지 않지만(즉시 직렬화), `clustering()` 은 `firstClustering`·`lastClustering` 필드에 **보관한다**(`SortedTablePartitionWriter.java:137,145`). 따라서:

> Row 래퍼는 재사용해도 되지만, **`clustering()` 은 매번 새 불변 객체를 돌려줘야 한다.**

그리고 이 flyweight 는 **flush 경로에서만** 쓴다. 질의 경로는 병합 이터레이터가 여러 소스의 현재 행을 동시에 붙들고 비교하므로 재사용 객체를 넘기면 조용히 오염된다.

- [ ] **Step 1: 오염 검출 테스트를 쓴다**

```java
package org.apache.cassandra.db.memtable.timeseries;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

import static org.junit.Assert.assertEquals;

public class TimeSeriesMemtableFlyweightTest extends CQLTester
{
    /**
     * flyweight 재사용 여부와 무관하게 flush 결과가 같아야 한다.
     * 다르면 clustering() 이 재사용 객체를 흘렸다는 뜻이고, 그것은 인덱스 블록의
     * 경계 클러스터링이 뒤 행의 값으로 덮여 SSTable 이 깨진다는 뜻이다.
     */
    @Test
    public void reusingTheRowViewDoesNotChangeFlushOutput() throws Throwable
    {
        assertEquals(flushAndReadBack(false), flushAndReadBack(true));
    }

    private String flushAndReadBack(boolean flyweight) throws Throwable
    {
        boolean saved = FlyweightRowView.enabled;
        try
        {
            FlyweightRowView.enabled = flyweight;
            createTable("CREATE TABLE %s (series text, ts timestamp, v double, q int, PRIMARY KEY (series, ts)) " +
                        "WITH compaction = {'class':'TimeSeriesCompactionStrategy','window_size':'1h'} " +
                        "AND memtable = {'class':'org.apache.cassandra.db.memtable.timeseries.TimeSeriesMemtableFactory'}");

            long base = 1785600000000L;
            for (int i = 0; i < 2000; i++)                 // 인덱스 블록을 여러 개 만들 만큼
                execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?)",
                        "S" + (i % 3), base + i * 1000L, i * 0.5, i);
            flush();

            StringBuilder sb = new StringBuilder();
            for (String s : new String[]{ "S0", "S1", "S2" })
                for (var row : execute("SELECT ts, v, q FROM %s WHERE series = ?", s))
                    sb.append(row.getTimestamp("ts").getTime()).append('|')
                      .append(row.getDouble("v")).append('|')
                      .append(row.getInt("q")).append('\n');
            return sb.toString();
        }
        finally
        {
            FlyweightRowView.enabled = saved;
        }
    }
}
```

- [ ] **Step 2: 실패를 확인한다**

```
.build/sh/ai-build
.build/sh/ai-ci-test --reuse org.apache.cassandra.db.memtable.timeseries.TimeSeriesMemtableFlyweightTest
```

Expected: 컴파일 실패 — `FlyweightRowView` 가 없다.

- [ ] **Step 3: `FlyweightRowView` 를 구현한다**

`Row` 인터페이스를 구현하되 `clustering()` 만 매번 새 `Clustering` 을 만들어 돌려준다. 나머지 접근자는 현재 인덱스의 저장 배열을 읽는다. `enabled` 가 false 면 `TimeSeriesMemtable` 은 기존처럼 실제 행 객체를 만든다.

- [ ] **Step 4: 통과를 확인한다**

```
.build/sh/ai-build
.build/sh/ai-ci-test --reuse org.apache.cassandra.db.memtable.timeseries.TimeSeriesMemtableFlyweightTest
.build/sh/ai-ci-test --reuse org.apache.cassandra.db.memtable.timeseries.TimeSeriesMemtableFlushTest
.build/sh/ai-ci-test --reuse org.apache.cassandra.db.memtable.timeseries.TimeSeriesMemtableDifferentialTest
```

- [ ] **Step 5: 커밋**

```bash
ant checkstyle checkstyle-test
git add src/java/org/apache/cassandra/db/memtable/timeseries/FlyweightRowView.java \
        src/java/org/apache/cassandra/db/memtable/timeseries/TimeSeriesMemtable.java \
        test/unit/org/apache/cassandra/db/memtable/timeseries/TimeSeriesMemtableFlyweightTest.java
git commit -m "Reuse one row view per flush, but never its clustering

SortedTablePartitionWriter keeps firstClustering and lastClustering across
rows, so a reused view must still hand out a fresh immutable clustering. The
query path keeps materialising real rows: merge iterators hold several sources'
current rows at once and would read a view that has already moved on."
```

---

### Task 5: 3노드 검증과 41번 서버 실측

**Files:**
- Create: `test/distributed/org/apache/cassandra/distributed/test/memtable/TimeSeriesMemtableDistributedTest.java`
- Create: `docker/memtable-bench.sh`
- Modify: `.build/sh/ci-timeseries-tests.sh` (새 테스트 클래스 등록)
- Modify: `doc/timeseries/timeseries-compaction.md` (memtable 절 추가)
- Create: `doc/timeseries/timeseries-memtable.md`

**Interfaces:**
- Consumes: Task 1~4 의 산출물 전부

- [ ] **Step 1: 3노드 dtest 를 쓴다**

부트스트랩으로 받은 데이터가 창 경계로 나뉘는지, 재시작 후 커밋로그 재생으로 손실이 없는지, repair 가 정상인지 확인한다. 기존 `TimeSeriesCompactionDistributedTest` 의 클러스터 구성 방식을 그대로 따른다.

- [ ] **Step 2: dtest 를 돌린다**

```
.build/sh/ai-ci-test --reuse org.apache.cassandra.distributed.test.memtable.TimeSeriesMemtableDistributedTest
```

Expected: `failures="0" errors="0"`, `tests` 가 0이 아님.

- [ ] **Step 3: CI 에 등록한다**

`.build/sh/ci-timeseries-tests.sh` 의 `TESTS` 배열에 다음 5줄을 추가한다:

```
  "testsome|org.apache.cassandra.db.memtable.timeseries.TimeSeriesMemtableSchemaSupportTest"
  "testsome|org.apache.cassandra.db.memtable.timeseries.TimeSeriesMemtableDifferentialTest"
  "testsome|org.apache.cassandra.db.memtable.timeseries.TimeSeriesMemtableFlushTest"
  "testsome|org.apache.cassandra.db.memtable.timeseries.TimeSeriesMemtableFlyweightTest"
  "test-jvm-dtest-some|org.apache.cassandra.distributed.test.memtable.TimeSeriesMemtableDistributedTest"
```

- [ ] **Step 4: 41번 서버 실측 — 최종 완료 기준**

프로덕션 노드 `192.168.0.41` 에 새 jar 을 넣고(`lib/apache-cassandra-timeseries-6.0.0.jar`, 기존 jar 은 `lib-backup/` 으로), 시험용 테이블 하나에만 memtable 을 켠 뒤 다음을 측정한다.

**적용 절차** (기존 배포와 동일):

```bash
# 1. 검증: 반드시 진짜 jar 인지 확인한다 (웹 아티팩트 경로는 로그인 페이지를 HTTP 200 으로 준다)
python3 -c "import zipfile;z=zipfile.ZipFile('build/apache-cassandra-6.0.0.jar');print(len(z.namelist()), z.testzip())"
# 2. drain -> stop.sh -> jar 교체 -> start.sh
# 3. 기동 후 oom_score_adj 가 -1000 인지 확인 (start.sh 의 OOM 보호는 조용히 실패한다)
cat /proc/$(pgrep -f '[o]rg.apache.cassandra.service.CassandraDaemon' | head -1)/oom_score_adj
```

**측정 항목** — 같은 테이블 모양·같은 유입률로 memtable 켜기 전/후를 비교한다:

| 지표 | 측정 방법 | 판정 |
| --- | --- | --- |
| 창당 SSTable | `sstablemetadata` 의 min/max timestamp 가 한 창 안인지 | 전부 한 창 안 |
| `Parking window` 발생 | `grep -c "Parking window" system.log` | 신규 0건 |
| `window-routing buffer` 오버플로 | `grep -c "window-routing buffer" system.log` | 신규 0건 |
| GB 유입당 컴팩션 바이트 | `nodetool compactionhistory` 누적 / 유입 바이트 | 켜기 전 대비 감소 |
| flush 처리량 | `system.log` 의 flush 완료 줄 `rowsPerSec` | 켜기 전 대비 |
| 행당 힙 | `nodetool info` 힙 + `tablestats` 의 memtable cell count | 켜기 전 대비 |
| 데이터 정합성 | 같은 구간을 memtable 켠 테이블과 안 켠 테이블에 동시 적재해 `SELECT` 결과 비교 | 완전 일치 |

**미달이면 미달로 기록한다.** 벤치마크 문서에 기대치를 맞추지 않는다.

- [ ] **Step 5: 문서를 쓰고 커밋한다**

`doc/timeseries/timeseries-memtable.md` 에 설정 방법, 지원/미지원 스키마, 41번 실측값, 알려진 한계를 적는다. `README.md` 의 문서 표에 링크를 추가한다.

```bash
git add test/distributed/ .build/sh/ci-timeseries-tests.sh doc/ README.md
git commit -m "Verify the time-series memtable on three nodes and in production

Records what node 41 actually measured, including anything that fell short."
```

---

## Self-Review

**1. 스펙 커버리지**

| 스펙 절 | 태스크 |
| --- | --- |
| §1 확장 지점 | Task 1 (팩토리), Task 2 (memtable) |
| §2 구조 · 창 배정 기준 | Task 2 |
| §3.1 fast/slow path | **Task 4 이후로 미뤘다** — 원시 배열 저장이 없으면 성립하지 않는 구분이므로, 이 계획에서는 저장 구조를 바꾸지 않는다(아래 참고) |
| §3.2 지연 정렬 | Task 2 (`AtomicBTreePartition` 이 이미 정렬을 보장하므로 이 계획 범위에서는 불필요) |
| §3.3 미지원 스키마 폴백 | Task 1 |
| §3.4 동시성 | Task 2 |
| §4 읽기 경로 | Task 4 |
| §5 flush 경로 | Task 3 |
| §5.1 콜드 창 청크 flush | **이 계획에 없다** (아래 참고) |
| §6 검증 | Task 2·4 (차등·오염 검출), Task 5 (dtest·프로덕션) |
| §7 범위 밖 | 전 태스크의 Global Constraints |

**의도적으로 뺀 것 두 가지 — 별도 계획으로 뗀다:**

- **원시 배열 저장 (스펙 §3.1, §3.3)**: 이 계획은 저장 구조를 `AtomicBTreePartition` 그대로 둔다. 그래야 차등 테스트의 불일치가 **곧 샤딩 결함**을 뜻하고, 저장 형식 변경과 샤딩 변경이 섞이지 않는다. 스펙 §8 의 3단계에 해당하며, 이 계획이 프로덕션에서 검증된 뒤 별도 계획으로 진행한다.
- **콜드 창 청크 flush (스펙 §5.1)**: 내구성 순서 규칙이 걸린 독립 기능이고, 계층화 쪽 불변식을 건드린다. 스펙 §8 의 4단계이며 별도 계획으로 진행한다.

이 계획만으로 얻는 것: **파킹을 내는 원인 제거(64 MiB 상한 소멸), flush 시 라우팅·버퍼링 비용 제거, flush 할당 감소.** 창 경계 분할 자체는 이미 동작하므로 컴팩션 총량은 크게 변하지 않는다 — 그 이득과 메모리 이득은 다음 계획(원시 배열 저장, 콜드 창 청크 flush)의 몫이다.

**2. 플레이스홀더 점검**

Task 2 Step 4 와 Task 3 Step 3, Task 4 Step 3 은 전체 코드 대신 **반드시 지킬 제약**을 명시했다. 구현 분량이 한 화면을 넘고 참고 구현(`ShardedSkipListMemtable`)이 저장소 안에 있어 그대로 베낄 수 있기 때문이다. 각 제약은 위반 시 무엇이 깨지는지까지 적었다.

**3. 타입 일관성**

- `TimeSeriesMemtableFactory.unsupportedReason(TableMetadata)` — Task 1 정의, Task 2 소비
- `WindowShard.windowStart()` — Task 2 정의, Task 3 소비
- `FlyweightRowView.enabled` — Task 4 정의, 같은 태스크의 테스트가 소비
- `WindowRoutingIterator.maxBufferedBytesPerPartition` — 기존 필드(`public static volatile long`), Task 3 의 테스트가 소비
