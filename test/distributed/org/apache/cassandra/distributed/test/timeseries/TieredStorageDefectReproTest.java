/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.distributed.test.timeseries;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Date;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.timeseries.tiering.TieredStorageService;
import org.apache.cassandra.db.timeseries.tiering.TransparentReads;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Reproduction attempts for suspected tiered-storage defects on a 3-node RF=3 cluster. Every test
 * asserts the behaviour a client of a plain Cassandra table would expect, so a failing test is a
 * confirmed defect and a passing one means the suspicion did not reproduce. Tests that were seen to
 * fail carry {@code @Ignore("CONFIRMED BUG: ...")} so the suite stays green; remove the annotation
 * when the defect is fixed.
 * <p>
 * All data uses tiny epoch-millis event times, so every window is closed and cold; re-encode cycles
 * are driven explicitly (never by the 60s sweeper).
 * <p>
 * System properties are used only as a JVM-global flag between the test thread and a re-encoder
 * paused inside an in-JVM instance (each instance has its own classloader, so no static is shared);
 * they are not configuration, hence the checkstyle suppression.
 */
// checkstyle: suppress below 'blockSystemPropertyUsage'
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TieredStorageDefectReproTest extends TestBaseImpl
{
    private static final String POLICY = "{\"hot_window\":\"2h\",\"chunk_window\":\"1h\",\"consistency\":\"QUORUM\"}";
    private static final String TAG = "tag-0";

    /** JVM-global flags (System properties are shared by every in-JVM instance and the test thread). */
    private static final String PAUSED = "aster.repro.reencoder.paused";
    private static final String RELEASE = "aster.repro.reencoder.release";

    private static Cluster CLUSTER;

    @BeforeClass
    public static void setUpCluster() throws IOException
    {
        CLUSTER = init(Cluster.build(3)
                              .withConfig(config -> config.set("hinted_handoff_enabled", false)
                                                          .with(GOSSIP).with(NETWORK))
                              .start(),
                       3);
    }

    @AfterClass
    public static void tearDownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    // ------------------------------------------------------------------------------------------
    // Scenario 1: a sub-QUORUM client write missed by the re-encoder's QUORUM read
    // ------------------------------------------------------------------------------------------

    /**
     * A write acknowledged at CL ONE lands on a single replica X. Rows written after it (so with
     * larger writetimes) reach every replica. The re-encoder on tag's primary reads the window at
     * QUORUM from the two replicas that are not X -- forced here by X being down, which is one
     * possible outcome of replica selection when X is up -- encodes what it saw and range-deletes the
     * window at the maximum writetime it saw, which is larger than the CL ONE row's. Once X is back,
     * that tombstone shadows the row on X and the chunk never contained it.
     */
    @Ignore("CONFIRMED BUG: a CL ONE row the re-encoder's QUORUM read missed is range-deleted at maxWt and never chunked (observed 5 rows, expected 6)")
    @Test
    public void s1_subQuorumWriteSurvivesReencode() throws Exception
    {
        String t = "s1_sub_quorum";
        createTieredTable(t, "value double");
        int primary = primaryFor(t, TAG);
        int lonely = primary == 3 ? 2 : 3;       // the replica that alone holds the CL ONE write
        retierAllNodes(t);                        // no data yet: only creates the shadow tables while all nodes are up

        // CL ONE write coordinated by X, whose mutations to the other replicas are dropped.
        for (int other = 1; other <= 3; other++)
            if (other != lonely)
                CLUSTER.filters().verbs(Verb.MUTATION_REQ.id).from(lonely).to(other).drop();
        CLUSTER.coordinator(lonely).execute(withKeyspace("INSERT INTO %s." + t + " (tag, ts, value) VALUES (?, ?, ?)"),
                                            ConsistencyLevel.ONE, TAG, new Date(30_000L), 42.0);
        CLUSTER.filters().reset();
        Uninterruptibles.sleepUninterruptibly(5, TimeUnit.MILLISECONDS);

        // Later, fully replicated rows in the same window.
        for (int i = 0; i < 5; i++)
            CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s." + t + " (tag, ts, value) VALUES (?, ?, ?)"),
                                           ConsistencyLevel.ALL, TAG, new Date(i * 60_000L), 1.0 + i);

        assertEquals("only X holds the CL ONE row", 6, localBaseRows(lonely, t));
        for (int node = 1; node <= 3; node++)
            if (node != lonely)
                assertEquals(5, localBaseRows(node, t));
        long lateWt = localWritetime(lonely, t, 30_000L);
        long maxOthers = localMaxWritetime(primary, t);
        assertTrue("precondition: the CL ONE row is older than the rows the re-encoder will see", lateWt < maxOthers);

        CLUSTER.get(lonely).flush(KEYSPACE);
        CLUSTER.get(lonely).shutdown().get();
        try
        {
            CLUSTER.get(primary).nodetoolResult("retier", KEYSPACE, t).asserts().success();
        }
        finally
        {
            CLUSTER.get(lonely).startup();
        }

        Object[][] rows = executeWithRetry(primary, "SELECT ts, value FROM %s." + t + " WHERE tag = ?", ConsistencyLevel.ALL, TAG);
        assertEquals("the acknowledged CL ONE write must not be destroyed by the re-encoder's range delete " +
                     "(re-encoder saw 5 rows, X held 6)", 6, rows.length);
        assertEquals(30_000L, ((Date) rows[1][0]).getTime());
        assertEquals(42.0, (Double) rows[1][1], 0.0);
    }

    // ------------------------------------------------------------------------------------------
    // Scenario 2: DROP column then ADD a column with the same name
    // ------------------------------------------------------------------------------------------

    /**
     * In plain Cassandra a column dropped and re-added reads as null for every row written before the
     * drop (the dropped-column timestamp filters the old cells). Chunk columns are resolved by name
     * only, so the pre-drop values come back under the new column.
     */
    @Ignore("CONFIRMED BUG: ChunkReadSupport resolves chunk columns by name with no dropped-time filter, so pre-DROP values reappear under a re-ADDed column (observed 100.0, expected null)")
    @Test
    public void s2_droppedThenReaddedColumnDoesNotResurrectChunkValues()
    {
        String t = "s2_drop_readd";
        createTieredTable(t, "value double, extra double");
        for (int i = 0; i < 3; i++)
            CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s." + t + " (tag, ts, value, extra) VALUES (?, ?, ?, ?)"),
                                           ConsistencyLevel.ALL, TAG, new Date(i * 60_000L), 1.0 + i, 100.0 + i);
        retierAllNodes(t);
        for (int node = 1; node <= 3; node++)
            awaitLocalBaseRows(node, t, 0);

        CLUSTER.schemaChange(withKeyspace("ALTER TABLE %s." + t + " DROP extra"));
        CLUSTER.schemaChange(withKeyspace("ALTER TABLE %s." + t + " ADD extra double"));

        Object[][] rows = CLUSTER.coordinator(1).execute(withKeyspace("SELECT ts, value, extra FROM %s." + t + " WHERE tag = ?"),
                                                         ConsistencyLevel.QUORUM, TAG);
        assertEquals(3, rows.length);
        for (int i = 0; i < 3; i++)
        {
            assertEquals(1.0 + i, (Double) rows[i][1], 0.0);
            assertNull("row " + i + ": a value written before DROP extra must not reappear under the re-added column",
                       rows[i][2]);
        }
    }

    // ------------------------------------------------------------------------------------------
    // Scenario 3: LWT on cold (chunked) rows
    // ------------------------------------------------------------------------------------------

    /** INSERT ... IF NOT EXISTS on a clustering that exists (in a chunk) must not apply. */
    @Ignore("CONFIRMED BUG (documented, tiered-storage.md 5.1.2): CAS read bypasses chunks, IF NOT EXISTS applies over a chunked row (observed applied=true)")
    @Test
    public void s3a_insertIfNotExistsOnChunkedRowIsNotApplied()
    {
        String t = "s3a_lwt_ine";
        createChunkedSingleTag(t);

        Object[][] result = CLUSTER.coordinator(1).execute(
            withKeyspace("INSERT INTO %s." + t + " (tag, ts, value) VALUES (?, ?, ?) IF NOT EXISTS"),
            ConsistencyLevel.QUORUM, TAG, new Date(60_000L), 999.0);
        Object[][] after = CLUSTER.coordinator(1).execute(
            withKeyspace("SELECT value FROM %s." + t + " WHERE tag = ? AND ts = ?"), ConsistencyLevel.QUORUM, TAG, new Date(60_000L));
        assertEquals("IF NOT EXISTS must see the chunked row", false, result[0][0]);
        assertEquals("the existing value must not be overwritten", 2.0, (Double) after[0][0], 0.0);
    }

    /** UPDATE ... IF value = (the value the row holds) must apply when that value lives in a chunk. */
    @Ignore("CONFIRMED BUG (documented, tiered-storage.md 5.1.2): IF col = <chunked value> never matches although SERIAL SELECT returns it (observed applied=false)")
    @Test
    public void s3b_conditionalUpdateMatchesChunkedValue()
    {
        String t = "s3b_lwt_if";
        createChunkedSingleTag(t);

        Object[][] serial = CLUSTER.coordinator(1).execute(
            withKeyspace("SELECT value FROM %s." + t + " WHERE tag = ? AND ts = ?"), ConsistencyLevel.SERIAL, TAG, new Date(60_000L));
        assertEquals("SERIAL read returns the chunked value", 2.0, (Double) serial[0][0], 0.0);

        Object[][] result = CLUSTER.coordinator(1).execute(
            withKeyspace("UPDATE %s." + t + " SET value = ? WHERE tag = ? AND ts = ? IF value = ?"),
            ConsistencyLevel.QUORUM, 555.0, TAG, new Date(60_000L), 2.0);
        assertEquals("IF value = 2.0 must match the value a SERIAL read just returned", true, result[0][0]);
    }

    // ------------------------------------------------------------------------------------------
    // Scenario 5: ADD COLUMN between the re-encoder's schema check and its range delete
    // ------------------------------------------------------------------------------------------

    /**
     * The re-encoder checks {@code regularColumnsChanged} and then writes the chunk and range-deletes
     * the window at maxWt. A column added in that gap, with a value written at a timestamp
     * {@code <= maxWt}, is deleted without ever having been encoded.
     * <p>
     * Observed: lost. But the control {@link #s5b_existingColumnWriteAtMaxWtDuringCycleKeepsItsValue}
     * loses its value the same way with no schema change, so the loss comes from the range delete's
     * timestamp rule (section 5.1's inherent limit), not from anything the schema race adds.
     */
    @Ignore("CONFIRMED BUG: a column ADDed after regularColumnsChanged() with a value at ts <= maxWt is range-deleted unencoded (observed null, expected 7.0); see s5b")
    @Test
    public void s5a_columnAddedAfterSchemaCheckKeepsItsValue() throws Exception
    {
        String t = "s5a_add_race";
        createTieredTable(t, "value double");
        insertThreeRows(t);
        int primary = primaryFor(t, TAG);
        long maxWt = localMaxWritetime(primary, t);

        runPausedBeforeChunkInsert(primary, t, () -> {
            CLUSTER.schemaChange(withKeyspace("ALTER TABLE %s." + t + " ADD extra double"));
            CLUSTER.coordinator(1).execute(withKeyspace("UPDATE %s." + t + " USING TIMESTAMP ? SET extra = ? WHERE tag = ? AND ts = ?"),
                                           ConsistencyLevel.QUORUM, maxWt, 7.0, TAG, new Date(60_000L));
        });

        Object[][] rows = CLUSTER.coordinator(1).execute(withKeyspace("SELECT extra FROM %s." + t + " WHERE tag = ? AND ts = ?"),
                                                         ConsistencyLevel.ALL, TAG, new Date(60_000L));
        assertEquals(1, rows.length);
        assertEquals("the new column's acknowledged value must survive the cycle's range delete", 7.0, rows[0][0]);
    }

    /**
     * Control for s5a: the same write timing on an EXISTING column, no schema change. If this also
     * loses the value, s5a's loss is the documented server-timestamp assumption (tiered-storage.md
     * section 5.1, "inherent limit"), not something the schema race adds.
     */
    @Ignore("CONFIRMED BUG (documented limit, tiered-storage.md 5.1): any write at ts <= maxWt landing after the window read is shadowed, schema change or not (observed 1.0, expected 77.0)")
    @Test
    public void s5b_existingColumnWriteAtMaxWtDuringCycleKeepsItsValue() throws Exception
    {
        String t = "s5b_existing_col";
        createTieredTable(t, "value double");
        insertThreeRows(t);
        int primary = primaryFor(t, TAG);
        long maxWt = localMaxWritetime(primary, t);

        runPausedBeforeChunkInsert(primary, t, () ->
            CLUSTER.coordinator(1).execute(withKeyspace("UPDATE %s." + t + " USING TIMESTAMP ? SET value = ? WHERE tag = ? AND ts = ?"),
                                           ConsistencyLevel.QUORUM, maxWt, 77.0, TAG, new Date(0L)));

        Object[][] rows = CLUSTER.coordinator(1).execute(withKeyspace("SELECT value FROM %s." + t + " WHERE tag = ? AND ts = ?"),
                                                         ConsistencyLevel.ALL, TAG, new Date(0L));
        assertEquals(1, rows.length);
        assertEquals("a write acknowledged during the cycle must survive its range delete", 77.0, rows[0][0]);
    }

    // ------------------------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------------------------

    private static void createTieredTable(String name, String regularColumns)
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s." + name + " (tag text, ts timestamp, " + regularColumns + ", " +
                                          "PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'} " +
                                          "AND read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("ALTER TABLE %s." + name + " WITH extensions = {'timeseries_tiering': '" + POLICY + "'}"));
    }

    private static void insertThreeRows(String t)
    {
        for (int i = 0; i < 3; i++)
            CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s." + t + " (tag, ts, value) VALUES (?, ?, ?)"),
                                           ConsistencyLevel.ALL, TAG, new Date(i * 60_000L), 1.0 + i);
    }

    /** One tag, three rows (values 1.0, 2.0, 3.0 at 0, 60s, 120s), encoded into a chunk on every replica. */
    private static void createChunkedSingleTag(String t)
    {
        createTieredTable(t, "value double");
        insertThreeRows(t);
        retierAllNodes(t);
        for (int node = 1; node <= 3; node++)
            awaitLocalBaseRows(node, t, 0);
    }

    private static void retierAllNodes(String name)
    {
        for (int node = 1; node <= 3; node++)
            CLUSTER.get(node).nodetoolResult("retier", KEYSPACE, name).asserts().success();
    }

    /**
     * Runs one re-encode cycle of {@code t} on {@code node} that pauses right before its chunk write
     * (after the schema check), runs {@code duringPause} on the test thread, then lets the cycle
     * finish -- the chunk write and the range delete at maxWt.
     */
    private static void runPausedBeforeChunkInsert(int node, String t, Runnable duringPause) throws Exception
    {
        System.clearProperty(PAUSED);
        System.clearProperty(RELEASE);
        setBeforeChunkInsertHook(node, true);
        String keyspace = KEYSPACE;
        try
        {
            Future<?> cycle = CLUSTER.get(node).asyncRunsOnInstance(() -> {
                TieredStorageService.instance.runOnce(keyspace, t, System.currentTimeMillis());
            }).call();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
            while (System.getProperty(PAUSED) == null)
            {
                if (cycle.isDone())
                {
                    cycle.get();
                    fail("the cycle finished without reaching the chunk write -- the test proves nothing");
                }
                if (System.nanoTime() > deadline)
                    fail("the cycle never reached the chunk write");
                Uninterruptibles.sleepUninterruptibly(20, TimeUnit.MILLISECONDS);
            }
            try
            {
                duringPause.run();
            }
            finally
            {
                System.setProperty(RELEASE, "true");
            }
            cycle.get(120, TimeUnit.SECONDS);
        }
        finally
        {
            System.setProperty(RELEASE, "true");
            setBeforeChunkInsertHook(node, false);
            System.clearProperty(PAUSED);
            System.clearProperty(RELEASE);
        }
    }

    /** Installs (or clears) a {@code beforeChunkInsertForTesting} hook that blocks until {@link #RELEASE} is set. */
    private static void setBeforeChunkInsertHook(int node, boolean install)
    {
        CLUSTER.get(node).runOnInstance(() -> {
            try
            {
                Field field = TieredStorageService.class.getDeclaredField("beforeChunkInsertForTesting");
                field.setAccessible(true);
                Consumer<TableMetadata> hook = null;
                if (install)
                {
                    hook = metadata -> {
                        System.setProperty(PAUSED, "true");
                        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
                        while (System.getProperty(RELEASE) == null && System.nanoTime() < deadline)
                            Uninterruptibles.sleepUninterruptibly(20, TimeUnit.MILLISECONDS);
                    };
                }
                field.set(null, hook);
            }
            catch (ReflectiveOperationException e)
            {
                throw new RuntimeException(e);
            }
        });
    }

    /** @return the node holding {@code tag}'s primary range. */
    private static int primaryFor(String table, String tag)
    {
        String keyspace = KEYSPACE;
        for (int node = 1; node <= 3; node++)
        {
            boolean primary = CLUSTER.get(node).callOnInstance(() -> {
                TableMetadata metadata = Schema.instance.getTableMetadata(keyspace, table);
                Token token = metadata.partitioner.getToken(UTF8Type.instance.decompose(tag));
                for (Range<Token> range : StorageService.instance.getPrimaryRanges(keyspace))
                    if (range.contains(token))
                        return true;
                return false;
            });
            if (primary)
                return node;
        }
        throw new AssertionError("no node owns " + tag + "'s primary range");
    }

    /** Rows {@code node} physically holds for {@link #TAG} (the storage engine's view, not the merged one). */
    private static int localBaseRows(int node, String table)
    {
        String cql = withKeyspace("SELECT ts FROM %s." + table + " WHERE tag = '" + TAG + "'");
        return CLUSTER.get(node).callOnInstance(() -> {
            TransparentReads.enterInternalBypass();
            try
            {
                return QueryProcessor.executeInternal(cql).size();
            }
            finally
            {
                TransparentReads.exitInternalBypass();
            }
        });
    }

    /** The maximum WRITETIME(value) {@code node} physically holds for {@link #TAG}. */
    private static long localMaxWritetime(int node, String table)
    {
        String cql = withKeyspace("SELECT WRITETIME(value) AS wt FROM %s." + table + " WHERE tag = '" + TAG + "'");
        return CLUSTER.get(node).callOnInstance(() -> {
            TransparentReads.enterInternalBypass();
            try
            {
                long max = Long.MIN_VALUE;
                for (UntypedResultSet.Row row : QueryProcessor.executeInternal(cql))
                    max = Math.max(max, row.getLong("wt"));
                return max;
            }
            finally
            {
                TransparentReads.exitInternalBypass();
            }
        });
    }

    /** WRITETIME(value) of the row at {@code tsMillis} as {@code node} physically holds it. */
    private static long localWritetime(int node, String table, long tsMillis)
    {
        String cql = withKeyspace("SELECT WRITETIME(value) AS wt FROM %s." + table + " WHERE tag = '" + TAG + "' AND ts = " + tsMillis);
        return CLUSTER.get(node).callOnInstance(() -> {
            TransparentReads.enterInternalBypass();
            try
            {
                return QueryProcessor.executeInternal(cql).one().getLong("wt");
            }
            finally
            {
                TransparentReads.exitInternalBypass();
            }
        });
    }

    private static void awaitLocalBaseRows(int node, String table, int expected)
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        int actual;
        do
        {
            actual = localBaseRows(node, table);
            if (actual == expected)
                return;
            Uninterruptibles.sleepUninterruptibly(50, TimeUnit.MILLISECONDS);
        } while (System.nanoTime() < deadline);
        assertEquals("node " + node + " base rows", expected, actual);
    }

    /** Retries a read while a just-restarted node is not yet seen as alive (Unavailable). */
    private static Object[][] executeWithRetry(int coordinator, String cql, ConsistencyLevel cl, Object... values)
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (true)
        {
            try
            {
                return CLUSTER.coordinator(coordinator).execute(withKeyspace(cql), cl, values);
            }
            catch (RuntimeException e)
            {
                if (System.nanoTime() > deadline)
                    throw e;
                Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
            }
        }
    }
}
