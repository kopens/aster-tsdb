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
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.timeseries.tiering.TransparentReads;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Multi-node validation of tiered storage (SP2/SP3) on a 3-node RF=3 cluster — the gap left by the
 * single-node unit and docker coverage. Targets the risks that only exist with real replication:
 * per-node primary-range partitioning of the re-encoder, coordinator-independent transparent reads,
 * quorum-path correctness of the range delete, idempotency when several nodes sweep the same table,
 * and survival of a node restart.
 *
 * All data uses small epoch-millis event times (far in the past), so every window is closed and
 * cold; re-encode cycles are driven explicitly with {@code nodetool retier} rather than the 60s
 * sweeper, keeping the test deterministic.
 * <p>
 * Three defects had to be fixed before this class could run meaningfully, and it is what holds all
 * three in place:
 * <ul>
 *   <li>the null-MBean-proxy failure of {@code nodetool retier} under jvm-dtest -- in-JVM instances
 *   never run {@code CassandraDaemon.setup()}, so {@code NodeProbe.tieredStorageProxy} was never
 *   assigned. Fixed where every other dtest proxy is assigned,
 *   {@code InternalNodeProbe.connect()}, rather than by making the test call
 *   {@code TieredStorageService.setup()} (which would also start the 60s sweeper and cost this
 *   class its determinism);</li>
 *   <li>the re-encoder's chunk-table creation, which could not propagate on a real cluster at all.
 *   See {@link ChunkTableSchemaPropagationTest} for the two-node reduction and the mechanism;</li>
 *   <li>the <em>timing</em> of that propagation, which {@link #everyTagIsEncodedExactlyOnceAcrossNodes}
 *   is the only thing that catches: committing the chunk table's DDL makes it visible on the
 *   creating node, not on its replicas, so the reads the very next line of the cycle issues at the
 *   policy's consistency level could hit a replica that was still behind. That replica answers
 *   {@code INCOMPATIBLE_SCHEMA}, the coordinator raises {@code ReadFailureException}, and the
 *   re-encoder's per-tag error handler skips the tag for the whole cycle -- silently losing an
 *   arbitrary, run-to-run varying subset of tags (24 expected, 16-23 observed). Fixed by giving
 *   {@code ChunkTables.ensureChunkTable} a cluster-wide postcondition.</li>
 * </ul>
 * The coverage SP4 Task 5 asks for on top of these five tests -- the tm_tag_point shape, a
 * three-column partition key, cold immutability on every coordinator, DESC clustering end to end --
 * is deliberately <b>not</b> written yet.
 */
public class TieredStorageDistributedTest extends TestBaseImpl
{
    private static final int TAGS = 24;                 // enough tags to land in every node's ranges
    private static Cluster CLUSTER;

    @BeforeClass
    public static void setUpCluster() throws IOException
    {
        CLUSTER = init(Cluster.build(3)
                              .withConfig(config -> config.set("hinted_handoff_enabled", false)
                                                          // the time-series memtable, for the cold-window flush test
                                                          .set("memtable", Map.of("configurations", Map.of(
                                                              "timeseries", Map.of("class_name", "TimeSeriesMemtable"))))
                                                          .with(GOSSIP).with(NETWORK))
                              .start(),
                       3);   // RF = 3
    }

    @AfterClass
    public static void tearDownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    /** The policy every table here installs: 2h hot window, 1h chunks, QUORUM. */
    private static final String POLICY = "{\"hot_window\":\"2h\",\"chunk_window\":\"1h\",\"consistency\":\"QUORUM\"}";

    /**
     * Installs {@code json} as {@code table}'s tiering policy. Extension values are plain strings --
     * only a {@code 0x} prefix selects hex -- so the JSON goes in verbatim, with its double quotes
     * escaped for the enclosing CQL single-quoted literal (there are none to escape in practice, but
     * a single quote inside the JSON would need doubling).
     */
    private static void setPolicy(String table, String json)
    {
        CLUSTER.schemaChange(withKeyspace("ALTER TABLE %s." + table + " WITH extensions = " +
                                          "{'timeseries_tiering': '" + json.replace("'", "''") + "'}"));
    }

    /** Creates the canonical table {@code name} and installs a tiering policy that makes all data cold. */
    private static void createTieredTable(String name)
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s." + name + " (tag text, ts timestamp, value double, " +
                                          "PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'} AND read_repair = 'NONE'"));
        setPolicy(name, POLICY);
    }

    /** Writes {@code perTag} samples per tag at QUORUM, all inside closed (cold) windows. */
    private static void insertColdData(String name, int perTag, double base)
    {
        for (int t = 0; t < TAGS; t++)
            for (int i = 0; i < perTag; i++)
                CLUSTER.coordinator(1 + (t % 3)).execute(
                    withKeyspace("INSERT INTO %s." + name + " (tag, ts, value) VALUES (?, ?, ?)"),
                    ConsistencyLevel.QUORUM, "tag-" + t, new Date(i * 60_000L), base + i);
    }

    /** Runs one re-encode cycle on every live node — each processes only the tags it owns. */
    private static void retierAllNodes(String name)
    {
        for (int node = 1; node <= 3; node++)
            CLUSTER.get(node).nodetoolResult("retier", KEYSPACE, name).asserts().success();
    }

    private static long countAt(int coordinator, String cql, ConsistencyLevel cl, Object... values)
    {
        Object[][] rows = CLUSTER.coordinator(coordinator).execute(withKeyspace(cql), cl, values);
        return (Long) rows[0][0];
    }

    @Test
    public void everyTagIsEncodedExactlyOnceAcrossNodes()
    {
        String t = "encode_once";
        createTieredTable(t);
        insertColdData(t, 5, 1.0);
        retierAllNodes(t);

        // Primary-range partitioning must cover every tag (no gaps) without double-encoding: with
        // chunk_window=1h and samples inside one window, exactly one chunk row per tag must exist.
        assertEquals("every tag must have exactly one chunk row",
                     TAGS, countAt(1, "SELECT count(*) FROM %s." + t + "__chunks", ConsistencyLevel.QUORUM));

        for (int tag = 0; tag < TAGS; tag++)
            assertEquals("tag-" + tag + " must be encoded once",
                         1L, countAt(1, "SELECT count(*) FROM %s." + t + "__chunks WHERE tag = ?",
                                     ConsistencyLevel.QUORUM, "tag-" + tag));

        // A second full round is a no-op: the re-encoder is idempotent even when 3 nodes sweep the
        // same table with only node-local overlap gates.
        retierAllNodes(t);
        assertEquals(TAGS, countAt(1, "SELECT count(*) FROM %s." + t + "__chunks", ConsistencyLevel.QUORUM));
    }

    @Test
    public void transparentReadIsCoordinatorIndependent()
    {
        String t = "coord_independent";
        createTieredTable(t);
        insertColdData(t, 5, 10.0);
        retierAllNodes(t);

        // The chunk for a tag is written by whichever node owns it, then replicated. Reading through
        // ANY coordinator, at any quorum-or-stronger CL, must return the same merged view.
        for (int coordinator = 1; coordinator <= 3; coordinator++)
        {
            for (ConsistencyLevel cl : new ConsistencyLevel[]{ ConsistencyLevel.QUORUM, ConsistencyLevel.ALL })
            {
                assertEquals("coordinator " + coordinator + " at " + cl,
                             5L, countAt(coordinator, "SELECT count(*) FROM %s." + t + " WHERE tag = ?", cl, "tag-0"));
                Object[][] rows = CLUSTER.coordinator(coordinator).execute(
                    withKeyspace("SELECT value FROM %s." + t + " WHERE tag = ? AND ts = ?"), cl, "tag-0", new Date(120_000L));
                assertEquals("coordinator " + coordinator + " at " + cl, 12.0, (Double) rows[0][0], 0.0);
            }
        }
    }

    @Test
    public void lateRowSurvivesTheDistributedRangeDelete()
    {
        String t = "late_row";
        createTieredTable(t);
        insertColdData(t, 5, 100.0);
        retierAllNodes(t);
        assertEquals(5L, countAt(1, "SELECT count(*) FROM %s." + t + " WHERE tag = ?", ConsistencyLevel.QUORUM, "tag-0"));

        // A correction arriving AFTER the window was encoded must outlive the range tombstone the
        // re-encode issued (USING TIMESTAMP maxWt) - the invariant the whole design rests on.
        CLUSTER.coordinator(2).execute(withKeyspace("INSERT INTO %s." + t + " (tag, ts, value) VALUES (?, ?, ?)"),
                                       ConsistencyLevel.QUORUM, "tag-0", new Date(120_000L), 999.0);
        Object[][] corrected = CLUSTER.coordinator(3).execute(
            withKeyspace("SELECT value FROM %s." + t + " WHERE tag = ? AND ts = ?"), ConsistencyLevel.QUORUM, "tag-0", new Date(120_000L));
        assertEquals("late correction must win over the chunk", 999.0, (Double) corrected[0][0], 0.0);

        // ... and be absorbed by the next cycle without changing what readers see.
        retierAllNodes(t);
        corrected = CLUSTER.coordinator(1).execute(
            withKeyspace("SELECT value FROM %s." + t + " WHERE tag = ? AND ts = ?"), ConsistencyLevel.QUORUM, "tag-0", new Date(120_000L));
        assertEquals("merged correction must persist through re-encoding", 999.0, (Double) corrected[0][0], 0.0);
        assertEquals(5L, countAt(1, "SELECT count(*) FROM %s." + t + " WHERE tag = ?", ConsistencyLevel.QUORUM, "tag-0"));
    }

    @Test
    public void dataSurvivesNodeRestart() throws Exception
    {
        String t = "restart";
        createTieredTable(t);
        insertColdData(t, 4, 50.0);
        retierAllNodes(t);
        CLUSTER.forEach(node -> node.flush(KEYSPACE));

        CLUSTER.get(3).shutdown().get();
        // With one replica down, QUORUM (2/3) still serves the merged hot+cold view.
        assertEquals("merged read must survive a replica being down",
                     4L, countAt(1, "SELECT count(*) FROM %s." + t + " WHERE tag = ?", ConsistencyLevel.QUORUM, "tag-1"));

        CLUSTER.get(3).startup();
        assertEquals("restarted node must serve the same merged view",
                     4L, countAt(3, "SELECT count(*) FROM %s." + t + " WHERE tag = ?", ConsistencyLevel.QUORUM, "tag-1"));
        assertTrue("chunks must survive the restart",
                   countAt(3, "SELECT count(*) FROM %s." + t + "__chunks", ConsistencyLevel.QUORUM) > 0);
    }

    /**
     * The one read path the other tests here cannot reach: a <b>digest mismatch</b>.
     * <p>
     * When every replica agrees, a coordinator read is served by {@code DigestResolver#getData}. When
     * they do not -- a replica that missed the re-encoder's range delete because it was restarting,
     * dropped the mutation, or is still holding a hint -- the coordinator re-reads full data and
     * resolves it through {@code DataResolver}, a completely different method. If the chunk merge is
     * not on that path too, a {@code SELECT} over a chunked window returns hot rows only: no warning,
     * no error, all cold history silently missing, and correct again as soon as the digests converge.
     * The base rows for that history were deleted when it was encoded, so there is nothing else to
     * serve it. Every test above passes with the merge missing from this path, because none of them
     * ever makes the replicas disagree.
     * <p>
     * The second assertion is the other half, and it is about <em>where</em> in {@code DataResolver}
     * the merge sits: after the replica merge, so read repair's merge listener never sees a synthetic
     * chunk row. A row present in the merged result and absent from every replica is exactly what read
     * repair writes back -- so a merge placed upstream of the listener would repair the whole decoded
     * chunk into the base table as real rows, undoing the compression and resurrecting rows the
     * re-encoder deliberately deleted. That is invisible to a client (the answer is still right) and
     * permanent, so it is asserted on the replicas' own storage rather than on the query result.
     */
    @Test
    public void digestMismatchStillMergesChunksWithoutRepairingThemBack()
    {
        String t = "digest_mismatch";
        // Default read_repair (BLOCKING) -- unlike the other tables here, the repair path IS the test.
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s." + t + " (tag text, ts timestamp, value double, " +
                                          "PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}"));
        setPolicy(t, POLICY);
        for (int i = 0; i < 5; i++)
            CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s." + t + " (tag, ts, value) VALUES (?, ?, ?)"),
                                           ConsistencyLevel.ALL, "tag-0", new Date(i * 60_000L), 1.0 + i);
        retierAllNodes(t);

        // The chunk is now the only copy: no replica has a base row left for tag-0.
        for (int node = 1; node <= 3; node++)
            awaitLocalBaseRows(node, t, 0);

        // Diverge exactly one replica, locally, without going through the coordinator -- the same
        // shape a dropped mutation or a hint replayed after the delete leaves behind.
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s." + t + " (tag, ts, value) VALUES (?, ?, ?)"),
                                       "tag-0", new Date(30_000L), 42.0);
        assertEquals(1, localBaseRows(2, t));

        // ALL contacts every replica, so the digests cannot agree and the read must go through
        // DataResolver rather than DigestResolver.
        Object[][] rows = CLUSTER.coordinator(1).execute(
            withKeyspace("SELECT ts, value FROM %s." + t + " WHERE tag = ?"), ConsistencyLevel.ALL, "tag-0");
        assertEquals("a digest mismatch must not cost the read its cold half", 6, rows.length);
        double[] expected = { 1.0, 42.0, 2.0, 3.0, 4.0, 5.0 };
        long[] expectedTs = { 0L, 30_000L, 60_000L, 120_000L, 180_000L, 240_000L };
        for (int i = 0; i < expected.length; i++)
        {
            assertEquals("row " + i + " timestamp", expectedTs[i], ((Date) rows[i][0]).getTime());
            assertEquals("row " + i + " value", expected[i], (Double) rows[i][1], 0.0);
        }

        // Read repair has run once the divergent row has reached the replicas that lacked it...
        for (int node = 1; node <= 3; node++)
            awaitLocalBaseRows(node, t, 1);
        // ...and it repaired that row and NOTHING ELSE. Six here would mean the decoded chunk was
        // written back into the base table as real rows.
        for (int node = 1; node <= 3; node++)
            assertEquals("node " + node + " must hold only the repaired row, not the decoded chunk",
                         1, localBaseRows(node, t));
    }

    /**
     * The cold-window flush and the re-encoder both read-modify-write chunk rows, with no
     * coordination between nodes. The re-encoder only ever works a tag on the node that owns its
     * primary range, but the flush encoded on whichever replica flushed first: two nodes could then
     * each read the same old chunk and write their own merge of it, and a timestamp tie is resolved
     * cell by cell -- so a row one replica had and the other lacked could vanish with its base copy
     * already gone. One writer per tag closes that: only the primary replica may chunk at flush;
     * the others flush their rows, which the re-encoder (on the primary) folds in and deletes.
     */
    @Test
    public void onlyThePrimaryReplicaChunksAColdWindowAtFlush()
    {
        String t = "cold_flush_owner";
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s." + t + " (tag text, ts timestamp, value double, " +
                                          "PRIMARY KEY (tag, ts)) WITH read_repair = 'NONE' AND memtable = 'timeseries' " +
                                          "AND compaction = {'class':'TimeSeriesCompactionStrategy','window_size':'1h','freeze_after':'2h'}"));
        setPolicy(t, POLICY);
        retierAllNodes(t); // no data yet: this only creates the shadow tables, which the flush path never does

        for (int i = 0; i < 5; i++)
            CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s." + t + " (tag, ts, value) VALUES (?, ?, ?)"),
                                           ConsistencyLevel.ALL, "tag-0", new Date(i * 60_000L), 1.0 + i);

        int primary = -1;
        for (int node = 1; node <= 3; node++)
            if (isPrimaryFor(node, t, "tag-0"))
                primary = node;
        assertTrue("some node must own tag-0's primary range", primary > 0);

        // The replicas that do NOT own the tag flush first -- before this fix the first flusher won.
        for (int node = 1; node <= 3; node++)
        {
            if (node == primary)
                continue;
            CLUSTER.get(node).flush(KEYSPACE);
            assertEquals("node " + node + " is not tag-0's primary and must flush its rows, not chunk them",
                         5, localBaseRows(node, t));
        }
        assertEquals("no chunk may exist before the primary has flushed",
                     0L, countAt(1, "SELECT count(*) FROM %s." + t + "__chunks WHERE tag = ?", ConsistencyLevel.ALL, "tag-0"));

        CLUSTER.get(primary).flush(KEYSPACE);
        assertEquals("the primary chunks the cold window and omits its rows", 0, localBaseRows(primary, t));
        assertEquals(1L, countAt(1, "SELECT count(*) FROM %s." + t + "__chunks WHERE tag = ?", ConsistencyLevel.ALL, "tag-0"));
        assertEquals(5L, countAt(1, "SELECT count(*) FROM %s." + t + " WHERE tag = ?", ConsistencyLevel.ALL, "tag-0"));
    }

    /** @return whether {@code node} holds the primary range of {@code tag}'s token in {@code table}. */
    private static boolean isPrimaryFor(int node, String table, String tag)
    {
        String keyspace = KEYSPACE;
        return CLUSTER.get(node).callOnInstance(() -> {
            TableMetadata metadata = Schema.instance.getTableMetadata(keyspace, table);
            Token token = metadata.partitioner.getToken(UTF8Type.instance.decompose(tag));
            for (Range<Token> range : StorageService.instance.getPrimaryRanges(keyspace))
                if (range.contains(token))
                    return true;
            return false;
        });
    }

    /**
     * @return how many rows {@code node} physically holds for {@code tag-0} in {@code table}'s base
     * table. Bracketed with tiering's internal bypass, so this is the storage engine's answer and not
     * the merged hot+cold view every other read in this class asserts on.
     */
    private static int localBaseRows(int node, String table)
    {
        String cql = withKeyspace("SELECT ts FROM %s." + table + " WHERE tag = 'tag-0'");
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

    /** Waits (briefly) for {@code node} to physically hold exactly {@code expected} base rows. */
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

    @Test
    public void allSamplesReadableAfterEncoding()
    {
        String t = "full_fidelity";
        createTieredTable(t);
        insertColdData(t, 8, 7.0);
        retierAllNodes(t);

        // Whole-dataset fidelity: every tag, every sample, exact values - through a coordinator that
        // is not necessarily the encoder.
        for (int tag = 0; tag < TAGS; tag++)
        {
            int coordinator = 1 + (tag % 3);
            Object[][] rows = CLUSTER.coordinator(coordinator).execute(
                withKeyspace("SELECT ts, value FROM %s." + t + " WHERE tag = ?"), ConsistencyLevel.QUORUM, "tag-" + tag);
            assertEquals("tag-" + tag + " sample count", 8, rows.length);
            for (int i = 0; i < 8; i++)
                assertEquals("tag-" + tag + " sample " + i, 7.0 + i, (Double) rows[i][1], 0.0);
        }
    }
}
