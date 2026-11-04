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

package org.apache.cassandra.db.timeseries.tiering;

import java.util.Date;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * {@code nodetool untier} moves every chunk back into the base table -- visible as ordinary rows, with the newest value
 * of anything written since chunking kept -- detaches the policy and drops the shadow tables, after which nothing of
 * shadow-table tiering is left on the table.
 */
public class UntierTest extends CQLTester
{
    private static final long HOUR = 3_600_000L;

    private void setPolicy(String json) throws Throwable
    {
        String hex = ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(json));
        alterTable("ALTER TABLE %s WITH extensions = {'" + TieringPolicy.EXTENSION_KEY + "': 0x" + hex + "};");
    }

    private UntypedResultSet raw(String query, Object... values) throws Throwable
    {
        TransparentReads.enterInternalBypass();
        try
        {
            return execute(query, values);
        }
        finally
        {
            TransparentReads.exitInternalBypass();
        }
    }

    @Test
    public void chunksBecomeRowsAgainAndTheShadowTablesGo() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, v double, n int, PRIMARY KEY (tag, ts)) WITH compaction = {'class': 'TimeSeriesCompactionStrategy'}");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        for (int i = 0; i < 120; i++)
            execute("INSERT INTO %s (tag, ts, v, n) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                    "t" + (i % 2), new Date(i * 60_000L), (double) i, i % 3 == 0 ? null : i, 100L + i);
        UntypedResultSet before = execute("SELECT tag, ts, v, n FROM %s");
        assertEquals(120, before.size());

        assertTrue(new TieredStorageService().runOnce(KEYSPACE, currentTable(), 10 * HOUR).windowsEncoded > 0);
        assertEquals("the re-encoder deleted the base rows", 0, raw("SELECT * FROM %s").size());

        // Written after chunking, newer than the chunk: must survive the restore.
        execute("UPDATE %s SET v = 999.0 WHERE tag = 't0' AND ts = ?", new Date(0L));

        Untier.Result result = Untier.run(KEYSPACE, currentTable(), 1_000_000);
        assertEquals(120, result.rowsRestored);
        assertTrue(result.shadowTablesDropped);

        assertNull(Schema.instance.getTableMetadata(KEYSPACE, ChunkTables.chunkTableName(currentTable())));
        assertNull(Schema.instance.getTableMetadata(KEYSPACE, ChunkTables.coverageTableName(currentTable())));
        assertNull(Schema.instance.getTableMetadata(KEYSPACE, ChunkTables.tagsTableName(currentTable())));
        assertFalse("the policy is detached",
                    getCurrentColumnFamilyStore().metadata().params.extensions.containsKey(TieringPolicy.EXTENSION_KEY));

        UntypedResultSet after = raw("SELECT tag, ts, v, n FROM %s");
        assertEquals("every row is a base row again", 120, after.size());
        java.util.Iterator<UntypedResultSet.Row> b = before.iterator(), a = after.iterator();
        while (b.hasNext())
        {
            UntypedResultSet.Row br = b.next(), ar = a.next();
            assertEquals(br.getString("tag"), ar.getString("tag"));
            assertEquals(br.getTimestamp("ts"), ar.getTimestamp("ts"));
            boolean overwritten = ar.getString("tag").equals("t0") && ar.getTimestamp("ts").getTime() == 0L;
            assertEquals(overwritten ? 999.0 : br.getDouble("v"), ar.getDouble("v"), 0.0);
            assertEquals(br.has("n"), ar.has("n"));
            if (br.has("n"))
                assertEquals(br.getInt("n"), ar.getInt("n"));
        }

        // Ordinary deletes of formerly cold rows work again: nothing guards them any more.
        execute("DELETE FROM %s WHERE tag = 't1' AND ts = ?", new Date(60_000L));
        assertEquals(119, execute("SELECT * FROM %s").size());
    }
}
