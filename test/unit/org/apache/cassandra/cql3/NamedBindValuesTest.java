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

package org.apache.cassandra.cql3;

import java.util.LinkedHashMap;
import java.util.Map;

import com.datastax.driver.core.SimpleStatement;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.transport.ProtocolVersion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Named values on an unprepared statement (protocol flag NAMES_FOR_VALUES) must bind by name, not by the order the
 * client sent them in. {@code QueryOptions.OptionsWithNames} reorders only {@code getValues()}; upstream 8a72868066
 * added {@code getValue(int)} and the {@code byte[]} fast path that {@code Marker} reads, and the wrapper passed both
 * through to the unordered values -- so {@code UPDATE ... SET holder = :holder WHERE owner = :owner} sent with owner
 * first wrote a new row keyed by the holder value. Reported by the edge session against v6.0.0 and v6.0.0-r6;
 * 4.1 and 5.0 bind by name.
 */
public class NamedBindValuesTest extends CQLTester
{
    @BeforeClass
    public static void setUp()
    {
        requireNetwork();
    }

    private static Map<String, Object> ordered(Object... nameValue)
    {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < nameValue.length; i += 2)
            m.put((String) nameValue[i], nameValue[i + 1]);
        return m;
    }

    @Test
    public void namedValuesBindByNameNotBySendOrder() throws Throwable
    {
        createTable("CREATE TABLE %s (owner text PRIMARY KEY, holder text, n int)");
        for (ProtocolVersion version : ProtocolVersion.SUPPORTED)
        {
            if (version.isSmallerThan(ProtocolVersion.V3))
                continue;
            execute("TRUNCATE %s");
            execute("INSERT INTO %s (owner, holder, n) VALUES ('o', 'h1', 1)");

            // Sent in the reverse of marker order: owner first, holder second.
            executeNet(version, new SimpleStatement(formatQuery("UPDATE %s SET holder = :holder WHERE owner = :owner"),
                                                    ordered("owner", "o", "holder", "h2")));
            assertRows(execute("SELECT owner, holder FROM %s"), row("o", "h2"));

            // Different types, so a positional bind fails validation instead of writing a wrong row.
            executeNet(version, new SimpleStatement(formatQuery("UPDATE %s SET n = :n, holder = :holder WHERE owner = :owner"),
                                                    ordered("owner", "o", "holder", "h3", "n", 7)));
            assertRows(execute("SELECT owner, holder, n FROM %s"), row("o", "h3", 7));

            // LWT: the condition values go through the same markers.
            assertTrue(executeNet(version, new SimpleStatement(formatQuery("UPDATE %s SET n = :next WHERE owner = :owner IF n = :expected"),
                                                               ordered("expected", 7, "owner", "o", "next", 8))).wasApplied());
            assertRows(execute("SELECT owner, holder, n FROM %s"), row("o", "h3", 8));

            // Reads bind the same way.
            assertEquals(8, executeNet(version, new SimpleStatement(formatQuery("SELECT n FROM %s WHERE owner = :owner AND n = :n ALLOW FILTERING"),
                                                                    ordered("n", 8, "owner", "o"))).one().getInt("n"));

            assertRows(execute("SELECT count(*) FROM %s"), row(1L));
        }
    }
}
