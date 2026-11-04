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
package org.apache.cassandra.tools.nodetool;

import org.apache.cassandra.tools.NodeProbe;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "untier", description = "Move a table's tiered-storage chunks back into the table and drop its shadow tables " +
                                        "(one-way exit from shadow-table tiering; detaches the tiering policy first)")
public class Untier extends AbstractCommand
{
    @Parameters(index = "0", arity = "1", description = "The keyspace of the table")
    private String keyspace;

    @Parameters(index = "1", arity = "1", description = "The table")
    private String table;

    @Option(names = { "-r", "--rows-per-second" }, description = "Restore rate limit in rows per second (default 50000)")
    private int rowsPerSecond = 50_000;

    @Override
    public void execute(NodeProbe probe)
    {
        probe.output().out.println(probe.untier(keyspace, table, rowsPerSecond));
    }
}
