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

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;

/**
 * {@code TRUNCATE} of a tiered base table cascades to its shadow tables.
 * <p>
 * Tiering deletes a window's base rows as soon as it has encoded them, so the chunk table holds the
 * only copy of a tiered table's history, and transparent reads merge it back into every
 * {@code SELECT}. Truncating the base table alone therefore changed nothing a client could see:
 * the history was still served, from the chunks. A truncate means "this table is empty", so the
 * chunk table, the coverage ledger and the tag registry are truncated with it -- in that order,
 * after the base table (see {@link #shadowTablesOf}).
 * <p>
 * The shadow tables are the base table's own data in another shape, so the cascade is authorized
 * by the caller's permission on the base table; it does not ask for permissions on tables the
 * caller never named. It applies whether or not a policy is currently attached: detaching the
 * policy leaves the chunks readable, and they are just as much the table's data then.
 */
public final class TieredTruncation
{
    private static final Logger logger = LoggerFactory.getLogger(TieredTruncation.class);

    private static final String[] SHADOW_SUFFIXES = {
        ChunkTables.chunkTableName(""), ChunkTables.coverageTableName(""), ChunkTables.tagsTableName("")
    };

    private TieredTruncation()
    {
    }

    /**
     * @return the shadow tables a {@code TRUNCATE} of {@code keyspace.table} must also truncate, in
     * the order it must truncate them: the chunk table before the coverage ledger, so that the
     * ledger is never narrower than the chunks it describes (a narrower ledger lets the read path
     * skip chunks that still exist), then the tag registry. Empty for every table that has never
     * been tiered.
     * <p>
     * The base table itself goes first, before any of these. Truncating a shadow table first would
     * leave a moment in which a re-encode cycle could chunk base rows that are about to be
     * truncated into a chunk table that has already been; the re-encoder in turn stops a cycle once
     * it sees the base table truncated (see {@code TieredStorageService}).
     */
    public static List<String> shadowTablesOf(String keyspace, String table)
    {
        if (SchemaConstants.isSystemKeyspace(keyspace))
            return Collections.emptyList();
        KeyspaceMetadata metadata = Schema.instance.getKeyspaceMetadata(keyspace);
        if (metadata == null)
            return Collections.emptyList();
        return ChunkTables.existingShadowTables(metadata, table);
    }

    /**
     * Called on every node, after it has truncated {@code truncated} locally, to drop what this
     * node caches about the data that is now gone: the chunk coverage (so neither the read path nor
     * the cold-write guard keeps acting on chunks that no longer exist) and the tags it has already
     * registered (so the first write to a tag after the registry was emptied registers it again).
     * <p>
     * Runs for a base table and for each of its shadow tables alike, because a TRUNCATE reaches the
     * nodes as one truncate per table and each one leaves a different cache stale.
     */
    public static void afterLocalTruncate(TableMetadata truncated)
    {
        try
        {
            if (SchemaConstants.isSystemKeyspace(truncated.keyspace))
                return;
            forgetBase(truncated.keyspace, truncated.name);
            for (String suffix : SHADOW_SUFFIXES)
            {
                if (truncated.name.endsWith(suffix) && truncated.name.length() > suffix.length())
                    forgetBase(truncated.keyspace, truncated.name.substring(0, truncated.name.length() - suffix.length()));
            }
        }
        catch (RuntimeException e)
        {
            // The truncate itself succeeded; a stale cache only costs up to one refresh interval.
            logger.warn("Tiered storage: could not drop cached state after truncating {}.{}",
                        truncated.keyspace, truncated.name, e);
        }
    }

    private static void forgetBase(String keyspace, String name)
    {
        TableMetadata base = Schema.instance.getTableMetadata(keyspace, name);
        if (base == null)
            return;
        ChunkCoverage.invalidate(base);
        TagRegistry.forget(base);
    }
}
