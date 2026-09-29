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
package org.apache.cassandra.cql3.statements;

import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.timeseries.tiering.TieredTruncation;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.TruncateException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.messages.ResultMessage;

public class TruncateStatement extends QualifiedStatement implements CQLStatement
{
    public TruncateStatement(QualifiedName name)
    {
        super(name);
    }

    public TruncateStatement prepare(ClientState state)
    {
        return this;
    }

    public void authorize(ClientState state) throws InvalidRequestException, UnauthorizedException
    {
        state.ensureTablePermission(keyspace(), name(), Permission.MODIFY);
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        Schema.instance.validateTable(keyspace(), name());
        Guardrails.dropTruncateTableEnabled.ensureEnabled(state);
    }

    @Override
    public ResultMessage execute(QueryState state, QueryOptions options, Dispatcher.RequestTime requestTime) throws InvalidRequestException, TruncateException
    {
        try
        {
            TableMetadata metaData = Schema.instance.getTableMetadata(keyspace(), name());
            if (metaData.isView())
                throw new InvalidRequestException("Cannot TRUNCATE materialized view directly; must truncate base table instead");

            if (metaData.isVirtual())
            {
                executeForVirtualTable(metaData.id);
            }
            else
            {
                StorageProxy.truncateBlocking(keyspace(), name());
                // Tiered storage: the chunk table holds the only copy of the table's tiered history,
                // and reads merge it back in -- so it is truncated with the base (see TieredTruncation).
                for (String shadow : TieredTruncation.shadowTablesOf(keyspace(), name()))
                    truncateShadow(shadow, () -> StorageProxy.truncateBlocking(keyspace(), shadow));
            }
        }
        catch (UnavailableException | TimeoutException e)
        {
            throw new TruncateException(e);
        }
        return null;
    }

    private interface ShadowTruncate
    {
        void run() throws UnavailableException, TimeoutException;
    }

    /**
     * Truncates one tiering shadow table after the base table has already been truncated, naming
     * the half-done state if it fails: the base is empty but part of its tiered history is still
     * readable, and re-running the TRUNCATE is what finishes it.
     */
    private void truncateShadow(String shadow, ShadowTruncate truncate) throws UnavailableException, TimeoutException
    {
        try
        {
            truncate.run();
        }
        catch (TimeoutException | RuntimeException e)
        {
            throw new TruncateException(String.format(
                "%s.%s was truncated, but its tiered-storage table %s.%s could not be (%s): part of the table's " +
                "tiered history is still readable. Re-run the TRUNCATE to finish it.",
                keyspace(), name(), keyspace(), shadow, e));
        }
    }

    public ResultMessage executeLocally(QueryState state, QueryOptions options)
    {
        try
        {
            TableMetadata metaData = Schema.instance.getTableMetadata(keyspace(), name());
            if (metaData.isView())
                throw new InvalidRequestException("Cannot TRUNCATE materialized view directly; must truncate base table instead");

            if (metaData.isVirtual())
            {
                executeForVirtualTable(metaData.id);
            }
            else
            {
                ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(name());
                cfs.truncateBlocking();
                for (String shadow : TieredTruncation.shadowTablesOf(keyspace(), name()))
                    truncateShadow(shadow, () -> Keyspace.open(keyspace()).getColumnFamilyStore(shadow).truncateBlocking());
            }
        }
        catch (Exception e)
        {
            throw new TruncateException(e);
        }
        return null;
    }

    private void executeForVirtualTable(TableId id)
    {
        VirtualKeyspaceRegistry.instance.getTableNullable(id).truncate();
    }

    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.TRUNCATE, keyspace(), name());
    }
}
