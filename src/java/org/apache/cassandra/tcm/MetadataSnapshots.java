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

package org.apache.cassandra.tcm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.tcm.serialization.VerboseMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public interface MetadataSnapshots
{
    Logger logger = LoggerFactory.getLogger(MetadataSnapshots.class);

    ClusterMetadata getSnapshot(Epoch epoch);
    ClusterMetadata getSnapshotBefore(Epoch epoch);
    ClusterMetadata getLatestSnapshot();
    List<Epoch> listSnapshotsSince(Epoch epoch);
    void storeSnapshot(ClusterMetadata metadata);

    static ByteBuffer toBytes(ClusterMetadata metadata) throws IOException
    {
        Version serializationVersion = Version.minCommonSerializationVersion();
        long serializedSize = VerboseMetadataSerializer.serializedSize(ClusterMetadata.serializer, metadata, serializationVersion);
        ByteBuffer bytes = ByteBuffer.allocate((int) serializedSize);
        try (DataOutputBuffer dob = new DataOutputBuffer(bytes))
        {
            VerboseMetadataSerializer.serialize(ClusterMetadata.serializer, metadata, dob, serializationVersion);
        }
        bytes.flip().rewind();
        return bytes;
    }

    @SuppressWarnings("resource")
    static ClusterMetadata fromBytes(ByteBuffer serialized) throws IOException
    {
        if (serialized == null)
            return null;

        // Aster fork: a snapshot is exactly one serialized ClusterMetadata, so the current layout must consume
        // it to the last byte. If it does not -- or throws -- the snapshot may have been written before upstream
        // 4da20e07e8 changed the Accord sections without a Version bump (LegacyAccordLayout); read it in that
        // layout instead, and accept the result only if that reading consumes every byte too. Neither fitting
        // fails as before: a reading that merely does not throw is not evidence of a correct one.
        Throwable current;
        try
        {
            DataInputBuffer in = new DataInputBuffer(serialized.duplicate(), false);
            ClusterMetadata metadata = VerboseMetadataSerializer.deserialize(ClusterMetadata.serializer, in);
            if (in.available() == 0)
                return metadata;
            current = new IOException(in.available() + " byte(s) left after reading the snapshot in the current layout");
        }
        catch (IOException | RuntimeException e)
        {
            current = e;
        }

        DataInputBuffer in = new DataInputBuffer(serialized.duplicate(), false);
        Version version = Version.fromInt(in.readUnsignedVInt32());
        ClusterMetadata legacy;
        try
        {
            legacy = ClusterMetadata.serializer.deserializeLegacyAccordLayout(in, version);
        }
        catch (IOException | RuntimeException e)
        {
            e.addSuppressed(current);
            throw e;
        }
        if (in.available() != 0)
        {
            IOException e = new IOException("Cluster metadata snapshot fits neither the current nor the pre-4da20e07e8 " +
                                            "Accord layout (" + in.available() + " byte(s) left in the latter)");
            e.addSuppressed(current);
            throw e;
        }
        logger.warn("Read a cluster metadata snapshot at epoch {} written by a build before upstream 4da20e07e8 " +
                    "(legacy Accord layout, version {}). It is rewritten in the current layout by the next snapshot; " +
                    "`nodetool cms snapshot` takes one now.", legacy.epoch, version);
        return legacy;
    }

    MetadataSnapshots NO_OP = new NoOp();

    class NoOp implements MetadataSnapshots
    {
        @Override
        public ClusterMetadata getSnapshot(Epoch epoch)
        {
            return null;
        }

        @Override
        public ClusterMetadata getSnapshotBefore(Epoch epoch) {return null;}

        @Override
        public ClusterMetadata getLatestSnapshot() {return null;}

        @Override
        public List<Epoch> listSnapshotsSince(Epoch epoch)
        {
            return Collections.emptyList();
        }

        @Override
        public void storeSnapshot(ClusterMetadata metadata) {}
    }

    class SystemKeyspaceMetadataSnapshots implements MetadataSnapshots
    {
        @Override
        public ClusterMetadata getSnapshot(Epoch epoch)
        {
            try
            {
                return fromBytes(SystemKeyspace.getSnapshot(epoch));
            }
            catch (IOException e)
            {
                logger.error("Could not load snapshot", e);
                return null;
            }
        }

        @Override
        public ClusterMetadata getSnapshotBefore(Epoch epoch)
        {
            try
            {
                return fromBytes(SystemKeyspace.findSnapshotBefore(epoch));
            }
            catch (IOException e)
            {
                logger.error("Could not load snapshot before " + epoch, e);
                return null;
            }
        }

        @Override
        public ClusterMetadata getLatestSnapshot()
        {
            try
            {
                ByteBuffer snapshot = SystemKeyspace.findLastSnapshot();
                if (snapshot != null)
                    return fromBytes(snapshot);
            }
            catch (IOException e)
            {
                logger.error("Could not find latest snapshot");
            }
            return null;
        }

        @Override
        public List<Epoch> listSnapshotsSince(Epoch epoch)
        {
            return SystemKeyspace.listSnapshotsSince(epoch);
        }

        @Override
        public void storeSnapshot(ClusterMetadata metadata)
        {
            try
            {
                SystemKeyspace.storeSnapshot(metadata.epoch, toBytes(metadata));
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}
