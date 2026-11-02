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
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.tcm.serialization.VerboseMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Golden-fixture compatibility for cluster metadata snapshots.
 * <p>
 * Upstream 4da20e07e8 changed the Accord sections of the {@code ClusterMetadata} layout without a Version bump,
 * which is what stopped node 41 booting v6.0.0-r2 on 2026-11-01. A fresh-cluster test cannot see that kind of
 * break; a snapshot written by a released build can. r4 read the old layout and rewrote it; r6 removed that
 * fallback once node 41, the only node with pre-r4 metadata, had booted r4.
 * <ul>
 * <li>{@code ...-r4-epoch6.bin}: the snapshot as r4 rewrote it (same single node, keyspace {@code fx}, epoch 6,
 * serialization V10). Every later build must read it exactly. If an upstream merge breaks this test, upstream
 * changed the layout again -- do not re-record the fixture; handle the old layout first.</li>
 * <li>{@code ...-r3-epoch6.bin}: the same metadata as r3 wrote it. It must stay unreadable: it documents why a
 * node holding pre-r4 metadata has to boot r4 once before this build.</li>
 * </ul>
 * Add a fixture per release that writes metadata in a new layout.
 */
public class ClusterMetadataSnapshotLayoutTest
{
    private static final String R3_SNAPSHOT = "cluster-metadata-snapshot-v6.0.0-r3-epoch6.bin";
    private static final String R4_SNAPSHOT = "cluster-metadata-snapshot-v6.0.0-r4-epoch6.bin";

    static
    {
        DatabaseDescriptor.toolInitialization();
    }

    private static ByteBuffer fixture(String name) throws IOException
    {
        return ByteBuffer.wrap(Files.readAllBytes(Paths.get("test/data/tcm", name)));
    }

    @Test
    public void theR4SnapshotIsReadExactlyAndWrittenBackIdentically() throws IOException
    {
        ByteBuffer bytes = fixture(R4_SNAPSHOT);
        DataInputBuffer in = new DataInputBuffer(bytes.duplicate(), false);
        ClusterMetadata metadata = VerboseMetadataSerializer.deserialize(ClusterMetadata.serializer, in);
        assertEquals("the current layout must consume an r4 snapshot to the last byte", 0, in.available());
        assertEquals(6, metadata.epoch.getEpoch());
        assertTrue(metadata.schema.getKeyspaceMetadata("fx").tables.get("sensor").isPresent());
        // Written back at the fixture's own serialization version (MetadataSnapshots.toBytes picks
        // Version.minCommonSerializationVersion(), which depends on the JVM's environment). Compared as metadata,
        // not as bytes: upstream writes DataPlacements in hash order of their ReplicationParams, whose hash takes
        // in Class.hashCode(), so two placements swap places between JVMs (2 runs in 20 here) -- same layout,
        // different byte order. The layout itself is pinned by the exact read above.
        Version version = Version.fromInt(new DataInputBuffer(bytes.duplicate(), false).readUnsignedVInt32());
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            VerboseMetadataSerializer.serialize(ClusterMetadata.serializer, metadata, out, version);
            ByteBuffer written = out.asNewBuffer();
            assertEquals("the layout this build writes has the size r4 wrote", bytes.remaining(), written.remaining());
            DataInputBuffer back = new DataInputBuffer(written, false);
            assertEquals(metadata, VerboseMetadataSerializer.deserialize(ClusterMetadata.serializer, back));
            assertEquals(0, back.available());
        }
        assertEquals(metadata.epoch, MetadataSnapshots.fromBytes(bytes).epoch);
    }

    @Test
    public void theR3SnapshotIsNotReadable() throws IOException
    {
        DataInputBuffer in = new DataInputBuffer(fixture(R3_SNAPSHOT), false);
        try
        {
            VerboseMetadataSerializer.deserialize(ClusterMetadata.serializer, in);
            assertTrue("the current layout must not consume a pre-4da20e07e8 snapshot exactly", in.available() != 0);
        }
        catch (IOException | RuntimeException expected)
        {
            // what node 41 hit on r2: No enum constant ...MultiStepOperation.Kind.
        }
    }
}
