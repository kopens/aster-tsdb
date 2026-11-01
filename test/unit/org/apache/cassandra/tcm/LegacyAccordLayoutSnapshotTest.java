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
import org.apache.cassandra.tcm.serialization.VerboseMetadataSerializer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Golden-fixture compatibility for cluster metadata snapshots (see LegacyAccordLayout).
 * <p>
 * The fixture is a real TCM snapshot written by aster-tsdb v6.0.0-r3 -- a build from before upstream
 * 4da20e07e8 -- on a fresh single node with one keyspace {@code fx}, taken with {@code nodetool cms snapshot}
 * at epoch 6, serialization version V10. Upstream changed the Accord sections of that layout without a Version
 * bump, which is what stopped node 41 booting on 2026-11-01; a fresh-cluster test cannot see that, a fixture
 * from a released build can. Add one per release that writes metadata in a new layout.
 */
public class LegacyAccordLayoutSnapshotTest
{
    private static final String R3_SNAPSHOT = "cluster-metadata-snapshot-v6.0.0-r3-epoch6.bin";

    static
    {
        DatabaseDescriptor.toolInitialization();
    }

    private static ByteBuffer fixture(String name) throws IOException
    {
        return ByteBuffer.wrap(Files.readAllBytes(Paths.get("test/data/tcm", name)));
    }

    /** Pins the break itself, so the fixture keeps proving something if the fallback is ever removed. */
    @Test
    public void theCurrentLayoutAloneCannotReadTheR3Snapshot() throws IOException
    {
        DataInputBuffer in = new DataInputBuffer(fixture(R3_SNAPSHOT), false);
        try
        {
            VerboseMetadataSerializer.deserialize(ClusterMetadata.serializer, in);
            assertTrue("the current layout must not consume a pre-4da20e07e8 snapshot exactly", in.available() != 0);
        }
        catch (IOException | RuntimeException expected)
        {
            // what node 41 hit: No enum constant ...MultiStepOperation.Kind.
        }
    }

    @Test
    public void theR3SnapshotIsReadAndRewrittenInTheCurrentLayout() throws IOException
    {
        ClusterMetadata legacy = MetadataSnapshots.fromBytes(fixture(R3_SNAPSHOT));
        assertEquals(6, legacy.epoch.getEpoch());
        assertTrue("the schema written by r3 must come back", legacy.schema.maybeGetKeyspaceMetadata("fx").isPresent());
        assertTrue(legacy.schema.getKeyspaceMetadata("fx").tables.get("sensor").isPresent());

        // The next snapshot rewrites it in upstream's layout, which then reads back without the fallback.
        ByteBuffer rewritten = MetadataSnapshots.toBytes(legacy);
        DataInputBuffer in = new DataInputBuffer(rewritten.duplicate(), false);
        ClusterMetadata current = VerboseMetadataSerializer.deserialize(ClusterMetadata.serializer, in);
        assertEquals("the rewritten snapshot is consumed exactly by the current layout", 0, in.available());
        assertEquals(legacy.epoch, current.epoch);
        assertEquals(rewritten, MetadataSnapshots.toBytes(current));
    }

    /** A snapshot that fits neither layout still fails: the fallback must not turn garbage into metadata. */
    @Test
    public void aSnapshotThatFitsNeitherLayoutStillFails() throws IOException
    {
        ByteBuffer bytes = fixture(R3_SNAPSHOT);
        bytes.limit(bytes.limit() - 7);
        try
        {
            MetadataSnapshots.fromBytes(bytes);
            fail("a truncated snapshot must not be read as metadata");
        }
        catch (IOException | RuntimeException expected)
        {
        }
    }
}
