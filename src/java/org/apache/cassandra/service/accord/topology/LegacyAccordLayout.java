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

package org.apache.cassandra.service.accord.topology;

import java.io.IOException;

import org.agrona.collections.Int2ObjectHashMap;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.serialization.Version;

/**
 * Aster fork: reads the Accord sections of {@code ClusterMetadata} as builds before upstream
 * {@code 4da20e07e8} ("Improve Rebootstrap") wrote them.
 * <p>
 * That commit replaced {@code AccordFastPath} with {@link AccordNodeInfos} in place and stopped serializing
 * {@code AccordStaleReplicas}, without bumping {@link Version}: a {@code V7}..{@code V10} snapshot written
 * before it carries {@code [AccordFastPath][ConsensusMigrationState][AccordStaleReplicas]}, one written after
 * it {@code [AccordNodeInfos][ConsensusMigrationState][vint 0]}, under the same version number. The current
 * reader misreads the former and startup dies a few fields later
 * ({@code No enum constant ...MultiStepOperation.Kind.}) -- what stopped node 41 booting on 2026-11-01.
 * <p>
 * Only the shape every cluster that never used Accord has is carried over: an empty fast path and no stale or
 * hard-removed replicas. An empty {@code AccordFastPath} is byte-for-byte an empty {@code AccordNodeInfos};
 * the stale-replicas section differs only by its trailing epoch. Non-empty Accord state is refused rather than
 * converted, since a wrong conversion would be written back into the cluster's metadata.
 * <p>
 * Read-only by design: everything is written in upstream's layout, so the fork stays wire- and disk-compatible
 * with upstream. Removable once no deployment still holds a snapshot written before {@code 4da20e07e8}.
 */
public final class LegacyAccordLayout
{
    private LegacyAccordLayout()
    {
    }

    /** Reads a pre-{@code 4da20e07e8} {@code AccordFastPath} section as the empty {@link AccordNodeInfos} it is. */
    public static AccordNodeInfos readFastPath(DataInputPlus in, Version version) throws IOException
    {
        int entries = in.readUnsignedVInt32();
        if (entries != 0)
            throw new IOException("Cluster metadata written before upstream 4da20e07e8 holds an Accord fast path with " +
                                  entries + " node entries; this build reads that layout only when Accord was never " +
                                  "used, and will not guess a conversion of live Accord node state");
        Epoch lastModified = Epoch.serializer.deserialize(in, version);
        return new AccordNodeInfos(new Int2ObjectHashMap<>(1, 0.65f, false), lastModified);
    }

    /** Skips a pre-{@code 4da20e07e8} {@code AccordStaleReplicas} section, which must be empty. */
    public static void skipStaleReplicas(DataInputPlus in, Version version) throws IOException
    {
        int flags = in.readUnsignedVInt32();
        if (flags != 0)
            throw new IOException("Cluster metadata written before upstream 4da20e07e8 records stale or hard-removed " +
                                  "Accord replicas (flags " + flags + "); this build reads that layout only when Accord " +
                                  "was never used");
        Epoch.serializer.deserialize(in, version);
    }
}
