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
import java.util.concurrent.atomic.AtomicBoolean;

import org.agrona.collections.Int2ObjectHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.JVMStabilityInspector;

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
 * with upstream. A node that read a legacy snapshot at startup takes a fresh one through upstream's own path
 * ({@link #rewriteSnapshotIfLegacyWasRead}, what {@code nodetool cms snapshot} does), so it needs this reader
 * for exactly one boot. Removable once every deployment has booted a build carrying it -- for this fork, once
 * node 41 has booted r4.
 */
public final class LegacyAccordLayout
{
    private static final Logger logger = LoggerFactory.getLogger(LegacyAccordLayout.class);

    private static final AtomicBoolean legacySnapshotRead = new AtomicBoolean();

    private LegacyAccordLayout()
    {
    }

    /** Called by {@code MetadataSnapshots.fromBytes} when it had to read a snapshot in this layout. */
    public static void noteLegacySnapshotRead()
    {
        legacySnapshotRead.set(true);
    }

    /**
     * Startup hook: if a snapshot was read in the legacy layout, take a new one now, in the current layout, so the
     * next boot does not need this class. Asynchronous and best-effort -- committing the snapshot transformation
     * goes through the CMS, and a CMS that cannot be reached right now must not hold up or fail the boot; the
     * legacy snapshot stays readable either way, so the cost of failing is only that this runs again next boot.
     */
    public static void rewriteSnapshotIfLegacyWasRead()
    {
        if (!legacySnapshotRead.get())
            return;
        ScheduledExecutors.optionalTasks.execute(() -> {
            try
            {
                Epoch epoch = ClusterMetadataService.instance().triggerSnapshot().epoch;
                legacySnapshotRead.set(false);
                logger.info("Rewrote the cluster metadata snapshot in the current layout at epoch {}; the legacy " +
                            "Accord layout is no longer needed to start this node", epoch);
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                logger.warn("Could not rewrite the cluster metadata snapshot read in the legacy Accord layout; it will " +
                            "be retried at the next start, or run `nodetool cms snapshot`", t);
            }
        });
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
