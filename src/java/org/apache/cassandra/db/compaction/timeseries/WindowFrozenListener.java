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

package org.apache.cassandra.db.compaction.timeseries;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Notification hook for {@link org.apache.cassandra.db.compaction.TimeSeriesCompactionStrategy}: fired every
 * time a window is frozen (or re-frozen after late data) into a single sstable. Design spec section 5.
 * <p>
 * The event is a latency optimisation, not the sole trigger: consumers must be able to fall back to scanning
 * for "frozen windows without downstream output" themselves,
 * because events can be lost across restarts. Events are scoped to one strategy-instance slice (the
 * CompactionStrategyManager splits instances per repair status and per disk).
 */
public interface WindowFrozenListener
{
    /**
     * Called every time a window is frozen (or re-frozen) into a single sstable, after the compaction
     * has committed. Implementations must be idempotent (the same window may be reported more than once)
     * and must not block: this runs on the compaction thread, so long-running consumers should enqueue.
     * <p>
     * <b>{@code frozen} is a bare reader, not a reference, and is only guaranteed to be live for the
     * duration of this call.</b> The two requirements interact: a consumer that enqueues the reader (as
     * it must, rather than blocking the compaction thread) can find it obsoleted before the queue is
     * drained - a later re-freeze, a retention drop, or an operator compaction can all replace it. Any
     * consumer that keeps the reader beyond this method MUST therefore acquire its own reference inside
     * this method:
     * <pre>
     *     Ref&lt;SSTableReader&gt; ref = frozen.tryRef();
     *     if (ref == null)
     *         return;                       // already obsoleted: nothing to do, a later event will follow
     *     queue.add(ref);                   // and release() it when done
     * </pre>
     * A null return from {@code tryRef()} is normal and not an error. Passing a {@code Ref} in directly
     * was rejected because it would oblige every listener - including ones that only read metadata
     * synchronously - to release it, and a leaked reference is a much worse failure than a missed event
     * (this hook is explicitly a latency optimisation that consumers must already be able to miss).
     */
    void onWindowFrozen(TableMetadata table, long windowStartMillis, SSTableReader frozen);
}
