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

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * Static registry for {@link WindowFrozenListener}s, mirroring the LocalSessions listener pattern
 * (repair/consistent/LocalSessions.java). No default listeners. Listener failures are logged and never affect the compaction result (design spec section 5).
 */
public final class WindowFrozenListeners
{
    private static final Logger logger = LoggerFactory.getLogger(WindowFrozenListeners.class);

    private static final Set<WindowFrozenListener> listeners = new CopyOnWriteArraySet<>();

    private WindowFrozenListeners()
    {
    }

    public static void registerListener(WindowFrozenListener listener)
    {
        listeners.add(listener);
    }

    public static void unregisterListener(WindowFrozenListener listener)
    {
        listeners.remove(listener);
    }

    @VisibleForTesting
    public static void unsafeClearListeners()
    {
        listeners.clear();
    }

    /** Delivers to every listener; a throwing listener is logged and skipped, the rest still run. */
    public static void fire(TableMetadata table, long windowStartMillis, SSTableReader frozen)
    {
        for (WindowFrozenListener listener : listeners)
        {
            try
            {
                listener.onWindowFrozen(table, windowStartMillis, frozen);
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                logger.error("WindowFrozenListener {} failed for {}.{} window {}",
                             listener, table.keyspace, table.name, windowStartMillis, t);
            }
        }
    }
}
