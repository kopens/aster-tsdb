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

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;

import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.MetricNameFactory;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Counts the tiering events that used to reach nothing but a log line: a range scan refused, a
 * corrupt chunk that failed a read, a coverage ledger that could not be read, a cold write refused,
 * a tag a re-encode cycle had to skip. Each is either a client hitting a limit of tiered storage or
 * the feature quietly not doing its job, and neither should need a log search to notice.
 * <p>
 * Two surfaces, both counted since this node started:
 * <ul>
 *     <li>per table, in {@code system_views.timeseries_tiering} -- which table, from CQL;</li>
 *     <li>summed over tables, as metrics of type {@code TimeSeriesTiering} (JMX, so any JMX exporter
 *     scrapes them), plus two gauges an alert can be written against directly:
 *     {@code MaxConsecutiveFailures} and {@code MaxMillisSinceLastSuccess}.</li>
 * </ul>
 * Kept out of upstream's {@code TableMetrics} on purpose: a fork-only class costs an upstream merge
 * nothing. Not exposed through {@code system_metrics}, which upstream stopped exposing by default.
 */
public final class TieringMetrics
{
    public enum Event
    {
        RANGE_SCAN_REFUSED("RangeScansRefused"),
        /** Counted per coordinator sub-range resolve that merged chunk keys into a range scan. */
        RANGE_SCAN_MERGED("RangeScansMerged"),
        CORRUPT_CHUNK_READ("CorruptChunkReads"),
        COVERAGE_READ_FAILURE("CoverageReadFailures"),
        COLD_WRITE_REFUSED("ColdWritesRefused"),
        TAG_SKIPPED("TagsSkipped");

        final String metricName;

        Event(String metricName)
        {
            this.metricName = metricName;
        }
    }

    /**
     * The metric group. Cassandra only accepts groups declared in {@code CassandraMetricsRegistry}'s
     * static list, which names this constant -- a compile-time constant, so naming it there does not
     * initialise this class from inside the registry's own initialiser.
     */
    public static final String TYPE_NAME = "TimeSeriesTiering";

    private static final MetricNameFactory factory = new DefaultNameFactory(TYPE_NAME);
    private static final Map<Event, Counter> TOTALS = new EnumMap<>(Event.class);
    /** {@code "keyspace.table"} -> one adder per {@link Event}, indexed by ordinal. */
    private static final ConcurrentHashMap<String, LongAdder[]> BY_TABLE = new ConcurrentHashMap<>();

    static
    {
        for (Event event : Event.values())
            TOTALS.put(event, Metrics.counter(factory.createMetricName(event.metricName)));
        Metrics.register(factory.createMetricName("MaxConsecutiveFailures"),
                         (Gauge<Integer>) TieredStorageService.instance::maxConsecutiveFailures);
        Metrics.register(factory.createMetricName("MaxMillisSinceLastSuccess"),
                         (Gauge<Long>) TieredStorageService.instance::maxMillisSinceLastSuccess);
    }

    private TieringMetrics()
    {
    }

    /** Registers the metrics now, so the gauges exist before the first event does. */
    public static void init()
    {
    }

    public static void record(TableMetadata table, Event event)
    {
        record(table.keyspace, table.name, event, 1);
    }

    public static void record(String keyspace, String table, Event event, long n)
    {
        if (n <= 0)
            return;
        TOTALS.get(event).inc(n);
        BY_TABLE.computeIfAbsent(keyspace + '.' + table, ignored -> newAdders())[event.ordinal()].add(n);
    }

    /** @return how many times {@code event} happened on {@code keyspace.table} since this node started. */
    public static long count(String keyspace, String table, Event event)
    {
        LongAdder[] adders = BY_TABLE.get(keyspace + '.' + table);
        return adders == null ? 0L : adders[event.ordinal()].sum();
    }

    private static LongAdder[] newAdders()
    {
        LongAdder[] adders = new LongAdder[Event.values().length];
        for (int i = 0; i < adders.length; i++)
            adders[i] = new LongAdder();
        return adders;
    }
}
