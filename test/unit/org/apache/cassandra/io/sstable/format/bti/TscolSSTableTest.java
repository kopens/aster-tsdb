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

package org.apache.cassandra.io.sstable.format.bti;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.SSTableTxnWriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * A {@link TscolFormat} sstable must read back exactly what the row-format sstable it was written from reads back:
 * the same unfiltered stream for every partition and every slice/column/direction shape, the same sequential scan
 * (what compaction and repair validation read), and the same CQL answers once it is the table's only sstable --
 * including after a compaction reads it as input.
 */
public class TscolSSTableTest extends CQLTester
{
    private static final long MINUTE = 60_000L;

    @Test
    public void formatIsRegisteredByName()
    {
        SSTableFormat<?, ?> format = DatabaseDescriptor.getSSTableFormats().get(TscolFormat.NAME);
        assertTrue(format instanceof TscolFormat);
        assertTrue(TscolFormat.instance() == format);
        assertTrue("selected_format is untouched", !TscolFormat.is(DatabaseDescriptor.getSelectedSSTableFormat()));
    }

    @Test
    public void columnarSSTableReadsExactlyLikeTheRowFormatOne() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, site text static, v double, n int, s text, PRIMARY KEY (tag, ts))");
        loadShapes();
        flush();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader rows = cfs.getLiveSSTables().iterator().next();
        SSTableReader columnar = rewriteAs(cfs, rows, TscolFormat.instance());
        assertTrue(columnar.descriptor.toString(), TscolFormat.is(columnar.descriptor.getFormat()));
        assertTrue(columnar instanceof TscolTableReader);
        assertTrue("columnar " + columnar.uncompressedLength() + " vs rows " + rows.uncompressedLength(),
                   columnar.uncompressedLength() < rows.uncompressedLength());

        ColumnMetadata v = cfs.metadata().getColumn(org.apache.cassandra.cql3.ColumnIdentifier.getInterned("v", false));
        List<ColumnFilter> filters = List.of(ColumnFilter.all(cfs.metadata()),
                                             ColumnFilter.selection(cfs.metadata(), RegularAndStaticColumns.of(v), false));
        List<Slices> slices = List.of(Slices.ALL,
                                      slices(cfs, slice(30 * MINUTE, 90 * MINUTE)),
                                      slices(cfs, slice(14 * MINUTE, 16 * MINUTE)),                 // across a block boundary
                                      slices(cfs, slice(5 * MINUTE, 6 * MINUTE), slice(100 * MINUTE, 101 * MINUTE)),
                                      slices(cfs, slice(500 * MINUTE, 600 * MINUTE)));               // beyond the data
        for (String tag : new String[]{ "a", "b", "c", "d", "e", "missing" })
        {
            DecoratedKey key = cfs.decorateKey(UTF8Type.instance.decompose(tag));
            for (ColumnFilter filter : filters)
                for (Slices s : slices)
                    for (boolean reversed : new boolean[]{ false, true })
                        assertSamePartition(tag + " " + s + " reversed=" + reversed + " " + filter,
                                            rows.rowIterator(key, s, filter, reversed, SSTableReadsListener.NOOP_LISTENER),
                                            columnar.rowIterator(key, s, filter, reversed, SSTableReadsListener.NOOP_LISTENER));
        }

        try (ISSTableScanner a = rows.getScanner(); ISSTableScanner b = columnar.getScanner())
        {
            while (a.hasNext())
            {
                assertTrue(b.hasNext());
                try (UnfilteredRowIterator pa = a.next(); UnfilteredRowIterator pb = b.next())
                {
                    assertSamePartition("scan " + pa.partitionKey(), pa, pb);
                }
            }
            assertTrue(!b.hasNext());
        }
        columnar.selfRef().release();
    }

    @Test
    public void cqlAnswersAreUnchangedWhenTheTableIsColumnarAndAfterCompactingIt() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, site text static, v double, n int, s text, PRIMARY KEY (tag, ts))");
        loadShapes();
        flush();
        List<String> queries = List.of("SELECT * FROM %s",
                                       "SELECT tag, ts, v, n, writetime(v), writetime(n) FROM %s WHERE tag = 'a'",
                                       "SELECT * FROM %s WHERE tag = 'a' AND ts >= '1970-01-01 00:14:00+0000' AND ts < '1970-01-01 00:16:00+0000' ORDER BY ts DESC",
                                       "SELECT count(*), avg(v) FROM %s WHERE tag = 'a'",
                                       "SELECT * FROM %s WHERE n = 3 ALLOW FILTERING",
                                       "SELECT DISTINCT tag, site FROM %s",
                                       "SELECT * FROM %s WHERE tag IN ('b', 'd', 'e')");
        List<UntypedResultSet> before = new ArrayList<>();
        for (String q : queries)
            before.add(execute(q));

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader rows = cfs.getLiveSSTables().iterator().next();
        SSTableReader columnar = rewriteAs(cfs, rows, TscolFormat.instance());
        cfs.addSSTables(Collections.singleton(columnar));
        cfs.getTracker().removeUnsafe(Set.of(rows));
        assertEquals(1, cfs.getLiveSSTables().size());
        assertTrue(TscolFormat.is(cfs.getLiveSSTables().iterator().next().descriptor.getFormat()));
        for (int i = 0; i < queries.size(); i++)
            assertSameResult(queries.get(i), before.get(i), execute(queries.get(i)));

        // A compaction reads the columnar sstable sequentially and writes the selected format.
        execute("INSERT INTO %s (tag, ts, v) VALUES ('a', '1970-01-01 03:00:00+0000', 9.0) USING TIMESTAMP 900");
        flush();
        List<UntypedResultSet> withLate = new ArrayList<>();
        for (String q : queries)
            withLate.add(execute(q));
        // Named explicitly so the columnar sstable is certainly an input (the strategy's own selection is not what
        // this test is about).
        StringBuilder files = new StringBuilder();
        for (SSTableReader sstable : cfs.getLiveSSTables())
            files.append(files.length() == 0 ? "" : ",").append(sstable.descriptor.fileFor(org.apache.cassandra.io.sstable.format.SSTableFormat.Components.DATA).absolutePath());
        org.apache.cassandra.db.compaction.CompactionManager.instance.forceUserDefinedCompaction(files.toString());
        // The columnar input was read and replaced by the selected format.
        for (SSTableReader sstable : cfs.getLiveSSTables())
            assertTrue(sstable.toString(), !TscolFormat.is(sstable.descriptor.getFormat()));
        for (int i = 0; i < queries.size(); i++)
            assertSameResult("after compaction: " + queries.get(i), withLate.get(i), execute(queries.get(i)));
    }

    /**
     * a: two hours at ten-second spacing with a late column update, a TTL'd cell and a null; b: a range deletion
     * (row-format fallback); c: static only; d: a row deletion (fallback); e: an INSERT that binds null.
     */
    private void loadShapes() throws Throwable
    {
        execute("INSERT INTO %s (tag, site) VALUES ('a', 'S1') USING TIMESTAMP 50");
        for (int i = 0; i < 720; i++)
            execute("INSERT INTO %s (tag, ts, v, n, s) VALUES ('a', ?, ?, ?, ?) USING TIMESTAMP ?",
                    new Date(i * 10_000L), 20.0 + (i % 7) * 0.5, i % 5, i % 9 == 0 ? null : "on", 100L + i);
        execute("UPDATE %s USING TIMESTAMP 5000 SET n = 99 WHERE tag = 'a' AND ts = ?", new Date(600_000L));
        execute("UPDATE %s USING TTL 86400 SET n = 3 WHERE tag = 'a' AND ts = ?", new Date(1_200_000L));
        for (int i = 0; i < 30; i++)
            execute("INSERT INTO %s (tag, ts, v) VALUES ('b', ?, ?) USING TIMESTAMP 200", new Date(i * MINUTE), (double) i);
        execute("DELETE FROM %s USING TIMESTAMP 300 WHERE tag = 'b' AND ts >= ? AND ts < ?", new Date(10 * MINUTE), new Date(20 * MINUTE));
        execute("INSERT INTO %s (tag, site) VALUES ('c', 'S3') USING TIMESTAMP 60");
        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (tag, ts, v) VALUES ('d', ?, 1.0) USING TIMESTAMP 400", new Date(i * MINUTE));
        execute("DELETE FROM %s USING TIMESTAMP 500 WHERE tag = 'd' AND ts = ?", new Date(3 * MINUTE));
        execute("INSERT INTO %s (tag, ts, v, s) VALUES ('e', ?, 2.0, null) USING TIMESTAMP 600", new Date(MINUTE));
    }

    static SSTableReader rewriteAs(ColumnFamilyStore cfs, SSTableReader source, SSTableFormat<?, ?> format) throws Throwable
    {
        Descriptor descriptor = cfs.newSSTableDescriptor(cfs.getDirectories().getDirectoryForNewSSTables(), format);
        SerializationHeader header = SerializationHeader.make(cfs.metadata(), Collections.singleton(source));
        try (SSTableTxnWriter writer = SSTableTxnWriter.create(cfs, descriptor, source.estimatedKeys(), 0, null, false, header);
             ISSTableScanner scanner = source.getScanner())
        {
            while (scanner.hasNext())
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    writer.append(partition);
                }
            Collection<SSTableReader> written = writer.finish(true);
            assertEquals(1, written.size());
            return written.iterator().next();
        }
    }

    private static Slice slice(long fromMillis, long toMillisExclusive)
    {
        return Slice.make(ClusteringBound.inclusiveStartOf(org.apache.cassandra.db.Clustering.make(TimestampType.instance.fromTimeInMillis(fromMillis))),
                          ClusteringBound.exclusiveEndOf(org.apache.cassandra.db.Clustering.make(TimestampType.instance.fromTimeInMillis(toMillisExclusive))));
    }

    private static Slices slices(ColumnFamilyStore cfs, Slice... slices)
    {
        Slices.Builder builder = new Slices.Builder(cfs.metadata().comparator);
        for (Slice s : slices)
            builder.add(s);
        return builder.build();
    }

    private static void assertSamePartition(String label, UnfilteredRowIterator expected, UnfilteredRowIterator actual)
    {
        try (UnfilteredRowIterator e = expected; UnfilteredRowIterator a = actual)
        {
            assertNotNull(label, a);
            assertEquals(label + " partition deletion", e.partitionLevelDeletion(), a.partitionLevelDeletion());
            assertEquals(label + " static row", e.staticRow(), a.staticRow());
            List<Unfiltered> el = new ArrayList<>(), al = new ArrayList<>();
            e.forEachRemaining(el::add);
            a.forEachRemaining(al::add);
            assertEquals(label, el, al);
        }
    }

    private static void assertSameResult(String label, UntypedResultSet expected, UntypedResultSet actual)
    {
        assertEquals(label + " row count", expected.size(), actual.size());
        Iterator<UntypedResultSet.Row> e = expected.iterator(), a = actual.iterator();
        int i = 0;
        while (e.hasNext())
        {
            UntypedResultSet.Row er = e.next(), ar = a.next();
            for (org.apache.cassandra.cql3.ColumnSpecification c : expected.metadata())
                assertEquals(label + " row " + i + " column " + c.name, er.getBytes(c.name.toString()), ar.getBytes(c.name.toString()));
            i++;
        }
    }
}
