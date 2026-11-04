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
package org.apache.cassandra.db.timeseries;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * The chunk-format-v4 column directory (§4): one entry per column, sorted by column name in natural
 * {@code String} order, the region padded to a multiple of 8.
 *
 * <p>Entry layout, exactly §4:
 * {@code typeCode u8 | colFlags u8 | statOrder u8 | nameLen u8 | name | sectionOffset u32 |
 * sectionLen u32 | blockCount u32 | [constLen varint + constBytes] | [min, max]}.
 *
 * <p><b>What the directory buys, and therefore what it must never lose.</b> §0 keeps three v3
 * properties here and strengthens one. The type code still selects <em>compression only</em>, never
 * meaning. {@code CONSTANT} and {@code ALL_NULL} columns are still O(1) bytes -- in the production
 * shape four of eight columns are zero-byte sections, which is the single largest win in the format
 * and is why {@code sectionLen == 0} is a checked invariant here rather than an emergent property.
 * An unprojected column is still skipped, and now skipped by jumping: {@code sectionOffset} is
 * explicit, so a reader never has to walk a prefix sum over sections it does not want.
 *
 * <p><b>Every cross-field rule §4 states is enforced on both sides.</b> The {@link Entry}
 * constructor is where they live, so an entry that could not be read cannot be built either, and
 * the encoder finds out at the moment it constructs a bad entry rather than at the moment a replica
 * fails to decode one:
 *
 * <ul>
 * <li>type code {@code 0x00} is a permanent zero trap and every code outside {@code 0x01..0x07} is
 *     corruption (§11) -- an unknown-but-plausible code inside a v4 payload is a flipped bit, since
 *     a genuinely new format would carry a different version byte;</li>
 * <li>{@code colFlags & 0xF0} must be zero, and the flag combinations that describe nothing --
 *     {@code ALL_NULL} together with {@code ALL_PRESENT} or with {@code CONSTANT} -- are rejected;</li>
 * <li>{@code sectionLen == 0} exactly when the column is {@code ALL_NULL} or an all-present
 *     {@code CONSTANT} (§2), and {@code blockCount == 0} exactly then too;</li>
 * <li>{@code CONSTANT} and {@code ALL_NULL} columns carry no statistics;</li>
 * <li>a column whose type code is {@code OPAQUE} carries {@code statOrder = NONE};</li>
 * <li>statistics exist exactly when {@code statOrder != NONE}, so an unusable extremum can never be
 *     stored (§5 rule 6: unused fields are zero, and a stat under no order is unusable);</li>
 * <li>{@code min} does not exceed {@code max} under the declared order.</li>
 * </ul>
 *
 * <p><b>The {@code OPAQUE} rule is stricter than it looks, deliberately.</b> §4 forces
 * {@code statOrder = NONE} on a column <em>downgraded</em> to {@code OPAQUE} because one
 * width-incompatible value made its fixed-width encoding impossible -- §12 ranks the interaction
 * fourth on its risk list, a one-line rule that is easy to forget and that only a corpus containing
 * a zero-length fixed-width value will catch. A decoder cannot tell a downgraded column from a
 * column that was a {@code blob} all along, so the rule is applied to every {@code OPAQUE} column:
 * in v4.0 a native {@code blob} column carries no statistics either. That costs pruning on blob
 * columns and buys an invariant that is checkable from the bytes alone; a v5 that wants blob stats
 * back should spend a distinct type code on it rather than a convention the reader cannot verify.
 *
 * <p><b>Statistics are omitted, never truncated</b> (§7: 256 bytes). The reason is not size, it is
 * that a truncated maximum is not an upper bound. Chop {@code "zzz...z"} to 256 bytes and the
 * result compares <em>below</em> the value it was meant to bound, so a reader that trusts it skips
 * a block that does contain matching rows -- a missing row, not a slow query. A truncated minimum
 * fails symmetrically. There is no cheap way to round a truncated string outward that survives
 * every order in {@link StatOrder}, so an over-long extremum means no statistics for that column at
 * all, and that is always sound. Do not "optimise" this into a truncation.
 *
 * <p><b>Byte determinism</b> (§5): entries are emitted in natural {@code String} order and the
 * order is verified on read, every numeric field is fixed width, the two length fields that §4
 * spells as varints are canonical minimal ones ({@link BlockPresence}'s helpers, which reject a
 * non-minimal encoding), and the pad to 8 is zero-filled and verified. {@code directoryLen} must
 * equal the padded length of what was actually parsed, so a directory cannot be trailed by extra
 * zero bytes that would give one directory two encodings.
 *
 * <p>Every allocation is bounded before it is made: a name is at most 255 bytes by construction of
 * its {@code u8} length, an extremum at most 256 by §7, and a constant's length is checked against
 * the bytes remaining <em>in the directory region</em> before the array is created. A corrupt
 * length throws; it cannot reserve memory.
 */
public final class ChunkV4Directory
{
    // §4 type codes. The code selects an encoding family, never a meaning -- the column's real type
    // lives in the schema, and 0x00 is permanently invalid so that a zeroed region cannot parse.
    public static final int TYPE_INVALID = 0x00;
    public static final int TYPE_DOUBLE = 0x01;
    public static final int TYPE_BOOLEAN = 0x02;
    public static final int TYPE_INT32 = 0x03;
    public static final int TYPE_INT64 = 0x04;
    public static final int TYPE_DATE32 = 0x05;
    public static final int TYPE_TEXT = 0x06;
    public static final int TYPE_OPAQUE = 0x07;

    public static final int FLAG_ALL_PRESENT = 0x01;
    public static final int FLAG_ALL_NULL = 0x02;
    public static final int FLAG_CONSTANT = 0x04;
    public static final int FLAG_HAS_STATS = 0x08;
    /** §4: the high nibble is reserved and must be zero; anything else is corruption. */
    public static final int FLAGS_RESERVED_MASK = 0xF0;

    /** §7: column names are at most 255 bytes, which is also all the {@code u8 nameLen} can say. */
    public static final int MAX_NAME_BYTES = 255;

    /** §7: an extremum longer than this means the column carries no statistics at all. */
    public static final int MAX_STAT_BYTES = 256;

    /** Fixed part of an entry: {@code typeCode | colFlags | statOrder | nameLen} then three u32s. */
    private static final int ENTRY_FIXED_BYTES = 4 + 12;

    private final List<Entry> entries;

    private ChunkV4Directory(List<Entry> entries)
    {
        this.entries = Collections.unmodifiableList(entries);
    }

    /**
     * §5 block-entry width for a type code: 8 for the types whose values are eight bytes, 4 for the
     * four-byte ones, 0 for the types that carry no per-block extrema. Lives here because the type
     * code lives here, and {@link ChunkV4BlockTable} must not re-derive it from a second table.
     *
     * <p>{@code BOOLEAN} is 0 not because a boolean has no order but because it has no
     * <em>byte</em> order -- {@code BooleanType} normalises any non-zero byte to true -- which is
     * why §5 gives boolean blocks the {@code HAS_FALSE}/{@code HAS_TRUE} flags instead of a min and
     * a max. {@code TEXT} and {@code OPAQUE} are 0 because a variable-width extremum will not fit a
     * fixed-width block entry; §9 lists per-block text dictionary codes as a v5 change precisely
     * because it would change this width.
     */
    public static int statWidth(int typeCode)
    {
        switch (typeCode)
        {
            case TYPE_DOUBLE:
            case TYPE_INT64:
                return 8;
            case TYPE_INT32:
            case TYPE_DATE32:
                return 4;
            case TYPE_BOOLEAN:
            case TYPE_TEXT:
            case TYPE_OPAQUE:
                return 0;
            default:
                throw new IllegalArgumentException(typeCode == TYPE_INVALID
                                                   ? "Corrupt v4 chunk: type code 0x00 is permanently invalid"
                                                   : "Corrupt v4 chunk: unknown column type code " + typeCode);
        }
    }

    /**
     * The only legal {@code blockCount} for a column with this {@code sectionLen}: zero when the
     * section is empty, and otherwise the §2 block count, which is the same for every column and
     * for the timestamp axis.
     *
     * <p>The field is redundant with the header -- and it is written, and checked, anyway. It is
     * the one place the "every axis is cut on the same row boundaries" property of §2 is stated in
     * the bytes rather than merely relied upon, and a column whose block count disagrees with the
     * header would silently address a different row range than its neighbours.
     */
    public static int expectedBlockCount(int sectionLen, int rowCount, int blockSizeLog2)
    {
        if (sectionLen < 0)
            throw new IllegalArgumentException("negative sectionLen " + sectionLen);
        return sectionLen == 0 ? 0 : ChunkV4Header.blockCount(rowCount, blockSizeLog2);
    }

    // -----------------------------------------------------------------------------------------
    // entry
    // -----------------------------------------------------------------------------------------

    /** One column's directory entry (§4). Immutable, and invalid combinations cannot be built. */
    public static final class Entry
    {
        public final int typeCode;
        public final int colFlags;
        public final StatOrder statOrder;
        public final String name;
        public final int sectionOffset;
        public final int sectionLen;
        public final int blockCount;
        /** The repeated value, non-null exactly when {@link #constant()}. Never mutated. */
        public final byte[] constBytes;
        /** Extrema under {@link #statOrder}, both non-null exactly when {@link #hasStats()}. */
        public final byte[] statMin;
        public final byte[] statMax;

        private final byte[] nameBytes;

        public Entry(int typeCode,
                     int colFlags,
                     StatOrder statOrder,
                     String name,
                     int sectionOffset,
                     int sectionLen,
                     int blockCount,
                     byte[] constBytes,
                     byte[] statMin,
                     byte[] statMax)
        {
            // Qualified: Entry declares a no-arg statWidth(), which hides the enclosing class's
            // statWidth(int) from simple-name resolution here.
            ChunkV4Directory.statWidth(typeCode);   // rejects 0x00 and every unallocated code
            if (statOrder == null)
                throw new IllegalArgumentException("column " + name + ": null statOrder");
            if ((colFlags & FLAGS_RESERVED_MASK) != 0)
                throw new IllegalArgumentException("column " + name + ": reserved colFlags bits set in 0x" +
                                                   Integer.toHexString(colFlags) + " (v4 §4)");

            boolean allPresent = (colFlags & FLAG_ALL_PRESENT) != 0;
            boolean allNull = (colFlags & FLAG_ALL_NULL) != 0;
            boolean constant = (colFlags & FLAG_CONSTANT) != 0;
            boolean hasStats = (colFlags & FLAG_HAS_STATS) != 0;

            if (allNull && allPresent)
                throw new IllegalArgumentException("column " + name + ": ALL_NULL and ALL_PRESENT together");
            if (allNull && constant)
                throw new IllegalArgumentException("column " + name + ": ALL_NULL and CONSTANT together");

            this.nameBytes = encodeName(name);

            // §2: a zero-byte section is exactly the O(1) column -- nothing present, or one value
            // repeated across every row. Any other column has rows to store and therefore blocks.
            boolean zeroSection = allNull || (constant && allPresent);
            if (sectionLen < 0 || sectionOffset < 0)
                throw new IllegalArgumentException("column " + name + ": negative section offset/length");
            if (zeroSection != (sectionLen == 0))
                throw new IllegalArgumentException("column " + name + ": sectionLen " + sectionLen +
                                                   " disagrees with flags 0x" + Integer.toHexString(colFlags) +
                                                   " (v4 §2: only ALL_NULL and all-present CONSTANT are 0 bytes)");
            if (sectionLen == 0 && sectionOffset != 0)
                throw new IllegalArgumentException("column " + name + ": empty section at offset " + sectionOffset +
                                                   ", unused fields are zero (v4 §5 rule 6)");
            if ((sectionOffset & 7) != 0 || (sectionLen & 7) != 0)
                throw new IllegalArgumentException("column " + name + ": section " + sectionOffset + '+' + sectionLen +
                                                   " is not 8-aligned (v4 §6)");
            if (blockCount < 0 || (blockCount == 0) != (sectionLen == 0))
                throw new IllegalArgumentException("column " + name + ": blockCount " + blockCount +
                                                   " disagrees with sectionLen " + sectionLen);

            if (constant != (constBytes != null))
                throw new IllegalArgumentException("column " + name + ": CONSTANT flag and constant value disagree");
            // A stat is a pair of extrema under an order. With no order there is nothing to compare
            // them with, so carrying them would be two encodings of one state -- and the OPAQUE
            // rule below reaches every case where the order was invalidated by a downgrade.
            if (hasStats != (statOrder != StatOrder.NONE))
                throw new IllegalArgumentException("column " + name + ": HAS_STATS and statOrder " + statOrder +
                                                   " disagree");
            if (hasStats != (statMin != null) || hasStats != (statMax != null))
                throw new IllegalArgumentException("column " + name + ": HAS_STATS and stat values disagree");
            if (typeCode == TYPE_OPAQUE && statOrder != StatOrder.NONE)
                throw new IllegalArgumentException("column " + name + ": OPAQUE carries statOrder " + statOrder +
                                                   "; §4 forces NONE because an OPAQUE column may be a downgraded " +
                                                   "fixed-width column whose extrema no longer describe its values");
            if ((allNull || constant) && hasStats)
                throw new IllegalArgumentException("column " + name + ": " + (allNull ? "ALL_NULL" : "CONSTANT") +
                                                   " columns carry no statistics (v4 §4)");
            if (hasStats)
            {
                // §7: omitted, never truncated -- a truncated maximum compares below the value it
                // was supposed to bound, which makes the reader skip blocks that do have matching
                // rows. See the class javadoc; this is the rule most likely to be "optimised" away.
                if (statMin.length > MAX_STAT_BYTES || statMax.length > MAX_STAT_BYTES)
                    throw new IllegalArgumentException("column " + name + ": extremum longer than " + MAX_STAT_BYTES +
                                                       " bytes must be omitted, not truncated (v4 §7)");
                if (statOrder.compare(statMin, statMax) > 0)
                    throw new IllegalArgumentException("column " + name + ": min exceeds max under " + statOrder);
            }

            this.typeCode = typeCode;
            this.colFlags = colFlags;
            this.statOrder = statOrder;
            this.name = name;
            this.sectionOffset = sectionOffset;
            this.sectionLen = sectionLen;
            this.blockCount = blockCount;
            this.constBytes = constBytes;
            this.statMin = statMin;
            this.statMax = statMax;
        }

        public boolean allPresent()
        {
            return (colFlags & FLAG_ALL_PRESENT) != 0;
        }

        public boolean allNull()
        {
            return (colFlags & FLAG_ALL_NULL) != 0;
        }

        public boolean constant()
        {
            return (colFlags & FLAG_CONSTANT) != 0;
        }

        public boolean hasStats()
        {
            return (colFlags & FLAG_HAS_STATS) != 0;
        }

        /** §5 block-entry width implied by this column's type code. */
        public int statWidth()
        {
            return ChunkV4Directory.statWidth(typeCode);
        }

        /** Serialized length of this entry, before the region's pad to 8. */
        public int encodedLength()
        {
            int length = ENTRY_FIXED_BYTES + nameBytes.length;
            if (constant())
                length += BlockPresence.varIntLength(constBytes.length) + constBytes.length;
            if (hasStats())
                length += BlockPresence.varIntLength(statMin.length) + statMin.length
                          + BlockPresence.varIntLength(statMax.length) + statMax.length;
            return length;
        }

        private void write(ByteBuffer dst)
        {
            dst.put((byte) typeCode);
            dst.put((byte) colFlags);
            dst.put((byte) statOrder.code);
            dst.put((byte) nameBytes.length);
            dst.put(nameBytes);
            dst.putInt(sectionOffset);
            dst.putInt(sectionLen);
            dst.putInt(blockCount);
            if (constant())
            {
                BlockPresence.writeVarInt(dst, constBytes.length);
                dst.put(constBytes);
            }
            if (hasStats())
            {
                BlockPresence.writeVarInt(dst, statMin.length);
                dst.put(statMin);
                BlockPresence.writeVarInt(dst, statMax.length);
                dst.put(statMax);
            }
        }
    }

    // -----------------------------------------------------------------------------------------
    // accessors
    // -----------------------------------------------------------------------------------------

    public int size()
    {
        return entries.size();
    }

    public Entry entry(int index)
    {
        return entries.get(index);
    }

    /** The entries in natural {@code String} name order, unmodifiable. */
    public List<Entry> entries()
    {
        return entries;
    }

    /**
     * The entry for {@code name}, or null. A binary search over the sorted list rather than a map
     * lookup: §5 rule 7 bans hash iteration from anything that reaches the bytes, and keeping the
     * directory a sorted sequence end to end means no code path can grow a hash-ordered view of it.
     */
    public Entry column(String name)
    {
        int low = 0;
        int high = entries.size() - 1;
        while (low <= high)
        {
            int mid = (low + high) >>> 1;
            int cmp = entries.get(mid).name.compareTo(name);
            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return entries.get(mid);
        }
        return null;
    }

    // -----------------------------------------------------------------------------------------
    // write
    // -----------------------------------------------------------------------------------------

    /** Total directory bytes for these entries, including the §6 pad to 8. */
    public static int encodedLength(List<Entry> entries)
    {
        long total = 0;
        for (Entry entry : entries)
            total += entry.encodedLength();
        long padded = BitPacking.roundUpToMultipleOf8(total);
        if (padded > Integer.MAX_VALUE)
            throw new IllegalArgumentException("directory of " + padded + " bytes overflows int");
        return (int) padded;
    }

    /**
     * Writes the whole directory region at {@code dst}'s position, advancing it by
     * {@link #encodedLength} and returning that count.
     *
     * <p>{@code rowCount} and {@code blockSizeLog2} come from the header being built alongside;
     * they are here so every entry's {@code blockCount} is checked against the §2 derivation at the
     * moment it is written, not only when a replica reads it back.
     *
     * <p>The entry order is verified rather than imposed. §5 rule 1 requires natural {@code String}
     * order, and names the exact trap that produces the wrong one: {@code new TreeMap<>(map)}
     * <em>adopts the source map's comparator</em> instead of forcing natural order, so a caller who
     * used that constructor gets a plausible, stable, wrong order and a chunk that no other replica
     * reproduces byte for byte. Sorting silently here would hide it; failing here names it.
     */
    public static int write(List<Entry> entries, int rowCount, int blockSizeLog2, ByteBuffer dst)
    {
        int length = encodedLength(entries);
        requireBigEndian(dst);
        if (dst.remaining() < length)
            throw new IllegalArgumentException("destination holds " + dst.remaining() + " < directory " + length);
        if (entries.size() > ChunkV4Header.MAX_COLUMNS)
            throw new IllegalArgumentException("too many columns: " + entries.size());

        for (int i = 0; i < entries.size(); i++)
        {
            Entry entry = entries.get(i);
            if (i > 0 && entries.get(i - 1).name.compareTo(entry.name) >= 0)
                throw new IllegalArgumentException("directory entries out of natural String order at " + i + ": '" +
                                                   entries.get(i - 1).name + "' then '" + entry.name +
                                                   "' (v4 §5 rule 1)");
            int expected = expectedBlockCount(entry.sectionLen, rowCount, blockSizeLog2);
            if (entry.blockCount != expected)
                throw new IllegalArgumentException("column " + entry.name + ": blockCount " + entry.blockCount +
                                                   ", header implies " + expected);
        }

        int base = dst.position();
        for (Entry entry : entries)
            entry.write(dst);
        // Explicitly zero-filled: §5 rule 5 exists because an encoder writing into a reused scratch
        // buffer leaks stale bytes into padding, produces byte-different output for identical
        // input -- and leaves every round-trip test green.
        while (dst.position() - base < length)
            dst.put((byte) 0);
        assert dst.position() - base == length : "wrote " + (dst.position() - base) + ", sized " + length;
        return length;
    }

    // -----------------------------------------------------------------------------------------
    // read
    // -----------------------------------------------------------------------------------------

    /**
     * Parses the directory of {@code payload} -- which must span exactly one chunk from position to
     * limit -- using the already-validated {@code header} for the region bounds, the column count
     * and the §2 block count. Neither buffer is modified.
     *
     * <p>Parsing runs inside a slice limited to the directory region, so "bytes remaining" always
     * means "bytes remaining in the directory": a corrupt length cannot walk into a section, and
     * every allocation is bounded by that remainder before it is made.
     */
    public static ChunkV4Directory read(ByteBuffer payload, ChunkV4Header header)
    {
        long payloadLength = payload.remaining();
        int start = payload.position() + header.directoryOffset();
        ByteBuffer region = payload.duplicate();
        region.order(ByteOrder.BIG_ENDIAN);
        region.position(start);
        region.limit(start + header.directoryLen);

        List<Entry> entries = new ArrayList<>(Math.min(header.columnCount, 1024));
        String previousName = null;
        for (int i = 0; i < header.columnCount; i++)
        {
            Entry entry = readEntry(region, header, payloadLength, i);
            // Strictly ascending catches a reordered directory and a duplicated column in one test,
            // and it is what makes column() a binary search rather than a scan.
            if (previousName != null && previousName.compareTo(entry.name) >= 0)
                throw new IllegalArgumentException("Corrupt v4 chunk: directory names out of natural String order " +
                                                   "at entry " + i + ": '" + previousName + "' then '" + entry.name +
                                                   '\'');
            previousName = entry.name;
            entries.add(entry);
        }

        int consumed = region.position() - start;
        long expected = BitPacking.roundUpToMultipleOf8(consumed);
        // Not a bounds check: a directory trailed by extra zeroes would decode identically and
        // re-encode shorter, so one directory would have two byte forms and chunkUnchanged would
        // report a difference forever (§5).
        if (expected != header.directoryLen)
            throw new IllegalArgumentException("Corrupt v4 chunk: directoryLen " + header.directoryLen +
                                               " but " + header.columnCount + " entries occupy " + consumed +
                                               " bytes, padded to " + expected);
        while (region.hasRemaining())
            if (region.get() != 0)
                throw new IllegalArgumentException("Corrupt v4 chunk: non-zero directory padding at byte " +
                                                   (region.position() - 1 - start) + " (v4 §5 rule 5)");
        return new ChunkV4Directory(entries);
    }

    private static Entry readEntry(ByteBuffer region, ChunkV4Header header, long payloadLength, int index)
    {
        require(region, 4, index, "entry header");
        int typeCode = region.get() & 0xFF;
        int colFlags = region.get() & 0xFF;
        int statOrderCode = region.get() & 0xFF;
        int nameLen = region.get() & 0xFF;
        // A nameless column cannot be looked up and cannot have come from a schema; rejecting it
        // also keeps the strictly-ascending name check from having to reason about duplicates of "".
        if (nameLen < 1)
            throw new IllegalArgumentException("Corrupt v4 chunk: entry " + index + " has an empty column name");
        require(region, nameLen, index, "column name");
        byte[] nameBytes = new byte[nameLen];
        region.get(nameBytes);
        String name = decodeName(nameBytes, index);

        require(region, 12, index, "section fields");
        long sectionOffset = region.getInt() & 0xFFFFFFFFL;
        long sectionLen = region.getInt() & 0xFFFFFFFFL;
        long blockCount = region.getInt() & 0xFFFFFFFFL;
        if (sectionOffset + sectionLen > payloadLength)
            throw new IllegalArgumentException("Corrupt v4 chunk: column " + name + " section ends at " +
                                               (sectionOffset + sectionLen) + ", payload holds " + payloadLength);
        if (sectionLen != 0 && sectionOffset < header.sectionsFrom())
            throw new IllegalArgumentException("Corrupt v4 chunk: column " + name + " section starts at " +
                                               sectionOffset + ", inside the header or directory which end at " +
                                               header.sectionsFrom());
        // The timestamp axis is a section like any other and cannot share bytes with a column's.
        // Full pairwise disjointness across columns belongs to the section parser, which walks them
        // in offset order; this catches the one overlap that is checkable from a single entry.
        if (sectionLen != 0 && header.tsSectionLen != 0
            && sectionOffset < header.tsSectionOffset + (long) header.tsSectionLen
            && header.tsSectionOffset < sectionOffset + sectionLen)
            throw new IllegalArgumentException("Corrupt v4 chunk: column " + name + " section " + sectionOffset + '+' +
                                               sectionLen + " overlaps the timestamp section " +
                                               header.tsSectionOffset + '+' + header.tsSectionLen);
        // sectionLen is already known to fit inside the payload, so the narrowing cast is safe.
        int expectedBlocks = expectedBlockCount((int) sectionLen, header.rowCount, header.blockSizeLog2);
        if (blockCount != expectedBlocks)
            throw new IllegalArgumentException("Corrupt v4 chunk: column " + name + " declares blockCount " +
                                               blockCount + ", header implies " + expectedBlocks);

        byte[] constBytes = null;
        if ((colFlags & FLAG_CONSTANT) != 0)
            constBytes = readLengthPrefixed(region, index, "constant value", Integer.MAX_VALUE);
        byte[] statMin = null;
        byte[] statMax = null;
        if ((colFlags & FLAG_HAS_STATS) != 0)
        {
            statMin = readLengthPrefixed(region, index, "stat min", MAX_STAT_BYTES);
            statMax = readLengthPrefixed(region, index, "stat max", MAX_STAT_BYTES);
        }

        return new Entry(typeCode, colFlags, StatOrder.forCode(statOrderCode), name,
                         (int) sectionOffset, (int) sectionLen, (int) blockCount, constBytes, statMin, statMax);
    }

    /**
     * Reads a canonical varint length followed by that many bytes, never allocating more than the
     * directory region still holds. {@link BlockPresence#readVarInt} rejects a non-minimal encoding,
     * which matters here for the same reason it matters in a block body: a length with two byte
     * forms gives the directory two byte forms.
     */
    private static byte[] readLengthPrefixed(ByteBuffer region, int index, String what, int max)
    {
        long length = BlockPresence.readVarInt(region);
        if (length > max)
            throw new IllegalArgumentException("Corrupt v4 chunk: entry " + index + ' ' + what + " is " + length +
                                               " bytes, limit " + max);
        // Checked before the array exists, against the region and not against the heap: this is the
        // step that turns a corrupt length into an exception instead of an OutOfMemoryError.
        if (length > region.remaining())
            throw new IllegalArgumentException("Corrupt v4 chunk: entry " + index + ' ' + what + " claims " + length +
                                               " bytes, directory holds " + region.remaining());
        byte[] value = new byte[(int) length];
        region.get(value);
        return value;
    }

    private static void require(ByteBuffer region, int bytes, int index, String what)
    {
        if (region.remaining() < bytes)
            throw new IllegalArgumentException("Corrupt v4 chunk: truncated directory entry " + index + ' ' + what +
                                               ", need " + bytes + " bytes, have " + region.remaining());
    }

    // -----------------------------------------------------------------------------------------
    // names
    // -----------------------------------------------------------------------------------------

    private static byte[] encodeName(String name)
    {
        if (name == null)
            throw new IllegalArgumentException("null column name");
        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        if (bytes.length < 1 || bytes.length > MAX_NAME_BYTES)
            throw new IllegalArgumentException("column name '" + name + "' encodes to " + bytes.length +
                                               " bytes, limit 1.." + MAX_NAME_BYTES);
        // getBytes() substitutes for an unpaired surrogate instead of failing, so a name that is not
        // representable would round-trip to a *different* name -- a renamed column, silently. The
        // re-decode costs one allocation per column at encode time and makes that impossible.
        if (!decodeName(bytes, -1).equals(name))
            throw new IllegalArgumentException("column name '" + name + "' does not survive UTF-8 encoding");
        return bytes;
    }

    /**
     * Strict UTF-8. {@code new String(bytes, UTF_8)} would substitute U+FFFD for malformed input,
     * so two different byte sequences would decode to one column name and the directory would have
     * two byte forms for one state -- the same ambiguity §5 rule 4 removes from varints.
     */
    private static String decodeName(byte[] bytes, int index)
    {
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                                                       .onMalformedInput(CodingErrorAction.REPORT)
                                                       .onUnmappableCharacter(CodingErrorAction.REPORT);
        try
        {
            return decoder.decode(ByteBuffer.wrap(bytes)).toString();
        }
        catch (CharacterCodingException e)
        {
            throw new IllegalArgumentException("Corrupt v4 chunk: entry " + index + " column name is not valid UTF-8: "
                                               + Arrays.toString(bytes), e);
        }
    }

    private static void requireBigEndian(ByteBuffer buffer)
    {
        if (buffer.order() != ByteOrder.BIG_ENDIAN)
            throw new IllegalArgumentException("v4 directory fields are big-endian, buffer order is " +
                                               buffer.order());
    }
}
