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

/**
 * The one place that decides which chunk version bytes name a real format and which name nothing:
 * {@link #unsupportedVersion} builds the rejection for every version byte the live decoders will
 * not read, and the columnar codecs ({@link ColumnarChunkCodec}, {@link ChunkV4Codec}) all route
 * through it so the classification cannot drift between entry points.
 * <p>
 * This class used to be the version-dispatching entry point over the single-column chunk formats
 * (gorilla v1, then chimp128 v2). Those codecs are gone -- the columnar format (version 4,
 * {@link ChunkV4Codec} behind {@link ColumnarChunkCodec}'s entry points) is the only one written
 * and read -- but their version bytes are never reassigned and must keep being recognised
 * <em>as formats</em>: a payload carrying one is a real chunk this build cannot decode, which is
 * systematic and must propagate ({@link UnsupportedChunkFormatException}), not be skipped as one
 * corrupt chunk.
 */
public final class ChunkCodecs
{
    /**
     * Version byte 1 was the gorilla single-column format, removed when chimp128 became the only
     * chunk codec. Never reassigned: it is recognised here purely so a chunk written by that build
     * is reported as an unreadable format rather than as random corruption.
     */
    static final byte GORILLA_VERSION_REMOVED = 1;

    /**
     * Version byte 2 was the chimp128 single-column format, the last codec this class dispatched
     * to. Its encoder and decoder were deleted together with the v3 columnar implementation (no
     * production deployment ever wrote a single-column chunk), but the
     * byte still names a known format and is never reassigned.
     */
    static final byte CHIMP128_V2_VERSION_REMOVED = 2;

    /**
     * Version byte 3 was the delta-of-delta/RLE columnar format, replaced outright by the
     * block-based v4 layout (v4 spec §0, §10) before any production table used it, so no v3 read
     * path was kept. Never reassigned: a v3 payload must be reported as an
     * unreadable <em>format</em> -- systematic, propagated, never swallowed -- rather than skipped
     * as one corrupt chunk. Without this branch a v3 payload would fall through to the
     * names-no-format {@link IllegalArgumentException} below, and the read path would silently skip
     * every chunk a v3 build wrote.
     */
    static final byte COLUMNAR_V3_VERSION_REMOVED = 3;

    private ChunkCodecs()
    {
    }

    /**
     * Builds the exception for a version byte this decoder will not read, choosing its <em>type</em>
     * by whether the byte names a format at all.
     * <p>
     * A byte naming a known format (1, 2, 3, 4) means the payload is real and this build cannot
     * decode it -- systematic, affecting every chunk written that way, so it is an
     * {@link UnsupportedChunkFormatException} that callers must not swallow. A byte naming nothing
     * is exactly what a header corrupted in its first byte looks like, so it stays a plain
     * {@link IllegalArgumentException} and the read path keeps skipping it as one bad chunk. There
     * is no way to tell a scrambled byte from a hypothetical future version, and the availability
     * trade-off SP3 chose for corruption is the right default for the ambiguous case.
     */
    static RuntimeException unsupportedVersion(byte version, String what)
    {
        if (version == GORILLA_VERSION_REMOVED)
            return new UnsupportedChunkFormatException(
                "Unsupported " + what + " version: 1 -- version 1 was the gorilla single-column format, removed " +
                "when chimp128 became the only chunk codec; chunks written by that build cannot be read by this one");
        if (version == COLUMNAR_V3_VERSION_REMOVED)
            return new UnsupportedChunkFormatException(
                "Unsupported " + what + " version: 3 -- version 3 was the columnar chunk format replaced outright " +
                "by the block-based v4 layout, and no v3 read path was kept (no v3 chunk " +
                "should exist in production); chunks written by a v3 build cannot be read by this one");
        if (version == CHIMP128_V2_VERSION_REMOVED || version == ColumnarChunkCodec.VERSION)
            return new UnsupportedChunkFormatException(
                "Unsupported " + what + " version: " + version + " -- that is a known chunk format, but not one " +
                "this decoder reads");
        return new IllegalArgumentException("Unsupported " + what + " version: " + version +
                                            " -- names no known chunk format (treated as a corrupt payload)");
    }
}
