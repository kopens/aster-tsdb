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

import java.util.Arrays;

/**
 * ALP -- Adaptive Lossless floating-Point (Afroozeh, Kuffo, Boncz, SIGMOD 2024) -- the planning and
 * value math behind the chunk format's only double encodings, v4's {@code 0x20 ALP} and
 * {@code 0x21 ALP_RD} blocks. {@link AlpBlockCodec} owns the v4 byte layout and calls in here for
 * every decision that two replicas must make identically; this class's own v3 container framing
 * (sub-format byte, LEB128 varints, gap-coded exceptions, MSB-first bitstream) was deleted with the
 * rest of the v3 read/write path.
 *
 * <p><b>Two variants.</b>
 * <ul>
 *   <li><i>Decimal</i> ALP. Most doubles produced by measurement are decimals of few digits
 *       ({@code 20.76}, {@code 157.0}). Each is re-expressed as the integer
 *       {@code i = round(v * 10^e * 10^-f)}, which decodes back as {@code i * 10^f * 10^-e}; the
 *       integers are frame-of-reference'd against the block minimum and bit-packed at the width the
 *       block needs. Any value that does not survive that round trip <em>bit for bit</em> is an
 *       exception, stored verbatim as its 64-bit pattern next to its row position.</li>
 *   <li>ALP <i>Real Double</i>, for values that are not decimal-like. Each 64-bit pattern is cut
 *       into a {@code leftBitWidth}-bit high part and a {@code 64 - leftBitWidth}-bit low part. The
 *       high parts of a real column take few distinct values (same sign, same or adjacent binades),
 *       so they are dictionary-coded; the low part is stored verbatim. Rows whose high part missed
 *       the dictionary carry it explicitly.</li>
 * </ul>
 *
 * <p><b>Losslessness is structural, not statistical.</b> Neither variant can lose a bit:
 * decimal ALP verifies every value against {@code Double.doubleToRawLongBits} and demotes any
 * mismatch to a verbatim exception, and ALP-RD reassembles {@code (left << rightBitWidth) | right}
 * from parts that were both stored exactly. {@code -0.0}, every NaN payload (quiet and signalling),
 * the infinities, subnormals, {@code MIN_VALUE} and {@code MAX_VALUE} therefore all survive; the
 * decimal path simply refuses them and pays 8 bytes each, and ALP-RD never even inspects them as
 * numbers. Nothing here converts a {@code long} bit pattern to a {@code double} and back as a way of
 * carrying it: the value pipeline is {@code long} raw bits end to end, and the {@code double} view
 * exists only inside the decimal arithmetic, whose result is checked against the original bits.
 *
 * <p><b>Determinism.</b> Two replicas encoding the same window must produce byte-identical payloads,
 * so every choice made here is a pure function of the input array:
 * <ul>
 *   <li>The {@code (e, f)} search samples with a <b>fixed integer stride</b> (never an RNG), scores
 *       all {@code 0 <= f <= e <= 18} pairs, and keeps the best {@link #CANDIDATE_LIMIT} under the
 *       strict total order (estimated bits asc, {@code e} asc, {@code f} asc). Those candidates are
 *       then measured exactly over the whole block by the caller.</li>
 *   <li>The ALP-RD dictionary is the top {@link #RD_MAX_DICTIONARY} high parts under (frequency
 *       desc, value asc) -- a total order over a histogram held in an <b>array</b>, never a
 *       {@code HashMap} whose iteration order is unspecified.</li>
 *   <li>The only floating-point operations are {@code *} and {@link Math#rint}. Java multiplication
 *       is IEEE-754 correctly rounded and (since Java 17) unconditionally strict -- no x87 extended
 *       precision, and Java never contracts {@code a*b+c} into an FMA -- while {@code Math.rint}'s
 *       specification admits exactly one result for every input, with no ulp allowance of the kind
 *       {@code Math.sin}/{@code pow} carry. Both are therefore reproducible across JVMs and CPUs.
 *       <b>Any future use of a transcendental, of {@code Math.round}'s half-up rule, or of
 *       {@code strictfp}-sensitive code here must be treated as a determinism regression.</b></li>
 * </ul>
 *
 * <p><b>The class is declared {@code strictfp} even though, from Java 17, that is redundant.</b>
 * chunk-format-v4 §5 rule 8 names {@code strictfp} on ALP as a rule and §12 names a non-strict
 * baseline as a concrete risk; the modifier costs nothing, and it is the only thing that keeps the
 * guarantee true if this arithmetic is ever moved to a class or a baseline where it is not free.
 * The value math -- {@link #tryEncode} and {@link #decodeOne} -- lives here and nowhere else, so
 * this is the class the modifier has to be on.
 */
// The modifier is redundant from Java 17 and javac says so; §5 rule 8 asks for it anyway, so the
// note is suppressed rather than the rule dropped. See the class javadoc.
@SuppressWarnings("strictfp")
final strictfp class AlpCodec
{
    /**
     * Largest {@code e}/{@code f} considered. 10^18 is the largest power of ten below
     * {@code Long.MAX_VALUE} and the bound the ALP paper uses; 10^e is also exactly representable as
     * a {@code double} for every {@code e <= 22}, so no table entry is itself an approximation.
     */
    static final int MAX_EXPONENT = 18;

    /**
     * Scaled integers are held to {@code |i| <= 2^53}, the range over which {@code long} and
     * {@code double} represent the same integers exactly. Outside it the {@code (long)} cast and the
     * widening back to {@code double} stop being inverses, so a value that scales past this is an
     * exception rather than a silently lossy encode. It also bounds the frame-of-reference span at
     * {@code 2^54}, hence {@link #MAX_BIT_WIDTH}.
     */
    static final long ENCODED_LIMIT = 1L << 53;
    private static final double ENCODED_LIMIT_D = (double) ENCODED_LIMIT;

    /** {@code max - min <= 2^54} for encoded integers within {@link #ENCODED_LIMIT}, so 55 bits suffice. */
    static final int MAX_BIT_WIDTH = 55;

    /** Returned by {@link #tryEncode} for a value the decimal path cannot represent exactly. */
    static final long NOT_ENCODABLE = Long.MIN_VALUE;

    /** ALP-RD cuts at most 16 bits off the top, so an exception's high part always fits in 2 bytes. */
    static final int RD_MAX_LEFT_BITS = 16;
    /** Dictionary entries; 8 is the paper's bound and keeps the per-row code at most 3 bits. */
    static final int RD_MAX_DICTIONARY = 8;

    /** Values inspected when scoring {@code (e, f)} pairs. Sampled by fixed stride -- never randomly. */
    static final int SAMPLE_LIMIT = 64;
    /** {@code (e, f)} pairs promoted from the sample to an exact whole-block measurement. */
    static final int CANDIDATE_LIMIT = 5;

    /** 10^i for i in [0, 18]. Every entry is exact: 10^i is representable for i <= 22. */
    private static final double[] POW10 =
    {
        1.0E0, 1.0E1, 1.0E2, 1.0E3, 1.0E4, 1.0E5, 1.0E6, 1.0E7, 1.0E8, 1.0E9,
        1.0E10, 1.0E11, 1.0E12, 1.0E13, 1.0E14, 1.0E15, 1.0E16, 1.0E17, 1.0E18
    };

    /**
     * 10^-i for i in [0, 18]. Only 10^0 is exact; the rest are the correctly-rounded double nearest
     * the true value, which is what makes them deterministic (a Java decimal literal has exactly one
     * legal {@code double} value). ALP multiplies by these rather than dividing by {@link #POW10}
     * deliberately -- it is what the reference implementation does, and the round trip is verified
     * bit-for-bit afterwards regardless, so the approximation costs recall, never correctness.
     */
    private static final double[] INV_POW10 =
    {
        1.0E0, 1.0E-1, 1.0E-2, 1.0E-3, 1.0E-4, 1.0E-5, 1.0E-6, 1.0E-7, 1.0E-8, 1.0E-9,
        1.0E-10, 1.0E-11, 1.0E-12, 1.0E-13, 1.0E-14, 1.0E-15, 1.0E-16, 1.0E-17, 1.0E-18
    };

    private AlpCodec()
    {
    }

    /**
     * The scaled integer representing {@code raw} under {@code (e, f)}, or {@link #NOT_ENCODABLE}
     * when it cannot be represented exactly.
     * <p>
     * The acceptance test is {@code Double.doubleToRawLongBits(decode(encode(v))) == raw} -- bit
     * equality, not {@code ==} on doubles. That distinction is the whole edge-case story:
     * {@code 0.0 == -0.0} is true but their patterns differ, so {@code -0.0} is correctly refused
     * here and stored verbatim; and NaN is refused twice over, once by the range guard (every
     * comparison against NaN is false, so the negated test fires) and once by bit inequality.
     */
    static long tryEncode(long raw, int e, int f)
    {
        double value = Double.longBitsToDouble(raw);
        double scaled = value * POW10[e] * INV_POW10[f];
        // Written negated so NaN and both infinities fall out here rather than reaching the cast,
        // where (long) saturates at Long.MAX_VALUE/MIN_VALUE and would fabricate a huge frame.
        if (!(scaled >= -ENCODED_LIMIT_D && scaled <= ENCODED_LIMIT_D))
            return NOT_ENCODABLE;
        long encoded = (long) Math.rint(scaled);
        return Double.doubleToRawLongBits(decodeOne(encoded, e, f)) == raw ? encoded : NOT_ENCODABLE;
    }

    /** The value a scaled integer stands for: {@code i * 10^f * 10^-e}. Never NaN or infinite. */
    static double decodeOne(long encoded, int e, int f)
    {
        return encoded * POW10[f] * INV_POW10[e];
    }

    /**
     * Stage 1 of the {@code (e, f)} search: score every {@code 0 <= f <= e <= 18} pair on a
     * fixed-stride sample and return the best {@link #CANDIDATE_LIMIT} under the strict total order
     * (estimated bits asc, {@code e} asc, {@code f} asc), written into {@code candidateE}/
     * {@code candidateF} in that order.
     *
     * <p><b>This is the only implementation of the search, on purpose.</b> {@code AlpBlockCodec}
     * measures these candidates exactly under the v4 block layout's cost function, but the candidate
     * set itself is the delicate part: it is where the sampling rule, the fixed stride and the
     * tie-break live, and §12 names an unspecified {@code (e, f)} tie-break as a concrete
     * determinism risk. A second copy of this loop is how two replicas start disagreeing about
     * which exponent pair a block uses, which is a byte difference, which is every chunk rewritten
     * every cycle.
     *
     * @return the number of candidates written, always at least one
     */
    static int exponentCandidates(long[] rawBits, int count, int[] candidateE, int[] candidateF)
    {
        // `ceil` rather than `floor` so the sample spans the whole block for every count, instead of
        // clustering at the head for counts just above SAMPLE_LIMIT.
        int stride = Math.max(1, (count + SAMPLE_LIMIT - 1) / SAMPLE_LIMIT);
        long[] candidateScore = new long[CANDIDATE_LIMIT];
        int candidates = 0;

        for (int e = 0; e <= MAX_EXPONENT; e++)
        {
            for (int f = 0; f <= e; f++)
            {
                long minimum = Long.MAX_VALUE;
                long maximum = Long.MIN_VALUE;
                int sampled = 0;
                int exceptions = 0;
                for (int i = 0; i < count && sampled < SAMPLE_LIMIT; i += stride, sampled++)
                {
                    long encoded = tryEncode(rawBits[i], e, f);
                    if (encoded == NOT_ENCODABLE)
                    {
                        exceptions++;
                    }
                    else
                    {
                        minimum = Math.min(minimum, encoded);
                        maximum = Math.max(maximum, encoded);
                    }
                }
                int width = exceptions == sampled ? 0 : bitWidthOf(maximum - minimum);
                // Estimated bits: packed payload plus the 64-bit value and ~16-bit position every
                // exception costs. Only used to rank candidates -- the winner is re-measured exactly.
                long score = (long) (sampled - exceptions) * width + (long) exceptions * (64 + 16);
                candidates = offer(candidateE, candidateF, candidateScore, candidates, e, f, score);
            }
        }
        return candidates;
    }

    /**
     * Inserts {@code (e, f, score)} into the descending-quality candidate list, keeping it sorted by
     * the strict total order (score asc, e asc, f asc). Called with {@code e}/{@code f} already
     * ascending, so the {@code <} test alone realises that order.
     *
     * @return the new candidate count
     */
    private static int offer(int[] es, int[] fs, long[] scores, int size, int e, int f, long score)
    {
        int position = size;
        while (position > 0 && score < scores[position - 1])
            position--;
        if (position >= CANDIDATE_LIMIT)
            return size;
        for (int i = Math.min(size, CANDIDATE_LIMIT - 1); i > position; i--)
        {
            es[i] = es[i - 1];
            fs[i] = fs[i - 1];
            scores[i] = scores[i - 1];
        }
        es[position] = e;
        fs[position] = f;
        scores[position] = score;
        return Math.min(size + 1, CANDIDATE_LIMIT);
    }

    /**
     * The ALP-RD dictionary for one {@code leftBits} cut: the top {@link #RD_MAX_DICTIONARY} left
     * parts under (frequency desc, value asc), written into {@code dictionary} in <b>that</b> order,
     * i.e. most frequent first.
     *
     * <p>Selection scans the histogram array in ascending value order and only displaces an
     * incumbent on a STRICTLY greater frequency, which realises that total order exactly -- and,
     * being an array scan, cannot inherit a hash table's unspecified iteration order (§5 rule 7).
     *
     * <p><b>This is the only implementation of the dictionary, on purpose</b>, for the reason given
     * on {@link #exponentCandidates}: the v4 block layer frames and costs these bytes itself
     * ({@code AlpBlockCodec} re-sorts the entries into ascending value order for §5 rule 2), but
     * <em>which</em> left parts are in the dictionary must be decided in exactly one place, or two
     * replicas encoding the same block disagree byte for byte.
     *
     * @param histogram scratch of at least {@code 1 << leftBits} entries; zeroed here, not by the
     *                  caller, so a reused buffer cannot leak a previous cut's counts into this one
     * @return the dictionary size, at least 1 whenever {@code count >= 1}
     */
    static int selectRdDictionary(long[] rawBits, int count, int leftBits, int[] histogram, int[] dictionary)
    {
        int rightBitWidth = Long.SIZE - leftBits;
        int slots = 1 << leftBits;
        Arrays.fill(histogram, 0, slots, 0);
        for (int i = 0; i < count; i++)
            histogram[(int) (rawBits[i] >>> rightBitWidth)]++;

        int[] frequencies = new int[RD_MAX_DICTIONARY];
        int dictionarySize = 0;
        for (int value = 0; value < slots; value++)
        {
            int frequency = histogram[value];
            if (frequency == 0)
                continue;
            int position = dictionarySize;
            while (position > 0 && frequency > frequencies[position - 1])
                position--;
            if (position >= RD_MAX_DICTIONARY)
                continue;
            for (int i = Math.min(dictionarySize, RD_MAX_DICTIONARY - 1); i > position; i--)
            {
                dictionary[i] = dictionary[i - 1];
                frequencies[i] = frequencies[i - 1];
            }
            dictionary[position] = value;
            frequencies[position] = frequency;
            dictionarySize = Math.min(dictionarySize + 1, RD_MAX_DICTIONARY);
        }
        return dictionarySize;
    }

    /** Bits needed to address {@code dictionarySize} codes; 0 for a single-entry dictionary. */
    static int codeBitsFor(int dictionarySize)
    {
        return dictionarySize <= 1 ? 0 : Integer.SIZE - Integer.numberOfLeadingZeros(dictionarySize - 1);
    }

    /** Bits needed to hold {@code span} as an unsigned value; 0 when the span is 0. */
    static int bitWidthOf(long span)
    {
        return Long.SIZE - Long.numberOfLeadingZeros(span);
    }
}
