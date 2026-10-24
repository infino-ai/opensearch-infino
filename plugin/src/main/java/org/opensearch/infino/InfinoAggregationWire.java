/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.common.Rounding;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.LongBounds;
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;
import org.opensearch.search.aggregations.metrics.AbstractHyperLogLogPlusPlus;
import org.opensearch.search.aggregations.metrics.InternalCardinality;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

/**
 * Builds the OpenSearch aggregation objects whose constructors are not
 * public, through their public stream constructors: the bytes are written
 * exactly as the object's own {@code writeTo} would write them, then read
 * back. The plugin is built against the exact OpenSearch version it runs
 * in, so the wire layout is the one that version reads.
 */
final class InfinoAggregationWire {
    private InfinoAggregationWire() {}

    /** HyperLogLog++ precision OpenSearch uses by default: 2^14 registers. */
    private static final int HLL_PRECISION = 14;
    private static final int HLL_REGISTERS = 1 << HLL_PRECISION;
    /**
     * Distinct values the linear-counting form holds before OpenSearch
     * switches the sketch to registers: a quarter of the registers' bytes as
     * ints, at the hash set's load factor.
     */
    private static final int LINEAR_COUNTING_MAX = (int) ((HLL_REGISTERS / 4) * 0.75);
    /** The linear-counting hash space is 2^25 wide; a hash's top 25 bits index it. */
    private static final int LINEAR_COUNTING_BITS = 25;
    /** Iterations of the register search; the estimator is monotone in the register split. */
    private static final int REGISTER_SEARCH_STEPS = 24;

    /** An {@code InternalDateHistogram}, as the reduce phase expects one from a shard. */
    static InternalDateHistogram dateHistogram(
        String name,
        Map<String, Object> metadata,
        List<InternalDateHistogram.Bucket> buckets,
        long minDocCount,
        long offset,
        Rounding rounding,
        InternalAggregations emptySubAggregations,
        LongBounds bounds,
        DocValueFormat format,
        boolean keyed
    ) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            // InternalAggregation.writeTo
            out.writeString(name);
            out.writeGenericValue(metadata);
            // InternalDateHistogram.doWriteTo
            InternalOrder.Streams.writeHistogramOrder(BucketOrder.key(true), out, false);
            out.writeVLong(minDocCount);
            if (minDocCount == 0) {
                // EmptyBucketInfo.writeTo
                rounding.writeTo(out);
                (emptySubAggregations == null ? InternalAggregations.EMPTY : emptySubAggregations).writeTo(out);
                out.writeOptionalWriteable(bounds);
            }
            out.writeLong(offset);
            out.writeNamedWriteable(format);
            out.writeBoolean(keyed);
            out.writeList(buckets);
            try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry())) {
                return new InternalDateHistogram(in);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** One bucket of a double-keyed terms aggregation, before it is written. */
    record DoubleBucket(double key, long docCount, InternalAggregations subAggregations) {}

    /**
     * A {@code DoubleTerms}, as the reduce phase expects one from a shard. Its
     * bucket class is not public in every OpenSearch release the plugin
     * builds against, so the object comes through its stream constructor.
     */
    static DoubleTerms doubleTerms(
        String name,
        Map<String, Object> metadata,
        BucketOrder order,
        int requiredSize,
        long minDocCount,
        long otherDocCount,
        List<DoubleBucket> buckets
    ) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            // InternalAggregation.writeTo
            out.writeString(name);
            out.writeGenericValue(metadata);
            // InternalTerms.doWriteTo: reduce order, order, required size, min doc count
            order.writeTo(out);
            order.writeTo(out);
            out.writeVInt(requiredSize);
            out.writeVLong(minDocCount);
            // InternalMappedTerms.writeTermTypeInfoTo
            out.writeZLong(0L); // doc count error
            out.writeNamedWriteable(DocValueFormat.RAW);
            out.writeVInt(requiredSize); // shard size
            out.writeBoolean(false); // show term doc count error
            out.writeVLong(otherDocCount);
            out.writeVInt(buckets.size());
            for (DoubleBucket b : buckets) {
                // InternalTerms.Bucket.writeTo, then DoubleTerms.Bucket.writeTermTo
                out.writeVLong(b.docCount());
                (b.subAggregations() == null ? InternalAggregations.EMPTY : b.subAggregations()).writeTo(out);
                out.writeDouble(b.key());
            }
            try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry())) {
                return new DoubleTerms(in);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * An {@code InternalCardinality} whose sketch estimates {@code count}.
     *
     * <p>OpenSearch's unique count is a HyperLogLog++ sketch, and the reduce
     * phase merges sketches, so the shard must hand back one; the engine
     * counts distinct values exactly. A count the linear-counting form can
     * hold is written as that many distinct entries, which the sketch reads
     * back exactly. A larger count is written as registers chosen so that
     * OpenSearch's own estimator returns the count: the split of registers
     * between two adjacent run lengths is searched against the estimator
     * read back from the bytes, so the bias correction it applies is
     * accounted for rather than modelled.
     */
    static InternalCardinality cardinality(String name, Map<String, Object> metadata, long count) {
        if (count <= LINEAR_COUNTING_MAX) {
            return readCardinality(name, metadata, linearCountingBytes(count));
        }
        double alpha = 0.7213 / (1 + 1.079 / HLL_REGISTERS);
        double target = alpha * HLL_REGISTERS * (double) HLL_REGISTERS / count; // the inverse sum the estimator must see
        // Registers all at run length a + 1 give the sum m * 2^-(a+1); all at a give m * 2^-a.
        // Run length 0 is an empty register: below ~1.5m the estimator needs some of
        // those (it counts them linearly, then corrects the raw estimate), so the split
        // may start at 0.
        int a = (int) Math.floor(Math.log(HLL_REGISTERS / target) / Math.log(2));
        a = Math.max(0, Math.min(a, 60));
        byte[] best = null;
        long bestError = Long.MAX_VALUE;
        for (int shift = 0; shift <= 1 && bestError > 0; shift++) {
            int runLen = a + shift;
            int lo = 0;
            int hi = HLL_REGISTERS;
            for (int step = 0; step < REGISTER_SEARCH_STEPS && lo < hi; step++) {
                int mid = (lo + hi) >>> 1;
                byte[] bytes = registerBytes(runLen, mid);
                long estimate = estimate(bytes);
                long error = estimate > count ? estimate - count : count - estimate;
                if (error < bestError) {
                    bestError = error;
                    best = bytes;
                }
                if (estimate < count) {
                    lo = mid + 1; // more registers at the longer run raise the estimate
                } else {
                    hi = mid;
                }
            }
        }
        return readCardinality(name, metadata, best);
    }

    /** Linear-counting form: {@code count} distinct encoded hashes with the plain (non-run-length) encoding. */
    private static byte[] linearCountingBytes(long count) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(HLL_PRECISION);
            out.writeBoolean(AbstractHyperLogLogPlusPlus.LINEAR_COUNTING);
            out.writeVLong(count);
            // The encoding of a hash whose index bits below the precision are not all zero is
            // its top 25 bits shifted left by one; keep bit 0 of the index set so that holds.
            for (long i = 0; i < count; i++) {
                long index = (i << (LINEAR_COUNTING_BITS - HLL_PRECISION - 10)) | 1L;
                out.writeInt((int) (index << 1));
            }
            return out.bytes().toBytesRef().bytes.clone();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Register form: {@code atLonger} registers at run length {@code runLen + 1}, the rest at {@code runLen}. */
    private static byte[] registerBytes(int runLen, int atLonger) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(HLL_PRECISION);
            out.writeBoolean(AbstractHyperLogLogPlusPlus.HYPERLOGLOG);
            for (int i = 0; i < HLL_REGISTERS; i++) {
                out.writeByte((byte) (i < atLonger ? runLen + 1 : runLen));
            }
            return out.bytes().toBytesRef().bytes.clone();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static long estimate(byte[] sketch) {
        try (StreamInput in = StreamInput.wrap(sketch)) {
            return AbstractHyperLogLogPlusPlus.readFrom(in, BigArrays.NON_RECYCLING_INSTANCE).cardinality(0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static InternalCardinality readCardinality(String name, Map<String, Object> metadata, byte[] sketch) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeString(name);
            out.writeGenericValue(metadata);
            // InternalCardinality.doWriteTo
            out.writeNamedWriteable(DocValueFormat.RAW);
            out.writeBoolean(true);
            out.writeBytes(sketch, 0, sketch.length);
            try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry())) {
                return new InternalCardinality(in);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static NamedWriteableRegistry registry() {
        NamedWriteableRegistry registry = InfinoServices.namedWriteableRegistry();
        if (registry == null) {
            throw new IllegalStateException("the plugin's named writeable registry is not set");
        }
        return registry;
    }
}
