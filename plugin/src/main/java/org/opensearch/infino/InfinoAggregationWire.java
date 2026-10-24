/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.common.Rounding;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.LongBounds;

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

    private static NamedWriteableRegistry registry() {
        NamedWriteableRegistry registry = InfinoPlugin.namedWriteableRegistry();
        if (registry == null) {
            throw new IllegalStateException("the plugin's named writeable registry is not set");
        }
        return registry;
    }
}
