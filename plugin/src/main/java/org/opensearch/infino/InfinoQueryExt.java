/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.common.UUIDs;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A search of engine-mode indices as the client wrote it, carried to their
 * shards in the {@code ext} section of the source, with an id of its own.
 *
 * <p>The shards plan from this copy with the action layer's planner: the
 * shard's own copy of the source is rewritten against its Lucene skeleton,
 * which holds no values (a range over a field it has no points for becomes
 * {@code match_none}), and the stock query and fetch phases would act on
 * clauses the table serves instead. So the source the cluster routes (see
 * {@link #route}) keeps only what the coordinating node needs to merge the
 * shards' results; the {@code ext} section is never rewritten. The id ties a
 * shard's fetch phase to its query phase (see {@link InfinoFetchSubPhase}).
 */
public final class InfinoQueryExt extends SearchExtBuilder {
    public static final String NAME = "infino_source";
    private static final String SOURCE = "source";
    private static final String REQUEST = "request";

    private final SearchSourceBuilder source;
    private final String request;

    public InfinoQueryExt(SearchSourceBuilder source, String request) {
        this.source = source;
        this.request = request;
    }

    public InfinoQueryExt(StreamInput in) throws IOException {
        this.source = new SearchSourceBuilder(in);
        this.request = in.readString();
    }

    /** The source as written, without its {@code ext}. */
    public SearchSourceBuilder source() {
        return source;
    }

    /** The id of the search this copy belongs to. */
    public String request() {
        return request;
    }

    /**
     * The source the cluster routes to the shards for {@code source}: a query
     * every shard runs the same way (the shard's answer comes from the
     * table), the page window and total tracking the coordinating node merges
     * by, the aggregations it reduces, and {@code source} itself as written,
     * in the {@code ext} section, with a fresh id.
     */
    static SearchSourceBuilder route(SearchSourceBuilder source) {
        SearchSourceBuilder routed = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        // Unset (-1) stays unset: the setters refuse it.
        if (source.from() >= 0) {
            routed.from(source.from());
        }
        if (source.size() >= 0) {
            routed.size(source.size());
        }
        if (source.trackTotalHitsUpTo() != null) {
            routed.trackTotalHitsUpTo(source.trackTotalHitsUpTo());
        }
        if (source.timeout() != null) {
            routed.timeout(source.timeout());
        }
        if (source.version() != null) {
            routed.version(source.version());
        }
        if (source.seqNoAndPrimaryTerm() != null) {
            routed.seqNoAndPrimaryTerm(source.seqNoAndPrimaryTerm());
        }
        if (source.fetchSource() != null) {
            // Whether to return the source; its filter, if any, goes with the copy.
            routed.fetchSource(source.fetchSource().fetchSource());
        }
        if (source.aggregations() != null) {
            for (AggregationBuilder b : source.aggregations().getAggregatorFactories()) {
                routed.aggregation(b);
            }
            for (PipelineAggregationBuilder b : source.aggregations().getPipelineAggregatorFactories()) {
                routed.aggregation(b);
            }
        }
        routed.ext(List.of(new InfinoQueryExt(source.shallowCopy().ext(List.of()), UUIDs.randomBase64UUID())));
        return routed;
    }

    /** The copy carried in a shard's source, or null when none was. */
    static InfinoQueryExt carried(SearchSourceBuilder shardSource) {
        if (shardSource != null) {
            for (SearchExtBuilder ext : shardSource.ext()) {
                if (ext instanceof InfinoQueryExt carried) {
                    return carried;
                }
            }
        }
        return null;
    }

    public static InfinoQueryExt fromXContent(XContentParser parser) throws IOException {
        SearchSourceBuilder source = null;
        String request = null;
        String field = null;
        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            if (token == XContentParser.Token.FIELD_NAME) {
                field = parser.currentName();
            } else if (SOURCE.equals(field) && token == XContentParser.Token.START_OBJECT) {
                source = SearchSourceBuilder.fromXContent(parser);
            } else if (REQUEST.equals(field) && token == XContentParser.Token.VALUE_STRING) {
                request = parser.text();
            } else {
                parser.skipChildren();
            }
        }
        return new InfinoQueryExt(source == null ? new SearchSourceBuilder() : source, request == null ? UUIDs.randomBase64UUID() : request);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source.writeTo(out);
        out.writeString(request);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(SOURCE, source);
        builder.field(REQUEST, request);
        return builder.endObject();
    }

    @Override
    public int hashCode() {
        return Objects.hash(NAME, source, request);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof InfinoQueryExt other && Objects.equals(source, other.source) && Objects.equals(request, other.request);
    }
}
