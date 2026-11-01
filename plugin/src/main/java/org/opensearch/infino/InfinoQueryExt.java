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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A search of default-mode indices as the client wrote it, carried to their
 * shards in the {@code ext} section of the source, with an id of its own and
 * the shards of each index the search reaches.
 *
 * <p>The shards plan from this copy with cloud mode's planner: the
 * shard's own copy of the source is rewritten against its Lucene skeleton,
 * which holds no values (a range over a field it has no points for becomes
 * {@code match_none}), and the stock query and fetch phases would act on
 * clauses the table serves instead. So the source the cluster routes (see
 * {@link #route}) keeps only what the coordinating node needs to merge the
 * shards' results; the {@code ext} section is never rewritten. The id ties a
 * shard's fetch phase to its query phase (see {@link InfinoFetchSubPhase});
 * the shards let one shard answer an aggregation that cannot be merged from
 * the shards' parts over every reached shard's table (see
 * {@link InfinoQueryPhaseSearcher}); the coordinating node's clock is what
 * {@code now} means in every shard's phases.
 */
public final class InfinoQueryExt extends SearchExtBuilder {
    public static final String NAME = "infino_source";
    private static final String SOURCE = "source";
    private static final String REQUEST = "request";
    private static final String SHARDS = "shards";
    private static final String NOW = "now";

    private final SearchSourceBuilder source;
    private final String request;
    /** The shard numbers of each index the search reaches, ascending, by index name. */
    private final Map<String, List<Integer>> shards;
    /** The coordinating node's clock when it routed the search, in epoch milliseconds. */
    private final long nowMillis;

    public InfinoQueryExt(SearchSourceBuilder source, String request, Map<String, List<Integer>> shards, long nowMillis) {
        this.source = source;
        this.request = request;
        this.shards = shards;
        this.nowMillis = nowMillis;
    }

    public InfinoQueryExt(StreamInput in) throws IOException {
        this.source = new SearchSourceBuilder(in);
        this.request = in.readString();
        this.shards = in.readMap(StreamInput::readString, i -> i.readList(r -> r.readVInt()));
        this.nowMillis = in.readLong();
    }

    /** The source as written, without its {@code ext}. */
    public SearchSourceBuilder source() {
        return source;
    }

    /** The id of the search this copy belongs to. */
    public String request() {
        return request;
    }

    /** The shard numbers of {@code index} the search reaches, ascending; null when the coordinating node did not say. */
    List<Integer> shards(String index) {
        return shards.get(index);
    }

    /** What {@code now} means in the search: the coordinating node's clock when it routed it. */
    long nowMillis() {
        return nowMillis;
    }

    /**
     * The source the cluster routes to the shards for {@code source}: a query
     * every shard runs the same way (the shard's answer comes from the
     * table), the page window and total tracking the coordinating node merges
     * by, the aggregations it reduces, and {@code source} itself as written,
     * in the {@code ext} section, with a fresh id and the shards the search
     * reaches.
     */
    static SearchSourceBuilder route(SearchSourceBuilder source, Map<String, List<Integer>> shards) {
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
        routed.ext(List.of(new InfinoQueryExt(source.shallowCopy().ext(List.of()), UUIDs.randomBase64UUID(), shards, System.currentTimeMillis())));
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
        Map<String, List<Integer>> shards = new LinkedHashMap<>();
        long now = System.currentTimeMillis();
        String field = null;
        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            if (token == XContentParser.Token.FIELD_NAME) {
                field = parser.currentName();
            } else if (SOURCE.equals(field) && token == XContentParser.Token.START_OBJECT) {
                source = SearchSourceBuilder.fromXContent(parser);
            } else if (REQUEST.equals(field) && token == XContentParser.Token.VALUE_STRING) {
                request = parser.text();
            } else if (SHARDS.equals(field) && token == XContentParser.Token.START_OBJECT) {
                for (Map.Entry<String, Object> e : parser.map().entrySet()) {
                    List<Integer> numbers = new ArrayList<>();
                    if (e.getValue() instanceof List<?> list) {
                        for (Object n : list) {
                            numbers.add(((Number) n).intValue());
                        }
                    }
                    shards.put(e.getKey(), numbers);
                }
            } else if (NOW.equals(field) && token == XContentParser.Token.VALUE_NUMBER) {
                now = parser.longValue();
            } else {
                parser.skipChildren();
            }
        }
        return new InfinoQueryExt(source == null ? new SearchSourceBuilder() : source, request == null ? UUIDs.randomBase64UUID() : request, shards, now);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source.writeTo(out);
        out.writeString(request);
        out.writeMap(shards, StreamOutput::writeString, (o, numbers) -> o.writeCollection(numbers, (w, n) -> w.writeVInt(n)));
        out.writeLong(nowMillis);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(SOURCE, source);
        builder.field(REQUEST, request);
        builder.field(SHARDS, shards);
        builder.field(NOW, nowMillis);
        return builder.endObject();
    }

    @Override
    public int hashCode() {
        return Objects.hash(NAME, source, request, shards, nowMillis);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof InfinoQueryExt other
            && Objects.equals(source, other.source)
            && Objects.equals(request, other.request)
            && Objects.equals(shards, other.shards)
            && nowMillis == other.nowMillis;
    }
}
