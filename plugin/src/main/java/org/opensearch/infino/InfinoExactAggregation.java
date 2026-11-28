/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.SignificantTermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An aggregation over default-mode shards that the coordinating node answers
 * once, over every reached shard's table, instead of reducing the shards'
 * parts: a cardinality or percentiles result is built from the engine's exact
 * values, and two such results cannot be merged into the exact value over
 * both. Each reached shard returns this in its place (see
 * {@link InfinoQueryPhaseSearcher}), naming its table and the rows its
 * search read (its skeleton reader's cut); the final reduce runs the
 * aggregation over all of them as one relation, with each table read at its
 * own shard's cut, so the answer is the exact one over every index and every
 * shard the search reached, whichever node each shard is on, and it counts
 * the rows the hits and totals count.
 *
 * <p>The search as written, its time and what the security plugin lets the
 * user see of each index are recorded on the coordinating node when it
 * routes the search ({@link #record}), so a shard's part carries only where
 * its rows are.
 */
final class InfinoExactAggregation extends InternalAggregation {

    static final String NAME = "infino_exact";

    /**
     * How long a routed search's record is kept unread. A search forgets its
     * record when it answers ({@link #forget}), so this bounds only a search
     * that never does.
     */
    private static final TimeValue RECORD_TTL = TimeValue.timeValueMinutes(5);
    /**
     * Records held at once at most; past this the least recently read go. A
     * search forgets its record when it answers, so this counts searches in
     * flight on this node: one evicted fails its search by name, never with
     * a wrong answer.
     */
    private static final long MAX_RECORDS = 10_000;

    /** What the coordinating node recorded of a search when it routed it. */
    record Routed(SearchSourceBuilder source, long nowMillis, Map<String, InfinoAccess> access) {}

    /** The searches this node routed whose exact aggregations it answers, by the search's id. */
    private static final Cache<String, Routed> ROUTED = CacheBuilder.<String, Routed>builder()
        .setMaximumWeight(MAX_RECORDS)
        .setExpireAfterAccess(RECORD_TTL)
        .build();

    /** One reached shard's rows: its index, its table, and the cut and generation its search read them at. */
    record Part(String index, String indexUuid, String table, Long cut, long generation) {
        Part(StreamInput in) throws IOException {
            this(in.readString(), in.readString(), in.readString(), in.readOptionalLong(), in.readLong());
        }

        void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            out.writeString(indexUuid);
            out.writeString(table);
            out.writeOptionalLong(cut);
            out.writeLong(generation);
        }
    }

    private final String request;
    private final List<Part> parts;

    InfinoExactAggregation(String name, Map<String, Object> metadata, String request, List<Part> parts) {
        super(name, metadata);
        this.request = request;
        this.parts = parts;
    }

    InfinoExactAggregation(StreamInput in) throws IOException {
        super(in);
        this.request = in.readString();
        this.parts = in.readList(Part::new);
    }

    /** Record, on the node routing it, the search {@code request} whose exact aggregations it answers. */
    static void record(String request, SearchSourceBuilder source, long nowMillis, Map<String, InfinoAccess> access) {
        ROUTED.put(request, new Routed(source, nowMillis, access));
    }

    /** Forget the record of {@code request}, once its search has answered or failed: its final reduce is done. */
    static void forget(String request) {
        ROUTED.invalidate(request);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(request);
        out.writeCollection(parts, (o, p) -> p.writeTo(o));
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    /**
     * The shards' parts as one; on the final reduce, the aggregation itself,
     * answered over every part's rows.
     */
    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        List<Part> all = new ArrayList<>();
        for (InternalAggregation a : aggregations) {
            all.addAll(((InfinoExactAggregation) a).parts);
        }
        if (!reduceContext.isFinalReduce()) {
            return new InfinoExactAggregation(getName(), getMetadata(), request, all);
        }
        // The answer is built as one shard's part would be, every bucket kept;
        // reduced alone as stock's final reduce reduces the shards' parts, a
        // terms level keeps its size buckets and counts the rest as other documents.
        InternalAggregation answered = answer(all);
        return InternalAggregations.reduce(List.of(InternalAggregations.from(List.of(answered))), reduceContext).get(getName());
    }

    private InternalAggregation answer(List<Part> all) {
        Routed routed = ROUTED.get(request);
        if (routed == null) {
            throw new IllegalStateException("[infino] the aggregation [" + getName() + "] is answered on the node that routed the search, and it no longer holds it");
        }
        AggregationBuilder builder = null;
        if (routed.source().aggregations() != null) {
            for (AggregationBuilder b : routed.source().aggregations().getAggregatorFactories()) {
                if (b.getName().equals(getName())) {
                    builder = b;
                }
            }
        }
        if (builder == null) {
            throw new IllegalStateException("[infino] the search names no aggregation [" + getName() + "]");
        }
        // A part the search reads no row of (the security plugin's own index) adds nothing to the answer.
        List<Part> read = new ArrayList<>();
        for (Part p : all) {
            InfinoAccess access = routed.access().get(p.index());
            if (access == null || !access.readsNothing()) {
                read.add(p);
            }
        }
        if (!read.isEmpty()) {
            all = read;
        }
        // One order whatever order the shards answered in, so the statement is the same.
        all.sort(Comparator.comparing(Part::index).thenComparing(Part::table));
        boolean severalIndices = all.stream().map(Part::index).distinct().count() > 1;
        if (severalIndices) {
            refuseTopHitsOverSeveral(builder);
        }
        List<InfinoSearch.Plan> plans = new ArrayList<>(all.size());
        List<String> indices = new ArrayList<>(all.size());
        for (Part p : all) {
            // Fail closed: a part of an index the routing node did not record the user's access to is not read.
            InfinoAccess access = routed.access().get(p.index());
            if (access == null) {
                throw InfinoAccess.refused("the access to [" + p.index() + "] was not recorded for the search");
            }
            if (severalIndices) {
                refuseRestrictedOverSeveral(builder, p.index(), access);
            }
            InfinoSearch.Plan plan = InfinoSearch.plan(p.table(), routed.source(), routed.nowMillis(), access);
            plans.add(InfinoQueryPhaseSearcher.visibleTo(plan, p.cut(), p.generation()));
            indices.add(p.index());
        }
        Part first = all.get(0);
        InfinoAggregationProcessor processor = new InfinoAggregationProcessor(new Index(first.index(), first.indexUuid()), false);
        return processor.evaluateOver(List.of(builder), plans.get(0), plans, indices, null).aggregations().get(getName());
    }

    /** Refuses an aggregation over several indices' rows read as one when a top_hits is under it: its hits name one index. */
    static void refuseTopHitsOverSeveral(AggregationBuilder builder) {
        if (holdsTopHits(builder)) {
            throw new InfinoAggregationProcessor.Unsupported("top_hits under [" + builder.getName() + "] over several indices is not served");
        }
    }

    /**
     * Refuses an aggregation over several indices' rows read as one when
     * {@code index} is read under a rule the union cannot hold per index. Each
     * index's rows come from its own plan, whose WHERE holds that index's
     * document rule and filtering alias, so those hold per index; but the
     * columns are planned under one index's access (its visible and masked
     * fields), and a significant_terms background reads one index's document
     * rule and alias filter over every table. A field or masking rule is then
     * refused, and so is a document rule or a filtering alias under which a
     * significant_terms is read.
     */
    static void refuseRestrictedOverSeveral(AggregationBuilder builder, String index, InfinoAccess access) {
        if (access.restrictsFields() || access.masks()) {
            throw InfinoAccess.refused(
                "[" + builder.getName() + "] over several indices, one of them [" + index + "] under a field or masking rule, is not served"
            );
        }
        if (access.restrictsDocuments() && holds(builder, SignificantTermsAggregationBuilder.class)) {
            throw InfinoAccess.refused(
                "[" + builder.getName() + "] with a significant_terms over several indices, one of them [" + index
                    + "] under a document rule or filtering alias, is not served"
            );
        }
    }

    /** Whether {@code builder} or one under it is a top_hits, whose hits name one index. */
    private static boolean holdsTopHits(AggregationBuilder builder) {
        return holds(builder, TopHitsAggregationBuilder.class);
    }

    /** Whether {@code builder} or one under it is of {@code kind}. */
    private static boolean holds(AggregationBuilder builder, Class<? extends AggregationBuilder> kind) {
        if (kind.isInstance(builder)) {
            return true;
        }
        for (AggregationBuilder b : builder.getSubAggregations()) {
            if (holds(b, kind)) {
                return true;
            }
        }
        return false;
    }

    /** Reduced even alone: one shard's part is answered the same way. */
    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return true;
    }

    @Override
    public Object getProperty(List<String> path) {
        throw new IllegalStateException("[infino] the aggregation [" + getName() + "] is read only once the coordinating node has answered it");
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        throw new IllegalStateException("[infino] the aggregation [" + getName() + "] is shown only once the coordinating node has answered it");
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && obj instanceof InfinoExactAggregation other && request.equals(other.request) && parts.equals(other.parts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), request, parts);
    }
}
