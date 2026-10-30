/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.apache.lucene.util.BytesRef;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.infino.InfinoScriptTranslator.SqlType;
import org.opensearch.infino.InfinoSearch.SortSpec;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationProcessor;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.ScrollContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QueryCollectorContext;
import org.opensearch.search.query.QueryPhaseSearcher;
import org.opensearch.search.query.QueryPhaseSearcherWrapper;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * The query phase of an engine-mode shard ({@code index.infino.mode: engine}),
 * answered from the shard's table; every other shard gets the stock phase.
 *
 * <p>The shard plans from the search as the client wrote it (see
 * {@link InfinoQueryExt}) with the action layer's planner and hit statement
 * ({@link InfinoSearch#plan}, {@link InfinoSearch#page}), under what the
 * security plugin lets the user see, so both modes serve the same requests
 * the same way. Each hit is kept whole, as the action layer builds it
 * (source filtered and masked, requested fields, script fields, collapse
 * value), for the shard's fetch phase to hand out ({@link InfinoFetchSubPhase});
 * the shard result names each hit by its document in the shard's Lucene
 * skeleton, as any shard's does.
 *
 * <p>The coordinating node merges the shards' results as it merges any
 * shard's: a sorted page carries each hit's sort values (with the id the
 * table breaks ties on), a collapsed one its collapse value, and the
 * aggregations are the table's, which the coordinating node reduces. A
 * cardinality or percentiles result is built from the engine's exact values
 * and cannot be merged from the shards' parts, so when one is asked for
 * over several shards, the first reached shard answers the aggregations
 * over every reached shard's table and the others over no rows. Every shard
 * reads {@code now} as the coordinating node set it. A scroll pages in a
 * fixed order, the id's when the request names none: each round resumes
 * after the last hit the coordinating node handed out from this shard, as
 * the action layer's scroll resumes after its last.
 */
public class InfinoQueryPhaseSearcher implements QueryPhaseSearcher {

    /** Hits a query phase keeps for its fetch phase; past this, the oldest go, and their fetch reads the table again. */
    private static final long MAX_KEPT_HITS = 100_000;
    /** How long a kept hit waits for its fetch phase. */
    private static final TimeValue KEPT_HIT_TTL = TimeValue.timeValueMinutes(5);
    /** How a date sort value read back from the table as text is parsed to the epoch millis a sort compares. */
    private static final String SORT_DATE_FORMAT = "strict_date_optional_time||epoch_millis";
    /** Pages one later scroll round reads at most while it looks for rows its reader knows (see scrollRows). */
    private static final int MAX_SCROLL_PAGES = 100;

    /** The hits of each shard's query phase, by search, shard and id, until its fetch phase takes them. */
    private static final Cache<String, SearchHit> KEPT = CacheBuilder.<String, SearchHit>builder()
        .setMaximumWeight(MAX_KEPT_HITS)
        .setExpireAfterAccess(KEPT_HIT_TTL)
        .build();

    private final QueryPhaseSearcher fallback = new QueryPhaseSearcherWrapper();

    /** Whether a shard's index is served by its table in engine mode. */
    static boolean isEngineShard(SearchContext context) {
        try {
            return InfinoPlugin.atEngine(context.indexShard().indexSettings().getSettings());
        } catch (RuntimeException e) {
            return false;
        }
    }

    /** The hit a query phase kept for the fetch phase of {@code request} on {@code shard}, taken; null when none is kept. */
    static SearchHit take(String request, ShardId shard, String id) {
        String key = key(request, shard, id);
        SearchHit hit = KEPT.get(key);
        if (hit != null) {
            KEPT.invalidate(key);
        }
        return hit;
    }

    private static String key(String request, ShardId shard, String id) {
        return request + '\u0000' + shard + '\u0000' + id;
    }

    @Override
    public boolean searchWith(
        SearchContext context,
        ContextIndexSearcher searcher,
        Query query,
        LinkedList<QueryCollectorContext> collectors,
        boolean hasFilterCollector,
        boolean hasTimeout
    ) throws IOException {
        if (!isEngineShard(context)) {
            return fallback.searchWith(context, searcher, query, collectors, hasFilterCollector, hasTimeout);
        }
        serve(context, searcher);
        // Nothing to rescore: the table's order is the answer.
        return false;
    }

    @Override
    public AggregationProcessor aggregationProcessor(SearchContext context) {
        if (!isEngineShard(context)) {
            return fallback.aggregationProcessor(context);
        }
        // The aggregations are computed with the hits (see serve): no
        // collector is set up over the Lucene skeleton, which holds no values.
        return new AggregationProcessor() {
            @Override
            public void preProcess(SearchContext searchContext) {}

            @Override
            public void postProcess(SearchContext searchContext) {}
        };
    }

    private static void serve(SearchContext context, ContextIndexSearcher searcher) throws IOException {
        InfinoQueryExt carried = InfinoQueryExt.carried(context.request().source());
        if (carried == null) {
            throw new IllegalStateException("[infino] a search of an engine-mode index reached its shard without the search as written");
        }
        ShardId shardId = context.indexShard().shardId();
        Index index = shardId.getIndex();
        String table = InfinoPlugin.tables().ensure(context.indexShard().indexSettings().getIndexMetadata(), shardId.id());
        InfinoAccess access = InfinoSecurity.access(index.getName(), InfinoSecurityCalls.SEARCH);

        ScrollContext scroll = context.scrollContext();
        boolean laterRound = scroll != null && scroll.totalHits != null;
        SearchSourceBuilder source = scroll == null ? carried.source() : scrollRound(carried.source(), scroll, laterRound);
        // The coordinating node's clock, so every shard reads `now` the same.
        long now = carried.nowMillis();
        InfinoSearch.Plan plan = InfinoSearch.plan(table, source, now, access);

        Collection<AggregationBuilder> builders = InfinoSearch.builders(source);
        // A cardinality or percentiles result is built from the engine's exact
        // values, and the coordinating node cannot merge such results from the
        // shards' parts: the first reached shard answers the aggregations over
        // every reached shard's table, the others over no rows.
        List<Integer> reached = carried.shards(index.getName());
        boolean overShards = !builders.isEmpty() && reached != null && reached.size() > 1 && InfinoAggregationProcessor.needsExactValues(builders);
        boolean exact = trackTotal(source);
        boolean sharedTotal = !builders.isEmpty() && !overShards && plan.vector() == null && plan.postWhere() == null && exact;
        boolean countWithAggs = sharedTotal && source.size() == 0;
        InfinoSearch.Page page = InfinoSearch.page(index, plan, source, !countWithAggs);
        long total = page.total();
        if (!builders.isEmpty()) {
            // A search over several shards (this index's, or other indices')
            // is reduced on the coordinating node from each shard's groups.
            boolean shardOfMany = context.request().numberOfShards() > 1;
            InfinoAggregationProcessor processor = new InfinoAggregationProcessor(index, shardOfMany);
            InfinoAggregationProcessor.Counted counted;
            if (!overShards) {
                counted = processor.evaluate(builders, plan, sharedTotal && !countWithAggs ? page.total() : null);
            } else if (reached.get(0) == shardId.id()) {
                counted = processor.evaluateOver(builders, plan, shardPlans(context, reached, source, now, access));
            } else {
                counted = processor.evaluateNothing(builders, plan);
            }
            context.queryResult().aggregations(counted.aggregations());
            if (countWithAggs) {
                total = counted.total();
            }
        }
        TotalHits totalHits = laterRound
            ? scroll.totalHits
            : new TotalHits(total, page.exact() ? TotalHits.Relation.EQUAL_TO : TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);

        List<SearchHit> rows = page.hits();
        if (laterRound) {
            rows = scrollRows(searcher, carried.source(), source, plan, index, rows);
        }
        boolean sorted = !plan.sorts().isEmpty();
        boolean collapsed = plan.collapse() != null;
        SortField[] sortFields = sorted ? sortFields(plan.sorts()) : collapsed ? new SortField[] { SortField.FIELD_SCORE } : null;
        List<ScoreDoc> docs = new ArrayList<>(rows.size());
        List<Object> collapseValues = new ArrayList<>();
        for (SearchHit h : rows) {
            int doc = docId(searcher, h.getId());
            if (doc < 0) {
                continue; // in the table, not in the skeleton reader this search holds (see scrollRows)
            }
            KEPT.put(key(carried.request(), shardId, h.getId()), h);
            float score = Float.isNaN(h.getScore()) && !sorted ? 1.0f : h.getScore();
            if (sorted) {
                docs.add(new FieldDoc(doc, score, sortValues(h.getSortValues(), plan.sorts())));
            } else if (collapsed) {
                docs.add(new FieldDoc(doc, score, new Object[] { score }));
            } else {
                docs.add(new ScoreDoc(doc, score));
            }
            if (collapsed) {
                DocumentField value = h.field(plan.collapse().name());
                collapseValues.add(value == null || value.getValue() == null ? null : collapseValue(value.getValue()));
            }
        }
        TopDocs topDocs;
        if (collapsed) {
            topDocs = new CollapseTopFieldDocs(
                plan.collapse().name(),
                totalHits,
                docs.toArray(new ScoreDoc[0]),
                sortFields,
                collapseValues.toArray()
            );
        } else if (sorted) {
            topDocs = new TopFieldDocs(totalHits, docs.toArray(new FieldDoc[0]), sortFields);
        } else {
            topDocs = new TopDocs(totalHits, docs.toArray(new ScoreDoc[0]));
        }
        DocValueFormat[] formats = null;
        if (sortFields != null) {
            formats = new DocValueFormat[sortFields.length];
            Arrays.fill(formats, DocValueFormat.RAW);
        }
        context.queryResult().topDocs(new TopDocsAndMaxScore(topDocs, page.maxScore()), formats);
        if (scroll != null && !laterRound) {
            scroll.totalHits = totalHits;
            scroll.maxScore = page.maxScore();
        }
    }

    /**
     * The search one round of a scroll runs: the action layer's scroll
     * source ({@link InfinoSearch#scrollSource}), which pages in a fixed
     * order; the first round also keeps the request's total and
     * aggregations, and every later one resumes after the last hit the
     * coordinating node handed out from this shard.
     */
    private static SearchSourceBuilder scrollRound(SearchSourceBuilder written, ScrollContext scroll, boolean laterRound) {
        SearchSourceBuilder round = InfinoSearch.scrollSource(written);
        if (!laterRound) {
            if (written.trackTotalHitsUpTo() != null) {
                round.trackTotalHitsUpTo(written.trackTotalHitsUpTo());
            } else {
                round.trackTotalHits(true);
            }
            if (written.aggregations() != null) {
                for (AggregationBuilder b : written.aggregations().getAggregatorFactories()) {
                    round.aggregation(b);
                }
                for (PipelineAggregationBuilder b : written.aggregations().getPipelineAggregatorFactories()) {
                    round.aggregation(b);
                }
            }
            return round;
        }
        if (scroll.lastEmittedDoc instanceof FieldDoc last) {
            round.searchAfter(searchAfter(last.fields));
        }
        return round;
    }

    /** The plan of the search over each reached shard's table, in shard order. */
    private static List<InfinoSearch.Plan> shardPlans(SearchContext context, List<Integer> reached, SearchSourceBuilder source, long now, InfinoAccess access) {
        IndexMetadata md = context.indexShard().indexSettings().getIndexMetadata();
        List<InfinoSearch.Plan> out = new ArrayList<>(reached.size());
        for (int shard : reached) {
            out.add(InfinoSearch.plan(InfinoPlugin.tables().ensure(md, shard), source, now, access));
        }
        return out;
    }

    /**
     * The rows of a later scroll round, read past the rows its skeleton
     * reader does not know. The reader is the one the scroll opened with,
     * so a row written since is in the table but has no document there; a
     * round whose page held only such rows would come back empty and end
     * the scroll before its last row. The round pages on, after the last row
     * read, until it holds as many known rows as it asks for or the table
     * runs out.
     */
    private static List<SearchHit> scrollRows(
        ContextIndexSearcher searcher,
        SearchSourceBuilder written,
        SearchSourceBuilder round,
        InfinoSearch.Plan plan,
        Index index,
        List<SearchHit> first
    ) throws IOException {
        int size = round.size() < 0 ? InfinoSearch.DEFAULT_SIZE : round.size();
        List<SearchHit> known = new ArrayList<>();
        List<SearchHit> page = first;
        for (int pages = 1;; pages++) {
            for (SearchHit h : page) {
                if (known.size() < size && docId(searcher, h.getId()) >= 0) {
                    known.add(h);
                }
            }
            if (known.size() >= size || page.size() < size || pages == MAX_SCROLL_PAGES) {
                return known;
            }
            SearchHit last = page.get(page.size() - 1);
            SearchSourceBuilder next = InfinoSearch.scrollSource(written).searchAfter(searchAfter(sortValues(last.getSortValues(), plan.sorts())));
            InfinoSearch.Plan nextPlan = InfinoSearch.plan(plan.table(), next, plan.nowMillis(), plan.access());
            page = InfinoSearch.page(index, nextPlan, next, false).hits();
        }
    }

    /** Whether the request asks for the exact total (the default). */
    private static boolean trackTotal(SearchSourceBuilder source) {
        Integer upTo = source.trackTotalHitsUpTo();
        return upTo == null || upTo != SearchContext.TRACK_TOTAL_HITS_DISABLED;
    }

    /** The Lucene document of an id in the shard's skeleton, or -1 when it has none. */
    private static int docId(ContextIndexSearcher searcher, String id) throws IOException {
        TopDocs td = searcher.search(new TermQuery(new Term(IdFieldMapper.NAME, Uid.encodeId(id))), 1);
        return td.scoreDocs.length == 0 ? -1 : td.scoreDocs[0].doc;
    }

    /**
     * The sort fields the coordinating node merges the shards' hits by: one
     * per sort of the plan (the request's, then the id), typed as the
     * table's values compare, each in its direction with missing values
     * last, as the plan orders them.
     */
    private static SortField[] sortFields(List<SortSpec> sorts) {
        SortField[] out = new SortField[sorts.size()];
        for (int i = 0; i < out.length; i++) {
            SortSpec s = sorts.get(i);
            SortField.Type type = sortType(s.type());
            SortField f = new SortField(s.name(), type, s.desc());
            f.setMissingValue(missing(type, s.desc()));
            out[i] = f;
        }
        return out;
    }

    private static SortField.Type sortType(SqlType type) {
        return switch (type) {
            case INT, DATE -> SortField.Type.LONG;
            case FLOAT -> SortField.Type.DOUBLE;
            default -> SortField.Type.STRING;
        };
    }

    /** The value a missing sort value stands in as, so it sorts last in its direction. */
    private static Object missing(SortField.Type type, boolean desc) {
        return switch (type) {
            case LONG -> desc ? Long.MIN_VALUE : Long.MAX_VALUE;
            case DOUBLE -> desc ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
            default -> desc ? SortField.STRING_FIRST : SortField.STRING_LAST;
        };
    }

    /** A hit's sort values from the table as its sort fields compare them. */
    private static Object[] sortValues(Object[] values, List<SortSpec> sorts) {
        Object[] out = new Object[sorts.size()];
        for (int i = 0; i < out.length; i++) {
            Object v = values != null && i < values.length ? values[i] : null;
            SortSpec s = sorts.get(i);
            SortField.Type type = sortType(s.type());
            if (v == null) {
                out[i] = type == SortField.Type.STRING ? null : missing(type, s.desc());
                continue;
            }
            out[i] = switch (type) {
                case LONG -> v instanceof Number n ? (Object) n.longValue() : (Object) epochMillis(v.toString());
                case DOUBLE -> v instanceof Number n ? n.doubleValue() : Double.parseDouble(v.toString());
                default -> new BytesRef(v.toString());
            };
        }
        return out;
    }

    /** The search_after values a scroll resumes from, from the sort values of the last hit handed out: missing stand-ins back to missing. */
    private static Object[] searchAfter(Object[] fields) {
        Object[] out = new Object[fields.length];
        for (int i = 0; i < fields.length; i++) {
            Object v = fields[i];
            if (v instanceof BytesRef b) {
                out[i] = b.utf8ToString();
            } else if (v instanceof Long l && (l == Long.MAX_VALUE || l == Long.MIN_VALUE)) {
                out[i] = null;
            } else if (v instanceof Double d && d.isInfinite()) {
                out[i] = null;
            } else {
                out[i] = v;
            }
        }
        return out;
    }

    /** A collapse value as the coordinating node compares it: text as bytes, numbers as longs. */
    private static Object collapseValue(Object v) {
        return v instanceof Number n ? (Object) n.longValue() : new BytesRef(v.toString());
    }

    private static long epochMillis(String text) {
        return DateFormatter.forPattern(SORT_DATE_FORMAT).parse(text).query(Instant::from).toEpochMilli();
    }
}
