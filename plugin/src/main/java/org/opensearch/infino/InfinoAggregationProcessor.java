package org.opensearch.infino;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;

import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationProcessor;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.internal.SearchContext;

/**
 * Serves aggregations from Infino SQL for Infino-enabled shards, over the
 * dynamic per-field columns (see {@link InfinoEngine}). Supported, over a
 * {@code match_all} query with no sub-aggregations:
 * <ul>
 *   <li>{@code value_count} → {@code COUNT(*)}</li>
 *   <li>{@code terms} on a string column → {@code GROUP BY field}</li>
 *   <li>{@code sum}/{@code avg}/{@code min}/{@code max} on a numeric column</li>
 * </ul>
 * Each agg's field must be a real Infino column of the right kind (checked via
 * {@link InfinoNative#columns}); anything else is delegated to the stock
 * {@link AggregationProcessor}. The decision is shared with
 * {@link InfinoQueryPhaseSearcher#canServe} so hits and aggs take the same path.
 */
public class InfinoAggregationProcessor implements AggregationProcessor {

    private static final Logger log = LogManager.getLogger(InfinoAggregationProcessor.class);
    private static final Pattern KEY = Pattern.compile("\"key\"\\s*:\\s*\"((?:[^\"\\\\]|\\\\.)*)\"");
    private static final Pattern DOC_COUNT = Pattern.compile("\"doc_count\"\\s*:\\s*(\\d+)");
    private static final Pattern NUM = Pattern.compile("-?\\d+(?:\\.\\d+)?(?:[eE][-+]?\\d+)?");

    private final AggregationProcessor delegate;

    public InfinoAggregationProcessor(AggregationProcessor delegate) {
        this.delegate = delegate;
    }

    @Override
    public void preProcess(SearchContext context) {
        if (!infinoAggs(context)) {
            delegate.preProcess(context); // sets up Lucene collectors
        }
    }

    @Override
    public void postProcess(SearchContext context) {
        if (!infinoAggs(context)) {
            delegate.postProcess(context);
            return;
        }
        try {
            List<InternalAggregation> aggs = new ArrayList<>();
            String table = InfinoQueryPhaseSearcher.tableOf(context);
            Map<String, String> cols = InfinoNative.columns(table);
            String where = InfinoSqlWhere.of(InfinoQueryPhaseSearcher.dslQuery(context), cols, table);
            if (where == null) where = "1=1"; // canServe guarantees non-null here
            long total = scalarLong(InfinoNative.sql("SELECT COUNT(*) AS c FROM " + table + " WHERE " + where));
            for (AggregationBuilder b : builders(context)) {
                aggs.add(build(b, table, total, where));
            }
            context.queryResult().aggregations(InternalAggregations.from(aggs));
            log.info("[infino] served {} aggregation(s) from {} (where {})", aggs.size(), table, where);
        } catch (Exception e) {
            log.warn("[infino] aggregation routing failed, delegating", e);
            delegate.postProcess(context);
        }
    }

    private boolean infinoAggs(SearchContext context) {
        return !builders(context).isEmpty() && InfinoQueryPhaseSearcher.canServe(context);
    }

    // ---- servability (shared with canServe) ----

    static boolean aggsServable(SearchContext context, Map<String, String> cols, String where) {
        Collection<AggregationBuilder> bs = builders(context);
        if (bs.isEmpty()) return true;
        if (where == null) return false; // aggs need a scoping predicate (match_all → "1=1")
        for (AggregationBuilder b : bs) {
            if (!b.getSubAggregations().isEmpty()) return false;
            if (b instanceof ValueCountAggregationBuilder) continue; // COUNT(*)
            if (b instanceof TermsAggregationBuilder) {
                if ("str".equals(cols.get(fieldOf(b)))) continue;
                return false;
            }
            if (b instanceof SumAggregationBuilder || b instanceof AvgAggregationBuilder
                || b instanceof MinAggregationBuilder || b instanceof MaxAggregationBuilder) {
                String ty = cols.get(fieldOf(b));
                if ("i64".equals(ty) || "f64".equals(ty)) continue;
                return false;
            }
            return false;
        }
        return true;
    }

    static Collection<AggregationBuilder> builders(SearchContext context) {
        try {
            if (context.request() == null || context.request().source() == null
                || context.request().source().aggregations() == null) {
                return List.of();
            }
            return context.request().source().aggregations().getAggregatorFactories();
        } catch (Exception e) {
            return List.of();
        }
    }

    private static String fieldOf(AggregationBuilder b) {
        return ((ValuesSourceAggregationBuilder<?>) b).field();
    }

    // ---- builders ----

    private InternalAggregation build(AggregationBuilder b, String table, long total, String where) {
        String name = b.getName();
        String tail = " FROM " + table + " WHERE " + where;
        if (b instanceof ValueCountAggregationBuilder vc) {
            return new InternalValueCount(name, total, vc.getMetadata());
        }
        if (b instanceof TermsAggregationBuilder t) {
            return terms(t, table, total, where);
        }
        String field = fieldOf(b);
        if (b instanceof SumAggregationBuilder s) {
            double v = scalarDouble(InfinoNative.sql("SELECT SUM(" + field + ") AS v" + tail), 0.0);
            return new InternalSum(name, v, DocValueFormat.RAW, s.getMetadata());
        }
        if (b instanceof MinAggregationBuilder mn) {
            double v = scalarDouble(InfinoNative.sql("SELECT MIN(" + field + ") AS v" + tail),
                Double.POSITIVE_INFINITY);
            return new InternalMin(name, v, DocValueFormat.RAW, mn.getMetadata());
        }
        if (b instanceof MaxAggregationBuilder mx) {
            double v = scalarDouble(InfinoNative.sql("SELECT MAX(" + field + ") AS v" + tail),
                Double.NEGATIVE_INFINITY);
            return new InternalMax(name, v, DocValueFormat.RAW, mx.getMetadata());
        }
        AvgAggregationBuilder a = (AvgAggregationBuilder) b;
        String json = InfinoNative.sql("SELECT SUM(" + field + ") AS s, COUNT(" + field + ") AS c" + tail);
        double sum = numField(json, "s", 0.0);
        long count = (long) numField(json, "c", 0.0);
        return new InternalAvg(name, sum, count, DocValueFormat.RAW, a.getMetadata());
    }

    private StringTerms terms(TermsAggregationBuilder t, String table, long total, String where) {
        int size = t.size();
        String field = t.field();
        String json = InfinoNative.sql(
            "SELECT " + field + " AS key, COUNT(*) AS doc_count FROM " + table + " WHERE " + where
                + " GROUP BY " + field + " ORDER BY doc_count DESC LIMIT " + size);
        List<String> keys = all(KEY, json);
        List<Long> counts = new ArrayList<>();
        Matcher m = DOC_COUNT.matcher(json);
        while (m.find()) counts.add(Long.parseLong(m.group(1)));

        List<StringTerms.Bucket> buckets = new ArrayList<>();
        long sumTop = 0;
        int n = Math.min(keys.size(), counts.size());
        for (int i = 0; i < n; i++) {
            long c = counts.get(i);
            sumTop += c;
            buckets.add(new StringTerms.Bucket(
                new BytesRef(keys.get(i)), c, InternalAggregations.from(new ArrayList<>()),
                false, 0L, DocValueFormat.RAW));
        }
        long otherDocCount = Math.max(0, total - sumTop);
        TermsAggregator.BucketCountThresholds thresholds =
            new TermsAggregator.BucketCountThresholds(1, 0, size, size);
        return new StringTerms(t.getName(), BucketOrder.count(false), BucketOrder.count(false), t.getMetadata(),
            DocValueFormat.RAW, size, false, otherDocCount, buckets, 0L, thresholds);
    }

    // ---- tiny JSON helpers (flat arrays of flat objects) ----

    private static long scalarLong(String json) {
        return (long) scalarDouble(json, 0.0);
    }

    /** First numeric value in the JSON, or dflt when absent/null. */
    private static double scalarDouble(String json, double dflt) {
        if (json == null) return dflt;
        Matcher m = NUM.matcher(json);
        return m.find() ? Double.parseDouble(m.group()) : dflt;
    }

    /** Numeric value of "key":num in the first row, or dflt. */
    private static double numField(String json, String key, double dflt) {
        if (json == null) return dflt;
        Matcher m = Pattern.compile("\"" + Pattern.quote(key) + "\"\\s*:\\s*(" + NUM.pattern() + ")").matcher(json);
        return m.find() ? Double.parseDouble(m.group(1)) : dflt;
    }

    private static List<String> all(Pattern p, String s) {
        List<String> out = new ArrayList<>();
        if (s == null) return out;
        Matcher m = p.matcher(s);
        while (m.find()) out.add(m.group(1));
        return out;
    }
}
