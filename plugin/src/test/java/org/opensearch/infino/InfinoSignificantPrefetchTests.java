/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.index.Index;
import org.opensearch.script.Script;
import org.opensearch.search.SearchModule;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.terms.SignificantStringTerms;
import org.opensearch.search.aggregations.bucket.terms.SignificantTerms;
import org.opensearch.search.aggregations.bucket.terms.SignificantTermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.heuristic.JLHScore;
import org.opensearch.search.aggregations.bucket.terms.heuristic.ScriptHeuristic;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * significant_terms read through the engine. A bucket's terms are all
 * scored and the best {@code shard_size} by score are kept, as a stock
 * shard keeps them. Under a terms parent every parent bucket's foreground
 * is one statement while its rows stay within {@link InfinoAggregationProcessor}'s
 * bound, and one statement per bucket past it; the statements the node
 * logs tell the two apart.
 */
public class InfinoSignificantPrefetchTests extends OpenSearchTestCase {

    /** The statements {@link InfinoNative#sql} logs while it is attached. */
    private static final class Statements extends AbstractAppender {
        final List<String> sql = new ArrayList<>();

        Statements() {
            super("infino-statements", null, null, false, null);
            start();
        }

        @Override
        public void append(LogEvent event) {
            String message = event.getMessage().getFormattedMessage();
            if (message.startsWith("[infino] sql: ")) {
                sql.add(message);
            }
        }
    }

    private static final String TABLE = "significant_prefetch";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // A numeric significant_terms result is built through the wire format, which needs the registry.
        InfinoServices.namedWriteableRegistry(new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedWriteables()));
        InfinoNative.dropTable(TABLE);
        InfinoNative.ensureTable(TABLE, "{\"properties\":{\"p\":{\"type\":\"keyword\"},\"tag\":{\"type\":\"keyword\"},\"code\":{\"type\":\"long\"}}}");
    }

    @Override
    public void tearDown() throws Exception {
        InfinoNative.dropTable(TABLE);
        super.tearDown();
    }

    /** Index one document per (p, tag) pair given. */
    private static void index(List<String[]> docs) {
        String[] ids = new String[docs.size()];
        String[] sources = new String[docs.size()];
        for (int i = 0; i < docs.size(); i++) {
            ids[i] = Integer.toString(i);
            sources[i] = "{\"p\":\"" + docs.get(i)[0] + "\",\"tag\":\"" + docs.get(i)[1] + "\"}";
        }
        assertEquals(docs.size(), InfinoNative.indexBatch(TABLE, ids, sources, ids.length).ok());
    }

    /** {@code n} documents with this p and tag. */
    private static void add(List<String[]> docs, String p, String tag, int n) {
        for (int i = 0; i < n; i++) {
            docs.add(new String[] { p, tag });
        }
    }

    /**
     * Group a: c 4, m 3, r1 2, r2 1 (10 documents); group b: c 12, m 7, r1 2
     * (21). In a, c and m are less common than overall and score 0; r2 (only
     * in a) scores highest, then r1.
     */
    private static void rareButSignificant() {
        List<String[]> docs = new ArrayList<>();
        add(docs, "a", "c", 4);
        add(docs, "a", "m", 3);
        add(docs, "a", "r1", 2);
        add(docs, "a", "r2", 1);
        add(docs, "b", "c", 12);
        add(docs, "b", "m", 7);
        add(docs, "b", "r1", 2);
        index(docs);
    }

    /** A run of {@code agg} over the table, with the foreground and background-count statements it ran. */
    private record Run(InternalAggregation result, List<String> foregrounds, List<String> backgrounds) {}

    private static Run run(AggregationBuilder agg, SearchSourceBuilder source) {
        source.size(0).aggregation(agg);
        Statements statements = new Statements();
        Logger logger = LogManager.getLogger(InfinoNative.class);
        Level was = logger.getLevel();
        Loggers.addAppender(logger, statements);
        Loggers.setLevel(logger, Level.DEBUG);
        InternalAggregation result;
        try {
            InfinoSearch.Plan plan = InfinoSearch.plan(TABLE, source, System.currentTimeMillis());
            result = new InfinoAggregationProcessor(new Index(TABLE, "_na_")).evaluate(List.of(agg), plan, null).aggregations().get(agg.getName());
        } finally {
            Loggers.removeAppender(logger, statements);
            Loggers.setLevel(logger, was);
        }
        List<String> out = new ArrayList<>();
        List<String> background = new ArrayList<>();
        for (String s : statements.sql) {
            if (s.contains("AS k0, COUNT(*) AS doc_count FROM")) {
                out.add(s);
            }
            if (s.contains("WHERE k0 IN (")) {
                background.add(s);
            }
        }
        return new Run(result, out, background);
    }

    private static SignificantTermsAggregationBuilder sig(int size, int shardSize, int minDocCount) {
        return AggregationBuilders.significantTerms("sig").field("tag").size(size).shardSize(shardSize).minDocCount(minDocCount);
    }

    /** The terms a shard's significant_terms carries, which the reduction then scores and cuts. */
    private static TreeSet<String> keys(InternalAggregation agg) {
        TreeSet<String> out = new TreeSet<>();
        for (SignificantTerms.Bucket b : ((SignificantStringTerms) agg).getBuckets()) {
            out.add(b.getKeyAsString());
        }
        return out;
    }

    /** The shard keeps r2 and r1, the best by score, though c and m are the most frequent. */
    public void testAQueryKeepsItsBestTermsByScore() {
        rareButSignificant();
        Run r = run(sig(2, 2, 1), new SearchSourceBuilder().query(new org.opensearch.index.query.TermQueryBuilder("p", "a")));
        assertEquals(new TreeSet<>(List.of("r1", "r2")), keys(r.result()));
        assertEquals(r.foregrounds().toString(), 1, r.foregrounds().size());
        assertFalse(r.foregrounds().get(0), r.foregrounds().get(0).contains("LIMIT"));
    }

    /** Under a parent, both buckets are one statement, and each keeps its own best by score. */
    public void testUnderAParentEachBucketKeepsItsBestTermsByScore() {
        rareButSignificant();
        AggregationBuilder parent = AggregationBuilders.terms("p").field("p").subAggregation(sig(2, 2, 1));
        Run r = run(parent, new SearchSourceBuilder());
        assertEquals(r.foregrounds().toString(), 1, r.foregrounds().size());
        assertTrue(r.foregrounds().get(0), r.foregrounds().get(0).contains("AS p0"));
        Map<String, TreeSet<String>> byParent = new java.util.HashMap<>();
        for (StringTerms.Bucket b : ((StringTerms) r.result()).getBuckets()) {
            byParent.put(b.getKeyAsString(), keys(b.getAggregations().get("sig")));
        }
        assertEquals(new TreeSet<>(List.of("r1", "r2")), byParent.get("a"));
        assertEquals(new TreeSet<>(List.of("c", "m")), byParent.get("b"));
    }

    /** min_doc_count is left to the reduction: r2, with 1 document, still takes the shard's one slot. */
    public void testMinDocCountDoesNotNarrowTheShard() {
        rareButSignificant();
        Run r = run(sig(1, 1, 2), new SearchSourceBuilder().query(new org.opensearch.index.query.TermQueryBuilder("p", "a")));
        assertEquals(new TreeSet<>(List.of("r2")), keys(r.result()));
        assertFalse(r.foregrounds().get(0), r.foregrounds().get(0).contains("HAVING"));
    }

    /**
     * shard_min_doc_count applies after the cut, as on a stock shard under
     * concurrent segment search: r2 takes the one slot, then is dropped.
     */
    public void testShardMinDocCountAppliesAfterTheCut() {
        rareButSignificant();
        SearchSourceBuilder a = new SearchSourceBuilder().query(new org.opensearch.index.query.TermQueryBuilder("p", "a"));
        Run r = run(sig(1, 1, 2).shardMinDocCount(2), a);
        assertEquals(new TreeSet<>(), keys(r.result()));
        assertFalse(r.foregrounds().get(0), r.foregrounds().get(0).contains("HAVING"));
        Run two = run(sig(2, 2, 1).shardMinDocCount(2), new SearchSourceBuilder().query(new org.opensearch.index.query.TermQueryBuilder("p", "a")));
        assertEquals(new TreeSet<>(List.of("r1")), keys(two.result()));
    }

    /** Two parents of {@code perParent} distinct tags each, one document per tag. */
    private static List<String> wideForegrounds(int perParent) {
        List<String[]> docs = new ArrayList<>();
        for (String p : new String[] { "a", "b" }) {
            for (int i = 0; i < perParent; i++) {
                docs.add(new String[] { p, p + i });
            }
        }
        index(docs);
        AggregationBuilder parent = AggregationBuilders.terms("p").field("p").subAggregation(sig(10, 25, 1));
        return run(parent, new SearchSourceBuilder()).foregrounds();
    }

    /**
     * 2 parents times 50,000 terms, each in one document: every bound ties
     * every score, so the fenced read proves no bucket, and both are read in
     * full in one statement, 100,000 rows, the bound.
     */
    public void testAtTheBoundBothBucketsAreReadInOneStatement() {
        List<String> read = wideForegrounds(50_000);
        assertEquals(read.toString(), 2, read.size());
        assertTrue(read.get(0), read.get(0).contains("ROW_NUMBER()"));
        assertTrue(read.get(1), read.get(1).contains("AS p0"));
        assertFalse(read.get(1), read.get(1).contains("ROW_NUMBER()"));
    }

    /** 2 parents times 50,001 terms is past it: after the fenced and the full read, each bucket reads its own. */
    public void testPastTheBoundEachBucketIsReadOnItsOwn() {
        List<String> read = wideForegrounds(50_001);
        assertEquals(read.toString(), 4, read.size());
        assertTrue(read.get(0), read.get(0).contains("ROW_NUMBER()"));
        assertTrue(read.get(1), read.get(1).contains("AS p0"));
        for (String s : read.subList(2, 4)) {
            assertFalse(s, s.contains("AS p0"));
        }
    }

    /**
     * The shape of big5's terms-significant-2: 7 parents (process.name), each
     * with all 800 values of the significant field (log_stream). 30 values
     * are each one parent's own, more frequent there; the rest are as common
     * in every parent and score 0. Each bucket's 51 best bounds come back,
     * 357 rows where every term was 5,600, and the 51st bound (0) is below
     * the 25th score, so no bucket is read again.
     */
    public void testBig5ShapeReadsOnlyEachBucketsBestBounds() {
        List<Object[]> docs = new ArrayList<>();
        for (int p = 0; p < 7; p++) {
            for (int i = 0; i < 800; i++) {
                int home = i / 30;
                int n = home == p ? 2 + i % 30 : 1;
                for (int d = 0; d < n; d++) {
                    docs.add(new Object[] { "p" + p, "s" + i });
                }
            }
        }
        indexDocs(docs, "tag");
        Run r = run(parentOver("tag", 10, 25), new SearchSourceBuilder());
        assertEquals(r.foregrounds().toString(), 1, r.foregrounds().size());
        String logged = r.foregrounds().get(0);
        assertTrue(logged, logged.contains("ROW_NUMBER()"));
        String sql = logged.substring("[infino] sql: ".length(), logged.lastIndexOf(" -> "));
        assertEquals(7 * 51, InfinoNative.sqlRows(sql).size());
        assertEquals(bruteBest(docs, 25, java.util.Set.of("p0", "p1", "p2", "p3", "p4", "p5", "p6")), shardBest(r));
    }

    /**
     * Bucket a's 60 terms are each in one of its documents and nowhere else,
     * so every bound equals every score: the 51st bound is not below the
     * 25th score, and a is read in full. Bucket b's terms have counts 1 to
     * 50: its 51st bound is below its 25th score, and it is not read again.
     * Both keep the best by score over every term.
     */
    public void testABucketWhoseFenceIsNotBelowItsKeptScoreIsReadInFull() {
        List<Object[]> docs = new ArrayList<>();
        for (int i = 0; i < 60; i++) {
            docs.add(new Object[] { "a", "a" + i });
        }
        for (int i = 0; i < 200; i++) {
            for (int d = 0; d <= i % 50; d++) {
                docs.add(new Object[] { "b", "b" + i });
            }
        }
        indexDocs(docs, "tag");
        Run r = run(parentOver("tag", 10, 25), new SearchSourceBuilder());
        assertEquals(r.foregrounds().toString(), 2, r.foregrounds().size());
        assertTrue(r.foregrounds().get(0), r.foregrounds().get(0).contains("ROW_NUMBER()"));
        String full = r.foregrounds().get(1);
        assertFalse(full, full.contains("ROW_NUMBER()"));
        assertTrue(full, full.contains("= 'a'"));
        assertFalse(full, full.contains("= 'b'"));
        assertEquals(bruteBest(docs, 25, java.util.Set.of("a", "b")), shardBest(r));
    }

    /** The bound-ordered foreground's statement, as built. */
    public void testFencedForegroundSql() {
        assertEquals(51, InfinoAggregationProcessor.fenceRows(25));
        String p = "(CAST(doc_count AS DOUBLE) / CAST(ptotal AS DOUBLE))";
        String q = "(CAST(ktotal AS DOUBLE) / CAST(1000 AS DOUBLE))";
        String jlh = "CASE WHEN (" + p + " - " + q + ") > 0.0 THEN (" + p + " - " + q + ") * (" + p + " / " + q + ") ELSE 0.0 END";
        assertEquals(jlh, InfinoAggregationProcessor.jlhScoreSql("doc_count", "ptotal", "ktotal", 1000));
        assertEquals(
            "SELECT * FROM (SELECT b.*, ROW_NUMBER() OVER (PARTITION BY p0 ORDER BY k0 IS NULL, ub DESC, k0) AS rn FROM (SELECT t.*, "
                + jlh
                + " AS ub FROM (SELECT g.*, SUM(doc_count) OVER (PARTITION BY p0) AS ptotal, SUM(doc_count) OVER (PARTITION BY k0) AS ktotal FROM ("
                + "SELECT \"p\" AS p0, (\"tag\") AS k0, COUNT(*) AS doc_count FROM \"t\" WHERE (x > 1) AND (\"p\" = 'a') GROUP BY 1, 2) g) t) b)"
                + " WHERE rn <= 5 ORDER BY p0, rn",
            InfinoAggregationProcessor.fencedForegroundSql(List.of("\"p\""), "(\"tag\")", "\"t\"", "x > 1", "(\"p\" = 'a')", 1000, 5)
        );
    }

    /** The engine's JLH from jlhScoreSql is Java's to the bit, over random counts. */
    public void testJlhInSqlIsJavasToTheBit() {
        JLHScore jlh = new JLHScore();
        for (int round = 0; round < 5; round++) {
            long supersetSize = randomFrom(1L, 31L, randomLongBetween(2, 1_000), randomLongBetween(1_000, 1L << 40));
            List<long[]> cases = new ArrayList<>();
            StringBuilder values = new StringBuilder();
            for (int i = 0; i < 300; i++) {
                long subsetSize = randomLongBetween(1, supersetSize);
                long subsetDf = randomLongBetween(1, subsetSize);
                // Half near the subset's share, where the score is near 0 or 0.
                long near = (long) Math.floor((double) subsetDf / subsetSize * supersetSize) + randomLongBetween(-1, 1);
                long supersetDf = randomBoolean() ? randomLongBetween(subsetDf, supersetSize) : Math.min(supersetSize, Math.max(subsetDf, near));
                cases.add(new long[] { subsetDf, subsetSize, supersetDf });
                values.append(i == 0 ? "" : ", ").append("(").append(subsetDf).append(", ").append(subsetSize).append(", ").append(supersetDf).append(")");
            }
            List<Map<String, Object>> rows = InfinoNative.sqlRows(
                "SELECT column1 AS a, column2 AS b, column3 AS c, "
                    + InfinoAggregationProcessor.jlhScoreSql("column1", "column2", "column3", supersetSize)
                    + " AS ub FROM (VALUES "
                    + values
                    + ")"
            );
            assertEquals(cases.size(), rows.size());
            for (Map<String, Object> row : rows) {
                long a = ((Number) row.get("a")).longValue(), b = ((Number) row.get("b")).longValue(), c = ((Number) row.get("c")).longValue();
                double want = jlh.getScore(a, b, c, supersetSize);
                double got = ((Number) row.get("ub")).doubleValue();
                assertEquals(a + "/" + b + " vs " + c + "/" + supersetSize + ": " + got + " vs " + want, Double.doubleToLongBits(want), Double.doubleToLongBits(got));
            }
        }
    }

    /**
     * Random parents and terms, the counts from a narrow range so bounds and
     * scores often tie, some documents without a parent or a term, the
     * parent level keeping all its buckets or fewer, string terms (whose
     * background is the table's value counts) or numbers (whose counts are
     * statements): each kept bucket keeps the best by score over every term,
     * in order, with the same counts. Several rounds over fresh tables.
     */
    public void testRandomFencedForegroundsKeepTheBestByScore() {
        for (int round = 0; round < 6; round++) {
            InfinoNative.dropTable(TABLE);
            InfinoNative.ensureTable(TABLE, "{\"properties\":{\"p\":{\"type\":\"keyword\"},\"tag\":{\"type\":\"keyword\"},\"code\":{\"type\":\"long\"}}}");
            boolean strings = randomBoolean();
            int parents = between(1, 5);
            int keep = between(1, 6);
            int maxCount = between(1, 4);
            List<Object[]> docs = new ArrayList<>();
            for (int p = 0; p < parents; p++) {
                int terms = between(0, 80);
                for (int t = 0; t < terms; t++) {
                    long code = between(0, 120);
                    int n = between(1, maxCount);
                    for (int d = 0; d < n; d++) {
                        docs.add(new Object[] { "p" + p, strings ? (Object) ("t" + code) : (Object) code });
                    }
                }
                for (int d = between(0, 3); d > 0; d--) {
                    docs.add(new Object[] { "p" + p, null });
                }
            }
            for (int d = between(0, 40); d > 0; d--) {
                long code = between(0, 120);
                docs.add(new Object[] { null, strings ? (Object) ("t" + code) : (Object) code });
            }
            if (docs.isEmpty()) {
                continue;
            }
            indexDocs(docs, strings ? "tag" : "code");
            int parentSize = between(1, parents + 1);
            Run r = run(parentOver(strings ? "tag" : "code", parentSize, keep), new SearchSourceBuilder());
            Map<String, List<String>> got = shardBest(r);
            java.util.Set<String> withParent = new TreeSet<>();
            for (Object[] d : docs) {
                if (d[0] != null) {
                    withParent.add((String) d[0]);
                }
            }
            assertEquals("round " + round, Math.min(parentSize, withParent.size()), got.size());
            assertEquals("round " + round, bruteBest(docs, keep, got.keySet()), got);
        }
    }

    /** A terms parent over p, keeping {@code parents} buckets, over significant {@code field} keeping {@code keep} terms. */
    private static AggregationBuilder parentOver(String field, int parents, int keep) {
        return AggregationBuilders.terms("p")
            .field("p")
            .size(parents)
            .shardSize(parents)
            .subAggregation(AggregationBuilders.significantTerms("sig").field(field).size(keep).shardSize(keep).minDocCount(1));
    }

    /** Index one document per (p, term) pair, either may be null, the term into {@code field}. */
    private static void indexDocs(List<Object[]> docs, String field) {
        String[] ids = new String[docs.size()];
        String[] sources = new String[docs.size()];
        for (int i = 0; i < docs.size(); i++) {
            ids[i] = Integer.toString(i);
            List<String> fields = new ArrayList<>();
            if (docs.get(i)[0] != null) {
                fields.add("\"p\":\"" + docs.get(i)[0] + "\"");
            }
            Object term = docs.get(i)[1];
            if (term != null) {
                fields.add("\"" + field + "\":" + (term instanceof String ? "\"" + term + "\"" : term.toString()));
            }
            sources[i] = "{" + String.join(",", fields) + "}";
        }
        assertEquals(docs.size(), InfinoNative.indexBatch(TABLE, ids, sources, ids.length).ok());
    }

    /**
     * Each kept bucket's significant terms, as the shard returns them, best
     * first: key, count in the bucket, count in the background.
     */
    private static Map<String, List<String>> shardBest(Run r) {
        Map<String, List<String>> out = new java.util.TreeMap<>();
        for (StringTerms.Bucket b : ((StringTerms) r.result()).getBuckets()) {
            List<String> terms = new ArrayList<>();
            SignificantTerms sig = b.getAggregations().get("sig");
            for (SignificantTerms.Bucket c : sig.getBuckets()) {
                assertEquals(b.getDocCount(), c.getSubsetSize());
                terms.add(c.getKeyAsString() + ":" + c.getSubsetDf() + ":" + c.getSupersetDf());
            }
            out.put(b.getKeyAsString(), terms);
        }
        return out;
    }

    /**
     * Each of {@code parents}' best terms worked out here from the documents,
     * as {@link #shardBest} prints them: every term of the bucket scored by
     * JLH against the whole table, best first, ties by key (strings by
     * bytes, numbers by value), the first {@code keep}.
     */
    private static Map<String, List<String>> bruteBest(List<Object[]> docs, int keep, java.util.Set<String> parents) {
        Map<Object, Long> background = new java.util.HashMap<>();
        Map<String, Long> sizes = new java.util.HashMap<>();
        Map<String, Map<Object, Long>> counts = new java.util.HashMap<>();
        for (Object[] d : docs) {
            if (d[1] != null) {
                background.merge(d[1], 1L, Long::sum);
            }
            if (d[0] != null) {
                sizes.merge((String) d[0], 1L, Long::sum);
                if (d[1] != null) {
                    counts.computeIfAbsent((String) d[0], k -> new java.util.HashMap<>()).merge(d[1], 1L, Long::sum);
                }
            }
        }
        JLHScore jlh = new JLHScore();
        Map<String, List<String>> out = new java.util.TreeMap<>();
        for (Map.Entry<String, Long> parent : sizes.entrySet()) {
            if (!parents.contains(parent.getKey())) {
                continue;
            }
            List<Map.Entry<Object, Long>> terms = new ArrayList<>(counts.getOrDefault(parent.getKey(), Map.of()).entrySet());
            long size = parent.getValue();
            java.util.function.ToDoubleFunction<Map.Entry<Object, Long>> score = e -> jlh.getScore(e.getValue(), size, background.get(e.getKey()), docs.size());
            terms.sort((x, y) -> {
                int byScore = Double.compare(score.applyAsDouble(y), score.applyAsDouble(x));
                if (byScore != 0) {
                    return byScore;
                }
                if (x.getKey() instanceof Long a && y.getKey() instanceof Long b) {
                    return Long.compare(a, b);
                }
                return new org.apache.lucene.util.BytesRef(x.getKey().toString()).compareTo(new org.apache.lucene.util.BytesRef(y.getKey().toString()));
            });
            List<String> best = new ArrayList<>();
            for (Map.Entry<Object, Long> e : terms.subList(0, Math.min(keep, terms.size()))) {
                best.add(e.getKey() + ":" + e.getValue() + ":" + background.get(e.getKey()));
            }
            out.put(parent.getKey(), best);
        }
        return out;
    }

    /**
     * Each parent bucket's best codes by JLH over every code, worked out
     * here from the counts: what the shard must keep.
     */
    private static Map<String, TreeSet<Long>> expectedBest(Map<String, Map<Long, Integer>> counts, List<String> parents, int keep) {
        Map<Long, Long> background = new java.util.HashMap<>();
        long supersetSize = 0;
        for (Map<Long, Integer> byCode : counts.values()) {
            for (Map.Entry<Long, Integer> e : byCode.entrySet()) {
                background.merge(e.getKey(), (long) e.getValue(), Long::sum);
                supersetSize += e.getValue();
            }
        }
        Map<String, TreeSet<Long>> out = new java.util.HashMap<>();
        for (String p : parents) {
            long subsetSize = counts.get(p).values().stream().mapToLong(Integer::longValue).sum();
            List<InfinoAggregationProcessor.Significant.Candidate> all = new ArrayList<>();
            for (Map.Entry<Long, Integer> e : counts.get(p).entrySet()) {
                all.add(term(e.getKey(), e.getValue(), Math.max(e.getValue(), background.get(e.getKey()))));
            }
            TreeSet<Long> best = new TreeSet<>();
            for (InfinoAggregationProcessor.Significant.Candidate c : InfinoAggregationProcessor.Significant.bestByScore(all, keep, 0, subsetSize, supersetSize, new JLHScore())) {
                best.add((Long) c.key());
            }
            out.put(p, best);
        }
        return out;
    }

    /**
     * Index {@code counts} (parent, then code, then documents) and run a terms
     * parent of size {@code parents} over significant codes. The codes are
     * numbers, whose background the table's value counts never read whole,
     * so their counts are statements.
     */
    private static Run runCounts(Map<String, Map<Long, Integer>> counts, int parents, int keep) {
        List<String> ids = new ArrayList<>();
        List<String> sources = new ArrayList<>();
        for (Map.Entry<String, Map<Long, Integer>> byParent : counts.entrySet()) {
            for (Map.Entry<Long, Integer> e : byParent.getValue().entrySet()) {
                for (int i = 0; i < e.getValue(); i++) {
                    ids.add(Integer.toString(ids.size()));
                    sources.add("{\"p\":\"" + byParent.getKey() + "\",\"code\":" + e.getKey() + "}");
                }
            }
        }
        assertEquals(ids.size(), InfinoNative.indexBatch(TABLE, ids.toArray(new String[0]), sources.toArray(new String[0]), ids.size()).ok());
        AggregationBuilder parent = AggregationBuilders.terms("p")
            .field("p")
            .size(parents)
            // The shard keeps exactly these parents, so the last, smallest one is a bucket of none.
            .shardSize(parents)
            .subAggregation(AggregationBuilders.significantTerms("sig").field("code").size(keep).shardSize(keep).minDocCount(1));
        return run(parent, new SearchSourceBuilder());
    }

    private static Map<String, TreeSet<Long>> keptByParent(Run r) {
        Map<String, TreeSet<Long>> out = new java.util.HashMap<>();
        for (StringTerms.Bucket b : ((StringTerms) r.result()).getBuckets()) {
            TreeSet<Long> codes = new TreeSet<>();
            for (SignificantTerms.Bucket c : ((SignificantTerms) b.getAggregations().get("sig")).getBuckets()) {
                codes.add(((Number) c.getKey()).longValue());
            }
            out.put(b.getKeyAsString(), codes);
        }
        return out;
    }

    /** The codes the background statements of {@code r} counted, all together. */
    private static int backgroundTerms(Run r) {
        int n = 0;
        for (String s : r.backgrounds()) {
            String list = s.substring(s.indexOf("WHERE k0 IN (") + "WHERE k0 IN (".length(), s.indexOf(") GROUP BY"));
            n += list.split(",").length;
        }
        return n;
    }

    /**
     * Two kept parents of 200 codes each, and a third, smaller parent the
     * terms level does not keep, which adds to every code's background.
     * A code's background count is at least its count over the kept
     * buckets, which bounds its JLH score from above; only the codes whose
     * bound reaches a bucket's fifth best score are counted, in rounds, and each bucket keeps the same codes as when every code is scored.
     */
    public void testOnlyTermsThatCanBeKeptAreCounted() {
        Map<String, Map<Long, Integer>> counts = new java.util.LinkedHashMap<>();
        for (String p : new String[] { "a", "b", "z" }) {
            counts.put(p, new java.util.LinkedHashMap<>());
        }
        for (long t = 0; t < 200; t++) {
            counts.get("a").put(t, (int) (10 + t % 7));
            counts.get("b").put(t, (int) (10 + (t * 3) % 11));
            counts.get("z").put(t, 3);
        }
        Run r = runCounts(counts, 2, 5);
        assertEquals(expectedBest(counts, List.of("a", "b"), 5), keptByParent(r));
        // 70 when ties in the first round fall as worked out by hand; far below 200 whichever way they fall.
        assertTrue(r.backgrounds().toString(), backgroundTerms(r) < 100);
    }

    /** Random counts: each kept bucket's codes are the best by score over every code, however few were counted. */
    public void testRandomCountsKeepTheBestByScore() {
        int codes = between(1, 120);
        int parents = between(1, 4);
        Map<String, Map<Long, Integer>> counts = new java.util.LinkedHashMap<>();
        for (int p = 0; p < parents + 1; p++) {
            Map<Long, Integer> byCode = new java.util.LinkedHashMap<>();
            for (long t = 1; t < codes; t++) {
                if (randomBoolean()) {
                    // The last parent, which the terms level does not keep, is smaller than every kept one.
                    byCode.put(t, p == parents ? 1 : between(1, 30));
                }
            }
            byCode.put(0L, p == parents ? 1 : 30 * codes);
            counts.put("p" + p, byCode);
        }
        int keep = between(1, 10);
        Run r = runCounts(counts, parents, keep);
        List<String> kept = new ArrayList<>(keptByParent(r).keySet());
        assertEquals(parents, kept.size());
        assertEquals(expectedBest(counts, kept, keep), keptByParent(r));
    }

    private static InfinoAggregationProcessor.Significant.Candidate term(Object key, long subsetDf, long supersetDf) {
        return new InfinoAggregationProcessor.Significant.Candidate(key, subsetDf, supersetDf, Map.of());
    }

    private static List<Object> keysOf(List<InfinoAggregationProcessor.Significant.Candidate> cs) {
        List<Object> out = new ArrayList<>();
        for (InfinoAggregationProcessor.Significant.Candidate c : cs) {
            out.add(c.key());
        }
        return out;
    }

    /** The cut is by score, highest first, whatever the counts. */
    public void testBestByScoreIgnoresCounts() {
        List<InfinoAggregationProcessor.Significant.Candidate> terms = List.of(term("c", 4, 16), term("m", 3, 10), term("r1", 2, 4), term("r2", 1, 1));
        assertEquals(List.of("r2", "r1"), keysOf(InfinoAggregationProcessor.Significant.bestByScore(terms, 2, 0, 10, 31, new JLHScore())));
        assertEquals(List.of("r2"), keysOf(InfinoAggregationProcessor.Significant.bestByScore(terms, 1, 0, 10, 31, new JLHScore())));
        assertEquals(4, InfinoAggregationProcessor.Significant.bestByScore(terms, 10, 0, 10, 31, new JLHScore()).size());
    }

    /** Among equal scores the key that sorts first is kept: bytes for strings, value for numbers. */
    public void testBestByScoreBreaksTiesByKey() {
        List<InfinoAggregationProcessor.Significant.Candidate> terms = List.of(term("y", 2, 2), term("x", 2, 2), term("z", 2, 2));
        assertEquals(List.of("x", "y"), keysOf(InfinoAggregationProcessor.Significant.bestByScore(terms, 2, 0, 10, 100, new JLHScore())));
        List<InfinoAggregationProcessor.Significant.Candidate> longs = List.of(term(10L, 2, 2), term(9L, 2, 2));
        assertEquals(List.of(9L), keysOf(InfinoAggregationProcessor.Significant.bestByScore(longs, 1, 0, 10, 100, new JLHScore())));
    }

    /** A script heuristic cannot run on the shard: every term goes on to the reduction. */
    public void testScriptHeuristicKeepsEveryTerm() {
        List<InfinoAggregationProcessor.Significant.Candidate> terms = List.of(term("c", 4, 16), term("r2", 1, 1), term("m", 3, 10));
        ScriptHeuristic script = new ScriptHeuristic(new Script("_subset_freq"));
        assertEquals(3, InfinoAggregationProcessor.Significant.bestByScore(terms, 1, 0, 10, 31, script).size());
    }
}
