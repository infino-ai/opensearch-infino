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
import org.opensearch.core.index.Index;
import org.opensearch.script.Script;
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
        InfinoNative.dropTable(TABLE);
        InfinoNative.ensureTable(TABLE, "{\"properties\":{\"p\":{\"type\":\"keyword\"},\"tag\":{\"type\":\"keyword\"}}}");
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

    /** A run of {@code agg} over the table, with the foreground statements it ran. */
    private record Run(InternalAggregation result, List<String> foregrounds) {}

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
        for (String s : statements.sql) {
            if (s.contains("AS k0, COUNT(*) AS doc_count FROM")) {
                out.add(s);
            }
        }
        return new Run(result, out);
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

    /** 2 parents times 5,000 terms is 10,000 rows, the bound: one statement reads both foregrounds. */
    public void testAtTheBoundBothBucketsAreReadInOneStatement() {
        List<String> read = wideForegrounds(5_000);
        assertEquals(read.toString(), 1, read.size());
        assertTrue(read.get(0), read.get(0).contains("AS p0"));
    }

    /** 2 parents times 5,001 terms is past it: after the batched read, each bucket reads its own. */
    public void testPastTheBoundEachBucketIsReadOnItsOwn() {
        List<String> read = wideForegrounds(5_001);
        assertEquals(read.toString(), 3, read.size());
        assertTrue(read.get(0), read.get(0).contains("AS p0"));
        for (String s : read.subList(1, 3)) {
            assertFalse(s, s.contains("AS p0"));
        }
    }

    /**
     * The shape of big5's terms-significant-2: 7 parents (process.name), each
     * with 800 values of the significant field (log_stream). Every term of
     * every bucket comes back, 5,600 rows where a cut by count returned 7 x 25
     * = 175, and that is still one statement.
     */
    public void testBig5ShapeIsOneStatementOfEveryTerm() {
        List<String[]> docs = new ArrayList<>();
        for (int p = 0; p < 7; p++) {
            for (int i = 0; i < 800; i++) {
                docs.add(new String[] { "p" + p, "s" + i });
            }
        }
        index(docs);
        AggregationBuilder parent = AggregationBuilders.terms("p").field("p").size(10).subAggregation(AggregationBuilders.significantTerms("sig").field("tag"));
        List<String> read = run(parent, new SearchSourceBuilder()).foregrounds();
        assertEquals(read.toString(), 1, read.size());
        String logged = read.get(0);
        String sql = logged.substring("[infino] sql: ".length(), logged.lastIndexOf(" -> "));
        int rows = InfinoNative.sqlRows(sql).size();
        logger.info("big5-shaped significant_terms foreground: {} rows in one statement", rows);
        assertEquals(5_600, rows);
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
