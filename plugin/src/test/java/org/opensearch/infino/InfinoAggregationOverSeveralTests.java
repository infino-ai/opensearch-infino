/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.index.Index;
import org.opensearch.infino.InfinoNative.Column;
import org.opensearch.search.SearchModule;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalCardinality;
import org.opensearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.opensearch.search.aggregations.metrics.Sum;
import org.opensearch.search.aggregations.metrics.ValueCount;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Cardinality and percentiles over several tables read as one, as a search
 * over several indices answers them: a field one index does not map is read
 * from the indices that map it, whichever index comes first, and a field two
 * indices map as different kinds of value is refused rather than answered
 * otherwise than stock would.
 */
public class InfinoAggregationOverSeveralTests extends OpenSearchTestCase {

    /** Maps neither field the aggregations read. */
    private static final String WITHOUT = "over_several_without";
    /** Maps user as a keyword and latency as a long. */
    private static final String WITH = "over_several_with";
    /** Maps user as a long. */
    private static final String USER_LONG = "over_several_user_long";
    /** Maps latency as an integer. */
    private static final String LATENCY_INT = "over_several_latency_int";
    /**
     * Map name as text with a keyword multi-field, who as an alias of uid, and
     * tags and n, which some documents give several values. Their documents
     * have the same ids, as two indices' documents may.
     */
    private static final String MULTI_A = "over_several_multi_a";
    private static final String MULTI_B = "over_several_multi_b";
    private static final String MULTI_MAPPING = "{\"properties\":{\"name\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\"}}},"
        + "\"uid\":{\"type\":\"keyword\"},\"who\":{\"type\":\"alias\",\"path\":\"uid\"},\"tags\":{\"type\":\"keyword\"},\"n\":{\"type\":\"long\"}}}";
    /** Rows of each of {@link #MULTI_A} and {@link #MULTI_B}. */
    private static final int MULTI_ROWS = 10;
    /** How far {@link #MULTI_B}'s names and uids are shifted from {@link #MULTI_A}'s, so the two share half of them. */
    private static final int SHIFT = 5;
    /** Every third document of each gives n two values: its own and this one's (A) or twice it (B). */
    private static final int SECOND_N = 10;
    /** Maps hostname as a keyword. */
    private static final String HOSTNAME = "over_several_hostname";
    /** Maps host.name as a keyword, with hostname an alias of it: the field renamed, its old name kept. */
    private static final String RENAMED = "over_several_renamed";
    /** Maps n as a long and a as an alias of it. */
    private static final String N_ALIASED = "over_several_n_aliased";
    /** Maps n as a long, and no alias. */
    private static final String N_PLAIN = "over_several_n_plain";
    /** Map p and q as longs, x an alias of p in one and of q in the other. */
    private static final String X_TO_P = "over_several_x_to_p";
    private static final String X_TO_Q = "over_several_x_to_q";
    /** Maps n as a long and a float, some documents of each giving it several values. */
    private static final String LIST_LONG = "over_several_list_long";
    private static final String LIST_FLOAT = "over_several_list_float";
    private static final List<String> TABLES = List.of(
        WITHOUT,
        WITH,
        USER_LONG,
        LATENCY_INT,
        MULTI_A,
        MULTI_B,
        HOSTNAME,
        RENAMED,
        N_ALIASED,
        N_PLAIN,
        X_TO_P,
        X_TO_Q,
        LIST_LONG,
        LIST_FLOAT
    );
    /** Rows of {@link #WITH}: user u(i mod USERS), latency i. */
    private static final int ROWS = 100;
    private static final int USERS = 10;
    private static final double MEDIAN = 50;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // A cardinality's sketch is read back through the plugin's registry, as on a node.
        InfinoServices.namedWriteableRegistry(new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedWriteables()));
        for (String t : TABLES) {
            InfinoNative.dropTable(t);
        }
        InfinoNative.ensureTable(WITHOUT, "{\"properties\":{\"other\":{\"type\":\"keyword\"}}}");
        InfinoNative.ensureTable(WITH, "{\"properties\":{\"user\":{\"type\":\"keyword\"},\"latency\":{\"type\":\"long\"}}}");
        InfinoNative.ensureTable(USER_LONG, "{\"properties\":{\"user\":{\"type\":\"long\"}}}");
        InfinoNative.ensureTable(LATENCY_INT, "{\"properties\":{\"latency\":{\"type\":\"integer\"}}}");
        index(WITHOUT, List.of("{\"other\":\"x\"}"));
        List<String> with = new ArrayList<>();
        for (int i = 0; i < ROWS; i++) {
            with.add("{\"user\":\"u" + (i % USERS) + "\",\"latency\":" + i + "}");
        }
        index(WITH, with);
        index(USER_LONG, List.of("{\"user\":5}"));
        index(LATENCY_INT, List.of("{\"latency\":" + ROWS + "}"));
        InfinoNative.ensureTable(MULTI_A, MULTI_MAPPING);
        InfinoNative.ensureTable(MULTI_B, MULTI_MAPPING);
        List<String> a = new ArrayList<>();
        List<String> b = new ArrayList<>();
        for (int i = 0; i < MULTI_ROWS; i++) {
            boolean even = i % 2 == 0;
            String nA = i % 3 == 0 ? "[" + i + "," + SECOND_N + "]" : Integer.toString(i);
            String nB = i % 3 == 0 ? "[" + i + "," + 2 * SECOND_N + "]" : Integer.toString(i);
            a.add("{\"name\":\"n" + i + "\",\"uid\":\"u" + i + "\",\"tags\":" + (even ? "[\"red\",\"blue\"]" : "\"red\"") + ",\"n\":" + nA + "}");
            int j = i + SHIFT;
            b.add("{\"name\":\"n" + j + "\",\"uid\":\"u" + j + "\",\"tags\":" + (even ? "[\"red\",\"green\"]" : "\"blue\"") + ",\"n\":" + nB + "}");
        }
        index(MULTI_A, a);
        index(MULTI_B, b);
        InfinoNative.ensureTable(HOSTNAME, "{\"properties\":{\"hostname\":{\"type\":\"keyword\"},\"user\":{\"type\":\"keyword\"}}}");
        InfinoNative.ensureTable(
            RENAMED,
            "{\"properties\":{\"host\":{\"properties\":{\"name\":{\"type\":\"keyword\"}}},"
                + "\"hostname\":{\"type\":\"alias\",\"path\":\"host.name\"},\"user\":{\"type\":\"keyword\"}}}"
        );
        index(HOSTNAME, List.of("{\"hostname\":\"a1\",\"user\":\"u1\"}", "{\"hostname\":\"a2\",\"user\":\"u2\"}"));
        index(RENAMED, List.of("{\"host\":{\"name\":\"b1\"},\"user\":\"u2\"}", "{\"host\":{\"name\":\"b2\"},\"user\":\"u3\"}"));
        InfinoNative.ensureTable(N_ALIASED, "{\"properties\":{\"n\":{\"type\":\"long\"},\"a\":{\"type\":\"alias\",\"path\":\"n\"}}}");
        InfinoNative.ensureTable(N_PLAIN, "{\"properties\":{\"n\":{\"type\":\"long\"}}}");
        index(N_ALIASED, List.of("{\"n\":1}", "{\"n\":2}"));
        index(N_PLAIN, List.of("{\"n\":100}"));
        String pq = "\"p\":{\"type\":\"long\"},\"q\":{\"type\":\"long\"}";
        InfinoNative.ensureTable(X_TO_P, "{\"properties\":{" + pq + ",\"x\":{\"type\":\"alias\",\"path\":\"p\"}}}");
        InfinoNative.ensureTable(X_TO_Q, "{\"properties\":{" + pq + ",\"x\":{\"type\":\"alias\",\"path\":\"q\"}}}");
        index(X_TO_P, List.of("{\"p\":1,\"q\":10}"));
        index(X_TO_Q, List.of("{\"p\":2,\"q\":20}"));
        InfinoNative.ensureTable(LIST_LONG, "{\"properties\":{\"n\":{\"type\":\"long\"}}}");
        InfinoNative.ensureTable(LIST_FLOAT, "{\"properties\":{\"n\":{\"type\":\"float\"}}}");
        index(LIST_LONG, List.of("{\"n\":1}", "{\"n\":[2,3]}"));
        index(LIST_FLOAT, List.of("{\"n\":1.5}", "{\"n\":[2.5,3.5]}"));
    }

    @Override
    public void tearDown() throws Exception {
        try {
            for (String t : TABLES) {
                InfinoNative.dropTable(t);
            }
        } finally {
            super.tearDown();
        }
    }

    private static void index(String table, List<String> sources) {
        String[] ids = new String[sources.size()];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = Integer.toString(i);
        }
        assertEquals(ids.length, InfinoNative.indexBatch(table, ids, sources.toArray(new String[0]), ids.length).ok());
    }

    /** {@code agg} over {@code tables} read as one, each table's rows under its own plan, the first table's plan leading. */
    private static InternalAggregations over(AggregationBuilder agg, String... tables) {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(agg);
        long now = System.currentTimeMillis();
        List<InfinoSearch.Plan> plans = new ArrayList<>();
        List<String> indices = new ArrayList<>();
        for (String t : tables) {
            plans.add(InfinoSearch.plan(t, source, now));
            indices.add(t);
        }
        return new InfinoAggregationProcessor(new Index(tables[0], "_na_")).evaluateOver(List.of(agg), plans.get(0), plans, indices, null)
            .aggregations();
    }

    private static long cardinality(String... tables) {
        return ((InternalCardinality) over(AggregationBuilders.cardinality("c").field("user"), tables).get("c")).getValue();
    }

    private static double median(String... tables) {
        return ((InternalTDigestPercentiles) over(AggregationBuilders.percentiles("p").field("latency").percentiles(MEDIAN), tables).get("p"))
            .percentile(MEDIAN);
    }

    public void testAFieldTheFirstIndexDoesNotMapIsReadFromTheOthers() {
        assertEquals(USERS, cardinality(WITH));
        // Planned against the first table's columns, the field read as unmapped in every table: 0.
        assertEquals(USERS, cardinality(WITHOUT, WITH));
        assertEquals(USERS, cardinality(WITH, WITHOUT));
        double alone = median(WITH);
        assertEquals(alone, median(WITHOUT, WITH), 0);
        assertEquals(alone, median(WITH, WITHOUT), 0);
    }

    public void testAFieldMappedAsDifferentKindsIsRefused() {
        InfinoAggregationProcessor.Unsupported e = expectThrows(InfinoAggregationProcessor.Unsupported.class, () -> cardinality(WITH, USER_LONG));
        assertEquals("[infino] aggregation [c] on [user] over several indices that map it as different types is not served", e.getMessage());
        e = expectThrows(InfinoAggregationProcessor.Unsupported.class, () -> cardinality(USER_LONG, WITH));
        assertEquals("[infino] aggregation [c] on [user] over several indices that map it as different types is not served", e.getMessage());
        // Under a bucket too.
        e = expectThrows(
            InfinoAggregationProcessor.Unsupported.class,
            () -> over(AggregationBuilders.terms("t").field("other").subAggregation(AggregationBuilders.cardinality("c").field("user")), WITH, USER_LONG)
        );
        assertEquals("[infino] aggregation [c] on [user] over several indices that map it as different types is not served", e.getMessage());
        // A field of different kinds that the aggregations do not read is no reason to refuse.
        assertEquals(MEDIAN, median(WITH, USER_LONG), 1);
    }

    /**
     * An index the search reads no row of (the security plugin's own) takes no
     * part in an aggregation over several indices read as one: its columns
     * would join the union's, so here its long {@code user} would meet the
     * others' keyword one and refuse the cardinality.
     */
    public void testAnIndexReadOfNoRowTakesNoPartOverSeveral() {
        Index a = new Index("over_several_a", "_na_");
        Index b = new Index("over_several_b", "_na_");
        Index unread = new Index("over_several_unread", "_na_");
        Map<Index, String> tables = Map.of(a, WITH, b, WITHOUT, unread, USER_LONG);
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.cardinality("c").field("user"));
        InfinoSearch.Searched searched = InfinoSearch.search(
            List.of(a, b, unread),
            tables::get,
            index -> index.equals(unread) ? InfinoAccess.NO_ROWS : InfinoAccess.NONE,
            source,
            null,
            null,
            null,
            null,
            System.nanoTime()
        );
        InternalCardinality c = searched.response().getAggregations().get("c");
        assertEquals(USERS, c.getValue());
    }

    public void testIntegersOfAnyWidthAreOneKind() {
        // latency is a long in one table, an integer in the other: stock reads both as longs.
        long now = System.currentTimeMillis();
        SearchSourceBuilder source = new SearchSourceBuilder().size(0);
        Column asLong = InfinoSearch.plan(WITH, source, now).cols().get("latency");
        Column asInt = InfinoSearch.plan(LATENCY_INT, source, now).cols().get("latency");
        assertTrue(InfinoAggregationProcessor.sameKind(asLong, asInt));
        assertFalse(
            InfinoAggregationProcessor.sameKind(
                InfinoSearch.plan(WITH, source, now).cols().get("user"),
                InfinoSearch.plan(USER_LONG, source, now).cols().get("user")
            )
        );
        InternalTDigestPercentiles p = (InternalTDigestPercentiles) over(
            AggregationBuilders.percentiles("p").field("latency").percentiles(100),
            WITH,
            LATENCY_INT
        ).get("p");
        assertEquals(ROWS, p.percentile(100), 0);
    }

    /**
     * A keyword multi-field and a field alias over several indices: the
     * plan keys their columns by the field's name, so the union's columns
     * are keyed so too. Keyed by the column's name, name.keyword met the
     * text column name and was refused as mapped as different types, and
     * the alias was not found at all.
     */
    public void testAKeywordSubfieldAndAnAliasAreReadOverSeveral() {
        CardinalityAggregationBuilder byKeyword = AggregationBuilders.cardinality("c").field("name.keyword");
        assertEquals(MULTI_ROWS, ((InternalCardinality) over(byKeyword, MULTI_A).get("c")).getValue());
        assertEquals(MULTI_ROWS + SHIFT, ((InternalCardinality) over(byKeyword, MULTI_A, MULTI_B).get("c")).getValue());
        // Two shards of one index: the same mapping twice.
        assertEquals(MULTI_ROWS, ((InternalCardinality) over(byKeyword, MULTI_A, MULTI_A).get("c")).getValue());
        CardinalityAggregationBuilder byAlias = AggregationBuilders.cardinality("c").field("who");
        assertEquals(MULTI_ROWS + SHIFT, ((InternalCardinality) over(byAlias, MULTI_A, MULTI_B).get("c")).getValue());
        Terms t = (Terms) over(
            AggregationBuilders.terms("t").field("who").size(1).order(BucketOrder.key(true)).subAggregation(byKeyword),
            MULTI_A,
            MULTI_B
        ).get("t");
        assertEquals("u0", t.getBuckets().get(0).getKeyAsString());
        assertEquals(1, ((InternalCardinality) t.getBuckets().get(0).getAggregations().get("c")).getValue());
    }

    /**
     * A terms level over a field some documents give several values, with
     * a cardinality and a sum over another such field under it, over two
     * indices whose documents have the same ids: each document is told apart
     * by its index as well as its id. By id alone, A's document 0 and B's
     * document 0 both tagged red were one document, and B's documents read
     * A's figures; the statement did not parse either, as the union was
     * given a second alias.
     */
    public void testFieldsOfSeveralValuesAreReadPerDocumentOfEachIndex() {
        Terms t = (Terms) over(
            AggregationBuilders.terms("t")
                .field("tags")
                .subAggregation(AggregationBuilders.cardinality("c").field("uid"))
                .subAggregation(AggregationBuilders.sum("s").field("n"))
                .subAggregation(AggregationBuilders.count("v").field("n")),
            MULTI_A,
            MULTI_B
        ).get("t");
        Map<String, long[]> got = new HashMap<>();
        for (Terms.Bucket b : t.getBuckets()) {
            got.put(
                b.getKeyAsString(),
                new long[] {
                    b.getDocCount(),
                    ((InternalCardinality) b.getAggregations().get("c")).getValue(),
                    (long) ((Sum) b.getAggregations().get("s")).getValue(),
                    ((ValueCount) b.getAggregations().get("v")).getValue() }
            );
        }
        // Worked out from the documents setUp writes: {documents, distinct uids, sum of n, values of n}.
        assertArrayEquals(new long[] { 15, 12, 145, 21 }, got.get("red"));
        assertArrayEquals(new long[] { 10, 8, 105, 14 }, got.get("blue"));
        assertArrayEquals(new long[] { 5, 5, 60, 7 }, got.get("green"));
        assertEquals(3, got.size());
        // A sum alone over both, beside a cardinality, reads every index's values.
        InternalAggregations top = over(AggregationBuilders.sum("s").field("n"), MULTI_A, MULTI_B);
        assertEquals(210, ((Sum) top.get("s")).getValue(), 0);
    }

    /**
     * A terms level under one over a field of several values, over two
     * indices whose documents have the same ids: each index's documents
     * count apart, so u5, tagged red in both, holds two documents.
     */
    public void testATermsLevelUnderOneOfSeveralValuesCountsEachIndexsDocuments() {
        Terms t = (Terms) over(
            AggregationBuilders.terms("t")
                .field("tags")
                .subAggregation(
                    AggregationBuilders.terms("u").field("uid").size(MULTI_ROWS + SHIFT).subAggregation(AggregationBuilders.cardinality("c").field("name.keyword"))
                ),
            MULTI_A,
            MULTI_B
        ).get("t");
        Terms red = t.getBucketByKey("red").getAggregations().get("u");
        // A's ten documents and B's five even ones: u0 to u9, u11 and u13.
        assertEquals(12, red.getBuckets().size());
        assertEquals(2, red.getBucketByKey("u5").getDocCount());
        assertEquals(1, ((InternalCardinality) red.getBucketByKey("u5").getAggregations().get("c")).getValue());
        assertEquals(1, red.getBucketByKey("u11").getDocCount());
        Terms blue = t.getBucketByKey("blue").getAggregations().get("u");
        assertEquals(2, blue.getBucketByKey("u6").getDocCount());
    }

    /**
     * A range over a field of several values, which reads the documents any
     * of whose values falls in a range by id from one table, is refused over
     * several indices rather than read from the first index's documents.
     */
    public void testARangeOverAFieldOfSeveralValuesIsRefusedOverSeveral() {
        InfinoAggregationProcessor.Unsupported e = expectThrows(
            InfinoAggregationProcessor.Unsupported.class,
            () -> over(
                AggregationBuilders.range("r").field("n").addRange(0, SHIFT).subAggregation(AggregationBuilders.cardinality("c").field("uid")),
                MULTI_A,
                MULTI_B
            )
        );
        assertEquals(
            "[infino] a bucket over [n], which documents give several values, read per document over several indices read as one is not served",
            e.getMessage()
        );
    }

    /** The message an aggregation on {@code field} over indices that do not map it to one field is refused with. */
    private static String notOneField(String agg, String field) {
        return "[infino] aggregation [" + agg + "] on [" + field + "] over several indices that do not map it to the same field is not served";
    }

    /**
     * A field renamed in one index, with an alias under its old name: the
     * name reads column hostname in one table and host.name in the other.
     * The union carries one column per name, so whichever plan came first,
     * the other index's values were left out, or its branch named a column
     * its table does not have. Refused, either way round; a field both map
     * alike is still served beside it.
     */
    public void testAFieldNamingDifferentColumnsIsRefused() {
        CardinalityAggregationBuilder byHost = AggregationBuilders.cardinality("c").field("hostname");
        for (String[] tables : List.of(new String[] { HOSTNAME, RENAMED }, new String[] { RENAMED, HOSTNAME })) {
            InfinoAggregationProcessor.Unsupported e = expectThrows(InfinoAggregationProcessor.Unsupported.class, () -> over(byHost, tables));
            assertEquals(notOneField("c", "hostname"), e.getMessage());
            e = expectThrows(
                InfinoAggregationProcessor.Unsupported.class,
                () -> over(AggregationBuilders.terms("t").field("user").subAggregation(byHost), tables)
            );
            assertEquals(notOneField("c", "hostname"), e.getMessage());
            assertEquals(3, ((InternalCardinality) over(AggregationBuilders.cardinality("c").field("user"), tables).get("c")).getValue());
        }
        // An alias to p in one index and to q in the other.
        InfinoAggregationProcessor.Unsupported e = expectThrows(
            InfinoAggregationProcessor.Unsupported.class,
            () -> over(AggregationBuilders.sum("s").field("x"), X_TO_P, X_TO_Q)
        );
        assertEquals(notOneField("s", "x"), e.getMessage());
    }

    /**
     * A number alias one index maps and the other does not, though it holds
     * the column: stock reads the alias as unmapped there. Read as doubles
     * like a field of numbers of two kinds, a percentile of it took in the
     * other index's n. Refused, and n itself is still served.
     */
    public void testANumberAliasOneIndexMapsIsRefused() {
        for (String[] tables : List.of(new String[] { N_ALIASED, N_PLAIN }, new String[] { N_PLAIN, N_ALIASED })) {
            InfinoAggregationProcessor.Unsupported e = expectThrows(
                InfinoAggregationProcessor.Unsupported.class,
                () -> over(AggregationBuilders.percentiles("p").field("a").percentiles(100), tables)
            );
            assertEquals(notOneField("p", "a"), e.getMessage());
            InternalTDigestPercentiles p = (InternalTDigestPercentiles) over(AggregationBuilders.percentiles("p").field("n").percentiles(100), tables)
                .get("p");
            assertEquals(100, p.percentile(100), 0);
        }
    }

    /**
     * A field mapped as a long in one index and a float in the other, some
     * documents of each giving it several values: their lists, of longs and
     * of doubles, cannot be read as one, and without them a metric left out
     * every document of several values. Refused.
     */
    public void testListsOfLongsAndOfDoublesAreRefused() {
        InfinoAggregationProcessor.Unsupported e = expectThrows(
            InfinoAggregationProcessor.Unsupported.class,
            () -> over(AggregationBuilders.sum("s").field("n"), LIST_LONG, LIST_FLOAT)
        );
        assertEquals("[infino] aggregation [s] on [n] over several indices that map it as different types is not served", e.getMessage());
    }
}
