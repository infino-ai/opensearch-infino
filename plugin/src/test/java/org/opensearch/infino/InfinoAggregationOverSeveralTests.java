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
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.metrics.InternalCardinality;
import org.opensearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

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
    private static final List<String> TABLES = List.of(WITHOUT, WITH, USER_LONG, LATENCY_INT);
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
}
