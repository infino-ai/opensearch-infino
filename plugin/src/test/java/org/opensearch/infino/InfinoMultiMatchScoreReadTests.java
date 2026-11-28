/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * A multi_match over several text fields whose score the table cannot give
 * (cross_fields, or a tie_breaker between 0 and 1) is refused only when the
 * request reads its hits by that score. A page ordered by a field or by
 * {@code _doc} (a scroll, a delete_by_query) scores nothing in stock, and is
 * served, as is a request for no hits.
 */
public class InfinoMultiMatchScoreReadTests extends OpenSearchTestCase {

    private static final String TABLE = "multi_match_score_read";
    private static final String CROSS_REFUSED = "[infino] multi_match of type [cross_fields] on [body, title] is not served: "
        + "the table matches and scores each text field apart, not as one field";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        InfinoNative.dropTable(TABLE);
        InfinoNative.ensureTable(TABLE, "{\"properties\":{\"title\":{\"type\":\"text\"},\"body\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}");
        String[] ids = { "1", "2" };
        String[] sources = { "{\"title\":\"disk full\",\"body\":\"red\",\"n\":2}", "{\"title\":\"red\",\"body\":\"disk\",\"n\":1}" };
        assertEquals(2, InfinoNative.indexBatch(TABLE, ids, sources, 2).ok());
    }

    @Override
    public void tearDown() throws Exception {
        InfinoNative.dropTable(TABLE);
        super.tearDown();
    }

    private static MultiMatchQueryBuilder cross() {
        return QueryBuilders.multiMatchQuery("disk", "title", "body").type(MultiMatchQueryBuilder.Type.CROSS_FIELDS);
    }

    private static SearchSourceBuilder source(QueryBuilder query) {
        return new SearchSourceBuilder().query(query);
    }

    private static InfinoSearch.Plan plan(SearchSourceBuilder source) {
        return InfinoSearch.plan(TABLE, source, System.currentTimeMillis());
    }

    private static void assertRefused(SearchSourceBuilder source, String message) {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> plan(source));
        assertEquals(source.toString(), message, e.getMessage());
    }

    public void testRefusedWhenTheHitsAreReadByTheirScore() {
        assertRefused(source(cross()), CROSS_REFUSED);
        assertRefused(source(cross()).sort(new ScoreSortBuilder()), CROSS_REFUSED);
        assertRefused(source(cross()).sort(new ScoreSortBuilder().order(SortOrder.ASC)), CROSS_REFUSED);
        assertRefused(source(cross()).sort("n", SortOrder.ASC).sort(new ScoreSortBuilder()), CROSS_REFUSED);
        // track_scores hands each hit its score beside the field sort.
        assertRefused(source(cross()).sort("n", SortOrder.ASC).trackScores(true), CROSS_REFUSED);
        assertRefused(source(QueryBuilders.boolQuery().filter(QueryBuilders.rangeQuery("n").gte(1)).must(cross())), CROSS_REFUSED);
        assertRefused(
            source(QueryBuilders.multiMatchQuery("disk", "title", "body").tieBreaker(0.3f)),
            "[infino] multi_match on [body, title] with tie_breaker [0.3] is not served: "
                + "the table scores several text fields by the best one (tie_breaker 0) or by their sum (tie_breaker 1)"
        );
    }

    public void testServedWhenTheHitsAreReadInAnotherOrder() {
        // Sorted by a field: stock scores nothing.
        assertNotNull(plan(source(cross()).sort("n", SortOrder.ASC)));
        // Sorted by _doc, as a scroll or a delete_by_query reads its pages.
        assertNotNull(plan(source(cross()).sort("_doc")));
        assertNotNull(plan(InfinoSearch.scrollSource(source(cross()).sort("_doc").size(100))));
        // No hits to read: aggregations or a count alone.
        assertNotNull(plan(source(cross()).size(0)));
        // A tie_breaker between 0 and 1 likewise.
        assertNotNull(plan(source(QueryBuilders.multiMatchQuery("disk", "title", "body").tieBreaker(0.3f)).sort("n", SortOrder.DESC)));
    }

    public void testServedBesideAnotherScoringClauseInEitherOrder() {
        MultiMatchQueryBuilder other = QueryBuilders.multiMatchQuery("red", "title", "body");
        for (QueryBuilder q : new QueryBuilder[] {
            QueryBuilders.boolQuery().must(cross()).must(other),
            QueryBuilders.boolQuery().must(other).must(cross()) }) {
            InfinoSearch.Plan plan = plan(source(q));
            assertTrue(q.toString(), plan.query().scoring().unrankable());
            assertNull(q.toString(), plan.query().scoring().refused());
        }
    }
}
