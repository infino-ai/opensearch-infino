/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.time.Instant;

/** The span of time one search of an index may read, when the index sets one. */
public class InfinoSearchWindowTests extends OpenSearchTestCase {

    private static final long NOW = Instant.parse("2026-01-10T00:00:00Z").toEpochMilli();

    private static IndexMetadata index(String field, String max) {
        Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
        if (field != null) {
            settings.put(InfinoSearchWindow.FIELD.getKey(), field);
        }
        if (max != null) {
            settings.put(InfinoSearchWindow.MAX.getKey(), max);
        }
        return IndexMetadata.builder("pages").settings(settings).build();
    }

    private static final IndexMetadata A_DAY = index("time", "1d");

    private static SearchSourceBuilder over(String from, String to) {
        return new SearchSourceBuilder().query(QueryBuilders.boolQuery().filter(QueryBuilders.rangeQuery("time").gte(from).lte(to)));
    }

    private static String refused(SearchSourceBuilder source, IndexMetadata index) {
        return expectThrows(IllegalArgumentException.class, () -> InfinoSearchWindow.check(source, index, () -> NOW)).getMessage();
    }

    private static void allowed(SearchSourceBuilder source, IndexMetadata index) {
        InfinoSearchWindow.check(source, index, () -> NOW);
    }

    public void testAnIndexThatSetsNoLimitIsSearchedOverAnySpan() {
        allowed(new SearchSourceBuilder(), index(null, null));
        allowed(new SearchSourceBuilder(), index("time", null));
        allowed(new SearchSourceBuilder(), index(null, "1d"));
        allowed(null, index(null, null));
    }

    public void testHitsAreListedOverNoMoreThanTheLimit() {
        allowed(over("2025-05-08T00:00:00Z", "2025-05-09T00:00:00Z"), A_DAY);
        allowed(over("2025-05-08T15:00:00Z", "2025-05-08T21:00:00Z"), A_DAY);
        String wide = refused(over("2025-05-01T00:00:00Z", "2025-06-01T00:00:00Z"), A_DAY);
        assertTrue(wide, wide.contains("at most [1d] of [time]"));
        assertTrue(wide, wide.contains("asks for [31d]"));
        assertTrue(wide, wide.contains(InfinoSearchWindow.MAX.getKey()));
    }

    public void testASearchThatNamesNoRangeOrHalfOfOneIsRefused() {
        String none = refused(new SearchSourceBuilder(), A_DAY);
        assertTrue(none, none.contains("names no such range"));
        assertTrue(refused(null, A_DAY).contains("names no such range"));
        SearchSourceBuilder open = new SearchSourceBuilder().query(QueryBuilders.rangeQuery("time").gte("2025-05-08T00:00:00Z"));
        assertTrue(refused(open, A_DAY).contains("names no such range"));
        // A range on another field bounds nothing.
        SearchSourceBuilder other = new SearchSourceBuilder().query(QueryBuilders.rangeQuery("hits").gte(1).lte(2));
        assertTrue(refused(other, A_DAY).contains("names no such range"));
    }

    public void testARangeIsReadWhereverEveryMatchMustSatisfyIt() {
        // The query itself, nested bools, a constant score, the post filter.
        allowed(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("time").gte("2025-05-08T00:00:00Z").lte("2025-05-08T06:00:00Z")), A_DAY);
        allowed(
            new SearchSourceBuilder().query(
                QueryBuilders.boolQuery()
                    .must(QueryBuilders.matchQuery("title", "pope"))
                    .filter(QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("time").gte("2025-05-08T00:00:00Z").lt("2025-05-08T06:00:00Z")))
            ),
            A_DAY
        );
        allowed(
            new SearchSourceBuilder().query(QueryBuilders.constantScoreQuery(QueryBuilders.rangeQuery("time").gte("2025-05-08T00:00:00Z").lte("2025-05-08T06:00:00Z"))),
            A_DAY
        );
        allowed(new SearchSourceBuilder().postFilter(QueryBuilders.rangeQuery("time").gte("2025-05-08T00:00:00Z").lte("2025-05-08T06:00:00Z")), A_DAY);
        // Two ranges narrow each other: a year, and a day inside it.
        allowed(
            new SearchSourceBuilder().query(
                QueryBuilders.boolQuery()
                    .filter(QueryBuilders.rangeQuery("time").gte("2025-01-01T00:00:00Z").lt("2026-01-01T00:00:00Z"))
                    .filter(QueryBuilders.rangeQuery("time").gte("2025-05-08T00:00:00Z").lt("2025-05-09T00:00:00Z"))
            ),
            A_DAY
        );
        // A range a document only may match bounds nothing.
        SearchSourceBuilder optional = new SearchSourceBuilder().query(
            QueryBuilders.boolQuery().should(QueryBuilders.rangeQuery("time").gte("2025-05-08T00:00:00Z").lte("2025-05-08T06:00:00Z"))
        );
        assertTrue(refused(optional, A_DAY).contains("names no such range"));
    }

    public void testTheEndsOfARangeAreReadAsADateFieldReadsThem() {
        // Milliseconds, as numbers and as text.
        long start = Instant.parse("2025-05-08T00:00:00Z").toEpochMilli();
        allowed(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("time").gte(start).lte(start + 3_600_000L)), A_DAY);
        allowed(
            new SearchSourceBuilder().query(QueryBuilders.rangeQuery("time").gte(Long.toString(start)).lte(Long.toString(start + 3_600_000L)).format("epoch_millis")),
            A_DAY
        );
        // Times counted back from now.
        allowed(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("time").gte("now-6h").lte("now")), A_DAY);
        String year = refused(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("time").gte("now-1y").lte("now")), A_DAY);
        assertTrue(year, year.contains("asks for [365d]"));
        // A format of the range's own.
        allowed(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("time").gte("2025/05/08").lte("2025/05/08").format("yyyy/MM/dd")), A_DAY);
    }

    public void testACountOrATotalIsAnsweredOverAnySpan() {
        SearchSourceBuilder year = over("2025-01-01T00:00:00Z", "2026-01-01T00:00:00Z").size(0);
        allowed(year, A_DAY);
        allowed(new SearchSourceBuilder().size(0), A_DAY);
        allowed(year.aggregation(AggregationBuilders.sum("views").field("hits")), A_DAY);
        allowed(
            new SearchSourceBuilder().size(0)
                .aggregation(AggregationBuilders.sum("s").field("hits"))
                .aggregation(AggregationBuilders.avg("a").field("hits"))
                .aggregation(AggregationBuilders.min("mn").field("hits"))
                .aggregation(AggregationBuilders.max("mx").field("hits"))
                .aggregation(AggregationBuilders.count("c").field("hits"))
                .aggregation(AggregationBuilders.stats("st").field("hits")),
            A_DAY
        );
    }

    public void testAPhraseIsRefusedWhateverRangeTheSearchNames() {
        // Inside the limit, and even as a count: the phrase is looked up over the whole table first.
        SearchSourceBuilder inside = new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.matchPhraseQuery("title", "pope leo"))
                .filter(QueryBuilders.rangeQuery("time").gte("2025-05-08T15:00:00Z").lte("2025-05-08T21:00:00Z"))
        );
        String why = refused(inside, A_DAY);
        assertTrue(why, why.contains("a phrase is looked up across the whole table"));
        assertTrue(why, why.contains("without the quotes"));
        assertTrue(refused(inside.size(0), A_DAY).contains("a phrase"));
        // However the phrase is written, and however deep.
        assertTrue(refused(new SearchSourceBuilder().size(0).query(QueryBuilders.queryStringQuery("title:\"pope leo\"")), A_DAY).contains("a phrase"));
        assertTrue(refused(new SearchSourceBuilder().size(0).query(QueryBuilders.simpleQueryStringQuery("\"pope leo\"")), A_DAY).contains("a phrase"));
        assertTrue(
            refused(
                new SearchSourceBuilder().size(0)
                    .query(QueryBuilders.boolQuery().filter(QueryBuilders.boolQuery().should(QueryBuilders.matchPhraseQuery("title", "pope leo")).minimumShouldMatch(1))),
                A_DAY
            ).contains("a phrase")
        );
        assertTrue(refused(new SearchSourceBuilder().size(0).postFilter(QueryBuilders.matchPhrasePrefixQuery("title", "pope le")), A_DAY).contains("a phrase"));
        // Words are a filter the range narrows, and are served.
        allowed(
            new SearchSourceBuilder().size(0)
                .query(QueryBuilders.boolQuery().filter(QueryBuilders.matchQuery("title", "pope leo")).filter(QueryBuilders.rangeQuery("time").gte("now-6h").lte("now"))),
            A_DAY
        );
        allowed(new SearchSourceBuilder().size(0).query(QueryBuilders.queryStringQuery("title:pope")), A_DAY);
        // And an index with no limit serves a phrase as before.
        allowed(inside, index(null, null));
    }

    public void testABreakdownIsHeldToTheLimit() {
        SearchSourceBuilder byLanguage = over("2025-01-01T00:00:00Z", "2026-01-01T00:00:00Z").size(0)
            .aggregation(AggregationBuilders.terms("langs").field("lang"));
        assertTrue(refused(byLanguage, A_DAY).contains("asks for [365d]"));
        SearchSourceBuilder overTime = new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.dateHistogram("t").field("time"));
        assertTrue(refused(overTime, A_DAY).contains("names no such range"));
        // A number that reads every distinct value is no plain total.
        SearchSourceBuilder distinct = new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.cardinality("titles").field("title"));
        assertTrue(refused(distinct, A_DAY).contains("names no such range"));
        // The same breakdown inside the limit is served.
        allowed(over("2025-05-08T00:00:00Z", "2025-05-09T00:00:00Z").size(0).aggregation(AggregationBuilders.terms("langs").field("lang")), A_DAY);
    }
}
