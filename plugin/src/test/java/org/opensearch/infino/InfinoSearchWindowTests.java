/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.OpenSearchParseException;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.rescore.QueryRescorerBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;

/** What one search of an index may ask of its table, when the index sets a limit: a span of time, and whether a phrase. */
public class InfinoSearchWindowTests extends OpenSearchTestCase {

    private static final long NOW = Instant.parse("2026-01-10T12:00:00Z").toEpochMilli();
    private static final long DAY = 86_400_000L;

    /** Fields of each kind a limit reads: dates in three formats, a text field with a sub-field, keywords, a number. */
    private static final String MAPPING = "{\"properties\":{"
        + "\"time\":{\"type\":\"date\"},"
        + "\"seconds\":{\"type\":\"date\",\"format\":\"epoch_second\"},"
        + "\"stamp\":{\"type\":\"date\",\"format\":\"yyyy/MM/dd HH:mm\"},"
        + "\"title\":{\"type\":\"text\",\"fields\":{\"raw\":{\"type\":\"keyword\"}}},"
        + "\"lang\":{\"type\":\"keyword\"},\"kind\":{\"type\":\"keyword\"},\"hits\":{\"type\":\"long\"}}}";

    private static IndexMetadata index(String field, String max, Boolean phrases) {
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
        if (phrases != null) {
            settings.put(InfinoSearchWindow.PHRASES.getKey(), phrases);
        }
        try {
            return IndexMetadata.builder("pages").settings(settings).putMapping(MAPPING).build();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static final IndexMetadata A_DAY = index("time", "1d", null);
    private static final IndexMetadata NO_PHRASES = index(null, null, false);

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
        allowed(new SearchSourceBuilder(), index(null, null, null));
        allowed(new SearchSourceBuilder(), index("time", null, null));
        allowed(new SearchSourceBuilder(), index(null, "1d", null));
        allowed(null, index(null, null, null));
    }

    public void testHitsAreListedOverNoMoreThanTheLimit() {
        allowed(over("2025-05-08T00:00:00Z", "2025-05-09T00:00:00Z"), A_DAY);
        allowed(over("2025-05-08T15:00:00Z", "2025-05-08T21:00:00Z"), A_DAY);
        String wide = refused(over("2025-05-01T00:00:00Z", "2025-06-01T00:00:00Z"), A_DAY);
        assertTrue(wide, wide.contains("at most [1d] of [time]"));
        assertTrue(wide, wide.contains("asks for [31d]"));
        assertTrue(wide, wide.contains(InfinoSearchWindow.MAX.getKey()));
    }

    /** The limit itself is allowed, and a millisecond past it is not. */
    public void testTheLimitIsInclusiveToTheMillisecond() {
        long start = Instant.parse("2025-05-08T00:00:00Z").toEpochMilli();
        allowed(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("time").gte(start).lte(start + DAY)), A_DAY);
        String past = refused(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("time").gte(start).lte(start + DAY + 1)), A_DAY);
        assertTrue(past, past.contains("asks for [1d]") || past.contains("asks for [24h]") || past.contains("asks for"));
        // An exclusive end is the instant it names.
        allowed(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("time").gte(start).lt(start + DAY)), A_DAY);
        assertTrue(refused(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("time").gte(start).lt(start + DAY + 1)), A_DAY).contains("asks for"));
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

    /**
     * An inclusive upper end is rounded up to the last instant of what it
     * names, as the statement reads it: the span is measured to there, so a
     * coarse end is as wide as it is read.
     */
    public void testAnInclusiveEndIsMeasuredToWhereItIsRead() {
        // Today, start to end: under a day.
        allowed(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("time").gte("now/d").lte("now/d")), A_DAY);
        // This year, start to end: a year, though both ends are written alike.
        String year = refused(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("time").gte("now/y").lte("now/y")), A_DAY);
        assertTrue(year, year.contains("asks for [36"));
        // A month rounded to as a month reads the whole month.
        String month = refused(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("time").gte("2025-05-08||/M").lte("2025-05-08||/M")), A_DAY);
        assertTrue(month, month.contains("asks for [3"));
        // A day named as a day reads that day, and two of them read two.
        allowed(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("time").gte("2025/05/08").lte("2025/05/08").format("yyyy/MM/dd")), A_DAY);
        assertTrue(
            refused(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("time").gte("2025/05/08").lte("2025/05/09").format("yyyy/MM/dd")), A_DAY).contains(
                "asks for"
            )
        );
        // Times counted back from now.
        allowed(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("time").gte("now-6h").lte("now")), A_DAY);
        String back = refused(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("time").gte("now-1y").lte("now")), A_DAY);
        assertTrue(back, back.contains("asks for [365d]"));
    }

    /** A bound is read in the range's format, else the field's mapped one, as the statement reads it. */
    public void testABoundIsReadInTheRangesFormatElseTheFields() {
        long start = Instant.parse("2024-01-01T00:00:00Z").getEpochSecond();
        // Numbers under epoch_second are seconds: a year, not nine hours of milliseconds.
        String year = refused(
            new SearchSourceBuilder().query(QueryBuilders.rangeQuery("time").gte(start).lte(start + 366L * 86_400).format("epoch_second")),
            A_DAY
        );
        assertTrue(year, year.contains("asks for [366d]"));
        allowed(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("time").gte(start).lte(start + 3600).format("epoch_second")), A_DAY);
        // A field mapped in seconds reads a bound with no format of its own as seconds, number or text.
        IndexMetadata bySeconds = index("seconds", "1d", null);
        assertTrue(
            refused(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("seconds").gte(start).lte(start + 366L * 86_400)), bySeconds).contains("asks for [366d]")
        );
        assertTrue(
            refused(
                new SearchSourceBuilder().query(QueryBuilders.rangeQuery("seconds").gte(Long.toString(start)).lte(Long.toString(start + 366L * 86_400))),
                bySeconds
            ).contains("asks for [366d]")
        );
        allowed(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("seconds").gte(start).lte(start + 3600)), bySeconds);
        // A field with a format of its own reads a narrow range written in it.
        IndexMetadata byStamp = index("stamp", "1d", null);
        allowed(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("stamp").gte("2025/05/08 15:00").lt("2025/05/08 21:00")), byStamp);
        assertTrue(
            refused(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("stamp").gte("2025/05/01 00:00").lt("2025/06/01 00:00")), byStamp).contains(
                "asks for [31d]"
            )
        );
    }

    /** A bound that cannot be read is refused as any search refuses it, not taken for a missing range. */
    public void testABoundThatCannotBeReadIsRefusedAsADate() {
        SearchSourceBuilder bad = new SearchSourceBuilder().query(QueryBuilders.rangeQuery("time").gte("yesterday-ish").lte("2025-05-08T06:00:00Z"));
        OpenSearchParseException e = expectThrows(OpenSearchParseException.class, () -> InfinoSearchWindow.check(bad, A_DAY, () -> NOW));
        assertTrue(e.getMessage(), e.getMessage().contains("failed to parse date field [yesterday-ish]"));
    }

    /** Ends too far apart to measure are too far apart to read, not a span that wraps round to a small one. */
    public void testEndsTooFarApartToMeasureAreRefused() {
        SearchSourceBuilder extreme = new SearchSourceBuilder().query(QueryBuilders.rangeQuery("time").gte(Long.MIN_VALUE + 1).lte(Long.MAX_VALUE - 1));
        assertTrue(refused(extreme, A_DAY).contains("more than can be measured"));
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

    /** Phrases are served unless the index turns them off, whatever its span limit. */
    public void testPhrasesAreTheirOwnSetting() {
        SearchSourceBuilder phrase = new SearchSourceBuilder().size(0).query(QueryBuilders.matchPhraseQuery("title", "pope leo"));
        allowed(phrase, index(null, null, null));
        allowed(phrase, A_DAY); // a span limit alone refuses no phrase
        allowed(phrase, index("time", "365d", true));
        String why = refused(phrase, NO_PHRASES);
        assertTrue(why, why.contains("does not search for a phrase"));
        assertTrue(why, why.contains("without the quotes"));
        assertTrue(why, why.contains(InfinoSearchWindow.PHRASES.getKey()));
        // With both set, a phrase inside the span is refused for the phrase.
        SearchSourceBuilder inside = new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.matchPhraseQuery("title", "pope leo"))
                .filter(QueryBuilders.rangeQuery("time").gte("2025-05-08T15:00:00Z").lte("2025-05-08T21:00:00Z"))
        );
        assertTrue(refused(inside, index("time", "1d", false)).contains("does not search for a phrase"));
    }

    public void testAPhraseIsFoundWhereverTheTableWouldLookOneUp() {
        // However it is written, and however deep.
        assertTrue(refused(new SearchSourceBuilder().size(0).query(QueryBuilders.queryStringQuery("title:\"pope leo\"")), NO_PHRASES).contains("a phrase"));
        assertTrue(refused(new SearchSourceBuilder().size(0).query(QueryBuilders.queryStringQuery("\"pope leo\" AND lang:en")), NO_PHRASES).contains("a phrase"));
        assertTrue(refused(new SearchSourceBuilder().size(0).query(QueryBuilders.simpleQueryStringQuery("\"pope leo\"")), NO_PHRASES).contains("a phrase"));
        assertTrue(
            refused(
                new SearchSourceBuilder().size(0)
                    .query(QueryBuilders.boolQuery().filter(QueryBuilders.boolQuery().should(QueryBuilders.matchPhraseQuery("title", "pope leo")).minimumShouldMatch(1))),
                NO_PHRASES
            ).contains("a phrase")
        );
        assertTrue(refused(new SearchSourceBuilder().size(0).postFilter(QueryBuilders.matchPhrasePrefixQuery("title", "pope le")), NO_PHRASES).contains("a phrase"));
        // In the query of a filter or filters aggregation, at any depth.
        assertTrue(
            refused(
                new SearchSourceBuilder().size(0)
                    .aggregation(
                        AggregationBuilders.terms("langs")
                            .field("lang")
                            .subAggregation(AggregationBuilders.filter("popes", QueryBuilders.matchPhraseQuery("title", "pope leo")))
                    ),
                NO_PHRASES
            ).contains("a phrase")
        );
        assertTrue(
            refused(
                new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.filters("f", QueryBuilders.matchPhraseQuery("title", "pope leo"))),
                NO_PHRASES
            ).contains("a phrase")
        );
        // In a rescore's query.
        assertTrue(
            refused(new SearchSourceBuilder().addRescorer(new QueryRescorerBuilder(QueryBuilders.matchPhraseQuery("title", "pope leo"))), NO_PHRASES).contains(
                "a phrase"
            )
        );
        // Over a list of fields a pattern reaches.
        assertTrue(
            refused(new SearchSourceBuilder().size(0).query(QueryBuilders.multiMatchQuery("pope leo", "ti*").type("phrase")), NO_PHRASES).contains("a phrase")
        );
        assertTrue(
            refused(new SearchSourceBuilder().size(0).query(QueryBuilders.multiMatchQuery("pope leo", "lang", "title").type("phrase")), NO_PHRASES).contains(
                "a phrase"
            )
        );
    }

    /**
     * A phrase query on a field that is not text is a comparison with the
     * whole value, as a filter on a keyword or a number is commonly sent:
     * it is no phrase lookup, and is served where phrases are not.
     */
    public void testAPhraseQueryOnAFieldThatIsNotTextIsAComparison() {
        allowed(
            new SearchSourceBuilder().size(0)
                .query(
                    QueryBuilders.boolQuery()
                        .filter(QueryBuilders.matchPhraseQuery("lang", "en"))
                        .filter(QueryBuilders.matchPhraseQuery("kind", "article"))
                        .filter(QueryBuilders.matchPhraseQuery("hits", 5))
                        .filter(QueryBuilders.matchPhraseQuery("title.raw", "Pope Leo XIV"))
                )
                .aggregation(AggregationBuilders.terms("kinds").field("kind")),
            NO_PHRASES
        );
        // A field the index does not map is not text either.
        allowed(new SearchSourceBuilder().size(0).query(QueryBuilders.matchPhraseQuery("unmapped", "x")), NO_PHRASES);
        allowed(new SearchSourceBuilder().size(0).query(QueryBuilders.multiMatchQuery("en", "lang", "kind").type("phrase")), NO_PHRASES);
        allowed(new SearchSourceBuilder().size(0).query(QueryBuilders.multiMatchQuery("en", "la*").type("phrase")), NO_PHRASES);
        // A quoted run of a query string that names such a field before it.
        allowed(new SearchSourceBuilder().size(0).query(QueryBuilders.queryStringQuery("lang:\"en\" AND kind:\"article\"")), NO_PHRASES);
        allowed(new SearchSourceBuilder().size(0).query(QueryBuilders.queryStringQuery("title:pope AND lang:(\"en\")")), NO_PHRASES);
        allowed(new SearchSourceBuilder().size(0).query(QueryBuilders.simpleQueryStringQuery("\"en\"").field("lang")), NO_PHRASES);
        // Words, quoted nowhere.
        allowed(new SearchSourceBuilder().size(0).query(QueryBuilders.queryStringQuery("title:pope")), NO_PHRASES);
        allowed(new SearchSourceBuilder().size(0).query(QueryBuilders.matchQuery("title", "pope leo")), NO_PHRASES);
    }
}
