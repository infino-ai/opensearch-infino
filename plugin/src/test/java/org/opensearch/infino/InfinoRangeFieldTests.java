/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.infino.InfinoAggregationProcessor.Grouped;
import org.opensearch.infino.InfinoAggregationProcessor.Leaves;
import org.opensearch.infino.InfinoNative.Column;
import org.opensearch.infino.InfinoQueryTranslator.Translation;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A range field is read from its two hidden columns, each range's lowest
 * and highest value, both inclusive: a range query by its relation, a term
 * as the ranges holding the value, exists, and a histogram that counts a
 * document in every bucket its range reaches. Anything else on it, and
 * anything on an ip_range, which has no columns, is refused rather than
 * answered with no hits.
 */
public class InfinoRangeFieldTests extends OpenSearchTestCase {

    private static final String TABLE = "range_fields_0";
    private static final String MULTI_TABLE = "range_fields_multi_0";
    private static final long NOW = Instant.parse("2026-10-24T12:00:00Z").toEpochMilli();
    private static final String MAPPING = """
        {"properties":{"r":{"type":"long_range"},"i":{"type":"integer_range"},"d":{"type":"double_range"},
         "span":{"type":"date_range","format":"yyyy-MM-dd||epoch_millis"},"ip":{"type":"ip_range"},
         "alias_r":{"type":"alias","path":"r"},"n":{"type":"long"}}}""";
    private static final String UTC = ", 'Timestamp(Millisecond, Some(\"UTC\"))')";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        InfinoNative.keepMapping(TABLE, MAPPING, true);
        InfinoNative.keepMapping(MULTI_TABLE, MAPPING, true);
    }

    private static Map<String, Column> columns(boolean lists) {
        Map<String, Column> cols = new LinkedHashMap<>();
        cols.put("osid", new Column("osid", "str", false, false));
        cols.put("n", new Column("n", "i64", false, false));
        for (String f : List.of("r", "i")) {
            cols.put(f + ".from", new Column(f + ".from", "i64", false, false));
            cols.put(f + ".to", new Column(f + ".to", "i64", false, false));
        }
        cols.put("d.from", new Column("d.from", "f64", false, false));
        cols.put("d.to", new Column("d.to", "f64", false, false));
        cols.put("span.from", new Column("span.from", "ts", false, false));
        cols.put("span.to", new Column("span.to", "ts", false, false));
        if (lists) {
            cols.put("r.from[]", new Column("r.from[]", "list", false, false, null, null, "i64"));
            cols.put("r.to[]", new Column("r.to[]", "list", false, false, null, null, "i64"));
        }
        return cols;
    }

    private static String where(QueryBuilder qb) {
        return InfinoQueryTranslator.translate(qb, columns(false), TABLE, NOW).where();
    }

    private static String whereMulti(QueryBuilder qb) {
        Translation t = InfinoQueryTranslator.translate(qb, columns(true), MULTI_TABLE, NOW);
        return t.where();
    }

    public void testARangeQueryReadsTheTwoEndsByItsRelation() {
        assertEquals("(\"r.from\" <= 1300000 AND \"r.to\" >= 1200000)", where(QueryBuilders.rangeQuery("r").gte(1200000).lte(1300000)));
        assertEquals(
            "(\"r.from\" >= 1200000 AND \"r.to\" <= 1300000)",
            where(QueryBuilders.rangeQuery("r").gte(1200000).lte(1300000).relation(ShapeRelation.WITHIN.getRelationName()))
        );
        assertEquals(
            "(\"r.from\" <= 1200000 AND \"r.to\" >= 1300000)",
            where(QueryBuilders.rangeQuery("r").gte(1200000).lte(1300000).relation(ShapeRelation.CONTAINS.getRelationName()))
        );
        // An exclusive bound is the next value inside it, a missing one the type's extreme.
        assertEquals("(\"r.from\" <= " + Long.MAX_VALUE + " AND \"r.to\" >= 6)", where(QueryBuilders.rangeQuery("r").gt(5)));
        assertEquals("(\"i.from\" <= 4 AND \"i.to\" >= " + Integer.MIN_VALUE + ")", where(QueryBuilders.rangeQuery("i").lt(5)));
        assertEquals("(\"d.from\" <= 2.5 AND \"d.to\" >= " + Math.nextUp(1.5) + ")", where(QueryBuilders.rangeQuery("d").gt(1.5).lte(2.5)));
        // Bounds that admit no value match nothing.
        assertEquals("1=0", where(QueryBuilders.rangeQuery("r").gt(5).lt(6)));
        // An alias reads its target's columns.
        assertEquals("(\"r.from\" <= 2 AND \"r.to\" >= 1)", where(QueryBuilders.rangeQuery("alias_r").gte(1).lte(2)));
    }

    public void testADateRangeReadsItsBoundsAsStockDoes() {
        long day = Instant.parse("2016-01-02T00:00:00Z").toEpochMilli();
        // In the field's own format; an lt bound ends a millisecond before it.
        assertEquals(
            "(\"span.from\" <= arrow_cast(" + (day - 1) + UTC + " AND \"span.to\" >= arrow_cast(" + Long.MIN_VALUE + UTC + ")",
            where(QueryBuilders.rangeQuery("span").lt("2016-01-02"))
        );
        // A term is the ranges reaching the day, its upper end rounded up as stock rounds it.
        assertEquals("(\"span.from\" <= arrow_cast(" + (day + 86_399_999) + UTC + " AND \"span.to\" >= arrow_cast(" + day + UTC + ")", where(QueryBuilders.termQuery("span", "2016-01-02")));
    }

    public void testATermOrTermsIsTheRangesHoldingTheValueAndExistsAnyRange() {
        assertEquals("(\"r.from\" <= 7 AND \"r.to\" >= 7)", where(QueryBuilders.termQuery("r", 7)));
        assertEquals(
            "((\"r.from\" <= 7 AND \"r.to\" >= 7) OR (\"r.from\" <= 9 AND \"r.to\" >= 9))",
            where(QueryBuilders.termsQuery("r", List.of(7, 9)))
        );
        assertEquals("\"r.from\" IS NOT NULL", where(QueryBuilders.existsQuery("r")));
    }

    public void testSeveralRangesAreReadPairByPair() {
        String w = whereMulti(QueryBuilders.termQuery("r", 7));
        assertTrue(w, w.startsWith("((\"r.from[]\" IS NULL AND (\"r.from\" <= 7 AND \"r.to\" >= 7)) OR osid IN (SELECT"));
        assertTrue(w, w.contains("unnest(range_source.\"r.from[]\") AS range_low, unnest(range_source.\"r.to[]\") AS range_high"));
        assertTrue(w, w.endsWith("WHERE (range_rows.range_low <= 7 AND range_rows.range_high >= 7)))"));
        assertEquals("(\"r.from\" IS NOT NULL OR \"r.from[]\" IS NOT NULL)", whereMulti(QueryBuilders.existsQuery("r")));
    }

    public void testWhatTheColumnsCannotAnswerIsRefused() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> where(QueryBuilders.prefixQuery("r", "1")));
        assertTrue(e.getMessage(), e.getMessage().contains("a query on [r], a field of type [long_range], is not served"));
        e = expectThrows(IllegalArgumentException.class, () -> where(QueryBuilders.termQuery("ip", "10.0.0.1")));
        assertTrue(e.getMessage(), e.getMessage().contains("a query on [ip], a field of type [ip_range], is not served"));
        e = expectThrows(IllegalArgumentException.class, () -> where(QueryBuilders.rangeQuery("r").gte(1.5)));
        assertEquals("Value [1.5] has a decimal part", e.getMessage());
        e = expectThrows(
            IllegalArgumentException.class,
            () -> InfinoAggregationProcessor.Grouped.terms(new TermsAggregationBuilder("t").field("r"), columns(false), TABLE, Leaves.NONE, null, false, NOW)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("aggregation [t] on [r], a field of type [long_range], is not served"));
    }

    public void testAHistogramReadsAValuePerIntervalOfEachRange() {
        Grouped g = Grouped.histogram(new HistogramAggregationBuilder("h").field("r").interval(200000), columns(false), TABLE, Leaves.NONE, null, NOW);
        assertTrue(g.source.multi());
        assertEquals("floor(((\"r\") - 0.0) / 200000.0) * 200000.0 + 0.0", g.keyExpr);
        String relation = InfinoAggregationProcessor.valuesRelation("\"" + TABLE + "\"", List.of(g.source));
        assertTrue(relation, relation.contains("unnest(generate_series(0, CAST(CEIL((CAST(\"value_layer_0_high\" AS DOUBLE) - CAST(\"value_layer_0_low\" AS DOUBLE)) / 200000.0) AS BIGINT)))"));
        assertTrue(relation, relation.contains("\"r.from\" AS \"value_layer_0_low\", \"r.to\" AS \"value_layer_0_high\""));
        // A row with no range reads none: GREATEST and LEAST would pass over its null ends.
        assertTrue(relation, relation.contains("WHERE \"value_layer_0_low\" IS NOT NULL AND \"value_layer_0_high\" IS NOT NULL AND "));
        // The documents in a bucket are those whose range reaches it.
        assertEquals("(\"r.from\" < 1400000 AND \"r.to\" >= 1200000)", g.docPredicate(1200000.0));
    }

    public void testHardBoundsCutEachRangeBeforeItIsRead() {
        HistogramAggregationBuilder h = new HistogramAggregationBuilder("h").field("r").interval(1).hardBounds(new org.opensearch.search.aggregations.bucket.histogram.DoubleBounds(0.0, 20.0));
        Grouped g = Grouped.histogram(h, columns(false), TABLE, Leaves.NONE, null, NOW);
        String relation = InfinoAggregationProcessor.valuesRelation("\"" + TABLE + "\"", List.of(g.source));
        assertTrue(relation, relation.contains("GREATEST(CAST(\"value_layer_0_low\" AS DOUBLE), 0.0)"));
        assertTrue(relation, relation.contains("LEAST(CAST(\"value_layer_0_high\" AS DOUBLE), 21.0)"));
    }

    public void testADateHistogramReadsADateRangeAtItsNarrowestBucket() {
        DateHistogramAggregationBuilder dh = new DateHistogramAggregationBuilder("h").field("span").calendarInterval(DateHistogramInterval.MONTH);
        Grouped g = Grouped.dateHistogram(dh, columns(false), TABLE, Leaves.NONE, null, NOW);
        String relation = InfinoAggregationProcessor.valuesRelation("\"" + TABLE + "\"", List.of(g.source));
        // A month is at least 28 days.
        assertTrue(relation, relation.contains(" / " + (28 * 86_400_000.0) + ")"));
        assertTrue(relation, relation.contains("arrow_cast(CAST(LEAST("));
        long jan = Instant.parse("2016-01-01T00:00:00Z").toEpochMilli();
        long feb = Instant.parse("2016-02-01T00:00:00Z").toEpochMilli();
        assertEquals("(\"span.from\" < arrow_cast(" + feb + UTC + " AND \"span.to\" >= arrow_cast(" + jan + UTC + ")", g.docPredicate(jan));
        // A numeric range field has no dates to bucket.
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> Grouped.dateHistogram(new DateHistogramAggregationBuilder("h").field("r").calendarInterval(DateHistogramInterval.MONTH), columns(false), TABLE, Leaves.NONE, null, NOW)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("needs a date value, [r] is of type [long_range]"));
    }

    public void testAZonedOrHardBoundedDateHistogramReadsMore() {
        assertEquals(43_200_000.0, Grouped.narrowestBucketMillis(null, DateHistogramInterval.DAY, java.time.ZoneId.of("America/New_York")), 0.0);
        assertEquals(86_400_000.0, Grouped.narrowestBucketMillis(DateHistogramInterval.days(1), null, java.time.ZoneOffset.UTC), 0.0);
        DateHistogramAggregationBuilder dh = new DateHistogramAggregationBuilder("h").field("span")
            .calendarInterval(DateHistogramInterval.MONTH)
            .hardBounds(new org.opensearch.search.aggregations.bucket.histogram.LongBounds("2015-06-01", "2016-06-01"));
        Grouped g = Grouped.dateHistogram(dh, columns(false), TABLE, Leaves.NONE, null, NOW);
        long june = Instant.parse("2015-06-01T00:00:00Z").toEpochMilli();
        long julyEnd = Instant.parse("2016-07-01T00:00:00Z").toEpochMilli() - 1;
        String relation = InfinoAggregationProcessor.valuesRelation("\"" + TABLE + "\"", List.of(g.source));
        assertTrue(relation, relation.contains("GREATEST(CAST(arrow_cast(\"value_layer_0_low\", 'Int64') AS DOUBLE), " + (double) june + ")"));
        assertTrue(relation, relation.contains("LEAST(CAST(arrow_cast(\"value_layer_0_high\", 'Int64') AS DOUBLE), " + (double) julyEnd + ")"));
        assertTrue(g.keyExpr, g.keyExpr.startsWith("CASE WHEN "));
    }
}
