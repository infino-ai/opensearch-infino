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
         "alias_r":{"type":"alias","path":"r"},"n":{"type":"long"},"f":{"type":"float_range"},"ts":{"type":"date"}}}""";
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
        cols.put("f.from", new Column("f.from", "f64", false, false));
        cols.put("f.to", new Column("f.to", "f64", false, false));
        cols.put("ts", new Column("ts", "ts", false, false));
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

    public void testATermsQueryReadsTheListsOnceForAllItsValues() {
        String w = whereMulti(QueryBuilders.termsQuery("r", List.of(7, 9, 11)));
        assertEquals(w, 1, w.split("osid IN \\(SELECT", -1).length - 1);
        assertTrue(w, w.contains("WHERE ((range_rows.range_low <= 7 AND range_rows.range_high >= 7) OR (range_rows.range_low <= 9 AND range_rows.range_high >= 9) OR "));
        // A long list is read as rows, so the statement's AND and OR stay few whatever its length.
        List<Integer> many = new java.util.ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            many.add(i * 3);
        }
        w = whereMulti(QueryBuilders.termsQuery("r", many));
        assertEquals(w, 1, w.split("osid IN \\(SELECT", -1).length - 1);
        assertTrue(w, w.contains("EXISTS (SELECT 1 FROM (VALUES (0, 0), (3, 3), "));
        assertTrue(w, w.split(" AND | OR ", -1).length < 20);
    }

    public void testAFloatRangeReadsQueryValuesAsFloats() {
        // 1.00000005 is nearer 1 than the next float, so it is 1, as stock reads it.
        assertEquals("(\"f.from\" <= 1.0 AND \"f.to\" >= 1.0)", where(QueryBuilders.termQuery("f", 1.00000005)));
        // An exclusive bound moves to the next float, not the next double.
        assertEquals(
            "(\"f.from\" <= " + Double.MAX_VALUE + " AND \"f.to\" >= " + (double) Math.nextUp(1.0f) + ")",
            where(QueryBuilders.rangeQuery("f").gt(1))
        );
        // A double range keeps doubles.
        assertEquals("(\"d.from\" <= 1.00000005 AND \"d.to\" >= 1.00000005)", where(QueryBuilders.termQuery("d", 1.00000005)));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> where(QueryBuilders.termQuery("f", 1e39)));
        assertEquals("[float] supports only finite values, but got [1.0E39]", e.getMessage());
        // Bounds in the wrong order once rounded to floats are refused, as stock refuses them.
        e = expectThrows(IllegalArgumentException.class, () -> where(QueryBuilders.rangeQuery("f").gt(0.99999999).lt(0.5)));
        assertEquals("Range query `from` value (1.0) is greater than `to` value (0.5)", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> where(QueryBuilders.rangeQuery("r").gte(5).lte(3)));
        assertEquals("Range query `from` value (5) is greater than `to` value (3)", e.getMessage());
        // Bounds that leave nothing only once an exclusive one moves match nothing.
        assertEquals(InfinoQueryTranslator.NONE, where(QueryBuilders.rangeQuery("r").gt(5).lt(6)));
    }

    public void testOverSeveralTablesEachRowReadsItsOwnTablesRanges() {
        InfinoRangeField first = InfinoRangeField.of(TABLE, columns(false), "r");
        assertFalse(first.multi());
        // Only the second table holds documents with several ranges.
        InfinoRangeField both = first.over(List.of(TABLE, MULTI_TABLE), t -> columns(t.equals(MULTI_TABLE)));
        assertTrue(both.multi());
        String p = both.pairs(false, (l, h) -> "(" + l + " <= 7 AND " + h + " >= 7)");
        assertTrue(p, p.startsWith("(CASE \"_infino_shard\" WHEN 0 THEN ((\"r.from\" <= 7 AND \"r.to\" >= 7)) WHEN 1 THEN (((\"r.from[]\" IS NULL AND "));
        assertTrue(p, p.contains("FROM \"" + MULTI_TABLE + "\" range_source"));
        assertFalse(p, p.contains("FROM \"" + TABLE + "\""));
        assertSame(first, first.over(List.of(TABLE), t -> columns(false)));
        // The ranges are read from every table's lists, a null one where a table has none.
        HistogramAggregationBuilder h = new HistogramAggregationBuilder("h").field("r").interval(5);
        InfinoRangeField.Samples samples = both.samples("h", 5, null, null, 100, 100);
        String relation = samples.relation("\"u\"", "\"r\"", "value_layer_0");
        assertTrue(relation, relation.contains("unnest(CASE WHEN \"r.from[]\" IS NULL THEN make_array(\"r.from\") ELSE \"r.from[]\" END)"));
        assertTrue(h.getName(), samples.reaching(5, 10).startsWith("(CASE \"_infino_shard\" WHEN 0 THEN "));
    }

    public void testHardBoundsOnADateColumnAddALooseRangeOnIt() {
        DateHistogramAggregationBuilder dh = new DateHistogramAggregationBuilder("h").field("ts")
            .calendarInterval(DateHistogramInterval.DAY)
            .hardBounds(new org.opensearch.search.aggregations.bucket.histogram.LongBounds("2016-01-02T06:00:00Z", "2016-01-04T06:00:00Z"));
        Grouped g = Grouped.dateHistogram(dh, columns(false), TABLE, List.of(TABLE), Leaves.NONE, null, NOW);
        long min = Instant.parse("2016-01-02T06:00:00Z").toEpochMilli();
        // A bucket is kept before the upper bound, as stock keeps it: the day holding the bound is the last.
        long until = Instant.parse("2016-01-05T00:00:00Z").toEpochMilli();
        assertEquals("\"ts\" >= " + InfinoAggregationProcessor.timestampLiteral(min) + " AND \"ts\" < " + InfinoAggregationProcessor.timestampLiteral(until), g.within);
        assertTrue(g.keyExpr, g.keyExpr.contains(" < " + InfinoAggregationProcessor.timestampLiteral(Instant.parse("2016-01-04T06:00:00Z").toEpochMilli())));
        DateHistogramAggregationBuilder unbounded = new DateHistogramAggregationBuilder("h").field("ts").calendarInterval(DateHistogramInterval.DAY);
        assertNull(Grouped.dateHistogram(unbounded, columns(false), TABLE, List.of(TABLE), Leaves.NONE, null, NOW).within);
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
        Grouped g = Grouped.histogram(new HistogramAggregationBuilder("h").field("r").interval(200000), columns(false), TABLE, List.of(TABLE), Leaves.NONE, null, NOW);
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
        Grouped g = Grouped.histogram(h, columns(false), TABLE, List.of(TABLE), Leaves.NONE, null, NOW);
        String relation = InfinoAggregationProcessor.valuesRelation("\"" + TABLE + "\"", List.of(g.source));
        assertTrue(relation, relation.contains("GREATEST(CAST(\"value_layer_0_low\" AS DOUBLE), 0.0)"));
        assertTrue(relation, relation.contains("LEAST(CAST(\"value_layer_0_high\" AS DOUBLE), 20.0)"));
        // As stock cuts a range, with no test of the bucket's key beside.
        assertFalse(g.keyExpr, g.keyExpr.startsWith("CASE"));
    }

    public void testADateHistogramReadsADateRangeAtItsNarrowestBucket() {
        DateHistogramAggregationBuilder dh = new DateHistogramAggregationBuilder("h").field("span").calendarInterval(DateHistogramInterval.MONTH);
        Grouped g = Grouped.dateHistogram(dh, columns(false), TABLE, List.of(TABLE), Leaves.NONE, null, NOW);
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
            () -> Grouped.dateHistogram(new DateHistogramAggregationBuilder("h").field("r").calendarInterval(DateHistogramInterval.MONTH), columns(false), TABLE, List.of(TABLE), Leaves.NONE, null, NOW)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("needs a date value, [r] is of type [long_range]"));
    }

    public void testAZonedOrHardBoundedDateHistogramReadsMore() {
        assertEquals(43_200_000.0, Grouped.narrowestBucketMillis(null, DateHistogramInterval.DAY, java.time.ZoneId.of("America/New_York")), 0.0);
        assertEquals(86_400_000.0, Grouped.narrowestBucketMillis(DateHistogramInterval.days(1), null, java.time.ZoneOffset.UTC), 0.0);
        DateHistogramAggregationBuilder dh = new DateHistogramAggregationBuilder("h").field("span")
            .calendarInterval(DateHistogramInterval.MONTH)
            .hardBounds(new org.opensearch.search.aggregations.bucket.histogram.LongBounds("2015-06-01", "2016-06-01"));
        Grouped g = Grouped.dateHistogram(dh, columns(false), TABLE, List.of(TABLE), Leaves.NONE, null, NOW);
        long june = Instant.parse("2015-06-01T00:00:00Z").toEpochMilli();
        long upper = Instant.parse("2016-06-01T00:00:00Z").toEpochMilli();
        String relation = InfinoAggregationProcessor.valuesRelation("\"" + TABLE + "\"", List.of(g.source));
        assertTrue(relation, relation.contains("GREATEST(CAST(arrow_cast(\"value_layer_0_low\", 'Int64') AS DOUBLE), " + (double) june + ")"));
        assertTrue(relation, relation.contains("LEAST(CAST(arrow_cast(\"value_layer_0_high\", 'Int64') AS DOUBLE), " + (double) upper + ")"));
        assertFalse(g.keyExpr, g.keyExpr.startsWith("CASE WHEN "));
    }
}
