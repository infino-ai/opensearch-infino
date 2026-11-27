/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.infino.InfinoNative.Column;
import org.opensearch.infino.InfinoQueryTranslator.Translation;
import org.opensearch.test.OpenSearchTestCase;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** The SQL a query-DSL tree translates to, over a log table's columns. */
public class InfinoQueryTranslatorTests extends OpenSearchTestCase {

    private static final String TABLE = "os_logs_0";
    private static final long NOW = Instant.parse("2026-10-24T12:00:00Z").toEpochMilli();

    private static Map<String, Column> columns() {
        Map<String, Column> cols = new LinkedHashMap<>();
        cols.put("osid", new Column("osid", "str", false, false));
        cols.put("@timestamp", new Column("@timestamp", "ts", false, false).rangeIndexed());
        cols.put("level", new Column("level", "str", false, false));
        cols.put("message", new Column("message", "str", true, false));
        cols.put("status", new Column("status", "i64", false, false).rangeIndexed());
        cols.put("host.name", new Column("host.name", "str", false, false));
        return cols;
    }

    private static Translation translate(org.opensearch.index.query.QueryBuilder qb) {
        return InfinoQueryTranslator.translate(qb, columns(), TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches);
    }

    /** Answers any read of the table with one row: whatever a query string's keyword clauses ask for, some row has it. */
    private static List<Map<String, Object>> aRowMatches(String sql) {
        return List.of(Map.of("infino_hit", 1));
    }

    public void testARangeOnADateColumnCarriesItsSpan() {
        // Both bounds as written: from the lower, inclusive, to the upper, exclusive.
        InfinoQueryTranslator.Span span = translate(QueryBuilders.rangeQuery("@timestamp").gte("2023-01-02T00:00:00").lt("2023-01-02T10:00:00")).span();
        assertEquals(new InfinoQueryTranslator.Span("@timestamp", Instant.parse("2023-01-02T00:00:00Z").toEpochMilli(), Instant.parse("2023-01-02T10:00:00Z").toEpochMilli()), span);
        // gt and lte on a day round up to its last millisecond, so the span starts and ends a millisecond past it.
        span = translate(QueryBuilders.rangeQuery("@timestamp").gt("2023-01-01").lte("2023-01-02").format("yyyy-MM-dd")).span();
        assertEquals(Instant.parse("2023-01-02T00:00:00Z").toEpochMilli(), span.fromMillis());
        assertEquals(Instant.parse("2023-01-03T00:00:00Z").toEpochMilli(), span.untilMillis());
        // An open range, a range on a number, or anything but a range carries none.
        assertNull(translate(QueryBuilders.rangeQuery("@timestamp").gte("2023-01-02T00:00:00")).span());
        assertNull(translate(QueryBuilders.rangeQuery("status").gte(500).lt(600)).span());
        assertNull(
            translate(QueryBuilders.boolQuery().filter(QueryBuilders.rangeQuery("@timestamp").gte("2023-01-02T00:00:00").lt("2023-01-03T00:00:00")).filter(QueryBuilders.termQuery("level", "error")))
                .span()
        );
        assertNull(translate(QueryBuilders.matchAllQuery()).span());
        assertEquals(
            "arrow_cast('2023-01-02T00:00:00Z', 'Timestamp(Millisecond, Some(\"UTC\"))')",
            InfinoAggregationProcessor.timestampLiteral(Instant.parse("2023-01-02T00:00:00Z").toEpochMilli())
        );
    }

    public void testMatchAllAndFilters() {
        assertEquals("1=1", translate(QueryBuilders.matchAllQuery()).where());
        assertEquals("\"level\" = 'error'", translate(QueryBuilders.termQuery("level", "error")).where());
        assertEquals("\"host.name\" = 'n1'", translate(QueryBuilders.termQuery("host.name.keyword", "n1")).where());
        assertEquals("\"status\" IN (200, 204)", translate(QueryBuilders.termsQuery("status", java.util.List.of(200, 204))).where());
        assertEquals("(\"status\" >= 500 AND \"status\" < 600)", translate(QueryBuilders.rangeQuery("status").gte(500).lt(600)).where());
        assertEquals("\"level\" IS NOT NULL", translate(QueryBuilders.existsQuery("level")).where());
        assertEquals("\"host.name\" LIKE 'n%'", translate(QueryBuilders.prefixQuery("host.name", "n")).where());
        assertEquals("\"host.name\" LIKE 'n_de%'", translate(QueryBuilders.wildcardQuery("host.name", "n?de*")).where());
        assertEquals("osid IN ('a', 'b')", translate(QueryBuilders.idsQuery().addIds("a", "b")).where());
    }

    /** An object exists where any field under it does, and a pattern names the fields it matches; no field means no row. */
    public void testExistsOnAnObjectOrAPattern() {
        assertEquals("(\"host.name\" IS NOT NULL)", translate(QueryBuilders.existsQuery("host")).where());
        assertEquals(
            "(\"@timestamp\" IS NOT NULL OR \"status\" IS NOT NULL OR \"host.name\" IS NOT NULL)",
            translate(QueryBuilders.existsQuery("*t*")).where()
        );
        assertEquals("1=0", translate(QueryBuilders.existsQuery("missing")).where());
    }

    /**
     * A field with a column of its own exists where a column under it holds a value too: a
     * table written before a geo_point was kept whole holds the points given as objects in the
     * columns under it, and the points written since in the field's own column.
     */
    public void testExistsOnAFieldWithAColumnCountsTheColumnsUnderIt() {
        Map<String, Column> cols = columns();
        cols.put("loc", new Column("loc", "str", false, false));
        cols.put("loc.lat", new Column("loc.lat", "f64", false, false));
        cols.put("loc.lon", new Column("loc.lon", "f64", false, false));
        assertEquals(
            "(\"loc\" IS NOT NULL OR \"loc.lat\" IS NOT NULL OR \"loc.lon\" IS NOT NULL)",
            InfinoQueryTranslator.translate(QueryBuilders.existsQuery("loc"), cols, TABLE, NOW).where()
        );
        // A field with no column under it reads its own alone.
        assertEquals("\"level\" IS NOT NULL", InfinoQueryTranslator.translate(QueryBuilders.existsQuery("level"), cols, TABLE, NOW).where());
    }

    public void testDateRangeUsesDateMath() {
        Translation t = translate(QueryBuilders.rangeQuery("@timestamp").gte("now-15m").lte("now"));
        assertEquals("(\"@timestamp\" >= '2026-10-24T11:45:00Z' AND \"@timestamp\" <= '2026-10-24T12:00:00Z')", t.where());
        // An exclusive bound on a day rounds past the day.
        t = translate(QueryBuilders.rangeQuery("@timestamp").gt("2026-10-23"));
        assertEquals("(\"@timestamp\" > '2026-10-23T23:59:59.999Z')", t.where());
    }

    public void testPhrasesOnExactColumnsAreEqualities() {
        // A match_phrase on a keyword column is the value as given (Dashboards' filter pills), never a quoted query.
        assertEquals("\"level\" = 'error'", translate(QueryBuilders.matchPhraseQuery("level", "error")).where());
        assertEquals("\"host.name\" = 'web-2'", translate(QueryBuilders.matchPhraseQuery("host.name", "web-2")).where());
        // A multi_match phrase over a text and a keyword column: the phrase on the text column, the value on the keyword one.
        Translation t = translate(QueryBuilders.multiMatchQuery("disk full", "message", "level").type(MultiMatchQueryBuilder.Type.PHRASE));
        assertEquals("(\"level\" = 'disk full' OR osid IN (SELECT osid FROM token_match('os_logs_0', 'message', '\"disk full\"', 'or')))", t.where());
        assertEquals("\"os_logs_0\"", t.from());
    }

    public void testTextClauseBecomesTheSource() {
        Translation t = translate(QueryBuilders.matchQuery("message", "disk full"));
        assertEquals("1=1", t.where());
        assertEquals("token_match('os_logs_0', 'message', 'disk full', 'or')", t.from());
        assertEquals("bm25_search('os_logs_0', 'message', 'disk full', 10, 'or')", t.rankedFrom(10));

        BoolQueryBuilder b = QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("message", "disk"))
            .must(QueryBuilders.matchPhraseQuery("message", "node 3"))
            .filter(QueryBuilders.termQuery("level", "error"));
        t = translate(b);
        assertEquals("token_match('os_logs_0', 'message', 'disk', 'or')", t.from());
        // A phrase goes to the engine quoted: adjacent terms in order, through its positions.
        assertEquals(
            "(1=1 AND osid IN (SELECT osid FROM token_match('os_logs_0', 'message', '\"node 3\"', 'and')) AND \"level\" = 'error')",
            t.where()
        );
    }

    /**
     * A match's text is its words: a leading + or - or a quote in it is no
     * clause syntax to the engine, as it is none to OpenSearch's analyzer.
     * Before, "-error timeout" asked the engine for "timeout and not error".
     */
    public void testAMatchsTextIsAskedForAsPlainWords() {
        assertEquals("token_match('os_logs_0', 'message', 'error timeout', 'or')", translate(QueryBuilders.matchQuery("message", "-error timeout")).from());
        assertEquals("token_match('os_logs_0', 'message', 'a b c', 'or')", translate(QueryBuilders.matchQuery("message", "+a \"b c")).from());
        String where = translate(QueryBuilders.boolQuery().should(QueryBuilders.termQuery("message", "-error"))).where();
        assertTrue(where, where.contains("text_match(\"message\", 'error', 'and')"));
        assertFalse(where, where.contains("-error"));
        // Text with no term matches nothing rather than reaching the engine empty.
        String none = translate(QueryBuilders.boolQuery().filter(QueryBuilders.matchQuery("message", "--"))).where();
        assertTrue(none, none.contains("1=0"));
    }

    /** name.keyword over a text field is the field's whole value, compared exactly, as a keyword subfield holds it. */
    public void testAKeywordViewOfATextFieldComparesTheWholeValue() {
        assertEquals("\"message\" = 'disk full'", translate(QueryBuilders.termQuery("message.keyword", "disk full")).where());
        assertEquals("\"message\" LIKE 'disk%'", translate(QueryBuilders.prefixQuery("message.keyword", "disk")).where());
    }

    /** The hit statement for a translated query, with no sort, collapse, post_filter or search_after. */
    private static String hits(Translation t, Column collapse, int fetch) {
        InfinoSearch.Plan plan = new InfinoSearch.Plan(
            TABLE,
            columns(),
            t,
            java.util.List.of(),
            null,
            null,
            collapse,
            null,
            java.util.List.of(),
            InfinoAccess.NONE,
            NOW
        );
        return InfinoSearch.hitStatement(plan, fetch, false);
    }

    /** The hit statement for a match_all sorted by {@code status}, under {@code collapse} (null for none). */
    private static String sortedHits(Column collapse) {
        InfinoSearch.Plan plan = new InfinoSearch.Plan(
            TABLE,
            columns(),
            translate(QueryBuilders.matchAllQuery()),
            java.util.List.of(InfinoSearch.SortSpec.of(columns().get("status"), true)),
            null,
            null,
            collapse,
            null,
            java.util.List.of(),
            InfinoAccess.NONE,
            NOW
        );
        return InfinoSearch.hitStatement(plan, 10, false);
    }

    /** The hit statement for {@code t} sorted by {@code status}. */
    private static String sortedHitsOf(Translation t) {
        InfinoSearch.Plan plan = new InfinoSearch.Plan(
            TABLE,
            columns(),
            t,
            java.util.List.of(InfinoSearch.SortSpec.of(columns().get("status"), true)),
            null,
            null,
            null,
            null,
            java.util.List.of(),
            InfinoAccess.NONE,
            NOW
        );
        return InfinoSearch.hitStatement(plan, 10, false);
    }

    /**
     * A sorted page ranks nothing, so its plain-words text clause is a
     * text_match on the table's rows, which the engine's top-k checks row by
     * row in the sort's order. A phrase needs the token index and keeps it.
     */
    public void testASortedPagesPlainWordsAreAPredicateOnTheTable() {
        String plain = sortedHitsOf(translate(QueryBuilders.matchQuery("message", "error timeout")));
        assertTrue(plain, plain.contains(" FROM \"os_logs_0\" WHERE (1=1) AND text_match(\"message\", 'error timeout', 'or') ORDER BY "));
        assertFalse(plain, plain.contains("token_match("));
        String phrase = sortedHitsOf(translate(QueryBuilders.matchPhraseQuery("message", "error timeout")));
        assertTrue(phrase, phrase.contains(" FROM token_match("));
    }

    public void testASortedPageReadsItsSourcesAfterTheSort() {
        // The sort weighs every row it keeps; its statement selects ids and sort
        // values only, and the page's sources are read by id afterwards.
        String sorted = sortedHits(null);
        assertTrue(sorted, sorted.startsWith("SELECT osid, \"status\" AS \"status\" FROM \"os_logs_0\" WHERE 1=1 ORDER BY "));
        assertFalse(sorted, sorted.contains("source"));
        // A collapse keeps the first row of each value through a window, whose rows carry their sources.
        String collapsed = sortedHits(columns().get("level"));
        assertTrue(collapsed, collapsed.startsWith("SELECT osid, source, "));
    }

    /** {@code t} over {@code sorts} then the row id, as a request's plan orders it. */
    private static InfinoSearch.Plan sortedPlan(Translation t, java.util.List<InfinoSearch.SortSpec> sorts) {
        return sortedPlan(columns(), t, sorts, null);
    }

    /** As {@link #sortedPlan(Translation, java.util.List)}, over {@code cols} and with {@code postWhere} (null for none). */
    private static InfinoSearch.Plan sortedPlan(Map<String, Column> cols, Translation t, java.util.List<InfinoSearch.SortSpec> sorts, String postWhere) {
        java.util.List<InfinoSearch.SortSpec> all = new java.util.ArrayList<>(sorts);
        all.add(InfinoSearch.SortSpec.rowId());
        return new InfinoSearch.Plan(TABLE, cols, t, all, null, postWhere, null, null, java.util.List.of(), InfinoAccess.NONE, NOW);
    }

    public void testASortedPageTheRangeIndexAloneAnswersReadsItsSourcesInItsStatement() {
        org.opensearch.search.sort.SortOrder desc = org.opensearch.search.sort.SortOrder.DESC;
        InfinoSearch.Keyset whole = InfinoSearch.Keyset.WHOLE;
        // A range-indexed numeric or date column then the row id, over every
        // row: the range index picks the page's rows, and the engine reads
        // the sources of those rows alone.
        InfinoSearch.Plan plan = sortedPlan(translate(QueryBuilders.matchAllQuery()), statusSort(desc, null));
        assertTrue(InfinoSearch.sourceInSortedStatement(plan, 10, whole));
        assertTrue(InfinoSearch.selectsSource(plan, 10, whole, true));
        String sql = InfinoSearch.hitStatement(plan, 10, false, whole, 10_000, InfinoSearch.selectsSource(plan, 10, whole, true));
        assertTrue(sql, sql.startsWith("SELECT osid, source, \"status\" AS \"status\", \"_id\" AS \"_id\" FROM \"os_logs_0\" WHERE 1=1 ORDER BY "));
        // A page whose hits need no source selects none.
        assertFalse(InfinoSearch.selectsSource(plan, 10, whole, false));
        String bare = InfinoSearch.hitStatement(plan, 10, false, whole, 10_000, InfinoSearch.selectsSource(plan, 10, whole, false));
        assertFalse(bare, bare.contains("source"));
        // One bounded range on the sort column is the range index's too.
        java.util.List<InfinoSearch.SortSpec> byTime = java.util.List.of(InfinoSearch.SortSpec.of(columns().get("@timestamp"), false));
        Translation span = translate(QueryBuilders.rangeQuery("@timestamp").gte("2023-01-02T00:00:00").lt("2023-01-03T00:00:00"));
        assertTrue(InfinoSearch.sourceInSortedStatement(sortedPlan(span, byTime), 10, whole));
        // The same range under a post_filter, or on another column than the sort's, is checked on each row.
        assertFalse(InfinoSearch.sourceInSortedStatement(sortedPlan(columns(), span, byTime, "\"status\" = 500"), 10, whole));
        assertFalse(InfinoSearch.sourceInSortedStatement(sortedPlan(span, statusSort(desc, null)), 10, whole));
        // Any other predicate has the engine walk the index checking each row,
        // and a walk that gives up reads every matching row: a term, plain
        // words, a phrase, or a range nested in a bool.
        assertFalse(InfinoSearch.sourceInSortedStatement(sortedPlan(translate(QueryBuilders.termQuery("level", "error")), byTime), 10, whole));
        assertFalse(InfinoSearch.sourceInSortedStatement(sortedPlan(translate(QueryBuilders.matchQuery("message", "error timeout")), byTime), 10, whole));
        assertFalse(InfinoSearch.sourceInSortedStatement(sortedPlan(translate(QueryBuilders.matchPhraseQuery("message", "error timeout")), byTime), 10, whole));
        Translation nested = translate(
            QueryBuilders.boolQuery().filter(QueryBuilders.rangeQuery("@timestamp").gte("2023-01-02T00:00:00").lt("2023-01-03T00:00:00"))
        );
        assertFalse(InfinoSearch.sourceInSortedStatement(sortedPlan(nested, byTime), 10, whole));
        // A selective filter, whose walk would give up, reads its sources by id.
        InfinoSearch.Plan rare = sortedPlan(translate(QueryBuilders.termQuery("level", "fatal")), byTime);
        String rareSql = InfinoSearch.hitStatement(rare, 10, false, whole, 10_000, InfinoSearch.selectsSource(rare, 10, whole, true));
        assertFalse(rareSql, rareSql.contains("source"));
        // A keyword column has no range index; a missing value or missing first is not the column's own order.
        java.util.List<InfinoSearch.SortSpec> byLevel = java.util.List.of(InfinoSearch.SortSpec.of(columns().get("level"), false));
        assertFalse(InfinoSearch.sourceInSortedStatement(sortedPlan(translate(QueryBuilders.matchAllQuery()), byLevel), 10, whole));
        assertFalse(InfinoSearch.sourceInSortedStatement(sortedPlan(translate(QueryBuilders.matchAllQuery()), statusSort(desc, "_first")), 10, whole));
        assertFalse(InfinoSearch.sourceInSortedStatement(sortedPlan(translate(QueryBuilders.matchAllQuery()), statusSort(desc, 0)), 10, whole));
        // A deep page reads its rows first and the returned ones' sources by id.
        assertFalse(InfinoSearch.sourceInSortedStatement(plan, InfinoSearch.SORTED_SOURCE_ROWS + 1, whole));
        int deepFetch = InfinoSearch.SORTED_SOURCE_ROWS + 1;
        String deep = InfinoSearch.hitStatement(plan, deepFetch, false, whole, 10_000, InfinoSearch.selectsSource(plan, deepFetch, whole, true));
        assertFalse(deep, deep.contains("source"));
        // After the sort's value alone, the present part is one more range on the column.
        InfinoSearch.Plan afterValue = afterPlan(new Object[] { 404 }, null);
        assertTrue(InfinoSearch.sourceInSortedStatement(afterValue, 10, InfinoSearch.Keyset.PRESENT));
        assertFalse(InfinoSearch.sourceInSortedStatement(afterValue, 10, InfinoSearch.Keyset.MISSING));
        // A keyset that also breaks ties by id is a predicate the engine walks, in either part.
        InfinoSearch.Plan after = afterPlan(new Object[] { 404, "7" }, null);
        assertFalse(InfinoSearch.sourceInSortedStatement(after, 10, InfinoSearch.Keyset.PRESENT));
        assertFalse(InfinoSearch.sourceInSortedStatement(after, 10, InfinoSearch.Keyset.MISSING));
        InfinoSearch.Keyset presentPart = InfinoSearch.Keyset.PRESENT;
        String present = InfinoSearch.hitStatement(after, 10, false, presentPart, 10_000, InfinoSearch.selectsSource(after, 10, presentPart, true));
        assertFalse(present, present.contains("source"));
        assertFalse(InfinoSearch.sourceInSortedStatement(afterPlan(new Object[] { null, "7" }, null), 10, whole));
        // A collapse's statement reads its sources as before.
        assertFalse(InfinoSearch.sourceInSortedStatement(afterPlan(null, columns().get("level")), 10, whole));
    }

    public void testAColumnWithNoRangeIndexReadsItsSourcesById() {
        // A numeric field mapped "index": false: the table keeps no range index on its column.
        Map<String, Column> cols = columns();
        cols.put("bytes", new Column("bytes", "i64", false, false));
        Translation all = InfinoQueryTranslator.translate(QueryBuilders.matchAllQuery(), cols, TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches);
        java.util.List<InfinoSearch.SortSpec> byBytes = java.util.List.of(InfinoSearch.SortSpec.of(cols.get("bytes"), true));
        InfinoSearch.Plan plan = sortedPlan(cols, all, byBytes, null);
        InfinoSearch.Keyset whole = InfinoSearch.Keyset.WHOLE;
        assertFalse(InfinoSearch.sourceInSortedStatement(plan, 10, whole));
        String sql = InfinoSearch.hitStatement(plan, 10, false, whole, 10_000, InfinoSearch.selectsSource(plan, 10, whole, true));
        assertFalse(sql, sql.contains("source"));
        // The same column with its range index does.
        cols.put("bytes", cols.get("bytes").rangeIndexed());
        byBytes = java.util.List.of(InfinoSearch.SortSpec.of(cols.get("bytes"), true));
        assertTrue(InfinoSearch.sourceInSortedStatement(sortedPlan(cols, all, byBytes, null), 10, whole));
    }

    /** The sorts of a request sorted on {@code status} in {@code order}, with {@code missing} (null for none). */
    private static java.util.List<InfinoSearch.SortSpec> statusSort(org.opensearch.search.sort.SortOrder order, Object missing) {
        org.opensearch.search.sort.FieldSortBuilder sort = org.opensearch.search.sort.SortBuilders.fieldSort("status").order(order);
        if (missing != null) {
            sort.missing(missing);
        }
        return InfinoSearch.sorts(java.util.List.of(sort), columns(), NOW);
    }

    /** A match_all over {@code sorts} then the row id, continuing after {@code after}. */
    private static InfinoSearch.Plan afterSorted(java.util.List<InfinoSearch.SortSpec> sorts, Object[] after) {
        return afterSorted(sorts, after, null);
    }

    /** A match_all sorted by {@code status} descending, then by the row id, continuing after {@code after}, under {@code collapse}. */
    private static InfinoSearch.Plan afterPlan(Object[] after, Column collapse) {
        return afterSorted(java.util.List.of(InfinoSearch.SortSpec.of(columns().get("status"), true)), after, collapse);
    }

    /** A match_all over {@code sorts} then the row id, continuing after {@code after}, under {@code collapse}. */
    private static InfinoSearch.Plan afterSorted(java.util.List<InfinoSearch.SortSpec> sorts, Object[] after, Column collapse) {
        java.util.List<InfinoSearch.SortSpec> all = new java.util.ArrayList<>(sorts);
        all.add(InfinoSearch.SortSpec.rowId());
        return new InfinoSearch.Plan(
            TABLE,
            columns(),
            translate(QueryBuilders.matchAllQuery()),
            all,
            null,
            null,
            collapse,
            after,
            java.util.List.of(),
            InfinoAccess.NONE,
            NOW
        );
    }

    public void testASortsMissingPutsTheMissingRowsWhereItSays() {
        org.opensearch.search.sort.SortOrder desc = org.opensearch.search.sort.SortOrder.DESC;
        // _last, or none, keeps them last in either direction.
        assertEquals(java.util.List.of("\"status\" DESC NULLS LAST"), InfinoSearch.orderBy(statusSort(desc, null)));
        assertEquals(java.util.List.of("\"status\" DESC NULLS LAST"), InfinoSearch.orderBy(statusSort(desc, "_last")));
        // _first puts them first, in either direction.
        assertEquals(java.util.List.of("\"status\" DESC NULLS FIRST"), InfinoSearch.orderBy(statusSort(desc, "_first")));
        assertEquals(
            java.util.List.of("\"status\" ASC NULLS FIRST"),
            InfinoSearch.orderBy(statusSort(org.opensearch.search.sort.SortOrder.ASC, "_first"))
        );
        // A value stands in for a missing one, which then sorts as that value.
        InfinoSearch.SortSpec custom = statusSort(desc, 300).get(0);
        assertEquals("COALESCE(\"status\", 300)", custom.expr());
        assertFalse(custom.nullsFirst());
        expectThrows(IllegalArgumentException.class, () -> statusSort(desc, "not a number"));
    }

    public void testASortsMissingValueIsReadAsStockReadsIt() {
        org.opensearch.search.sort.SortOrder desc = org.opensearch.search.sort.SortOrder.DESC;
        // An integer stays exact past a double's precision, and a fraction is cut to a long.
        assertEquals("COALESCE(\"status\", 9007199254740993)", statusSort(desc, 9007199254740993L).get(0).expr());
        assertEquals("COALESCE(\"status\", 3)", statusSort(desc, 3.5).get(0).expr());
        assertEquals("COALESCE(\"status\", 300)", statusSort(desc, "300").get(0).expr());
        expectThrows(IllegalArgumentException.class, () -> statusSort(desc, "3.5"));
        // A date's is its epoch milliseconds, as a literal of the column's type.
        org.opensearch.search.sort.FieldSortBuilder date = org.opensearch.search.sort.SortBuilders.fieldSort("@timestamp").order(desc).missing(1000);
        assertEquals(
            "COALESCE(\"@timestamp\", arrow_cast(1000, 'Timestamp(Millisecond, Some(\"UTC\"))'))",
            InfinoSearch.sorts(java.util.List.of(date), columns(), NOW).get(0).expr()
        );
        org.opensearch.search.sort.FieldSortBuilder dateText = org.opensearch.search.sort.SortBuilders.fieldSort("@timestamp").order(desc).missing("2020-01-01");
        expectThrows(IllegalArgumentException.class, () -> InfinoSearch.sorts(java.util.List.of(dateText), columns(), NOW));
    }

    public void testACollapseUnderASortsMissingValueKeepsItsOwnValue() {
        // The sort selects the stand-in under the column's name; the collapse value is the column's own.
        java.util.List<InfinoSearch.SortSpec> sorts = new java.util.ArrayList<>(statusSort(org.opensearch.search.sort.SortOrder.DESC, 3));
        sorts.add(InfinoSearch.SortSpec.rowId());
        InfinoSearch.Plan plan = new InfinoSearch.Plan(
            TABLE,
            columns(),
            translate(QueryBuilders.matchAllQuery()),
            sorts,
            null,
            null,
            columns().get("status"),
            null,
            java.util.List.of(),
            InfinoAccess.NONE,
            NOW
        );
        String sql = InfinoSearch.hitStatement(plan, 10, false);
        assertTrue(sql, sql.contains("COALESCE(\"status\", 3) AS \"status\""));
        assertTrue(sql, sql.contains(", \"status\" AS \"collapse_key\""));
    }

    public void testACollapseKeyAliasStepsAroundAFieldOfItsName() {
        // A field named collapse_key (or collapse_rank) would make the
        // statement's own alias ambiguous, so the alias takes a leading underscore.
        Map<String, Column> cols = new LinkedHashMap<>(columns());
        cols.put("collapse_key", new Column("collapse_key", "str", false, false));
        cols.put("collapse_rank", new Column("collapse_rank", "i64", false, false));
        java.util.List<InfinoSearch.SortSpec> sorts = java.util.List.of(InfinoSearch.SortSpec.of(cols.get("collapse_key"), false), InfinoSearch.SortSpec.rowId());
        InfinoSearch.Plan plan = new InfinoSearch.Plan(
            TABLE,
            cols,
            translate(QueryBuilders.matchAllQuery()),
            sorts,
            null,
            null,
            cols.get("status"),
            null,
            java.util.List.of(),
            InfinoAccess.NONE,
            NOW
        );
        assertEquals("_collapse_key", InfinoSearch.freeAlias(plan, "collapse_key"));
        String sql = InfinoSearch.hitStatement(plan, 10, false);
        assertTrue(sql, sql.contains(", \"collapse_key\" AS \"collapse_key\", "));
        assertTrue(sql, sql.contains(", \"status\" AS \"_collapse_key\""));
        assertTrue(sql, sql.contains("AS \"_collapse_rank\" FROM ") && sql.contains("WHERE \"_collapse_rank\" = 1"));
    }

    public void testABooleanSortsMissingValueIsALong() {
        // Stock's comparator reads a boolean as a long (true is 1), and its
        // missing value as one: 1 ties with true, and text is refused.
        Map<String, Column> cols = new LinkedHashMap<>(columns());
        cols.put("ok", new Column("ok", "bool", false, false));
        org.opensearch.search.sort.FieldSortBuilder one = org.opensearch.search.sort.SortBuilders.fieldSort("ok").missing(1);
        InfinoSearch.SortSpec s = InfinoSearch.sorts(java.util.List.of(one), cols, NOW).get(0);
        assertEquals("COALESCE(CAST(\"ok\" AS BIGINT), 1)", s.expr());
        assertEquals(InfinoScriptTranslator.SqlType.INT, s.type());
        assertFalse(s.missingStandIn(Long.MAX_VALUE));
        org.opensearch.search.sort.FieldSortBuilder text = org.opensearch.search.sort.SortBuilders.fieldSort("ok").missing("1");
        assertEquals("COALESCE(CAST(\"ok\" AS BIGINT), 1)", InfinoSearch.sorts(java.util.List.of(text), cols, NOW).get(0).expr());
        org.opensearch.search.sort.FieldSortBuilder word = org.opensearch.search.sort.SortBuilders.fieldSort("ok").missing("true");
        expectThrows(IllegalArgumentException.class, () -> InfinoSearch.sorts(java.util.List.of(word), cols, NOW));
    }

    public void testAFloatSortsMissingValueAtItsMappingsPrecision() {
        // A float field's column holds each value as the 32-bit float stock
        // keeps, and stock's comparator reads the missing value as one too.
        Map<String, Column> cols = new LinkedHashMap<>(columns());
        cols.put("took", new Column("took", "f64", false, false));
        org.opensearch.search.sort.FieldSortBuilder sort = org.opensearch.search.sort.SortBuilders.fieldSort("took").missing(3.1);
        assertEquals("COALESCE(\"took\", 3.0999999046325684)", InfinoSearch.sorts(java.util.List.of(sort), cols, java.util.Set.of("took"), NOW).get(0).expr());
        org.opensearch.search.sort.FieldSortBuilder text = org.opensearch.search.sort.SortBuilders.fieldSort("took").missing("3.1");
        assertEquals("COALESCE(\"took\", 3.0999999046325684)", InfinoSearch.sorts(java.util.List.of(text), cols, java.util.Set.of("took"), NOW).get(0).expr());
        // A double keeps the value as written.
        assertEquals("COALESCE(\"took\", 3.1)", InfinoSearch.sorts(java.util.List.of(sort), cols, NOW).get(0).expr());
    }

    public void testAnInfiniteMissingValuePutsTheMissingRowsOnItsSide() {
        // No present value is infinite, so +Infinity is past every one: last
        // ascending, first descending; -Infinity the other way; NaN as
        // +Infinity. The statement holds no infinite literal.
        Map<String, Column> cols = new LinkedHashMap<>(columns());
        cols.put("took", new Column("took", "f64", false, false));
        org.opensearch.search.sort.SortOrder asc = org.opensearch.search.sort.SortOrder.ASC;
        org.opensearch.search.sort.SortOrder desc = org.opensearch.search.sort.SortOrder.DESC;
        Object[][] cases = {
            { "Infinity", asc, "\"took\" ASC NULLS LAST" },
            { "Infinity", desc, "\"took\" DESC NULLS FIRST" },
            { Double.POSITIVE_INFINITY, desc, "\"took\" DESC NULLS FIRST" },
            { "-Infinity", asc, "\"took\" ASC NULLS FIRST" },
            { "-Infinity", desc, "\"took\" DESC NULLS LAST" },
            { "NaN", asc, "\"took\" ASC NULLS LAST" },
            { 1e300, desc, "\"took\" DESC NULLS LAST" } };
        for (Object[] c : cases) {
            org.opensearch.search.sort.FieldSortBuilder sort = org.opensearch.search.sort.SortBuilders.fieldSort("took")
                .order((org.opensearch.search.sort.SortOrder) c[1])
                .missing(c[0]);
            java.util.List<InfinoSearch.SortSpec> sorts = InfinoSearch.sorts(java.util.List.of(sort), cols, NOW);
            if (c[0] instanceof Double d && Double.isFinite(d)) {
                // A finite double stays one, past a float's range.
                assertEquals("COALESCE(\"took\", 1.0E300)", sorts.get(0).expr());
                // The same value on a float field is a float's infinity: the missing rows past every present one.
                sorts = InfinoSearch.sorts(java.util.List.of(sort), cols, java.util.Set.of("took"), NOW);
                assertEquals(java.util.List.of("\"took\" DESC NULLS FIRST"), InfinoSearch.orderBy(sorts));
                continue;
            }
            assertEquals(java.util.Arrays.toString(c), java.util.List.of((String) c[2]), InfinoSearch.orderBy(sorts));
        }
        // The hit hands back the infinity, which a search_after reads back as missing.
        org.opensearch.search.sort.FieldSortBuilder inf = org.opensearch.search.sort.SortBuilders.fieldSort("took").order(desc).missing("Infinity");
        assertTrue(InfinoSearch.sorts(java.util.List.of(inf), cols, NOW).get(0).missingStandIn(Double.POSITIVE_INFINITY));
    }

    public void testASearchAfterPageIsOneStatementWhenNoMissingRowFollowsThePresentOnes() {
        // Under missing: _first the missing rows come first, so none follow a
        // present value; under a missing value none is missing at all.
        org.opensearch.search.sort.SortOrder desc = org.opensearch.search.sort.SortOrder.DESC;
        assertFalse(InfinoSearch.splitsMissingTail(afterSorted(statusSort(desc, "_first"), new Object[] { 404, "7" })));
        assertFalse(InfinoSearch.splitsMissingTail(afterSorted(statusSort(desc, 300), new Object[] { 404, "7" })));
        assertTrue(InfinoSearch.splitsMissingTail(afterSorted(statusSort(desc, "_last"), new Object[] { 404, "7" })));
    }

    public void testAScrollResumesFromItsLastHitsSortValuesAsTheyAre() {
        // The plan reads a stand-in back as missing by its own sort; a scroll
        // sorted under missing: Long.MAX_VALUE resumes from that value.
        assertArrayEquals(
            new Object[] { Long.MAX_VALUE, Long.MIN_VALUE, Double.POSITIVE_INFINITY, "a", null },
            InfinoQueryPhaseSearcher.searchAfter(
                new Object[] { Long.MAX_VALUE, Long.MIN_VALUE, Double.POSITIVE_INFINITY, new org.apache.lucene.util.BytesRef("a"), null }
            )
        );
        assertFalse(statusSort(org.opensearch.search.sort.SortOrder.ASC, Long.MAX_VALUE).get(0).missingStandIn(Long.MAX_VALUE));
        assertTrue(statusSort(org.opensearch.search.sort.SortOrder.ASC, null).get(0).missingStandIn(Long.MAX_VALUE));
    }

    public void testASearchAfterPageUnderMissingFirst() {
        java.util.List<InfinoSearch.SortSpec> first = statusSort(org.opensearch.search.sort.SortOrder.DESC, "_first");
        // After a present value the missing rows are behind the page, so they are not read.
        InfinoSearch.Plan present = afterSorted(first, new Object[] { 404, "7" });
        String sql = InfinoSearch.hitStatement(present, 10, false);
        assertTrue(sql, sql.contains("AND ((\"status\" < 404) OR (\"status\" = 404 AND (\"_id\" > CAST('7' AS DECIMAL(38, 0)) OR \"_id\" IS NULL)))"));
        assertFalse(sql, sql.contains("\"status\" IS NULL"));
        // After a missing value every present row follows.
        String afterMissing = InfinoSearch.hitStatement(afterSorted(first, new Object[] { null, "7" }), 10, false);
        assertTrue(afterMissing, afterMissing.contains("((\"status\" IS NOT NULL) OR (\"status\" IS NULL AND (\"_id\" > CAST('7' AS DECIMAL(38, 0)) OR \"_id\" IS NULL)))"));
    }

    public void testAMissingStandInIsReadBackAsMissing() {
        // A default-mode hit hands back the least long for a row missing a value
        // sorted descending (missing last), the greatest ascending, and the
        // other way round under missing: _first; the client sends it back.
        InfinoSearch.SortSpec desc = InfinoSearch.SortSpec.of(columns().get("status"), true);
        assertTrue(desc.missingStandIn(Long.MIN_VALUE));
        assertTrue(desc.missingStandIn("-9223372036854775808"));
        assertFalse(desc.missingStandIn(Long.MAX_VALUE));
        assertFalse(desc.missingStandIn(404));
        InfinoSearch.SortSpec asc = InfinoSearch.SortSpec.of(columns().get("status"), false);
        assertTrue(asc.missingStandIn(Long.MAX_VALUE));
        assertFalse(asc.missingStandIn(Long.MIN_VALUE));
        InfinoSearch.SortSpec descFirst = statusSort(org.opensearch.search.sort.SortOrder.DESC, "_first").get(0);
        assertTrue(descFirst.missingStandIn(Long.MAX_VALUE));
        assertFalse(descFirst.missingStandIn(Long.MIN_VALUE));
        // A missing value given as a value leaves no row missing: the least long is a value then.
        assertFalse(statusSort(org.opensearch.search.sort.SortOrder.DESC, 300).get(0).missingStandIn(Long.MIN_VALUE));
        assertFalse(statusSort(org.opensearch.search.sort.SortOrder.ASC, Long.MAX_VALUE).get(0).missingStandIn(Long.MAX_VALUE));
        // Text has no stand-in: its missing value is handed back as null.
        assertFalse(InfinoSearch.SortSpec.of(columns().get("level"), true).missingStandIn("-9223372036854775808"));
    }

    public void testASearchAfterPageReadsThePresentRowsBeforeTheMissingOnes() {
        // After a present first value, the rows past it are a plain range on
        // the sort column; the rows missing it sort after them all and are
        // their own statement, read only for a short page.
        InfinoSearch.Plan plan = afterPlan(new Object[] { 404, "7" }, null);
        assertTrue(InfinoSearch.splitsMissingTail(plan));
        String whole = InfinoSearch.hitStatement(plan, 10, false);
        assertTrue(whole, whole.contains("(\"status\" < 404 OR \"status\" IS NULL)"));
        String present = InfinoSearch.hitStatement(plan, 10, false, InfinoSearch.Keyset.PRESENT);
        assertTrue(present, present.contains("AND ((\"status\" < 404) OR (\"status\" = 404 AND (\"_id\" > CAST('7' AS DECIMAL(38, 0)) OR \"_id\" IS NULL)))"));
        assertFalse(present, present.contains("\"status\" IS NULL"));
        assertTrue(present, present.endsWith("ORDER BY \"status\" DESC NULLS LAST, \"_id\" ASC NULLS LAST LIMIT 10"));
        String missing = InfinoSearch.hitStatement(plan, 3, false, InfinoSearch.Keyset.MISSING);
        assertTrue(missing, missing.contains("WHERE (1=1) AND \"status\" IS NULL ORDER BY "));
        assertTrue(missing, missing.endsWith(" LIMIT 3"));
        // After a missing first value only the deeper sorts decide, and a
        // collapse ranks a value's rows across both parts: one statement each.
        assertFalse(InfinoSearch.splitsMissingTail(afterPlan(new Object[] { null, "7" }, null)));
        assertFalse(InfinoSearch.splitsMissingTail(afterPlan(new Object[] { 404, "7" }, columns().get("level"))));
        assertFalse(InfinoSearch.splitsMissingTail(afterPlan(null, null)));
    }

    public void testAScrollPageIsReadInItsSortsOrder() {
        // A page a later page resumes from (a scroll's) is read in its sort's
        // order even when the sort orders nothing, so each hit carries the row
        // id the next page resumes after: the column's presence is not counted.
        InfinoSearch.Plan plan = afterPlan(null, null);
        assertFalse(InfinoSearch.readsUnsorted(plan, true));
        String hits = InfinoSearch.hitStatement(plan, 10, false);
        assertTrue(hits, hits.contains("\"_id\" AS \"_id\""));
        assertTrue(hits, hits.endsWith("ORDER BY \"status\" DESC NULLS LAST, \"_id\" ASC NULLS LAST LIMIT 10"));
    }

    public void testAnUnsortedPageKeepsTheSortsUnrankedRows() {
        // A text query sorted on a field no row holds is read with no order,
        // from the same unranked relation the sorted read selects from: not
        // BM25's best, and with no score.
        InfinoSearch.Plan plan = new InfinoSearch.Plan(
            TABLE,
            columns(),
            translate(QueryBuilders.matchQuery("message", "disk")),
            java.util.List.of(InfinoSearch.SortSpec.of(columns().get("status"), true), InfinoSearch.SortSpec.rowId()),
            null,
            null,
            null,
            null,
            java.util.List.of(),
            InfinoAccess.NONE,
            NOW
        );
        String sorted = InfinoSearch.hitStatement(plan, 1, false);
        String unsorted = InfinoSearch.unsortedStatement(plan, 1, false, true);
        // Plain words are a text_match on the table in both: rows in the page's order, each checked.
        assertEquals("SELECT osid, source FROM \"os_logs_0\" WHERE (1=1) AND text_match(\"message\", 'disk', 'or') LIMIT 1", unsorted);
        assertTrue(sorted, sorted.contains(" FROM \"os_logs_0\" WHERE (1=1) AND text_match(\"message\", 'disk', 'or') ORDER BY "));
        assertEquals(
            "SELECT osid, source, osseq FROM \"os_logs_0\" WHERE 1=1 LIMIT 5",
            InfinoSearch.unsortedStatement(afterPlan(null, null), 5, true, true)
        );
        // A hit that neither returns its source nor reads fields from it does not read it.
        assertEquals("SELECT osid, osseq FROM \"os_logs_0\" WHERE 1=1 LIMIT 5", InfinoSearch.unsortedStatement(afterPlan(null, null), 5, true, false));
    }

    public void testAPageWithNoOrderLeavesTheOrderToTheEngine() {
        // A page with no sort and no score carries no ORDER BY: the engine
        // reads a scan under a LIMIT with no order in file order, so each page
        // of one search starts where the last one ended. Its source is read
        // in the same scan.
        assertEquals(
            "SELECT osid, source FROM \"os_logs_0\" WHERE \"level\" = 'error' LIMIT 10",
            hits(translate(QueryBuilders.termQuery("level", "error")), null, 10)
        );
    }

    public void testAPageWithNoOrderThatIsNotAPlainScanIsReadInRowIdOrder() {
        // A filter joined by id, or rows read from the token index, come out
        // of the engine in no fixed order once it has read them all, so the
        // page orders them by the row id. A plain scan, a plain-words
        // text_match included, keeps no order.
        String byRowId = " ORDER BY " + InfinoSearch.ROW_ID_ORDER + " LIMIT 10";
        String noSort = hits(translate(QueryBuilders.boolQuery().mustNot(QueryBuilders.matchPhraseQuery("message", "disk full"))), null, 10);
        assertTrue(noSort, noSort.contains("osid IN (SELECT osid FROM token_match("));
        assertTrue(noSort, noSort.endsWith(byRowId));
        // A sort on a field no row holds reads the same way.
        String phraseOut = unsortedHits(QueryBuilders.boolQuery().mustNot(QueryBuilders.matchPhraseQuery("message", "disk full")), columns());
        assertTrue(phraseOut, phraseOut.contains("osid IN (SELECT osid FROM token_match("));
        assertTrue(phraseOut, phraseOut.endsWith(byRowId));
        Map<String, Column> cols = columns();
        cols.put("level[]", Column.list("level[]", "str"));
        String listed = unsortedHits(QueryBuilders.termQuery("level", "error"), cols);
        assertTrue(listed, listed.contains("osid IN (SELECT list_rows.list_id FROM "));
        assertTrue(listed, listed.endsWith(byRowId));
        String phrase = unsortedHits(QueryBuilders.matchPhraseQuery("message", "disk full"), columns());
        assertTrue(phrase, phrase.startsWith("SELECT osid, source FROM token_match("));
        assertTrue(phrase, phrase.endsWith(byRowId));
        assertEquals(
            "SELECT osid, source FROM \"os_logs_0\" WHERE (1=1) AND text_match(\"message\", 'disk', 'or') LIMIT 10",
            unsortedHits(QueryBuilders.matchQuery("message", "disk"), columns())
        );
        assertEquals("SELECT osid, source FROM \"os_logs_0\" WHERE \"level\" = 'error' LIMIT 10", unsortedHits(QueryBuilders.termQuery("level", "error"), columns()));
    }

    /**
     * A query_string atom on a keyword field reads the field's list column as
     * the query DSL does: a document that gave the field several values holds
     * them there, its own column null, so "has a value", a wildcard and a
     * regexp each match it by any of its values.
     */
    public void testQueryStringAtomsReadAKeywordsListColumn() {
        Map<String, Column> cols = columns();
        cols.put("level[]", Column.list("level[]", "str"));
        for (String q : List.of("level:*", "level:[* TO *]")) {
            String where = InfinoQueryTranslator.translate(QueryBuilders.queryStringQuery(q), cols, TABLE, NOW).where();
            assertEquals(q, "(\"level\" IS NOT NULL OR \"level[]\" IS NOT NULL)", where);
        }
        String wildcard = InfinoQueryTranslator.translate(QueryBuilders.queryStringQuery("level:er*"), cols, TABLE, NOW).where();
        assertTrue(wildcard, wildcard.contains("(\"level[]\" IS NULL AND \"level\" LIKE 'er%')"));
        assertTrue(wildcard, wildcard.contains("WHERE list_rows.list_value LIKE 'er%'"));
        String regexp = InfinoQueryTranslator.translate(QueryBuilders.queryStringQuery("level:/er.*/"), cols, TABLE, NOW).where();
        assertTrue(regexp, regexp.contains("(\"level[]\" IS NULL AND regexp_like(\"level\", '^(?:er.*)$'))"));
        assertTrue(regexp, regexp.contains("WHERE regexp_like(list_rows.list_value, '^(?:er.*)$')"));
        // On the default fields a pattern reaches a keyword field too, as a bare term does: a simple_query_string prefix.
        String bare = InfinoQueryTranslator.translate(QueryBuilders.simpleQueryStringQuery("er*").field("level"), cols, TABLE, NOW).where();
        assertTrue(bare, bare.contains("WHERE list_rows.list_value LIKE 'er%'"));
        String bareRegexp = InfinoQueryTranslator.translate(QueryBuilders.queryStringQuery("/er.*/").field("level"), cols, TABLE, NOW).where();
        assertTrue(bareRegexp, bareRegexp.contains("WHERE regexp_like(list_rows.list_value, '^(?:er.*)$')"));
        // Without a list column each reads the own column alone, as before.
        assertEquals("\"level\" IS NOT NULL", translate(QueryBuilders.queryStringQuery("level:*")).where());
        assertEquals("\"level\" LIKE 'er%'", translate(QueryBuilders.queryStringQuery("level:er*")).where());
    }

    /** The unsorted statement for {@code qb} over {@code cols}, sorted on a field no row holds. */
    private static String unsortedHits(org.opensearch.index.query.QueryBuilder qb, Map<String, Column> cols) {
        InfinoSearch.Plan plan = new InfinoSearch.Plan(
            TABLE,
            cols,
            InfinoQueryTranslator.translate(qb, cols, TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches),
            java.util.List.of(InfinoSearch.SortSpec.of(cols.get("status"), true), InfinoSearch.SortSpec.rowId()),
            null,
            null,
            null,
            null,
            java.util.List.of(),
            InfinoAccess.NONE,
            NOW
        );
        return InfinoSearch.unsortedStatement(plan, 10, false, true);
    }

    public void testACollapseWithNoOrderKeepsTheRowIdOrder() {
        // The window's rows come out grouped by value, so the first row of
        // each value and the page's rows both follow the row id's order.
        String collapsed = hits(translate(QueryBuilders.matchAllQuery()), columns().get("level"), 10);
        assertTrue(collapsed, collapsed.contains("PARTITION BY \"level\" ORDER BY " + InfinoSearch.ROW_ID_ORDER + ")"));
        assertTrue(collapsed, collapsed.endsWith(" = 1 ORDER BY " + InfinoSearch.ROW_ID_ORDER + " LIMIT 10"));
        assertEquals("\"_id\" ASC NULLS LAST", InfinoSearch.ROW_ID_ORDER);
    }

    public void testTiesBreakByTheEngineRowId() {
        // Rows sharing the sort value are ordered by the engine's row id, which
        // the engine answers from its range index; a page continuing from a row
        // reads the id back as a decimal, past what an integer literal holds.
        String id = "31415926535897932384626433832795";
        InfinoSearch.Plan plan = new InfinoSearch.Plan(
            TABLE,
            columns(),
            translate(QueryBuilders.matchAllQuery()),
            java.util.List.of(InfinoSearch.SortSpec.of(columns().get("status"), true), InfinoSearch.SortSpec.rowId()),
            null,
            null,
            null,
            new Object[] { 404, id },
            java.util.List.of(),
            InfinoAccess.NONE,
            NOW
        );
        String sql = InfinoSearch.hitStatement(plan, 10, false);
        assertTrue(sql, sql.startsWith("SELECT osid, \"status\" AS \"status\", \"_id\" AS \"_id\" FROM \"os_logs_0\" WHERE "));
        assertTrue(sql, sql.contains("\"_id\" > CAST('" + id + "' AS DECIMAL(38, 0))"));
        assertTrue(sql, sql.endsWith("ORDER BY \"status\" DESC NULLS LAST, \"_id\" ASC NULLS LAST LIMIT 10"));
        assertNull(InfinoSearch.rowIdLiteral("12; DROP TABLE x"));
        assertNull(InfinoSearch.rowIdLiteral(""));
    }

    public void testRankedSearchUnderAFilterAsksForThePage() {
        // The engine ranks within the rows the filter keeps, as Lucene filters
        // while it collects: the filter rides with the ranked search, which is
        // asked for the page alone.
        BoolQueryBuilder b = QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("message", "disk"))
            .filter(QueryBuilders.termQuery("level", "error"))
            .filter(QueryBuilders.rangeQuery("status").gte(500));
        assertEquals(
            "SELECT osid, source, score FROM bm25_search('os_logs_0', 'message', 'disk', 10, 'or') "
                + "WHERE (1=1 AND \"level\" = 'error' AND (\"status\" >= 500)) ORDER BY score DESC, \"_id\" ASC LIMIT 10",
            hits(translate(b), null, 10)
        );
        // No filter: the page, as before.
        assertEquals(
            "SELECT osid, source, score FROM bm25_search('os_logs_0', 'message', 'disk', 10, 'or') WHERE 1=1 ORDER BY score DESC, \"_id\" ASC LIMIT 10",
            hits(translate(QueryBuilders.matchQuery("message", "disk")), null, 10)
        );
        // Every filter shape the translator writes over the table's columns (a
        // negation, a pattern, a null check, a list, a disjunction, date math,
        // ids) is evaluated by the engine before it ranks, so each asks for the
        // page; InfinoRankedFilterIT checks the engine does so.
        java.util.List<org.opensearch.index.query.QueryBuilder> filters = java.util.List.of(
            QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("level", "info")),
            QueryBuilders.regexpQuery("level", "err.*"),
            QueryBuilders.wildcardQuery("level", "e*r"),
            QueryBuilders.existsQuery("host.name"),
            QueryBuilders.termsQuery("level", "error", "fatal"),
            QueryBuilders.boolQuery().should(QueryBuilders.termQuery("level", "fatal")).should(QueryBuilders.rangeQuery("status").gte(500)).minimumShouldMatch(1),
            QueryBuilders.rangeQuery("@timestamp").gte("now-1d"),
            QueryBuilders.idsQuery().addIds("a", "b")
        );
        for (org.opensearch.index.query.QueryBuilder filter : filters) {
            Translation t = translate(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("message", "disk")).filter(filter));
            assertFalse(filter.toString(), InfinoQueryTranslator.joinsAfterRanking(t.where()));
            assertTrue(filter.toString(), hits(t, null, 10).contains("bm25_search('os_logs_0', 'message', 'disk', 10, 'or')"));
        }
    }

    public void testRankedSearchOrdersItsTiesAsWritten() {
        // Stock orders hits that tie on the score by their place in the index,
        // the first written first. The engine's row id holds that order, so
        // every ranked statement breaks its ties on it: a filtered page, one
        // narrowed after the ranking, and a collapse's window and its groups.
        String tie = "score DESC, \"_id\" ASC";
        BoolQueryBuilder filtered = QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("message", "disk"))
            .mustNot(QueryBuilders.termQuery("level", "info"));
        assertTrue(hits(translate(filtered), null, 10).endsWith("ORDER BY " + tie + " LIMIT 10"));
        Map<String, Column> cols = columns();
        cols.put("title", new Column("title", "str", true, false));
        BoolQueryBuilder joined = QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("message", "disk"))
            .filter(QueryBuilders.matchQuery("title", "outage"));
        String joinedSql = hits(InfinoQueryTranslator.translate(joined, cols, TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches), null, 10);
        assertTrue(joinedSql, joinedSql.endsWith("ORDER BY " + tie + " LIMIT 10"));
        String collapsed = hits(translate(filtered), columns().get("level"), 10);
        assertTrue(collapsed, collapsed.contains("ORDER BY " + tie + ")"));
        assertTrue(collapsed, collapsed.endsWith("ORDER BY " + tie + " LIMIT 10"));
    }

    public void testRankedSearchKeepsTheWindowWhereRowsGoAfterTheRanking() {
        // A text clause on another column is a subquery joined above the
        // ranked search, so the search ranks more candidates than the page.
        Map<String, Column> cols = columns();
        cols.put("title", new Column("title", "str", true, false));
        BoolQueryBuilder b = QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("message", "disk"))
            .filter(QueryBuilders.matchQuery("title", "outage"));
        Translation t = InfinoQueryTranslator.translate(b, cols, TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches);
        assertTrue(InfinoQueryTranslator.joinsAfterRanking(t.where()));
        assertTrue(hits(t, null, 10).contains("bm25_search('os_logs_0', 'message', 'disk', 10000, 'or')"));
        // A collapse keeps one row per value after the ranking: a filtered one
        // keeps the window, an unfiltered one the page, as before.
        Column level = columns().get("level");
        BoolQueryBuilder filtered = QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("message", "disk"))
            .filter(QueryBuilders.rangeQuery("status").gte(500));
        assertTrue(hits(translate(filtered), level, 10).contains("bm25_search('os_logs_0', 'message', 'disk', 10000, 'or')"));
        assertTrue(hits(translate(QueryBuilders.matchQuery("message", "disk")), level, 10).contains(", 10, 'or')"));
        // A plain filter has no subquery.
        assertFalse(InfinoQueryTranslator.joinsAfterRanking("\"level\" = 'error'"));
    }

    public void testQueryStringOverSeveralTextColumnsRanksAcrossThem() {
        // Stock: message:monkey is one clause, the run "jackal bear" one dis_max over the default fields; summed.
        Map<String, Column> cols = columns();
        cols.put("title", new Column("title", "str", true, false));
        Translation t = InfinoQueryTranslator.translate(
            QueryBuilders.queryStringQuery("message: monkey jackal bear").field("message").field("title"),
            cols,
            TABLE,
            NOW
        , InfinoQueryTranslatorTests::aRowMatches);
        assertNull(t.principal());
        InfinoQueryTranslator.Across across = t.scoring().across();
        assertNotNull(across);
        assertEquals(
            List.of(
                List.of(new InfinoQueryTranslator.TextClause("message", "monkey", "or")),
                List.of(new InfinoQueryTranslator.TextClause("message", "jackal bear", "or"), new InfinoQueryTranslator.TextClause("title", "jackal bear", "or"))
            ),
            across.clauses()
        );
        assertEquals(t.where(), across.match());
        String sql = hits(t, null, 10);
        // One engine search ranks every clause by its best column and sums them, for the page alone: the match is no filter of its.
        assertEquals(
            "SELECT osid, score, \"_id\" AS infino_row FROM bm25_search_fields('os_logs_0', "
                + "'[{\"query\":\"monkey\",\"fields\":[\"message\"]},{\"query\":\"jackal bear\",\"fields\":[\"message\",\"title\"]}]', 10, 'or')"
                + " ORDER BY score DESC, infino_row ASC LIMIT 10",
            sql
        );

        // One run alone: one clause, scored by its best column.
        Translation one = InfinoQueryTranslator.translate(QueryBuilders.queryStringQuery("jackal bear").field("message").field("title"), cols, TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches);
        assertEquals(1, one.scoring().across().clauses().size());
        assertTrue(
            hits(one, null, 10).contains("bm25_search_fields('os_logs_0', '[{\"query\":\"jackal bear\",\"fields\":[\"message\",\"title\"]}]', 10, 'or')")
        );

        // A must range beside it adds its constant and filters the engine's ranking.
        BoolQueryBuilder b = QueryBuilders.boolQuery()
            .must(QueryBuilders.rangeQuery("status").gte(500))
            .must(QueryBuilders.queryStringQuery("jackal bear").field("message").field("title"));
        Translation filtered = InfinoQueryTranslator.translate(b, cols, TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches);
        assertNotNull(filtered.scoring().across());
        assertEquals(1.0, filtered.scoring().offset(), 0);
        String filteredSql = hits(filtered, null, 10);
        assertTrue(
            filteredSql,
            filteredSql.contains("\"fields\":[\"message\",\"title\"]}]', 10, 'or') WHERE ((\"status\" >= 500) AND 1=1) ORDER BY score DESC")
        );

        // Forms read otherwise are left unranked, as before.
        for (QueryStringQueryBuilder qs : List.of(
            QueryBuilders.queryStringQuery("jackal AND bear").field("message").field("title"),
            QueryBuilders.queryStringQuery("\"jackal bear\"").field("message").field("title"),
            QueryBuilders.queryStringQuery("jack*").field("message").field("title"),
            QueryBuilders.queryStringQuery("jackal bear").field("message").field("title").defaultOperator(Operator.AND),
            QueryBuilders.queryStringQuery("jackal bear").field("message", 2f).field("title"),
            QueryBuilders.queryStringQuery("jackal bear").field("message").field("level"),
            QueryBuilders.queryStringQuery("jackal jackal").field("message").field("title")
        )) {
            assertNull(qs.toString(), InfinoQueryTranslator.translate(qs, cols, TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches).scoring().across());
        }
        // Beside another scoring text clause, or on one column, it is not ranked across.
        BoolQueryBuilder two = QueryBuilders.boolQuery()
            .must(QueryBuilders.queryStringQuery("jackal bear").field("message").field("title"))
            .should(QueryBuilders.matchQuery("title", "owl"));
        assertNull(InfinoQueryTranslator.translate(two, cols, TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches).scoring().across());
        assertNull(InfinoQueryTranslator.translate(QueryBuilders.queryStringQuery("jackal bear").defaultField("message"), cols, TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches).scoring().across());
        // An ascending score sort reads it unscored.
        assertNull(one.ascending().scoring().across());
    }

    public void testTheWordAfterAnOrIsAClauseOfItsOwn() {
        // Stock: jackal OR bear owl cat is jackal | bear | (owl cat); a run starts only at the word after the OR's.
        Map<String, Column> cols = columns();
        cols.put("title", new Column("title", "str", true, false));
        InfinoQueryTranslator.Across across = InfinoQueryTranslator.translate(
            QueryBuilders.queryStringQuery("jackal OR bear owl cat").field("message").field("title"),
            cols,
            TABLE,
            NOW
        , InfinoQueryTranslatorTests::aRowMatches).scoring().across();
        assertNotNull(across);
        assertEquals(
            List.of(List.of("jackal"), List.of("bear"), List.of("owl cat")),
            across.clauses().stream().map(c -> c.stream().map(InfinoQueryTranslator.TextClause::query).distinct().toList()).toList()
        );
        for (List<InfinoQueryTranslator.TextClause> clause : across.clauses()) {
            assertEquals(List.of("message", "title"), clause.stream().map(InfinoQueryTranslator.TextClause::column).toList());
        }
        // A field:word after an OR is its own clause, and the word after it may start a run.
        InfinoQueryTranslator.Across fielded = InfinoQueryTranslator.translate(
            QueryBuilders.queryStringQuery("jackal OR message:bear owl cat").field("message").field("title"),
            cols,
            TABLE,
            NOW
        , InfinoQueryTranslatorTests::aRowMatches).scoring().across();
        assertNotNull(fielded);
        assertEquals(
            List.of(List.of("jackal"), List.of("bear"), List.of("owl cat")),
            fielded.clauses().stream().map(c -> c.stream().map(InfinoQueryTranslator.TextClause::query).distinct().toList()).toList()
        );
        assertEquals(List.of(new InfinoQueryTranslator.TextClause("message", "bear", "or")), fielded.clauses().get(1));
        assertEquals(
            List.of(new InfinoQueryTranslator.TextClause("message", "owl cat", "or"), new InfinoQueryTranslator.TextClause("title", "owl cat", "or")),
            fielded.clauses().get(2)
        );
    }

    public void testAFieldTheMappingDoesNotHaveAddsNothing() {
        // Stock's dis_max has no clause for an unmapped field: the others still rank the page.
        Map<String, Column> cols = columns();
        cols.put("title", new Column("title", "str", true, false));
        InfinoQueryTranslator.Across across = InfinoQueryTranslator.translate(
            QueryBuilders.queryStringQuery("jackal bear").field("message").field("title").field("nosuch"),
            cols,
            TABLE,
            NOW
        , InfinoQueryTranslatorTests::aRowMatches).scoring().across();
        assertNotNull(across);
        assertEquals(
            List.of(List.of(new InfinoQueryTranslator.TextClause("message", "jackal bear", "or"), new InfinoQueryTranslator.TextClause("title", "jackal bear", "or"))),
            across.clauses()
        );
        // Nor does a field:word on such a field: it matches nothing, and the other clauses rank.
        InfinoQueryTranslator.Across fielded = InfinoQueryTranslator.translate(
            QueryBuilders.queryStringQuery("jackal OR nosuch:bear owl").field("message").field("title"),
            cols,
            TABLE,
            NOW
        , InfinoQueryTranslatorTests::aRowMatches).scoring().across();
        assertNotNull(fielded);
        assertEquals(
            List.of(List.of("jackal"), List.of("owl")),
            fielded.clauses().stream().map(c -> c.stream().map(InfinoQueryTranslator.TextClause::query).distinct().toList()).toList()
        );
        // A mapped column with no text index still leaves it unranked.
        QueryStringQueryBuilder keyword = QueryBuilders.queryStringQuery("jackal bear").field("message").field("title").field("level");
        assertNull(InfinoQueryTranslator.translate(keyword, cols, TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches).scoring().across());
        // A field pattern is the fields it matches, as stock expands it over the mapping: here the two text columns.
        assertEquals(
            InfinoQueryTranslator.translate(QueryBuilders.queryStringQuery("jackal bear").field("title").field("message"), cols, TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches).scoring().across(),
            InfinoQueryTranslator.translate(QueryBuilders.queryStringQuery("jackal bear").field("title").field("mess*"), cols, TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches).scoring().across()
        );
    }

    private static Translation translateWithTitle(org.opensearch.index.query.QueryBuilder qb) {
        Map<String, Column> cols = columns();
        cols.put("title", new Column("title", "str", true, false));
        return InfinoQueryTranslator.translate(qb, cols, TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches);
    }

    private static final String RANGE_FILTER = "WHERE (\"@timestamp\" >= '2023-01-03T00:00:00Z') AND (\"@timestamp\" < '2023-01-03T10:00:00Z')";

    /**
     * A query string over several text columns beside a filter is counted from
     * the token index: per column one any-word match (a one-word all-words
     * clause joins it) under the filter, each later one leaving out the rows an
     * earlier one holds, so a row holding words in several columns counts once,
     * and the cap stops the count.
     */
    public void testTextOrIsCountedFromTheTokenIndex() {
        BoolQueryBuilder b = QueryBuilders.boolQuery()
            .must(QueryBuilders.rangeQuery("@timestamp").gte("2023-01-03T00:00:00Z").lt("2023-01-03T10:00:00Z"))
            .must(QueryBuilders.queryStringQuery("message: monkey jackal bear").field("message").field("title"));
        Translation t = translateWithTitle(b);
        assertNull(t.principal());
        assertEquals(
            "SELECT COUNT(*) AS c FROM (SELECT one FROM ("
                + "SELECT 1 AS one FROM token_match('os_logs_0', 'message', 'monkey jackal bear', 'or') " + RANGE_FILTER
                + " UNION ALL SELECT 1 AS one FROM token_match('os_logs_0', 'title', 'jackal bear', 'or') " + RANGE_FILTER
                + " AND NOT COALESCE((text_match(\"message\", 'monkey jackal bear', 'or')), FALSE)"
                + ") matched LIMIT 10001) capped",
            InfinoQueryTranslator.textCountStatement(TABLE, t.where(), 10001)
        );
        // With no filter a part has only the earlier parts to leave out.
        Translation bare = translateWithTitle(QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("message", "disk")).should(QueryBuilders.matchQuery("title", "disk")));
        assertEquals(
            "SELECT COUNT(*) AS c FROM (SELECT one FROM ("
                + "SELECT 1 AS one FROM token_match('os_logs_0', 'message', 'disk', 'or')"
                + " UNION ALL SELECT 1 AS one FROM token_match('os_logs_0', 'title', 'disk', 'or') WHERE NOT COALESCE((text_match(\"message\", 'disk', 'or')), FALSE)"
                + ") matched LIMIT 3) capped",
            InfinoQueryTranslator.textCountStatement(TABLE, bare.where(), 3)
        );
    }

    /** An all-words clause of several words keeps its own part; one column's one clause needs no union. */
    public void testAllWordsClauseKeepsItsOwnMatch() {
        Translation t = translateWithTitle(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("level", "error"))
                .should(QueryBuilders.matchQuery("message", "disk full").operator(Operator.AND))
                .should(QueryBuilders.matchQuery("message", "cache"))
                .minimumShouldMatch(1)
        );
        assertEquals(
            "SELECT COUNT(*) AS c FROM (SELECT one FROM ("
                + "SELECT 1 AS one FROM token_match('os_logs_0', 'message', 'cache', 'or') WHERE (\"level\" = 'error')"
                + " UNION ALL SELECT 1 AS one FROM token_match('os_logs_0', 'message', 'disk full', 'and') WHERE (\"level\" = 'error')"
                + " AND NOT COALESCE((text_match(\"message\", 'cache', 'or')), FALSE)"
                + ") matched LIMIT 11) capped",
            InfinoQueryTranslator.textCountStatement(TABLE, t.where(), 11)
        );
        Translation one = translateWithTitle(
            QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("message", "disk")).should(QueryBuilders.termQuery("level", "error"))
        );
        // An OR mixing text with a filter is no text OR: the scan counts it.
        assertNull(InfinoQueryTranslator.textCountStatement(TABLE, one.where(), 11));
        Translation single = translateWithTitle(
            QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("level", "error")).should(QueryBuilders.matchQuery("message", "disk cache")).minimumShouldMatch(1)
        );
        assertEquals(
            "SELECT COUNT(*) AS c FROM (SELECT 1 AS one FROM token_match('os_logs_0', 'message', 'disk cache', 'or') WHERE (\"level\" = 'error') LIMIT 11) capped",
            InfinoQueryTranslator.textCountStatement(TABLE, single.where(), 11)
        );
    }

    /** Anything but filters AND one OR of plain-words text_match calls keeps the scan. */
    public void testOtherTextPredicatesKeepTheScan() {
        // A phrase reads the token index through a subquery.
        assertNull(count(translateWithTitle(QueryBuilders.boolQuery().should(QueryBuilders.matchPhraseQuery("message", "disk full")).should(QueryBuilders.matchQuery("title", "x")))));
        // An excluded text clause.
        assertNull(
            count(
                translateWithTitle(
                    QueryBuilders.boolQuery()
                        .filter(QueryBuilders.termQuery("level", "error"))
                        .mustNot(QueryBuilders.matchQuery("message", "disk"))
                        .should(QueryBuilders.matchQuery("title", "x"))
                        .minimumShouldMatch(1)
                )
            )
        );
        // Two text clauses both required: an AND of text, not one OR.
        assertNull(
            count(
                translateWithTitle(
                    QueryBuilders.boolQuery()
                        .filter(QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("message", "a")).should(QueryBuilders.matchQuery("title", "a")))
                        .filter(QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("message", "b")).should(QueryBuilders.matchQuery("title", "b")))
                )
            )
        );
        // Statements the translator does not write, read conservatively.
        String tm = "text_match(\"message\", 'a', 'or')";
        assertNull(InfinoQueryTranslator.textCountStatement(TABLE, "(\"status\" BETWEEN 1 AND 2) AND " + tm, 5));
        assertNull(InfinoQueryTranslator.textCountStatement(TABLE, "\"status\" = 1 OR \"status\" = 2 AND " + tm, 5));
        assertNull(InfinoQueryTranslator.textCountStatement(TABLE, "(" + tm + " AND text_match(\"title\", 'b', 'or'))", 5));
        assertNull(InfinoQueryTranslator.textCountStatement(TABLE, "NOT COALESCE((" + tm + "), FALSE)", 5));
        assertNull(InfinoQueryTranslator.textCountStatement(TABLE, "(" + tm + " OR text_match(\"title\", '+b', 'or'))", 5));
        assertNull(InfinoQueryTranslator.textCountStatement(TABLE, "(" + tm, 5));
        assertNull(InfinoQueryTranslator.textCountStatement(TABLE, "\"level\" = 'error'", 5));
        // Quotes hide what is inside them: an AND in a literal or a doubled quote is text.
        assertEquals(
            "SELECT COUNT(*) AS c FROM (SELECT 1 AS one FROM token_match('os_logs_0', 'me\"ss', 'it''s', 'or') WHERE (\"level\" = 'x AND y') LIMIT 5) capped",
            InfinoQueryTranslator.textCountStatement(TABLE, "(\"level\" = 'x AND y' AND text_match(\"me\"\"ss\", 'it''s', 'or'))", 5)
        );
    }

    private static String count(Translation t) {
        return InfinoQueryTranslator.textCountStatement(TABLE, t.where(), 10001);
    }

    public void testTextUnderShouldIsARowPredicate() {
        BoolQueryBuilder b = QueryBuilders.boolQuery()
            .should(QueryBuilders.matchQuery("message", "disk"))
            .should(QueryBuilders.termQuery("level", "error"));
        Translation t = translate(b);
        assertNull(t.principal());
        assertEquals("\"os_logs_0\"", t.from());
        assertEquals("((text_match(\"message\", 'disk', 'or') OR \"level\" = 'error'))", t.where());
        assertTrue(InfinoQueryTranslator.readsTextIndex(t.where()));
    }

    /**
     * Only plain words become text_match: a phrase or a clause the engine
     * reads with a + or - sigil keeps the token_match subquery, which reads them.
     */
    /**
     * A query string's optional words that all reach one text column are that
     * column matching any of them: one clause, the ranked source of the page,
     * not a clause per word. Words that reach another column, a phrase, or a
     * required clause keep the clauses apart.
     */
    public void testOptionalWordsOnOneTextColumnAreOneRankedClause() {
        Translation t = translate(QueryBuilders.queryStringQuery("message: monkey jackal bear").defaultField("message"));
        assertEquals("token_match('os_logs_0', 'message', 'monkey jackal bear', 'or')", t.from());
        assertEquals(InfinoQueryTranslator.ALL, t.where());
        Translation phrase = translate(QueryBuilders.queryStringQuery("message: monkey \"jackal bear\"").defaultField("message"));
        assertFalse(phrase.from(), phrase.from().contains("'monkey jackal bear'"));
        Translation required = translate(QueryBuilders.queryStringQuery("message:monkey AND jackal").defaultField("message"));
        assertFalse(required.from(), required.from().contains("'monkey jackal'"));
    }

    /**
     * A word the analyzer splits ({@code disk-full}) is a clause needing all
     * its tokens, so it stays apart; one-token words beside it still merge.
     */
    public void testAWordOfSeveralTokensKeepsItsClause() {
        Translation merged = translate(
            QueryBuilders.queryStringQuery("message:disk OR message:cache").defaultOperator(org.opensearch.index.query.Operator.AND)
        );
        assertEquals("token_match('os_logs_0', 'message', 'disk cache', 'or')", merged.from());
        Translation split = translate(
            QueryBuilders.queryStringQuery("message:disk-full OR message:cache").defaultOperator(org.opensearch.index.query.Operator.AND)
        );
        assertFalse(split.from() + " " + split.where(), (split.from() + split.where()).contains("'disk full cache'"));
        assertFalse(split.from() + " " + split.where(), (split.from() + split.where()).contains("'disk-full cache'"));
        assertTrue(InfinoQueryTranslator.oneToken("disk"));
        assertTrue(InfinoQueryTranslator.oneToken("e2e"));
        assertFalse(InfinoQueryTranslator.oneToken("disk-full"));
        assertFalse(InfinoQueryTranslator.oneToken("disk.full,"));
        assertFalse(InfinoQueryTranslator.oneToken("di*"));
        assertFalse(InfinoQueryTranslator.oneToken("日本"));
        assertFalse(InfinoQueryTranslator.oneToken(""));
    }

    public void testAWordPastTheTokenLengthKeepsItsClause() {
        String atBound = "a".repeat(254) + "b";
        String pastBound = "a".repeat(255) + "b";
        assertTrue(InfinoQueryTranslator.oneToken(atBound));
        assertFalse(InfinoQueryTranslator.oneToken(pastBound));
        Translation merged = translate(
            QueryBuilders.queryStringQuery("message:" + atBound + " OR message:cache").defaultOperator(org.opensearch.index.query.Operator.AND)
        );
        assertEquals("token_match('os_logs_0', 'message', '" + atBound + " cache', 'or')", merged.from());
        Translation split = translate(
            QueryBuilders.queryStringQuery("message:" + pastBound + " OR message:cache").defaultOperator(org.opensearch.index.query.Operator.AND)
        );
        assertFalse(split.from() + " " + split.where(), (split.from() + split.where()).contains(pastBound + " cache"));
    }

    /**
     * Only ASCII letters and digits merge. The engine splits Thai into a token
     * per character ({@code ไทย} is three), as it does Han and Hiragana, so
     * such a word keeps the clause that requires all its tokens; other
     * non-ASCII words keep theirs too.
     */
    public void testANonAsciiWordKeepsItsClause() {
        assertFalse(InfinoQueryTranslator.oneToken("ไทย"));
        assertFalse(InfinoQueryTranslator.oneToken("ພາສາ"));
        assertFalse(InfinoQueryTranslator.oneToken("ひらがな"));
        assertFalse(InfinoQueryTranslator.oneToken("カタカナ"));
        assertFalse(InfinoQueryTranslator.oneToken("café"));
        assertFalse(InfinoQueryTranslator.oneToken("İstanbul"));
        assertFalse(InfinoQueryTranslator.oneToken("ｄｉｓｋ"));
        assertFalse(InfinoQueryTranslator.oneToken("disk_full"));
        assertTrue(InfinoQueryTranslator.oneToken("Disk42"));
        Translation split = translate(
            QueryBuilders.queryStringQuery("message:ไทย OR message:cache").defaultOperator(org.opensearch.index.query.Operator.AND)
        );
        assertFalse(split.from() + " " + split.where(), (split.from() + split.where()).contains("ไทย cache"));
    }

    public void testAPhraseOrASigilKeepsTheSubquery() {
        assertTrue(InfinoQueryTranslator.plainQuery("disk full"));
        assertFalse(InfinoQueryTranslator.plainQuery("\"disk full\""));
        assertFalse(InfinoQueryTranslator.plainQuery("disk -full"));
        assertFalse(InfinoQueryTranslator.plainQuery("+disk cache"));
        String where = translate(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.matchPhraseQuery("message", "disk full"))
                .should(QueryBuilders.termQuery("level", "error"))
        ).where();
        assertTrue(where, where.contains("osid IN (SELECT osid FROM token_match('os_logs_0', 'message', '\"disk full\"', 'and'))"));
    }

    public void testShouldIsOptionalBesideMust() {
        BoolQueryBuilder b = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("level", "error"))
            .should(QueryBuilders.termQuery("status", 500));
        assertEquals("(\"level\" = 'error')", translate(b).where());
        b.minimumShouldMatch(1);
        assertEquals("(\"level\" = 'error' AND (\"status\" = 500))", translate(b).where());
    }

    public void testQueryStringReadsMinimumShouldMatch() {
        // Two of three optional words: the engine query still ranks, and a count narrows it.
        Translation ranked = translate(QueryBuilders.queryStringQuery("alpha beta gamma").defaultField("message").minimumShouldMatch("2"));
        assertEquals(new InfinoQueryTranslator.TextClause("message", "alpha beta gamma", "or"), ranked.principal());
        String two = ranked.where();
        assertTrue(two, two.contains(">= 2)"));
        assertEquals(3, two.split("THEN 1 ELSE 0 END", -1).length - 1);
        // One of the optional words alone asks nothing more than the list: the plain translation, ranked, no count.
        Translation one = translate(QueryBuilders.queryStringQuery("alpha beta gamma").defaultField("message").minimumShouldMatch("1"));
        Translation plain = translate(QueryBuilders.queryStringQuery("alpha beta gamma").defaultField("message"));
        assertEquals(plain.where(), one.where());
        assertEquals(plain.principal(), one.principal());
        // More than there are matches nothing, as stock does; exactly as many is a count of all three.
        assertEquals("1=0", translate(QueryBuilders.queryStringQuery("alpha beta gamma").defaultField("message").minimumShouldMatch("4")).where());
        assertEquals("1=0", translate(QueryBuilders.queryStringQuery("alpha beta gamma").defaultField("message").minimumShouldMatch("200%")).where());
        assertEquals("1=0", translate(QueryBuilders.queryStringQuery("+alpha beta gamma").defaultField("message").minimumShouldMatch("3")).where());
        assertTrue(translate(QueryBuilders.queryStringQuery("alpha beta gamma").defaultField("message").minimumShouldMatch("3")).where().contains(">= 3)"));
        assertTrue(translate(QueryBuilders.queryStringQuery("+alpha beta gamma").defaultField("message").minimumShouldMatch("2")).where().contains(">= 2)"));
        // A malformed form is the request's error, reported as stock reports a query it cannot build.
        for (String bad : new String[] { "2<", "two" }) {
            QueryShardException e = expectThrows(
                QueryShardException.class,
                () -> translate(QueryBuilders.queryStringQuery("alpha beta gamma").defaultField("message").minimumShouldMatch(bad))
            );
            assertEquals(RestStatus.BAD_REQUEST, e.status());
            assertTrue(e.getMessage(), e.getMessage().startsWith("failed to create query: "));
        }
        // Stock's reading of the forms: 100% of three is three, -1 of three is two.
        assertTrue(translate(QueryBuilders.queryStringQuery("alpha beta gamma").defaultField("message").minimumShouldMatch("100%")).where().contains(">= 3)"));
        assertTrue(translate(QueryBuilders.queryStringQuery("alpha beta gamma").defaultField("message").minimumShouldMatch("-1")).where().contains(">= 2)"));
        // One is a plain OR over the optional words, ANDed with the required one.
        String required = translate(QueryBuilders.queryStringQuery("+alpha beta gamma").defaultField("message").minimumShouldMatch("1")).where();
        assertTrue(required, required.contains(" AND ("));
        // Without it, nothing changes; and simple_query_string reads it too.
        String none = translate(QueryBuilders.queryStringQuery("alpha beta gamma").defaultField("message")).where();
        assertFalse(none, none.contains("THEN 1 ELSE 0 END"));
        assertTrue(translate(QueryBuilders.simpleQueryStringQuery("alpha beta gamma").field("message").minimumShouldMatch("2")).where().contains(">= 2)"));
    }

    public void testQueryStringMinimumShouldMatchWithNoOptionalClause() {
        // Stock applies the value to a clause list with no optional clause too, and it then matches nothing.
        assertEquals(
            "1=0",
            translate(QueryBuilders.queryStringQuery("alpha beta gamma").defaultField("message").defaultOperator(Operator.AND).minimumShouldMatch("2"))
                .where()
        );
        assertEquals("1=0", translate(QueryBuilders.queryStringQuery("-delta").defaultField("message").minimumShouldMatch("1")).where());
        // A lone clause with + is such a list; the same clause without it is no list at all.
        assertEquals("1=0", translate(QueryBuilders.queryStringQuery("+alpha").defaultField("message").minimumShouldMatch("1")).where());
        assertEquals("1=0", translate(QueryBuilders.queryStringQuery("+(alpha beta)").defaultField("message").minimumShouldMatch("1")).where());
        Translation lone = translate(QueryBuilders.queryStringQuery("alpha").defaultField("message"));
        Translation loneAsked = translate(QueryBuilders.queryStringQuery("alpha").defaultField("message").minimumShouldMatch("2"));
        assertEquals(lone.where(), loneAsked.where());
        assertEquals(lone.principal(), loneAsked.principal());
        // A value asking for none changes nothing.
        Translation required = translate(QueryBuilders.queryStringQuery("+alpha").defaultField("message"));
        assertEquals(required.where(), translate(QueryBuilders.queryStringQuery("+alpha").defaultField("message").minimumShouldMatch("-1")).where());
        // Stock builds every clause before it counts: a numeric clause the column cannot read is still refused.
        assertNull(translate(QueryBuilders.queryStringQuery("status:abc message:alpha message:beta").defaultField("message").minimumShouldMatch("4")));
    }

    public void testQueryStringMinimumShouldMatchSkipsBlankPhrasesOnText() {
        // Stock's analysis drops "" and " " on a text field: 75% of two optional clauses is one.
        String blank = translate(QueryBuilders.queryStringQuery("alpha \"\" beta").defaultField("message").minimumShouldMatch("75%")).where();
        assertEquals(translate(QueryBuilders.queryStringQuery("alpha \"\" beta").defaultField("message")).where(), blank);
        assertEquals(
            blank,
            translate(QueryBuilders.queryStringQuery("alpha \" \" beta").defaultField("message").minimumShouldMatch("75%")).where()
        );
        assertEquals(blank, translate(QueryBuilders.simpleQueryStringQuery("alpha \"\" beta").field("message").minimumShouldMatch("75%")).where());
        assertTrue(
            translate(QueryBuilders.queryStringQuery("alpha \"\" beta gamma").defaultField("message").minimumShouldMatch("3")).where()
                .contains(">= 3)")
        );
        // On a keyword column the empty value is a clause: 75% of three is two.
        assertTrue(
            translate(QueryBuilders.queryStringQuery("level:alpha level:\"\" level:beta").minimumShouldMatch("75%")).where().contains(">= 2)")
        );
    }

    public void testQueryStringMinimumShouldMatchOnAKeywordDefaultField() {
        // A keyword column keeps foo-bar whole: one exact value, a lone term, no list.
        assertEquals(
            translate(QueryBuilders.queryStringQuery("level:foo-bar")).where(),
            translate(QueryBuilders.queryStringQuery("foo-bar").defaultField("level").minimumShouldMatch("2")).where()
        );
        // Beside another clause it is one counted clause.
        String two = translate(QueryBuilders.queryStringQuery("foo-bar message:alpha").defaultField("level").minimumShouldMatch("2")).where();
        assertTrue(two, two.contains("\"level\" = 'foo-bar'"));
        assertTrue(two, two.contains(">= 2)"));
        // A run of words is one value there for stock: refused.
        IllegalArgumentException run = expectThrows(
            IllegalArgumentException.class,
            () -> translate(QueryBuilders.queryStringQuery("foo bar").defaultField("level").minimumShouldMatch("2"))
        );
        assertTrue(run.getMessage(), run.getMessage().contains("keyword fields only"));
        // Next to a text column it may still be split: refused as before.
        expectThrows(
            IllegalArgumentException.class,
            () -> translate(QueryBuilders.queryStringQuery("foo-bar").field("message").field("level").minimumShouldMatch("2"))
        );
    }

    public void testAShortPageRankedInAWindowIsRankedAgain() {
        // A count over the optional clauses removes rows after the ranking: a short page may have used up the window.
        Translation counted = translate(QueryBuilders.queryStringQuery("alpha beta gamma").defaultField("message").minimumShouldMatch("2"));
        InfinoSearch.Plan plan = new InfinoSearch.Plan(
            TABLE,
            columns(),
            counted,
            java.util.List.of(),
            null,
            null,
            null,
            null,
            java.util.List.of(),
            InfinoAccess.NONE,
            NOW
        );
        assertTrue(InfinoSearch.rankedInWindow(plan));
        String wider = InfinoSearch.hitStatement(plan, 10, false, InfinoSearch.Keyset.WHOLE, 50_000);
        assertTrue(wider, wider.contains(", 50000, 'or')"));
        assertTrue(InfinoSearch.hitStatement(plan, 10, false).contains(", 10000, 'or')"));
        // A filter the engine applies while it ranks leaves no window to run out.
        Translation plain = translate(QueryBuilders.queryStringQuery("alpha beta gamma").defaultField("message"));
        assertFalse(
            InfinoSearch.rankedInWindow(
                new InfinoSearch.Plan(TABLE, columns(), plain, java.util.List.of(), null, null, null, null, java.util.List.of(), InfinoAccess.NONE, NOW)
            )
        );
    }

    public void testQueryStringMinimumShouldMatchRefusedWhereStockCountsOtherClauses() {
        // Over several fields stock reads a run of words as one dis_max clause.
        IllegalArgumentException fields = expectThrows(
            IllegalArgumentException.class,
            () -> translate(QueryBuilders.queryStringQuery("alpha beta").field("message").field("level").minimumShouldMatch("2"))
        );
        assertTrue(fields.getMessage(), fields.getMessage().contains("more than one field"));
        // Each word with its field is a clause of its own, as in stock.
        assertTrue(
            translate(QueryBuilders.queryStringQuery("message:alpha message:beta").field("message").field("level").minimumShouldMatch("2"))
                .where()
                .contains(">= 2)")
        );
        // A word the analyzer may split is one clause or several in stock, by where it sits.
        IllegalArgumentException lone = expectThrows(
            IllegalArgumentException.class,
            () -> translate(QueryBuilders.queryStringQuery("foo-bar").defaultField("message").minimumShouldMatch("2"))
        );
        assertTrue(lone.getMessage(), lone.getMessage().contains("[foo-bar]"));
        expectThrows(
            IllegalArgumentException.class,
            () -> translate(QueryBuilders.queryStringQuery("alpha foo-bar").defaultField("message").minimumShouldMatch("2"))
        );
        // Quoted, it is one phrase clause in both.
        assertTrue(
            translate(QueryBuilders.queryStringQuery("alpha \"foo-bar\"").defaultField("message").minimumShouldMatch("2")).where().contains(">= 2)")
        );
        // simple_query_string's own syntax makes other clauses of an operator: there -gamma is optional.
        IllegalArgumentException simple = expectThrows(
            IllegalArgumentException.class,
            () -> translate(QueryBuilders.simpleQueryStringQuery("alpha beta -gamma").field("message").minimumShouldMatch("2"))
        );
        assertTrue(simple.getMessage(), simple.getMessage().contains("[-gamma]"));
        // Inside a phrase a word is no operator in either syntax; after it, ~ still is.
        assertNotNull(translate(QueryBuilders.simpleQueryStringQuery("\"alpha OR beta\"").field("message").minimumShouldMatch("2")));
        assertTrue(
            translate(QueryBuilders.simpleQueryStringQuery("gamma \"alpha -beta\"").field("message").minimumShouldMatch("2"))
                .where()
                .contains(">= 2)")
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> translate(QueryBuilders.simpleQueryStringQuery("alpha OR beta").field("message").minimumShouldMatch("2"))
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> translate(QueryBuilders.simpleQueryStringQuery("\"alpha beta\"~2 gamma").field("message").minimumShouldMatch("2"))
        );
        // An operator touching a phrase's quote is still a word of its own.
        IllegalArgumentException touching = expectThrows(
            IllegalArgumentException.class,
            () -> translate(QueryBuilders.simpleQueryStringQuery("\"alpha\"OR beta").field("message").minimumShouldMatch("2"))
        );
        assertTrue(touching.getMessage(), touching.getMessage().contains("[OR]"));
        assertNotNull(translate(QueryBuilders.simpleQueryStringQuery("\"alpha\"beta gamma").field("message").minimumShouldMatch("2")));
        // The standard tokenizer splits a word where its script changes.
        expectThrows(
            IllegalArgumentException.class,
            () -> translate(QueryBuilders.queryStringQuery("alpha ไทยabc beta").defaultField("message").minimumShouldMatch("3"))
        );
        assertTrue(
            translate(QueryBuilders.queryStringQuery("alpha ไทย beta").defaultField("message").minimumShouldMatch("3")).where().contains(">= 3)")
        );
        assertTrue(
            translate(QueryBuilders.queryStringQuery("alpha abc123 beta").defaultField("message").minimumShouldMatch("3")).where().contains(">= 3)")
        );
        // The standard tokenizer splits a word longer than 255 characters there.
        assertTrue(
            translate(QueryBuilders.queryStringQuery("alpha " + "a".repeat(255) + " beta").defaultField("message").minimumShouldMatch("3"))
                .where()
                .contains(">= 3)")
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> translate(QueryBuilders.queryStringQuery("alpha " + "a".repeat(256) + " beta").defaultField("message").minimumShouldMatch("3"))
        );
        // Without minimum_should_match, nothing here is refused.
        assertNotNull(translate(QueryBuilders.queryStringQuery("alpha beta").field("message").field("level")));
        assertNotNull(translate(QueryBuilders.queryStringQuery("foo-bar").defaultField("message")));
    }

    /** What a word typed into Discover's search bar searches: every field, each as stock reads the word on it. */
    private static final String DISK_ON_EVERY_FIELD = "(\"level\" = 'disk' OR \"host.name\" = 'disk' OR text_match(\"message\", 'disk', 'or'))";

    /**
     * A multi_match or a query string naming no field searches the index's
     * default fields, every field unless index.query.default_field says
     * otherwise, as stock does: a text field by its words, a keyword field by
     * its whole value, and a number or date field by the value read as its
     * type, skipped where it cannot be (lenient, which a query over every
     * field is unless it says not). Before, a multi_match naming no field
     * matched nothing and a query string searched the text fields alone.
     */
    public void testAQueryNamingNoFieldSearchesEveryField() {
        assertEquals("1=1", translate(QueryBuilders.queryStringQuery("*")).where());
        // Discover's search bar: a multi_match naming no field.
        assertEquals(DISK_ON_EVERY_FIELD, translate(QueryBuilders.multiMatchQuery("disk").lenient(true)).where());
        assertEquals(DISK_ON_EVERY_FIELD, translate(QueryBuilders.multiMatchQuery("disk")).where());
        assertEquals(DISK_ON_EVERY_FIELD, translate(QueryBuilders.multiMatchQuery("disk", "*")).where());
        assertEquals(DISK_ON_EVERY_FIELD, translate(QueryBuilders.queryStringQuery("disk")).where());
        assertEquals(DISK_ON_EVERY_FIELD, translate(QueryBuilders.queryStringQuery("disk").defaultField("*")).where());
        assertEquals(DISK_ON_EVERY_FIELD, translate(QueryBuilders.simpleQueryStringQuery("disk")).where());
        // As Discover wraps it, beside its time filter.
        assertEquals(
            "(" + DISK_ON_EVERY_FIELD + " AND (\"@timestamp\" >= '2026-10-24T00:00:00Z'))",
            translate(
                QueryBuilders.boolQuery()
                    .must(QueryBuilders.multiMatchQuery("disk").type(MultiMatchQueryBuilder.Type.BEST_FIELDS).lenient(true))
                    .filter(QueryBuilders.rangeQuery("@timestamp").gte("2026-10-24T00:00:00Z"))
            ).where()
        );
        // A number is a value of a number field too, and a date of a date field: the whole day it names.
        assertEquals(
            "((\"@timestamp\" >= '1970-01-01T00:00:00.500Z' AND \"@timestamp\" <= '1970-01-01T00:00:00.500Z') OR \"level\" = '500' OR \"status\" = 500 "
                + "OR \"host.name\" = '500' OR text_match(\"message\", '500', 'or'))",
            translate(QueryBuilders.multiMatchQuery("500")).where()
        );
        assertEquals(
            "((\"@timestamp\" >= '2026-10-24T00:00:00Z' AND \"@timestamp\" <= '2026-10-24T23:59:59.999Z') OR \"level\" = '2026-10-24' "
                + "OR \"host.name\" = '2026-10-24' OR text_match(\"message\", '2026-10-24', 'or'))",
            translate(QueryBuilders.queryStringQuery("2026-10-24")).where()
        );
        // Not lenient, a field the word is no value of fails the request, as stock's does.
        IllegalArgumentException strict = expectThrows(IllegalArgumentException.class, () -> translate(QueryBuilders.multiMatchQuery("disk").lenient(false)));
        assertTrue(strict.getMessage(), strict.getMessage().contains("is no value of the field"));
        expectThrows(IllegalArgumentException.class, () -> translate(QueryBuilders.multiMatchQuery("disk", "status")));
        assertEquals("1=0", translate(QueryBuilders.multiMatchQuery("disk", "status").lenient(true)).where());
        expectThrows(IllegalArgumentException.class, () -> translate(QueryBuilders.queryStringQuery("disk").defaultField("*").lenient(false)));
        // A pattern is the fields it matches.
        assertEquals("\"host.name\" = 'disk'", translate(QueryBuilders.multiMatchQuery("disk", "host.*")).where());
        assertEquals("token_match('os_logs_0', 'message', 'disk', 'or')", translate(QueryBuilders.multiMatchQuery("disk", "mess*")).from());
        // Stock refuses a query string naming both.
        IllegalArgumentException both = expectThrows(
            IllegalArgumentException.class,
            () -> translate(QueryBuilders.queryStringQuery("disk").field("message").defaultField("message"))
        );
        assertEquals("cannot use [fields] parameter in conjunction with [default_field]", both.getMessage());
    }

    /**
     * A keyword multi-field is a field of its own, its whole value; the
     * table's own columns, a list column, a vector field and an alias's
     * second name are not.
     */
    public void testEveryFieldIsEachFieldOnce() {
        Map<String, Column> cols = columns();
        cols.put("message.keyword", cols.get("message").exact());
        cols.put("msg", cols.get("message"));
        cols.put("tags[]", Column.list("tags[]", "str"));
        cols.put("embedding", new Column("embedding", "vec", false, true));
        cols.put("source", new Column("source", "str", false, false));
        // A metadata field (its name starts with _): * leaves it out, as stock does.
        cols.put("_note", new Column("_note", "str", false, false));
        assertEquals(
            "(\"level\" = 'disk' OR \"host.name\" = 'disk' OR \"message\" = 'disk' OR text_match(\"message\", 'disk', 'or'))",
            InfinoQueryTranslator.translate(QueryBuilders.multiMatchQuery("disk"), cols, TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches).where()
        );
        // A pattern that names it reaches it.
        assertEquals(
            "\"_note\" = 'disk'",
            InfinoQueryTranslator.translate(QueryBuilders.multiMatchQuery("disk", "_n*"), cols, TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches).where()
        );
    }

    /**
     * A wildcard or regular expression a query string's {@code fields} name a
     * keyword field for matches its whole value, as the same pattern written
     * {@code field:pattern} does (Dashboards sends {@code host:pay*} that
     * way); before, it matched nothing. On a number field stock reads no
     * pattern: refused, or skipped when lenient.
     */
    public void testAPatternOnAKeywordFieldMatchesItsValue() {
        assertEquals("\"host.name\" LIKE 'n%'", translate(QueryBuilders.queryStringQuery("n*").field("host.name")).where());
        assertEquals(
            translate(QueryBuilders.queryStringQuery("host.name:n*")).where(),
            translate(QueryBuilders.queryStringQuery("n*").field("host.name")).where()
        );
        assertEquals("regexp_like(\"host.name\", '^(?:n.*)$')", translate(QueryBuilders.queryStringQuery("/n.*/").field("host.name")).where());
        assertEquals(
            "(\"level\" LIKE 'n%' OR regexp_like(\"message\", '\\bn[^\\s]*', 'i') OR \"host.name\" LIKE 'n%')",
            translate(QueryBuilders.queryStringQuery("n*")).where()
        );
        IllegalArgumentException number = expectThrows(IllegalArgumentException.class, () -> translate(QueryBuilders.queryStringQuery("5*").field("status")));
        assertTrue(number.getMessage(), number.getMessage().contains("not a text or keyword field"));
        assertEquals("1=0", translate(QueryBuilders.queryStringQuery("5*").field("status").lenient(true)).where());
    }

    /**
     * Stock's query string reads a run of words with no operator between them
     * as one clause, one value on a keyword field: beside a keyword field the
     * tree keeps the run as one term, alone or beside other clauses. A word
     * right before OR ends a run, so {@code disk full OR x} is three clauses;
     * simple_query_string has no runs.
     */
    public void testARunOfWordsOverKeywordFields() {
        assertEquals(
            "(\"level\" = 'disk full' OR \"host.name\" = 'disk full' OR text_match(\"message\", 'disk full', 'or'))",
            translate(QueryBuilders.queryStringQuery("disk full")).where()
        );
        assertEquals(
            translate(QueryBuilders.multiMatchQuery("disk full").operator(Operator.AND)).where(),
            translate(QueryBuilders.queryStringQuery("disk full").defaultOperator(Operator.AND)).where()
        );
        // The run is one value beside the clauses around it: error before OR is a clause of its own.
        String beside = translate(QueryBuilders.queryStringQuery("disk full error OR x")).where();
        assertTrue(beside, beside.contains("\"level\" = 'disk full'") && beside.contains("\"level\" = 'error'") && beside.contains("\"level\" = 'x'"));
        assertFalse(beside, beside.contains("\"level\" = 'disk'"));
        String field = translate(QueryBuilders.queryStringQuery("level:error disk full")).where();
        assertTrue(field, field.contains("\"level\" = 'error'") && field.contains("\"host.name\" = 'disk full'"));
        assertTrue(field, field.contains("text_match(\"message\", 'disk full', 'or')"));
        // On a named field a run is that field's one value, or a text field's words.
        String named = translate(QueryBuilders.queryStringQuery("level:(disk full) OR message:(disk full)")).where();
        assertTrue(named, named.contains("\"level\" = 'disk full'") && named.contains("text_match(\"message\", 'disk full', 'or')"));
        String each = translate(QueryBuilders.queryStringQuery("disk full OR x")).where();
        assertTrue(each, each.contains("\"level\" = 'disk'") && each.contains("\"level\" = 'full'") && each.contains("\"level\" = 'x'"));
        assertNotNull(translate(QueryBuilders.queryStringQuery("disk AND full")));
        String simple = translate(QueryBuilders.simpleQueryStringQuery("disk full")).where();
        assertTrue(simple, simple.contains("\"level\" = 'disk'") && simple.contains("\"level\" = 'full'"));
        // A number or date field matches no word that is not a number or a date: beside it a run is the words, as before.
        Map<String, Column> dated = new LinkedHashMap<>();
        dated.put("@timestamp", columns().get("@timestamp"));
        dated.put("status", columns().get("status"));
        dated.put("message", columns().get("message"));
        assertEquals(
            "token_match('os_logs_0', 'message', 'monkey jackal bear', 'or')",
            InfinoQueryTranslator.translate(QueryBuilders.queryStringQuery("message: monkey jackal bear"), dated, TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches).from()
        );
        // Beside 500 the run is "jackal 500", which no number or date field reads: the text column alone, ranked.
        assertEquals(
            "token_match('os_logs_0', 'message', 'monkey jackal 500', 'or')",
            InfinoQueryTranslator.translate(QueryBuilders.queryStringQuery("message:monkey jackal 500"), dated, TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches).from()
        );
        // Over text fields alone a run is the words, as before.
        assertEquals("token_match('os_logs_0', 'message', 'disk full', 'or')", translate(QueryBuilders.queryStringQuery("disk full").defaultField("message")).from());
    }

    /**
     * A keyword, number or date field that none of a query string's clauses
     * on the default fields matches in any row adds no hit and no score in
     * stock, so the query is read over the text columns, which rank it as
     * stock does (big5's message: monkey jackal bear over every field). One
     * read asks for such a row; where one exists the fields stay, unranked.
     */
    public void testKeywordFieldsNoRowMatchesLeaveTheTextToRank() {
        List<String> reads = new java.util.ArrayList<>();
        java.util.function.Function<String, List<Map<String, Object>>> none = sql -> {
            reads.add(sql);
            return List.of();
        };
        Translation big5 = InfinoQueryTranslator.translate(QueryBuilders.queryStringQuery("message: monkey jackal bear"), columns(), TABLE, NOW, none);
        assertEquals("token_match('os_logs_0', 'message', 'monkey jackal bear', 'or')", big5.from());
        assertEquals(1, reads.size());
        String read = reads.get(0);
        assertTrue(read, read.startsWith("SELECT 1 AS infino_hit FROM \"os_logs_0\" WHERE "));
        assertTrue(read, read.contains("\"level\" = 'jackal bear'") && read.contains("\"host.name\" = 'jackal bear'") && read.endsWith(" LIMIT 1"));
        // Beside a range, as big5's filtered search sends it: the same, read once.
        reads.clear();
        Translation filtered = InfinoQueryTranslator.translate(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.rangeQuery("@timestamp").gte("2023-01-03T00:00:00").lt("2023-01-03T10:00:00"))
                .must(QueryBuilders.queryStringQuery("message: monkey jackal bear")),
            columns(),
            TABLE,
            NOW,
            none
        );
        assertEquals("token_match('os_logs_0', 'message', 'monkey jackal bear', 'or')", filtered.from());
        assertEquals(1, reads.size());
        // The read asks only among the rows the range keeps: the others are no hits.
        assertTrue(reads.get(0), reads.get(0).contains(" AND (\"@timestamp\" >= '2023-01-03T00:00:00Z' AND \"@timestamp\" < '2023-01-03T10:00:00Z') LIMIT 1"));
        // A lone word: ranked by the text column.
        assertEquals(
            "token_match('os_logs_0', 'message', 'monkey', 'or')",
            InfinoQueryTranslator.translate(QueryBuilders.queryStringQuery("monkey"), columns(), TABLE, NOW, none).from()
        );
        // Where a row has the value, the keyword fields stay and the run is their one value.
        String kept = translate(QueryBuilders.queryStringQuery("message: monkey jackal bear")).where();
        assertTrue(kept, kept.contains("\"level\" = 'jackal bear'") && kept.contains("text_match(\"message\", 'monkey', 'and')"));
        // In filter context nothing scores: the fields stay and nothing is read.
        reads.clear();
        String filter = InfinoQueryTranslator.translate(
            QueryBuilders.boolQuery().filter(QueryBuilders.queryStringQuery("message: monkey jackal bear")),
            columns(),
            TABLE,
            NOW,
            none
        ).where();
        assertTrue(filter, filter.contains("\"level\" = 'jackal bear'"));
        assertEquals(0, reads.size());
        // Named fields: the same, and a value a field cannot read is still refused unless the query is lenient.
        assertEquals(
            "token_match('os_logs_0', 'message', 'monkey jackal bear', 'or')",
            InfinoQueryTranslator.translate(QueryBuilders.queryStringQuery("message: monkey jackal bear").field("message").field("level"), columns(), TABLE, NOW, none)
                .from()
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> InfinoQueryTranslator.translate(QueryBuilders.queryStringQuery("monkey").field("message").field("status"), columns(), TABLE, NOW, none)
        );
        // A run under minimum_should_match stays refused, the fields read or not: stock
        // counts it as one clause over every field, which the tree does not count.
        reads.clear();
        IllegalArgumentException counted = expectThrows(
            IllegalArgumentException.class,
            () -> InfinoQueryTranslator.translate(QueryBuilders.queryStringQuery("message: monkey jackal bear").minimumShouldMatch("2"), columns(), TABLE, NOW, none)
        );
        assertTrue(counted.getMessage(), counted.getMessage().contains("minimum_should_match [2]"));
        assertEquals(0, reads.size());
    }

    /**
     * A query naming no field, one clause over one text column and keyword
     * fields, ranks as stock's dis_max does whether or not a keyword value
     * matches: by the text's BM25, a keyword match competing with it (the
     * better of the two, tie_breaker 0) or adding to it (tie_breaker 1).
     * No row matching, the keyword fields are dropped and the text ranks
     * alone: the same scores.
     */
    public void testAKeywordMatchRanksBesideTheText() {
        InfinoQueryTranslator.TextClause disk = new InfinoQueryTranslator.TextClause("message", "disk", "or");
        String bonus = "CASE WHEN (\"level\" = 'disk' OR \"host.name\" = 'disk') THEN 1.0 ELSE 0.0 END";
        for (org.opensearch.index.query.QueryBuilder qb : List.of(
            QueryBuilders.queryStringQuery("disk"),
            QueryBuilders.multiMatchQuery("disk"),
            QueryBuilders.multiMatchQuery("disk").type(MultiMatchQueryBuilder.Type.BEST_FIELDS).lenient(true),
            QueryBuilders.boolQuery().must(QueryBuilders.multiMatchQuery("disk")).filter(QueryBuilders.rangeQuery("@timestamp").gte("2026-10-24T00:00:00Z"))
        )) {
            Translation t = translate(qb);
            assertNull(qb.toString(), t.principal());
            InfinoQueryTranslator.Scoring scoring = t.scoring();
            assertFalse(qb.toString(), scoring.unrankable());
            assertEquals(qb.toString(), disk, scoring.text());
            assertEquals(qb.toString(), List.of(bonus), scoring.bonuses().stream().map(InfinoQueryTranslator.Bonus::expr).toList());
            assertTrue(qb.toString(), scoring.best());
            String sql = InfinoSearch.scoredStatement(unsortedPlan(t, columns()), "osid", t.where(), 10, false);
            assertTrue(sql, sql.contains("GREATEST(infino_bm25, infino_bonus) AS score"));
        }
        // most_fields sums its fields: each value field that matches adds its own 1, as stock sums them.
        InfinoQueryTranslator.Scoring most = translate(QueryBuilders.multiMatchQuery("disk").type(MultiMatchQueryBuilder.Type.MOST_FIELDS)).scoring();
        assertEquals(disk, most.text());
        assertFalse(most.best());
        assertEquals(
            List.of("CASE WHEN (\"level\" = 'disk') THEN 1.0 ELSE 0.0 END", "CASE WHEN (\"host.name\" = 'disk') THEN 1.0 ELSE 0.0 END"),
            most.bonuses().stream().map(InfinoQueryTranslator.Bonus::expr).toList()
        );
        // A boolean field scores a match by BM25 in stock, not by 1: left unranked.
        Map<String, Column> withFlag = columns();
        withFlag.put("ok", new Column("ok", "bool", false, false));
        assertTrue(
            InfinoQueryTranslator.translate(QueryBuilders.queryStringQuery("true"), withFlag, TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches)
                .scoring()
                .unrankable()
        );
        // So does a keyword field with use_similarity.
        Map<String, Column> similar = columns();
        similar.put("host.name", similar.get("host.name").scoredBySimilarity());
        assertTrue(
            InfinoQueryTranslator.translate(QueryBuilders.queryStringQuery("disk"), similar, TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches).scoring().unrankable()
        );
        Translation mostTranslation = translate(QueryBuilders.multiMatchQuery("disk").type(MultiMatchQueryBuilder.Type.MOST_FIELDS));
        String summed = InfinoSearch.scoredStatement(unsortedPlan(mostTranslation, columns()), "osid", mostTranslation.where(), 10, false);
        assertTrue(summed, summed.contains("infino_bm25 + infino_bonus AS score"));
        // No row has the value: a query string drops the keyword fields, ranked by the text alone.
        java.util.function.Function<String, List<Map<String, Object>>> none = sql -> List.of();
        Translation alone = InfinoQueryTranslator.translate(QueryBuilders.queryStringQuery("disk"), columns(), TABLE, NOW, none);
        assertEquals(disk, alone.ranked());
        assertEquals(InfinoQueryTranslator.Scoring.NONE, alone.scoring());
        // A multi_match reads no row: the keyword fields stay, their bonus never taken, the same order.
        assertEquals(disk, InfinoQueryTranslator.translate(QueryBuilders.multiMatchQuery("disk"), columns(), TABLE, NOW, none).scoring().text());
        // Left unranked: a tie_breaker between 0 and 1 (no combination the table gives),
        // several clauses (stock sums each clause's dis_max), and a boost.
        assertTrue(translate(QueryBuilders.multiMatchQuery("disk").tieBreaker(0.3f)).scoring().unrankable());
        assertTrue(translate(QueryBuilders.queryStringQuery("disk OR full")).scoring().unrankable());
        assertTrue(translate(QueryBuilders.queryStringQuery("disk").boost(2f)).scoring().unrankable());
        // Beside another scoring clause it no longer scores alone.
        assertTrue(
            translate(QueryBuilders.boolQuery().must(QueryBuilders.queryStringQuery("disk")).should(QueryBuilders.matchQuery("message", "full")))
                .scoring()
                .unrankable()
        );
    }

    /**
     * The read that asks whether a keyword field matches is made only where
     * it could change the answer, and among the rows the query may return:
     * patterns alone score every match the same either way, so are not read;
     * a filter or must clause around the query narrows the read.
     */
    public void testTheKeywordReadIsMadeWhereItMatters() {
        List<String> reads = new java.util.ArrayList<>();
        java.util.function.Function<String, List<Map<String, Object>>> none = sql -> {
            reads.add(sql);
            return List.of();
        };
        for (String patterns : List.of("/pay-[0-9]+/", "pay*", "pay* OR /n.*/")) {
            reads.clear();
            String where = InfinoQueryTranslator.translate(QueryBuilders.queryStringQuery(patterns), columns(), TABLE, NOW, none).where();
            assertEquals(patterns, 0, reads.size());
            assertTrue(where, where.contains("\"level\""));
        }
        // Discover's shape with Lucene syntax: a query string and a time filter.
        reads.clear();
        Translation discover = InfinoQueryTranslator.translate(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.queryStringQuery("disk"))
                .filter(QueryBuilders.rangeQuery("@timestamp").gte("2026-10-24T00:00:00Z").lte("2026-10-24T12:00:00Z"))
                .filter(QueryBuilders.matchAllQuery()),
            columns(),
            TABLE,
            NOW,
            none
        );
        assertEquals(List.of(
            "SELECT 1 AS infino_hit FROM \"os_logs_0\" WHERE (\"level\" = 'disk' OR \"host.name\" = 'disk') "
                + "AND (\"@timestamp\" >= '2026-10-24T00:00:00Z' AND \"@timestamp\" <= '2026-10-24T12:00:00.999Z') LIMIT 1"
        ), reads);
        assertEquals("token_match('os_logs_0', 'message', 'disk', 'or')", discover.from());
        // A clause read by its text, or a nested bool's own filter, narrows as it can: the text clause
        // is left out, the inner filter and the outer one kept.
        reads.clear();
        InfinoQueryTranslator.translate(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("message", "full"))
                .must(QueryBuilders.boolQuery().must(QueryBuilders.queryStringQuery("disk")).filter(QueryBuilders.termQuery("status", 500)))
                .filter(QueryBuilders.termQuery("host.name", "n1")),
            columns(),
            TABLE,
            NOW,
            none
        );
        assertEquals(1, reads.size());
        assertTrue(reads.get(0), reads.get(0).contains("AND \"host.name\" = 'n1'") && reads.get(0).contains("AND \"status\" = 500"));
        assertFalse(reads.get(0), reads.get(0).contains("text_match"));
    }

    public void testARunIsOneTermWhenAsked() {
        assertEquals(
            new InfinoLuceneQuery.Bool(
                List.of(),
                List.of(new InfinoLuceneQuery.Term("message", "monkey", false, 0, 0), new InfinoLuceneQuery.Term(null, "jackal bear", false, 0, 0)),
                List.of()
            ),
            InfinoLuceneQuery.parseTop("message: monkey jackal bear", Operator.OR, true).node()
        );
        // Under AND the run is a required clause; a word before AND or OR, or after one, is a clause of its own.
        assertEquals(
            new InfinoLuceneQuery.Bool(
                List.of(new InfinoLuceneQuery.Term(null, "a b", false, 0, 0), new InfinoLuceneQuery.Term(null, "c", false, 0, 0), new InfinoLuceneQuery.Term(null, "d", false, 0, 0)),
                List.of(),
                List.of()
            ),
            InfinoLuceneQuery.parseTop("a b c AND d", Operator.AND, true).node()
        );
        assertEquals(
            new InfinoLuceneQuery.Bool(
                List.of(),
                List.of(new InfinoLuceneQuery.Term(null, "a", false, 0, 0), new InfinoLuceneQuery.Term(null, "b", false, 0, 0), new InfinoLuceneQuery.Term(null, "c d", false, 0, 0)),
                List.of()
            ),
            InfinoLuceneQuery.parseTop("a OR b c d", Operator.OR, true).node()
        );
        // In a field's group the run takes the field.
        assertEquals(new InfinoLuceneQuery.Term("level", "disk full", false, 0, 0), InfinoLuceneQuery.parseTop("level:(disk full)", Operator.OR, true).node());
        // Without asking, a term per word, as before.
        assertEquals(3, ((InfinoLuceneQuery.Bool) InfinoLuceneQuery.parseTop("message: monkey jackal bear", Operator.OR).node()).should().size());
    }

    public void testRunsAreReadAsLucenesParserReadsThem() {
        assertTrue(InfinoLuceneQuery.hasRun("disk full"));
        assertTrue(InfinoLuceneQuery.hasRun("level:error disk full"));
        assertTrue(InfinoLuceneQuery.hasRun("a OR b c d"));
        assertTrue(InfinoLuceneQuery.hasRun("(disk full) OR x"));
        assertFalse(InfinoLuceneQuery.hasRun("disk"));
        assertFalse(InfinoLuceneQuery.hasRun("disk full OR x"));
        assertFalse(InfinoLuceneQuery.hasRun("disk AND full"));
        assertFalse(InfinoLuceneQuery.hasRun("+disk full"));
        assertFalse(InfinoLuceneQuery.hasRun("disk level:full"));
        assertFalse(InfinoLuceneQuery.hasRun("disk^2 full"));
        assertFalse(InfinoLuceneQuery.hasRun("disk ful*"));
        assertFalse(InfinoLuceneQuery.hasRun("\"disk full\""));
    }

    /**
     * index.query.default_field names the fields a query naming none
     * searches; the query string's own default_field or fields come first.
     */
    public void testTheIndexsDefaultFieldIsSearched() {
        String index = "dflt";
        String table = InfinoNative.tableFor(index);
        org.opensearch.cluster.metadata.IndexMetadata md = org.opensearch.cluster.metadata.IndexMetadata.builder(index)
            .settings(
                org.opensearch.common.settings.Settings.builder()
                    .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
                    .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID, "dflt-uuid")
                    .put(InfinoPlugin.INFINO_ENABLED.getKey(), true)
                    .put(InfinoPlugin.INFINO_MODE.getKey(), InfinoPlugin.MODE_PROXY)
                    .putList(org.opensearch.index.IndexSettings.DEFAULT_FIELD_SETTING.getKey(), "level", "mess*")
            )
            .build();
        InfinoPlugin.tables().observe(org.opensearch.cluster.metadata.Metadata.builder().put(md, false).build());
        try {
            String levelOrMessage = "(\"level\" = 'disk' OR text_match(\"message\", 'disk', 'or'))";
            assertEquals(levelOrMessage, InfinoQueryTranslator.translate(QueryBuilders.multiMatchQuery("disk"), columns(), table, NOW, InfinoQueryTranslatorTests::aRowMatches).where());
            assertEquals(levelOrMessage, InfinoQueryTranslator.translate(QueryBuilders.queryStringQuery("disk"), columns(), table, NOW, InfinoQueryTranslatorTests::aRowMatches).where());
            // Not every field, so not lenient: a field the word is no value of fails, as stock's does.
            Map<String, Column> cols = columns();
            cols.put("level", cols.get("status"));
            expectThrows(IllegalArgumentException.class, () -> InfinoQueryTranslator.translate(QueryBuilders.multiMatchQuery("disk"), cols, table, NOW, InfinoQueryTranslatorTests::aRowMatches));
            // The query's own fields come first.
            assertEquals(
                "\"host.name\" = 'disk'",
                InfinoQueryTranslator.translate(QueryBuilders.queryStringQuery("disk").defaultField("host.name"), columns(), table, NOW, InfinoQueryTranslatorTests::aRowMatches).where()
            );
        } finally {
            InfinoPlugin.tables().observe(org.opensearch.cluster.metadata.Metadata.EMPTY_METADATA);
        }
    }

    public void testQueryStringClausesReadAsLuceneReadsThem() {
        // Terms on the default fields go to the engine as one query with Lucene's occurrences as sigils.
        assertEquals("token_match('os_logs_0', 'message', '+disk +cache', 'or')", translate(QueryBuilders.queryStringQuery("disk AND cache").defaultField("message")).from());
        assertEquals("token_match('os_logs_0', 'message', 'disk cache -full', 'or')", translate(QueryBuilders.queryStringQuery("disk cache -full").defaultField("message")).from());
        assertEquals(
            "token_match('os_logs_0', 'message', '+disk +cache', 'or')",
            translate(QueryBuilders.queryStringQuery("disk cache").defaultField("message").defaultOperator(org.opensearch.index.query.Operator.AND)).from()
        );
        // Field clauses are predicates; AND makes both sides required.
        assertEquals("(\"level\" = 'error' AND \"status\" = 500)", translate(QueryBuilders.queryStringQuery("level:error AND status:500")).where());
        // A group of terms stays one engine query; a prohibited field clause is a NOT.
        Translation t = translate(QueryBuilders.queryStringQuery("(disk OR cache) AND NOT level:info").defaultField("message"));
        assertEquals("token_match('os_logs_0', 'message', 'disk cache', 'or')", t.from());
        assertEquals("(1=1 AND NOT COALESCE((\"level\" = 'info'), FALSE))", t.where());
        // Ranges and comparisons, with date math on a date column.
        assertEquals("(\"status\" >= 200 AND \"status\" <= 299)", translate(QueryBuilders.queryStringQuery("status:[200 TO 299]")).where());
        assertEquals("(\"status\" > 499)", translate(QueryBuilders.queryStringQuery("status:>499")).where());
        assertEquals("(\"@timestamp\" >= '2026-10-24T00:00:00Z')", translate(QueryBuilders.queryStringQuery("@timestamp:[2026-10-24 TO *]")).where());
        assertEquals("\"status\" IS NOT NULL", translate(QueryBuilders.queryStringQuery("status:*")).where());
        // A field's group distributes the field; a lone prohibited clause excludes from everything.
        assertEquals("((\"level\" = 'error' OR \"level\" = 'warn'))", translate(QueryBuilders.queryStringQuery("level:(error OR warn)")).where());
        assertEquals("(NOT COALESCE((\"level\" = 'info'), FALSE))", translate(QueryBuilders.queryStringQuery("-level:info")).where());
        assertEquals("(\"status\" >= -10 AND \"status\" <= 3)", translate(QueryBuilders.queryStringQuery("status:[-10 TO 3]")).where());
        // Wildcards and regular expressions: a term of a text column, the whole value of an exact one.
        assertEquals("regexp_like(\"message\", '\\bdis[^\\s]*', 'i')", translate(QueryBuilders.queryStringQuery("message:dis*")).where());
        assertEquals("regexp_like(\"message\", '\\bdis[^\\s]*', 'i')", translate(QueryBuilders.queryStringQuery("dis*").defaultField("message")).where());
        assertEquals("\"host.name\" LIKE 'n%'", translate(QueryBuilders.queryStringQuery("host.name:n*")).where());
        assertEquals("regexp_like(\"message\", '\\b(?:d.sk)\\b', 'i')", translate(QueryBuilders.queryStringQuery("message:/d.sk/")).where());
        assertEquals("regexp_like(\"level\", '^(?:err.*)$')", translate(QueryBuilders.queryStringQuery("level:/err.*/")).where());
        // Boosts change nothing; fuzziness and proximity are refused with the clause named; so is a bad string.
        assertEquals("token_match('os_logs_0', 'message', '+disk +cache', 'or')", translate(QueryBuilders.queryStringQuery("disk^2 AND cache").defaultField("message")).from());
        assertTrue(expectThrows(IllegalArgumentException.class, () -> translate(QueryBuilders.queryStringQuery("disk~1"))).getMessage().contains("fuzziness"));
        assertTrue(expectThrows(IllegalArgumentException.class, () -> translate(QueryBuilders.queryStringQuery("\"disk full\"~2"))).getMessage().contains("proximity"));
        assertTrue(expectThrows(IllegalArgumentException.class, () -> translate(QueryBuilders.queryStringQuery("(disk"))).getMessage().contains("parenthesis"));
    }

    public void testMatchOnAKeywordIsAnEquality() {
        Translation t = translate(QueryBuilders.matchQuery("level", "error"));
        assertNull(t.principal());
        assertEquals("\"level\" = 'error'", t.where());
        // On a numeric column the same: an equality, not a refusal.
        assertEquals("\"status\" = 500", translate(QueryBuilders.matchQuery("status", "500")).where());
    }

    public void testBoolSeams() {
        // should beside must_not with no must or filter: the should still filters.
        BoolQueryBuilder b = QueryBuilders.boolQuery().should(QueryBuilders.termQuery("level", "a")).mustNot(QueryBuilders.termQuery("level", "b"));
        assertEquals("((\"level\" = 'a') AND NOT COALESCE((\"level\" = 'b'), FALSE))", translate(b).where());
        // minimum_should_match counts the clauses that hold.
        b = QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("level", "a"))
            .should(QueryBuilders.termQuery("status", 1))
            .should(QueryBuilders.termQuery("host.name", "n"))
            .minimumShouldMatch(2);
        assertEquals(
            "(((CASE WHEN \"level\" = 'a' THEN 1 ELSE 0 END + CASE WHEN \"status\" = 1 THEN 1 ELSE 0 END + CASE WHEN \"host.name\" = 'n' THEN 1 ELSE 0 END) >= 2))",
            translate(b).where()
        );
        // Two required text clauses on one column under all-terms join the ranked query.
        b = QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("message", "disk").operator(org.opensearch.index.query.Operator.AND))
            .must(QueryBuilders.matchQuery("message", "full").operator(org.opensearch.index.query.Operator.AND));
        Translation t = translate(b);
        assertEquals("token_match('os_logs_0', 'message', 'disk full', 'and')", t.from());
        assertEquals("(1=1 AND 1=1)", t.where());
    }

    public void testPatternsOnTextColumnsMatchTerms() {
        assertEquals("regexp_like(\"message\", '\\bdis', 'i')", translate(QueryBuilders.prefixQuery("message", "dis")).where());
        assertEquals("regexp_like(\"message\", '\\bd[^\\s]sk\\b', 'i')", translate(QueryBuilders.wildcardQuery("message", "d?sk")).where());
        assertEquals("regexp_like(\"message\", '\\b(?:d.sk)\\b', 'i')", translate(QueryBuilders.regexpQuery("message", "d.sk")).where());
        assertEquals("\"level\" LIKE 'err%'", translate(QueryBuilders.prefixQuery("level", "err")).where());
    }

    public void testUnknownFieldMatchesNothingAndUnknownShapeIsNull() {
        assertEquals("1=0", translate(QueryBuilders.termQuery("nope", 1)).where());
        assertNull(translate(QueryBuilders.fuzzyQuery("message", "dsk")));
    }

    // ---- scoring ----

    /**
     * A should holding a match beside a term (#68): the match is no ranked
     * source (the term alone may match), so the match is the scored text and
     * the term adds its boost where it matches. The hits are ranked in bands
     * that share the term's share, each by the match's BM25 with the band's
     * bounds as its filter, and the hits the match misses score the term's
     * share alone.
     */
    public void testAShouldTermAddsToTheMatchsScore() {
        BoolQueryBuilder b = QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("message", "disk")).should(QueryBuilders.termQuery("level", "error"));
        Translation t = translate(b);
        assertNull(t.principal());
        InfinoQueryTranslator.Scoring s = t.scoring();
        assertEquals(new InfinoQueryTranslator.TextClause("message", "disk", "or"), s.text());
        assertEquals(0.0, s.offset(), 0);
        assertEquals(1, s.bonuses().size());
        assertEquals("CASE WHEN \"level\" = 'error' THEN 1.0 ELSE 0.0 END", s.bonuses().get(0).expr());
        assertTrue(s.reorders());
        String sql = hits(t, null, 10);
        String bonus = "((CASE WHEN \"level\" = 'error' THEN 1.0 ELSE 0.0 END))";
        // A plain word is the engine's text_match, as the translator writes it into the predicate.
        String match = "text_match(\"message\", 'disk', 'or')";
        // The band without the term's share, and the band with it: the match's ten best in each.
        assertTrue(sql, sql.contains("FROM bm25_search('os_logs_0', 'message', 'disk', 10, 'or') WHERE (((1=1 OR \"level\" = 'error'))) AND " + bonus + " < 0.5"));
        assertTrue(sql, sql.contains("FROM bm25_search('os_logs_0', 'message', 'disk', 10, 'or') WHERE (((1=1 OR \"level\" = 'error'))) AND " + bonus + " >= 0.5"));
        // The hits the match misses, by the term's share.
        assertTrue(sql, sql.contains("WHERE (((1=0 OR \"level\" = 'error'))) AND NOT COALESCE((" + match + "), FALSE) ORDER BY infino_bonus DESC, infino_row ASC LIMIT 10"));
        assertTrue(sql, sql.startsWith("SELECT osid, source, infino_bm25 + infino_bonus AS score FROM ("));
        assertTrue(sql, sql.endsWith(") AS infino_scored ORDER BY score DESC, infino_row ASC LIMIT 10"));
    }

    /**
     * Where documents give the term's field several values, its match is a
     * subquery by id, which the engine plans in a filter only: the bonus
     * reads it through a join by id, in every branch, and the bands rank
     * the window, their bounds being no filter the ranked search takes.
     */
    public void testAShouldTermOnAListIsJoinedIntoTheScore() {
        Map<String, Column> cols = columns();
        cols.put("level[]", Column.list("level[]", "str"));
        BoolQueryBuilder b = QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("message", "disk")).should(QueryBuilders.termQuery("level", "error"));
        Translation t = InfinoQueryTranslator.translate(b, cols, TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches);
        InfinoQueryTranslator.Bonus bonus = t.scoring().bonuses().get(0);
        assertEquals("CASE WHEN infino_match0.infino_hit IS NOT NULL THEN 1.0 ELSE 0.0 END", bonus.expr());
        assertTrue(bonus.join(), bonus.join().startsWith(" LEFT JOIN (SELECT osid AS infino_hit FROM \"os_logs_0\" WHERE ((\"level[]\" IS NULL AND \"level\" = 'error') OR osid IN (SELECT "));
        assertTrue(bonus.join(), bonus.join().endsWith(") AS infino_match0 ON osid = infino_match0.infino_hit"));
        String sql = hits(t, null, 10);
        assertEquals(2, sql.split("FROM bm25_search\\('os_logs_0', 'message', 'disk', 10000, 'or'\\) LEFT JOIN \\(SELECT osid AS infino_hit ", -1).length - 1);
        assertTrue(sql, sql.contains("FROM \"os_logs_0\" LEFT JOIN (SELECT osid AS infino_hit "));
        // A term on a field no document gives several values stays in the score itself.
        assertNull(translate(b).scoring().bonuses().get(0).join());
    }

    /** A term in must adds its boost to every hit alike: the ranked search is the match's, as before, and the score shifts. */
    public void testAMustTermShiftsEveryScore() {
        BoolQueryBuilder b = QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("message", "disk")).must(QueryBuilders.termQuery("level", "error"));
        Translation t = translate(b);
        assertEquals(1.0, t.scoring().offset(), 0);
        assertFalse(t.scoring().reorders());
        assertFalse(hits(t, null, 10).contains("infino_bonus"));
        // Must clauses with no text add up; a filter adds nothing.
        b = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("level", "error"))
            .must(QueryBuilders.rangeQuery("status").gte(500).boost(2))
            .filter(QueryBuilders.existsQuery("host.name"));
        assertEquals(3.0, translate(b).scoring().offset(), 0);
        assertSame(InfinoQueryTranslator.Scoring.ZERO, translate(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("level", "error"))).scoring());
    }

    /**
     * A bool whose clauses all score nothing scores its hits 0, as Lucene
     * does: filter and must_not clauses, or must and should clauses that are
     * such bools themselves. A should clause that scores, a must query or an
     * empty bool scores.
     */
    public void testABoolOfFilterAndMustNotClausesScoresZero() {
        for (org.opensearch.index.query.QueryBuilder zero : java.util.List.of(
            QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("level", "error")),
            QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("level", "error")),
            QueryBuilders.boolQuery().filter(QueryBuilders.matchQuery("message", "disk")).mustNot(QueryBuilders.termQuery("level", "info")),
            QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("level", "error"))),
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("level", "error"))
                .should(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("status", 200))),
            QueryBuilders.boolQuery().should(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("status", 200)))
        )) {
            Translation t = translate(zero);
            assertSame(zero.toString(), InfinoQueryTranslator.Scoring.ZERO, t.scoring());
            assertEquals(zero.toString(), 0.0f, InfinoSearch.constantScore(t.scoring()), 0);
            assertSame(zero.toString(), InfinoQueryTranslator.Scoring.ZERO, t.unscored().scoring());
            assertTrue(zero.toString(), t.ascending().scoring().zero());
        }
        for (org.opensearch.index.query.QueryBuilder scores : java.util.List.of(
            QueryBuilders.boolQuery(),
            QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("level", "error")).should(QueryBuilders.termQuery("level", "fatal")),
            QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery()).filter(QueryBuilders.termQuery("level", "error")),
            QueryBuilders.matchAllQuery()
        )) {
            assertFalse(scores.toString(), translate(scores).scoring().zero());
            assertEquals(scores.toString(), 1.0f, InfinoSearch.constantScore(translate(scores).scoring()), 0);
        }
    }

    /**
     * A text clause in filter context scores nothing: alone, its rows are
     * read through the token index unranked, and no hit carries its BM25.
     */
    public void testAFilterTextIsReadUnranked() {
        Translation t = translate(QueryBuilders.boolQuery().filter(QueryBuilders.matchQuery("message", "disk")));
        assertEquals(new InfinoQueryTranslator.TextClause("message", "disk", "or"), t.principal());
        assertNull(t.ranked());
        assertSame(InfinoQueryTranslator.Scoring.ZERO, t.scoring());
        assertEquals("token_match('os_logs_0', 'message', 'disk', 'or')", t.from());
        assertFalse(hits(t, null, 10).contains("bm25_search"));
        // Beside a must that scores every hit alike, the filter still ranks nothing.
        t = translate(QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery()).filter(QueryBuilders.matchQuery("message", "disk")));
        assertNull(t.ranked());
        assertFalse(t.scoring().zero());
        assertFalse(hits(t, null, 10).contains("bm25_search"));
    }

    /**
     * Beside a text clause that scores, a filter's text is a predicate on the
     * scoring clause's rows: the hits rank by the scoring clause alone, and
     * the two never join into one ranked query, however alike their modes.
     */
    public void testAFilterTextBesideAScoringTextIsAPredicate() {
        Translation t = translate(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("message", "full").operator(org.opensearch.index.query.Operator.AND))
                .filter(QueryBuilders.matchQuery("message", "disk").operator(org.opensearch.index.query.Operator.AND))
        );
        assertEquals(new InfinoQueryTranslator.TextClause("message", "full", "and"), t.ranked());
        assertTrue(t.where(), t.where().contains("disk"));
        assertTrue(hits(t, null, 10), hits(t, null, 10).contains("bm25_search('os_logs_0', 'message', 'full', "));
        // The filter comes first in the request: the scoring clause is still the ranked one.
        t = translate(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.matchQuery("message", "disk"))
                .should(QueryBuilders.matchQuery("message", "full"))
        );
        assertNull(t.ranked());
        assertEquals(new InfinoQueryTranslator.TextClause("message", "full", "or"), t.scoring().text());
        assertTrue(t.where(), t.where().contains("disk"));
        // A must clause's text inside a filter scores nothing either.
        t = translate(QueryBuilders.boolQuery().filter(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("message", "disk"))));
        assertNull(t.ranked());
        assertSame(InfinoQueryTranslator.Scoring.ZERO, t.scoring());
    }

    /**
     * A should clause beside a must match scores where it matches without
     * narrowing the hits: two of them, boosted, cut the hits into a band for
     * each sum their shares can take.
     */
    public void testShouldClausesBesideAMustMatchAreBands() {
        BoolQueryBuilder b = QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("message", "disk"))
            .should(QueryBuilders.termsQuery("level", "error", "warn").boost(2))
            .should(QueryBuilders.rangeQuery("status").gte(500));
        Translation t = translate(b);
        assertNotNull(t.principal());
        assertNull(t.scoring().text());
        assertEquals(java.util.List.of(0.0, 1.0, 2.0, 3.0), InfinoSearch.bonusSums(t.scoring().bonuses()));
        String sql = hits(t, null, 10);
        // Four bands, split halfway between the sums; no rows the match misses.
        assertEquals(4, sql.split("FROM bm25_search\\(", -1).length - 1);
        assertTrue(sql, sql.contains(" >= 0.5 AND "));
        assertTrue(sql, sql.contains(" >= 2.5"));
        assertFalse(sql, sql.contains("token_match"));
    }

    /** With no text clause, the should clauses' shares are the score: the filtered rows ordered by it. */
    public void testShouldClausesWithNoTextOrderByTheirSum() {
        BoolQueryBuilder b = QueryBuilders.boolQuery().should(QueryBuilders.termQuery("level", "error")).should(QueryBuilders.rangeQuery("status").gte(500));
        String sql = hits(translate(b), null, 10);
        assertTrue(sql, sql.contains(", 0.0 AS infino_bm25, (CASE WHEN \"level\" = 'error' THEN 1.0 ELSE 0.0 END) + (CASE WHEN (\"status\" >= 500) THEN 1.0 ELSE 0.0 END) AS infino_bonus, \"_id\" AS infino_row FROM \"os_logs_0\" WHERE (("));
        assertFalse(sql, sql.contains("bm25_search"));
    }

    /**
     * Two text clauses in should would each add their BM25, and the table
     * ranks by one: with no ranked clause, the hits keep the one score they
     * all had rather than leave the hits only the texts match at 0. So does a
     * should clause the table cannot score, beside no ranked clause; beside
     * one, it adds nothing and the others add theirs.
     */
    public void testAShouldClauseThatCannotScoreKeepsTheHitsAlike() {
        BoolQueryBuilder b = QueryBuilders.boolQuery()
            .should(QueryBuilders.matchQuery("message", "disk"))
            .should(QueryBuilders.matchQuery("message", "full"))
            .should(QueryBuilders.termQuery("level", "error"));
        assertAlikeUnrankable(translate(b).scoring());
        b = QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("message", "disk")).should(QueryBuilders.queryStringQuery("level:error OR status:500"));
        assertAlikeUnrankable(translate(b).scoring());
        b = QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("message", "disk"))
            .should(QueryBuilders.queryStringQuery("level:error OR status:500"))
            .should(QueryBuilders.termQuery("level", "error"));
        Translation t = translate(b);
        assertNotNull(t.principal());
        assertEquals(1, t.scoring().bonuses().size());
        // A must clause the table cannot score, with no ranked clause, keeps the hits alike too; what the must terms add stays.
        b = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("level", "error"))
            .must(QueryBuilders.multiMatchQuery("disk", "message", "host.name"))
            .should(QueryBuilders.termQuery("status", 500));
        InfinoQueryTranslator.Scoring s = translate(b).scoring();
        assertFalse(s.reorders());
        assertEquals(1.0, s.offset(), 0);
    }

    /**
     * Past 64 sums of the scoring clauses' shares, the text clause's best
     * 10,000 are ranked once and scored after, rather than in a band per sum.
     */
    public void testPastSixtyFourSumsTheTextIsRankedOnce() {
        BoolQueryBuilder b = QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("message", "disk"));
        for (int i = 0; i < 7; i++) {
            // Boosts 1, 2, 4, ... 64: every subset sums apart, 128 sums.
            b.should(QueryBuilders.termQuery("level", "l" + i).boost(1 << i));
        }
        Translation t = translate(b);
        assertNull(InfinoSearch.bonusSums(t.scoring().bonuses()));
        String sql = hits(t, null, 10);
        assertEquals(1, sql.split("FROM bm25_search\\(", -1).length - 1);
        assertTrue(sql, sql.contains("FROM bm25_search('os_logs_0', 'message', 'disk', 10000, 'or') WHERE "));
        // Six boosts give 64 sums: a band each.
        b = QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("message", "disk"));
        for (int i = 0; i < 6; i++) {
            b.should(QueryBuilders.termQuery("level", "l" + i).boost(1 << i));
        }
        assertEquals(64, InfinoSearch.bonusSums(translate(b).scoring().bonuses()).size());
    }

    /** A prefix, wildcard, regexp, ids or match_phrase on a keyword scores its boost, as their constant-score queries do in Lucene. */
    public void testPatternAndIdsClausesScoreTheirBoost() {
        BoolQueryBuilder b = QueryBuilders.boolQuery()
            .should(QueryBuilders.matchQuery("message", "disk"))
            .should(QueryBuilders.prefixQuery("level", "err").boost(3))
            .should(QueryBuilders.wildcardQuery("host.name", "n*"))
            .should(QueryBuilders.regexpQuery("host.name", "n.*"))
            .should(QueryBuilders.idsQuery().addIds("a"))
            .should(QueryBuilders.matchPhraseQuery("level", "error"));
        InfinoQueryTranslator.Scoring s = translate(b).scoring();
        assertNotNull(s.text());
        assertEquals(5, s.bonuses().size());
        assertEquals(java.util.List.of(3.0), s.bonuses().get(0).values());
    }

    /** A should clause beside a must that the table cannot read is left unscored, and the query is served as before. */
    public void testAnUnreadableShouldBesideAMustStaysUnscored() {
        BoolQueryBuilder b = QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("message", "disk"))
            .should(QueryBuilders.nestedQuery("host", QueryBuilders.matchQuery("message", "x"), org.apache.lucene.search.join.ScoreMode.Avg));
        Translation t = translate(b);
        assertNotNull(t);
        assertSame(InfinoQueryTranslator.Scoring.NONE, t.scoring());
    }

    /** The log columns with a boolean field and a keyword field mapped with use_similarity. */
    private static Map<String, Column> bm25TermColumns() {
        Map<String, Column> cols = columns();
        cols.put("flag", new Column("flag", "bool", false, false));
        cols.put("tag", new Column("tag", "str", false, false).scoredBySimilarity());
        return cols;
    }

    /** Answers the count a BM25 term's score reads: {@code docs} holding the field, {@code hits} matching the term. */
    private static java.util.function.Function<String, java.util.List<Map<String, Object>>> counts(
        java.util.List<String> statements,
        long docs,
        long hits
    ) {
        return sql -> {
            statements.add(sql);
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("infino_docs", docs);
            row.put("infino_hits", hits);
            return java.util.List.of(row);
        };
    }

    /**
     * A term on a boolean field, or on a keyword field with use_similarity,
     * is a plain term query in Lucene, scored by BM25 (the reviewer's point
     * on #83): the field has no norms, so each hit it matches scores
     * {@code idf / 2.2}, 0.245 for 3 of 5 documents, not its boost.
     */
    public void testATermLuceneScoresByBm25ScoresItsIdf() {
        for (org.opensearch.index.query.QueryBuilder term : java.util.List.<org.opensearch.index.query.QueryBuilder>of(
            QueryBuilders.termQuery("flag", true),
            QueryBuilders.matchQuery("flag", "true"),
            QueryBuilders.termQuery("tag", "x")
        )) {
            java.util.List<String> statements = new java.util.ArrayList<>();
            BoolQueryBuilder b = QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("message", "disk")).should(term);
            Translation t = InfinoQueryTranslator.translate(b, bm25TermColumns(), TABLE, NOW, counts(statements, 5, 3));
            InfinoQueryTranslator.Scoring s = t.scoring();
            assertEquals(1, s.bonuses().size());
            assertEquals(1, s.bonuses().get(0).values().size());
            assertEquals(term.toString(), 0.244998, s.bonuses().get(0).values().get(0), 1e-6);
            assertEquals(statements.toString(), 1, statements.size());
            assertTrue(statements.get(0), statements.get(0).startsWith("SELECT COUNT(\"" + (term.toString().contains("tag") ? "tag" : "flag") + "\") AS infino_docs, SUM(CASE WHEN "));
            assertTrue(statements.get(0), statements.get(0).endsWith(" THEN 1 ELSE 0 END) AS infino_hits FROM \"os_logs_0\""));
        }
        // In must it adds the same to every hit; a boost scales it.
        java.util.List<String> statements = new java.util.ArrayList<>();
        BoolQueryBuilder b = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("flag", true).boost(2));
        Translation t = InfinoQueryTranslator.translate(b, bm25TermColumns(), TABLE, NOW, counts(statements, 5, 3));
        assertEquals(2 * 0.244998, t.scoring().offset(), 1e-5);
        // A term on a plain keyword or on a number still scores its boost, with nothing counted.
        statements.clear();
        b = QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("message", "disk")).should(QueryBuilders.termQuery("level", "error"));
        t = InfinoQueryTranslator.translate(b, bm25TermColumns(), TABLE, NOW, counts(statements, 5, 3));
        assertEquals(java.util.List.of(1.0), t.scoring().bonuses().get(0).values());
        assertTrue(statements.isEmpty());
        // A keyword multi-field of a use_similarity field is a plain keyword unless its own mapping says otherwise.
        b = QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("message", "disk")).should(QueryBuilders.termQuery("tag.keyword", "x"));
        t = InfinoQueryTranslator.translate(b, bm25TermColumns(), TABLE, NOW, counts(statements, 5, 3));
        assertEquals(java.util.List.of(1.0), t.scoring().bonuses().get(0).values());
        assertTrue(statements.isEmpty());
    }

    /**
     * Under a similarity other than the default BM25 (the index's default or
     * the field's own), a term on a boolean field or a use_similarity keyword
     * keeps its boost as its score, as Lucene's boolean similarity gives it,
     * and nothing is counted for it.
     */
    public void testATermUnderAnotherSimilarityKeepsItsBoost() {
        Map<String, Column> cols = columns();
        cols.put("flag", new Column("flag", "bool", false, false).scoredByOtherSimilarity());
        cols.put("tag", new Column("tag", "str", false, false).scoredBySimilarity().scoredByOtherSimilarity());
        for (org.opensearch.index.query.QueryBuilder term : java.util.List.<org.opensearch.index.query.QueryBuilder>of(
            QueryBuilders.termQuery("flag", true),
            QueryBuilders.termQuery("tag", "x").boost(2)
        )) {
            java.util.List<String> statements = new java.util.ArrayList<>();
            BoolQueryBuilder b = QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("message", "disk")).should(term);
            Translation t = InfinoQueryTranslator.translate(b, cols, TABLE, NOW, counts(statements, 5, 3));
            assertEquals(term.toString(), java.util.List.of((double) term.boost()), t.scoring().bonuses().get(0).values());
            assertTrue(statements.toString(), statements.isEmpty());
        }
    }

    /**
     * A field scores by another similarity when its mapping names one other
     * than BM25, or names none under an index whose default is another; a
     * keyword multi-field by its own mapping's, not its field's.
     */
    public void testAMappingNamesTheFieldsScoredByAnotherSimilarity() {
        String mapping = "{\"properties\":{"
            + "\"f\":{\"type\":\"boolean\"},"
            + "\"b\":{\"type\":\"keyword\",\"similarity\":\"boolean\",\"fields\":{\"raw\":{\"type\":\"keyword\"}}},"
            + "\"m\":{\"type\":\"keyword\",\"similarity\":\"BM25\"}}}";
        InfinoNative.Mapping bm25 = InfinoNative.Mapping.of(mapping, true);
        assertEquals(java.util.Set.of("b"), bm25.otherSimilarityFields());
        InfinoNative.Mapping other = InfinoNative.Mapping.of(mapping, false);
        assertEquals(java.util.Set.of("f", "b", "b.raw"), other.otherSimilarityFields());

        assertTrue(InfinoTables.defaultBm25(org.opensearch.common.settings.Settings.EMPTY));
        assertTrue(InfinoTables.defaultBm25(org.opensearch.common.settings.Settings.builder().put("index.similarity.default.type", "BM25").build()));
        assertFalse(InfinoTables.defaultBm25(org.opensearch.common.settings.Settings.builder().put("index.similarity.default.type", "boolean").build()));
        assertFalse(
            InfinoTables.defaultBm25(
                org.opensearch.common.settings.Settings.builder()
                    .put("index.similarity.default.type", "BM25")
                    .put("index.similarity.default.k1", "2.0")
                    .build()
            )
        );
    }

    /** A bool with no clause is a match_all, as stock reads it: it scores its boost, in must and in should. */
    public void testABoolWithNoClauseScoresAsMatchAll() {
        Translation t = translate(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("message", "disk")).must(QueryBuilders.boolQuery()));
        assertEquals(1.0, t.scoring().offset(), 1e-9);
        t = translate(QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("message", "disk")).should(QueryBuilders.boolQuery().boost(3)));
        assertEquals(java.util.List.of(3.0), t.scoring().bonuses().get(0).values());
    }

    /**
     * Where documents give the boolean field several values, a term's
     * frequency and the field's average length vary by document: the table
     * cannot score it, and with no ranked clause the hits keep one score.
     * So does a nested query whose entries a BM25-scored term scores.
     */
    public void testABm25TermTheTableCannotScoreKeepsTheHitsAlike() {
        Map<String, Column> cols = bm25TermColumns();
        cols.put("flag[]", Column.list("flag[]", "bool"));
        BoolQueryBuilder b = QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("message", "disk")).should(QueryBuilders.termQuery("flag", true));
        assertAlikeUnrankable(InfinoQueryTranslator.translate(b, cols, TABLE, NOW, counts(new java.util.ArrayList<>(), 5, 3)).scoring());

        Map<String, Column> orders = orderColumns();
        orders.put("items.gift", new Column("items.gift", "bool", false, false));
        b = QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("items.sku", "a"))
            .should(QueryBuilders.nestedQuery("items", QueryBuilders.termQuery("items.gift", true), org.apache.lucene.search.join.ScoreMode.Avg));
        assertAlikeUnrankable(InfinoQueryTranslator.translate(b, orders, "os_orders_0", NOW, new Rows(orders(), true)).scoring());
    }

    /** Hits that keep one score although stock scores them apart: nothing reorders them, and the scoring says no ranked read can. */
    private static void assertAlikeUnrankable(InfinoQueryTranslator.Scoring scoring) {
        assertFalse(scoring.reorders());
        assertEquals(0, scoring.offset(), 0);
        assertTrue(scoring.unrankable());
    }

    /** A request sorted by _doc first is ordered by the index, unscored, as stock orders it; one sorted by _score is not. */
    public void testASortByDocFirstIsNotScored() {
        org.opensearch.search.builder.SearchSourceBuilder source = new org.opensearch.search.builder.SearchSourceBuilder();
        assertFalse(InfinoSearch.sortsByDocFirst(source));
        source.sort("_doc");
        assertTrue(InfinoSearch.sortsByDocFirst(source));
        assertFalse(InfinoSearch.sortsByDocFirst(new org.opensearch.search.builder.SearchSourceBuilder().sort(new org.opensearch.search.sort.ScoreSortBuilder()).sort("_doc")));
        BoolQueryBuilder b = QueryBuilders.boolQuery().should(QueryBuilders.termQuery("level", "error")).should(QueryBuilders.rangeQuery("status").gte(500));
        Translation t = translate(b);
        assertTrue(t.scoring().reorders());
        assertSame(InfinoQueryTranslator.Scoring.NONE, t.unscored().scoring());
        assertEquals(t.where(), t.unscored().where());
        assertFalse(hits(t.unscored(), null, 10).contains("infino_bonus"));
    }

    /**
     * A request sorted by _score ascending reads the least sums first when
     * no text clause scores; with one, whose BM25 ranks only its best hits,
     * it is unscored. A descending _score sort keeps the greatest first.
     */
    public void testAnAscendingScoreSortReadsTheLeastSumsFirst() {
        org.opensearch.search.builder.SearchSourceBuilder source = new org.opensearch.search.builder.SearchSourceBuilder();
        assertFalse(InfinoSearch.sortsByScoreAscending(source));
        assertFalse(InfinoSearch.sortsByScoreAscending(new org.opensearch.search.builder.SearchSourceBuilder().sort(new org.opensearch.search.sort.ScoreSortBuilder())));
        assertFalse(InfinoSearch.sortsByScoreAscending(new org.opensearch.search.builder.SearchSourceBuilder().sort("_doc").sort(new org.opensearch.search.sort.ScoreSortBuilder().order(org.opensearch.search.sort.SortOrder.ASC))));
        assertTrue(InfinoSearch.sortsByScoreAscending(new org.opensearch.search.builder.SearchSourceBuilder().sort(new org.opensearch.search.sort.ScoreSortBuilder().order(org.opensearch.search.sort.SortOrder.ASC))));

        BoolQueryBuilder b = QueryBuilders.boolQuery().should(QueryBuilders.termQuery("level", "error")).should(QueryBuilders.rangeQuery("status").gte(500));
        Translation t = translate(b);
        assertTrue(hits(t, null, 10).endsWith(") AS infino_scored ORDER BY score DESC, infino_row ASC LIMIT 10"));
        assertTrue(t.ascending().scoring().ascending());
        assertEquals(t.scoring().bonuses(), t.ascending().scoring().bonuses());
        String sql = hits(t.ascending(), null, 10);
        assertTrue(sql, sql.endsWith(") AS infino_scored ORDER BY score ASC, infino_row ASC LIMIT 10"));

        Translation text = translate(QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("message", "disk")).should(QueryBuilders.termQuery("level", "error")));
        assertNotNull(text.scoring().text());
        assertSame(InfinoQueryTranslator.Scoring.NONE, text.ascending().scoring());
    }

    /** The plan of a translated query, with no sort, collapse, post_filter or search_after. */
    private static InfinoSearch.Plan plan(Translation t) {
        return new InfinoSearch.Plan(TABLE, columns(), t, java.util.List.of(), null, null, null, null, java.util.List.of(), InfinoAccess.NONE, NOW);
    }

    /**
     * A short page is its own total only where no ranked window narrows it:
     * an optional text clause ranks its matches in one, so a filter keeping
     * a hit past the window would leave the page short of the total.
     */
    public void testAShortPageIsItsTotalOnlyWithoutARankedWindow() {
        assertTrue(InfinoSearch.shortPageIsTotal(plan(translate(QueryBuilders.termQuery("level", "error")))));
        BoolQueryBuilder unranked = QueryBuilders.boolQuery().should(QueryBuilders.termQuery("level", "error")).should(QueryBuilders.rangeQuery("status").gte(500));
        assertTrue(InfinoSearch.shortPageIsTotal(plan(translate(unranked))));
        assertFalse(InfinoSearch.shortPageIsTotal(plan(translate(QueryBuilders.matchQuery("message", "disk")))));
        BoolQueryBuilder optionalText = QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("message", "disk")).should(QueryBuilders.termQuery("level", "error"));
        Translation t = translate(optionalText);
        assertNull(t.principal());
        assertFalse(InfinoSearch.shortPageIsTotal(plan(t)));
    }

    /**
     * A page whose short read is its total counts no total beside it: a
     * short page answers with its rows and starts no count; a ranked page
     * still counts beside its rows.
     */
    public void testAPageCountsBesideOnlyWhenAShortPageCannotBeItsTotal() {
        InfinoSearch.Plan filtered = plan(translate(QueryBuilders.termQuery("level", "error")));
        assertFalse(InfinoSearch.countsBeside(filtered, true, true, 10, false));
        InfinoSearch.Plan ranked = plan(translate(QueryBuilders.matchQuery("message", "disk")));
        assertTrue(InfinoSearch.countsBeside(ranked, true, true, 10, false));
        assertFalse(InfinoSearch.countsBeside(ranked, true, true, 10, true));
        assertFalse(InfinoSearch.countsBeside(ranked, false, true, 10, false));
        assertFalse(InfinoSearch.countsBeside(ranked, true, false, 10, false));
        assertFalse(InfinoSearch.countsBeside(ranked, true, true, 0, false));
    }

    // ---- nested ----

    private static final String KIBANA = "os_kibana_0";

    /** A saved-objects table: references are a nested field, flattened to a column (and a list column) per member. */
    private static Map<String, Column> savedObjectColumns() {
        Map<String, Column> cols = new LinkedHashMap<>();
        cols.put("osid", new Column("osid", "str", false, false));
        cols.put("source", new Column("source", "str", false, false));
        cols.put("type", new Column("type", "str", false, false));
        cols.put("namespace", new Column("namespace", "str", false, false));
        cols.put("namespaces", new Column("namespaces", "str", false, false));
        cols.put("title", new Column("title", "str", true, false));
        cols.put("references.name", new Column("references.name", "str", false, false));
        cols.put("references.type", new Column("references.type", "str", false, false));
        cols.put("references.id", new Column("references.id", "str", false, false));
        cols.put("references.weight", new Column("references.weight", "i64", false, false));
        cols.put("references.note", new Column("references.note", "str", true, false));
        cols.put("references.name[]", Column.list("references.name[]", "str"));
        cols.put("references.type[]", Column.list("references.type[]", "str"));
        cols.put("references.id[]", Column.list("references.id[]", "str"));
        cols.put("references.weight[]", Column.list("references.weight[]", "i64"));
        return cols;
    }

    /** Saved objects by id; s2 holds the id and the type has_reference asks for, but in different entries. */
    private static final Map<String, String> SAVED_OBJECTS = savedObjects();

    private static Map<String, String> savedObjects() {
        Map<String, String> docs = new LinkedHashMap<>();
        docs.put("s1", "{\"type\":\"dashboard\",\"references\":[{\"name\":\"panel_0\",\"type\":\"visualization\",\"id\":\"mig-header\",\"weight\":1},{\"name\":\"panel_1\",\"type\":\"visualization\",\"id\":\"other\",\"weight\":5}]}");
        docs.put("s2", "{\"type\":\"dashboard\",\"references\":[{\"name\":\"panel_0\",\"type\":\"index-pattern\",\"id\":\"mig-header\",\"weight\":2},{\"name\":\"panel_1\",\"type\":\"visualization\",\"id\":\"x\",\"weight\":2}]}");
        docs.put("s3", "{\"type\":\"dashboard\",\"references\":[{\"name\":\"panel_0\",\"type\":\"visualization\",\"id\":\"mig-header\",\"weight\":9}]}");
        docs.put("s4", "{\"type\":\"visualization\",\"references\":{\"name\":\"panel_0\",\"type\":\"visualization\",\"id\":\"mig-header\",\"weight\":\"3.7\"}}");
        docs.put("s6", "{\"type\":\"dashboard\",\"references\":[]}");
        docs.put("s7", "{\"type\":\"dashboard\",\"references\":[{\"name\":\"panel_0\",\"type\":\"visualization\",\"id\":\"second\",\"weight\":4},{\"name\":\"panel_1\",\"type\":\"visualization\",\"id\":\"other\",\"weight\":6}]}");
        return docs;
    }

    /**
     * Reads the candidates a nested query asks for: every saved object with a reference, whatever
     * else the narrowing asks, so the entry check alone decides; {@code whole}, every one given.
     */
    private static final class Rows implements java.util.function.Function<String, java.util.List<Map<String, Object>>> {
        final java.util.List<String> statements = new java.util.ArrayList<>();
        final Map<String, String> docs;
        final boolean whole;

        Rows(Map<String, String> docs) {
            this(docs, false);
        }

        Rows(Map<String, String> docs, boolean whole) {
            this.docs = docs;
            this.whole = whole;
        }

        @Override
        public java.util.List<Map<String, Object>> apply(String sql) {
            statements.add(sql);
            java.util.List<Map<String, Object>> out = new java.util.ArrayList<>();
            for (Map.Entry<String, String> doc : docs.entrySet()) {
                if (!whole && (doc.getValue().contains("\"references\":[]") || !doc.getValue().contains("\"references\""))) {
                    continue;
                }
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("osid", doc.getKey());
                row.put("source", doc.getValue());
                out.add(row);
            }
            return out;
        }
    }

    private static Translation translateSaved(org.opensearch.index.query.QueryBuilder qb, Rows rows) {
        return InfinoQueryTranslator.translate(qb, savedObjectColumns(), KIBANA, NOW, rows);
    }

    /** Dashboards' has_reference clause: a nested query on references, the id and the type on one entry. */
    private static org.opensearch.index.query.NestedQueryBuilder hasReference(String type, String id) {
        return QueryBuilders.nestedQuery(
            "references",
            QueryBuilders.boolQuery().must(QueryBuilders.termQuery("references.id", id)).must(QueryBuilders.termQuery("references.type", type)),
            org.apache.lucene.search.join.ScoreMode.Avg
        );
    }

    /**
     * The query Dashboards 3.9 sends for a saved-objects find with has_reference (the
     * relationships of a visualization, or a dashboard's panels): the nested clause beside
     * the type and namespace filters, as the node received it.
     */
    public void testHasReferenceMatchesWithinOneEntry() {
        BoolQueryBuilder typeAndNamespace = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("type", "dashboard"))
            .mustNot(QueryBuilders.existsQuery("namespaces"))
            .should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("namespace")))
            .minimumShouldMatch(1);
        BoolQueryBuilder find = QueryBuilders.boolQuery()
            .filter(QueryBuilders.boolQuery().must(hasReference("visualization", "mig-header")).should(typeAndNamespace).minimumShouldMatch(1));
        Rows rows = new Rows(SAVED_OBJECTS);
        Translation t = translateSaved(find, rows);
        assertNotNull("the has_reference query is served", t);
        assertEquals("\"os_kibana_0\"", t.from());
        // s2 has mig-header and a visualization, in different entries: not a match. s4's one reference is an object, not an array.
        assertEquals(
            "((osid IN ('s1', 's3', 's4') AND ((\"type\" = 'dashboard' AND ((NOT COALESCE((\"namespace\" IS NOT NULL), FALSE)))"
                + " AND NOT COALESCE((\"namespaces\" IS NOT NULL), FALSE)))))",
            t.where()
        );
        // The columns narrow the candidates to documents holding both values anywhere under references.
        assertEquals(1, rows.statements.size());
        assertEquals(
            "SELECT osid, source FROM \"os_kibana_0\" WHERE ((\"references.name\" IS NOT NULL OR \"references.type\" IS NOT NULL OR \"references.id\" IS NOT NULL"
                + " OR \"references.weight\" IS NOT NULL OR \"references.note\" IS NOT NULL OR \"references.name[]\" IS NOT NULL OR \"references.type[]\" IS NOT NULL"
                + " OR \"references.id[]\" IS NOT NULL OR \"references.weight[]\" IS NOT NULL) AND (((\"references.id[]\" IS NULL AND \"references.id\" = 'mig-header')"
                + " OR osid IN (SELECT list_rows.list_id FROM (SELECT list_source.osid AS list_id, unnest(list_source.\"references.id[]\") AS list_value"
                + " FROM \"os_kibana_0\" list_source WHERE list_source.\"references.id[]\" IS NOT NULL) list_rows WHERE list_rows.list_value = 'mig-header'))"
                + " AND ((\"references.type[]\" IS NULL AND \"references.type\" = 'visualization') OR osid IN (SELECT list_rows.list_id FROM (SELECT list_source.osid"
                + " AS list_id, unnest(list_source.\"references.type[]\") AS list_value FROM \"os_kibana_0\" list_source WHERE list_source.\"references.type[]\""
                + " IS NOT NULL) list_rows WHERE list_rows.list_value = 'visualization')))) LIMIT "
                + (InfinoQueryTranslator.NESTED_CANDIDATES_MAX + 1),
            rows.statements.get(0)
        );
    }

    /** has_reference_operator OR and AND over several references: a should or a must of nested clauses, each on an entry of its own. */
    public void testSeveralReferencesAnyOrAll() {
        BoolQueryBuilder any = QueryBuilders.boolQuery()
            .should(hasReference("visualization", "mig-header"))
            .should(hasReference("visualization", "second"))
            .minimumShouldMatch(1);
        assertEquals("((osid IN ('s1', 's3', 's4') OR osid IN ('s7')))", translateSaved(any, new Rows(SAVED_OBJECTS)).where());
        BoolQueryBuilder all = QueryBuilders.boolQuery().must(hasReference("visualization", "mig-header")).must(hasReference("visualization", "other"));
        assertEquals("(osid IN ('s1', 's3', 's4') AND osid IN ('s1', 's7'))", translateSaved(all, new Rows(SAVED_OBJECTS)).where());
        // No entry is a dashboard: nothing, with no rows read past the narrowing.
        assertEquals("1=0", translateSaved(hasReference("dashboard", "mig-header"), new Rows(SAVED_OBJECTS)).where());
    }

    /** Negation, ranges, terms, exists and match within one entry, a long read as the field indexes it. */
    public void testEntryClauses() {
        // A visualization other than mig-header weighing at least 2: s1's "other" (5), s2's "x" (2), s7's two.
        org.opensearch.index.query.QueryBuilder q = QueryBuilders.nestedQuery(
            "references",
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("references.type", "visualization"))
                .filter(QueryBuilders.rangeQuery("references.weight").gte(2))
                .mustNot(QueryBuilders.termQuery("references.id", "mig-header")),
            org.apache.lucene.search.join.ScoreMode.None
        );
        Rows rows = new Rows(SAVED_OBJECTS);
        assertEquals("osid IN ('s1', 's2', 's7')", translateSaved(q, rows).where());
        // The negation narrows nothing: the candidates are those with a visualization and a weight past 2 anywhere.
        assertFalse(rows.statements.get(0), rows.statements.get(0).contains("COALESCE"));
        // s4's weight "3.7" is a long field's 3.
        q = QueryBuilders.nestedQuery("references", QueryBuilders.termQuery("references.weight", 3), org.apache.lucene.search.join.ScoreMode.None);
        assertEquals("osid IN ('s4')", translateSaved(q, new Rows(SAVED_OBJECTS)).where());
        q = QueryBuilders.nestedQuery(
            "references",
            QueryBuilders.boolQuery().must(QueryBuilders.termsQuery("references.id", "x", "second")).must(QueryBuilders.matchQuery("references.name", "panel_0")),
            org.apache.lucene.search.join.ScoreMode.None
        );
        assertEquals("osid IN ('s7')", translateSaved(q, new Rows(SAVED_OBJECTS)).where());
        q = QueryBuilders.nestedQuery("references", QueryBuilders.existsQuery("references.weight"), org.apache.lucene.search.join.ScoreMode.None);
        assertEquals("osid IN ('s1', 's2', 's3', 's4', 's7')", translateSaved(q, new Rows(SAVED_OBJECTS)).where());
        // A field outside the path is no field of an entry, as in stock's nested documents.
        q = QueryBuilders.nestedQuery("references", QueryBuilders.termQuery("type", "dashboard"), org.apache.lucene.search.join.ScoreMode.None);
        assertEquals("1=0", translateSaved(q, new Rows(SAVED_OBJECTS)).where());
        // A path with no field under it is unmapped: nothing.
        q = QueryBuilders.nestedQuery("missing", QueryBuilders.termQuery("missing.id", "a"), org.apache.lucene.search.join.ScoreMode.None);
        Rows none = new Rows(SAVED_OBJECTS);
        assertEquals("1=0", translateSaved(q, none).where());
        assertTrue(none.statements.isEmpty());
    }

    /** What the entry check cannot read is refused with the clause named, never answered over the flattened values. */
    public void testUnservableNestedShapesAreRefused() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> translateSaved(
                QueryBuilders.nestedQuery("references", QueryBuilders.wildcardQuery("references.id", "mig-*"), org.apache.lucene.search.join.ScoreMode.None),
                new Rows(SAVED_OBJECTS)
            )
        );
        assertTrue(e.getMessage(), e.getMessage().contains("[wildcard] inside the nested query on [references] is not served"));
        e = expectThrows(
            IllegalArgumentException.class,
            () -> translateSaved(
                QueryBuilders.nestedQuery("references", QueryBuilders.matchQuery("references.note", "disk"), org.apache.lucene.search.join.ScoreMode.None),
                new Rows(SAVED_OBJECTS)
            )
        );
        assertTrue(e.getMessage(), e.getMessage().contains("[match] on the text field [references.note] inside the nested query on [references]"));
        e = expectThrows(
            IllegalArgumentException.class,
            () -> translateSaved(
                hasReference("visualization", "mig-header").innerHit(new org.opensearch.index.query.InnerHitBuilder()),
                new Rows(SAVED_OBJECTS)
            )
        );
        assertTrue(e.getMessage(), e.getMessage().contains("inner_hits on the nested query on [references] is not served"));
        // Past the candidate cap: refused rather than read whole.
        Map<String, String> many = new LinkedHashMap<>();
        for (int i = 0; i <= InfinoQueryTranslator.NESTED_CANDIDATES_MAX; i++) {
            many.put("d" + i, SAVED_OBJECTS.get("s3"));
        }
        e = expectThrows(IllegalArgumentException.class, () -> translateSaved(hasReference("visualization", "mig-header"), new Rows(many)));
        assertTrue(e.getMessage(), e.getMessage().contains("documents to check entry by entry"));
        // A candidate whose source holds no entries cannot be checked.
        e = expectThrows(
            IllegalArgumentException.class,
            () -> translateSaved(hasReference("visualization", "mig-header"), new Rows(Map.of("s9", "{\"type\":\"dashboard\"}"), true))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("document [s9] has values under [references] its source does not hold"));
    }

    // ---- nested at depth ----

    /** Orders whose items are nested and whose items' tags are nested inside them, flattened to a column per member. */
    private static Map<String, Column> orderColumns() {
        Map<String, Column> cols = new LinkedHashMap<>();
        cols.put("osid", new Column("osid", "str", false, false));
        cols.put("source", new Column("source", "str", false, false));
        cols.put("items.sku", new Column("items.sku", "str", false, false));
        cols.put("items.tags.name", new Column("items.tags.name", "str", false, false));
        cols.put("items.tags.color", new Column("items.tags.color", "str", false, false));
        cols.put("items.sku[]", Column.list("items.sku[]", "str"));
        cols.put("items.tags.name[]", Column.list("items.tags.name[]", "str"));
        cols.put("items.tags.color[]", Column.list("items.tags.color[]", "str"));
        return cols;
    }

    /**
     * o1's item a has a tag named size that is red. o2's item a has a size tag and a red tag, but not one
     * tag that is both. o3 has the red size tag, but on item b, not on item a.
     */
    private static Map<String, String> orders() {
        Map<String, String> docs = new LinkedHashMap<>();
        docs.put("o1", "{\"items\":[{\"sku\":\"a\",\"tags\":[{\"name\":\"size\",\"color\":\"red\"},{\"name\":\"fit\",\"color\":\"blue\"}]}]}");
        docs.put("o2", "{\"items\":[{\"sku\":\"a\",\"tags\":[{\"name\":\"size\",\"color\":\"blue\"},{\"name\":\"fit\",\"color\":\"red\"}]}]}");
        docs.put("o3", "{\"items\":[{\"sku\":\"a\",\"tags\":[{\"name\":\"fit\",\"color\":\"blue\"}]},{\"sku\":\"b\",\"tags\":[{\"name\":\"size\",\"color\":\"red\"}]}]}");
        return docs;
    }

    private static org.opensearch.index.query.NestedQueryBuilder redSizeTag() {
        return QueryBuilders.nestedQuery(
            "items.tags",
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("items.tags.name", "size"))
                .must(QueryBuilders.termQuery("items.tags.color", "red")),
            org.apache.lucene.search.join.ScoreMode.None
        );
    }

    /**
     * A nested query inside a nested query holds every clause on one inner entry, and that entry inside the
     * one outer entry its siblings hold on: not a name and a color from two tags (o2), nor an inner match
     * on an item the outer clause does not hold on (o3).
     */
    public void testNestedUnderNestedMatchesOneEntryAtEachLevel() {
        org.opensearch.index.query.QueryBuilder q = QueryBuilders.nestedQuery("items", redSizeTag(), org.apache.lucene.search.join.ScoreMode.None);
        assertEquals("osid IN ('o1', 'o3')", InfinoQueryTranslator.translate(q, orderColumns(), "os_orders_0", NOW, new Rows(orders(), true)).where());
        q = QueryBuilders.nestedQuery(
            "items",
            QueryBuilders.boolQuery().must(QueryBuilders.termQuery("items.sku", "a")).must(redSizeTag()),
            org.apache.lucene.search.join.ScoreMode.None
        );
        assertEquals("osid IN ('o1')", InfinoQueryTranslator.translate(q, orderColumns(), "os_orders_0", NOW, new Rows(orders(), true)).where());
        // A must_not of the inner nested query holds on every item without a red size tag: o1's has one, o2's does not, o3's item a does not.
        q = QueryBuilders.nestedQuery(
            "items",
            QueryBuilders.boolQuery().must(QueryBuilders.termQuery("items.sku", "a")).mustNot(redSizeTag()),
            org.apache.lucene.search.join.ScoreMode.None
        );
        assertEquals("osid IN ('o2', 'o3')", InfinoQueryTranslator.translate(q, orderColumns(), "os_orders_0", NOW, new Rows(orders(), true)).where());
    }

    /**
     * A path three levels deep is read through objects, arrays and dotted keys alike, an entry at a time:
     * p2 has the rel and the href asked for in two links, and p3 writes its parent as a dotted key.
     */
    public void testNestedPathSeveralLevelsDeep() {
        Map<String, Column> cols = new LinkedHashMap<>();
        cols.put("osid", new Column("osid", "str", false, false));
        cols.put("source", new Column("source", "str", false, false));
        cols.put("site.meta.links.rel", new Column("site.meta.links.rel", "str", false, false));
        cols.put("site.meta.links.href", new Column("site.meta.links.href", "str", false, false));
        cols.put("site.meta.links.rel[]", Column.list("site.meta.links.rel[]", "str"));
        cols.put("site.meta.links.href[]", Column.list("site.meta.links.href[]", "str"));
        Map<String, String> docs = new LinkedHashMap<>();
        docs.put("p1", "{\"site\":{\"meta\":{\"links\":[{\"rel\":\"next\",\"href\":\"a\"},{\"rel\":\"prev\",\"href\":\"b\"}]}}}");
        docs.put("p2", "{\"site\":{\"meta\":{\"links\":[{\"rel\":\"next\",\"href\":\"b\"},{\"rel\":\"prev\",\"href\":\"a\"}]}}}");
        docs.put("p3", "{\"site.meta\":{\"links\":[{\"rel\":\"next\",\"href\":\"a\"}]}}");
        org.opensearch.index.query.QueryBuilder q = QueryBuilders.nestedQuery(
            "site.meta.links",
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("site.meta.links.rel", "next"))
                .must(QueryBuilders.termQuery("site.meta.links.href", "a")),
            org.apache.lucene.search.join.ScoreMode.None
        );
        assertEquals("osid IN ('p1', 'p3')", InfinoQueryTranslator.translate(q, cols, "os_pages_0", NOW, new Rows(docs, true)).where());
    }

    /**
     * A nested clause scores its documents by their matching entries, read by
     * its score_mode: has_reference's pair of terms scores 2.0 on the entry
     * that holds both, and a sum over the entries adds each one's. Read once,
     * though the clause both filters and scores.
     */
    public void testANestedClauseScoresItsEntries() {
        Rows rows = new Rows(SAVED_OBJECTS);
        BoolQueryBuilder b = QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("title", "panels")).should(hasReference("visualization", "mig-header"));
        Translation t = translateSaved(b, rows);
        assertEquals(1, rows.statements.size());
        assertEquals(new InfinoQueryTranslator.TextClause("title", "panels", "or"), t.scoring().text());
        assertEquals(1, t.scoring().bonuses().size());
        assertEquals("CASE WHEN osid IN ('s1', 's3', 's4') THEN 2.0 ELSE 0.0 END", t.scoring().bonuses().get(0).expr());

        org.opensearch.index.query.NestedQueryBuilder visualizations = QueryBuilders.nestedQuery(
            "references",
            QueryBuilders.termQuery("references.type", "visualization"),
            org.apache.lucene.search.join.ScoreMode.Total
        );
        t = translateSaved(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("title", "panels")).must(visualizations), new Rows(SAVED_OBJECTS));
        assertEquals(
            "CASE WHEN osid IN ('s2', 's3', 's4') THEN 1.0 WHEN osid IN ('s1', 's7') THEN 2.0 ELSE 0.0 END",
            t.scoring().bonuses().get(0).expr()
        );
        // score_mode none: the clause filters and scores nothing.
        visualizations = QueryBuilders.nestedQuery(
            "references",
            QueryBuilders.termQuery("references.type", "visualization"),
            org.apache.lucene.search.join.ScoreMode.None
        );
        t = translateSaved(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("title", "panels")).must(visualizations), new Rows(SAVED_OBJECTS));
        assertSame(InfinoQueryTranslator.Scoring.NONE, t.scoring());
    }

    /** Every document has an _id, _index, _seq_no and _version; _routing is the routed ones, kept by a default-mode table only. */
    public void testExistsOnAMetadataField() {
        for (String field : new String[] { "_id", "_index", "_seq_no", "_version" }) {
            assertEquals("1=1", translate(QueryBuilders.existsQuery(field)).where());
        }
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> translate(QueryBuilders.existsQuery("_routing")));
        assertEquals("[infino] exists on [_routing] is not served: the table keeps no routing values", e.getMessage());
        Map<String, Column> routed = new LinkedHashMap<>(columns());
        routed.put("osrouting", new Column("osrouting", "str", false, false));
        assertEquals("\"osrouting\" IS NOT NULL", InfinoQueryTranslator.translate(QueryBuilders.existsQuery("_routing"), routed, TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches).where());
        // _source cannot be searched, as stock says.
        QueryShardException source = expectThrows(QueryShardException.class, () -> translate(QueryBuilders.existsQuery("_source")));
        assertEquals("The _source field is not searchable", source.getMessage());
    }

    /** A multi_match whose last term is a prefix is refused, as match_bool_prefix is, never answered as a plain match. */
    public void testPrefixMultiMatchIsRefused() {
        for (MultiMatchQueryBuilder.Type type : new MultiMatchQueryBuilder.Type[] { MultiMatchQueryBuilder.Type.BOOL_PREFIX, MultiMatchQueryBuilder.Type.PHRASE_PREFIX }) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> translate(QueryBuilders.boolQuery().should(QueryBuilders.multiMatchQuery("disk fu", "message").type(type)))
            );
            assertEquals("[infino] multi_match of type [" + type.parseField().getPreferredName() + "] is not served", e.getMessage());
        }
        assertNotNull(translate(QueryBuilders.multiMatchQuery("disk full", "message").type(MultiMatchQueryBuilder.Type.BEST_FIELDS)));
    }

    /** case_insensitive compares a string field's values lowercased; on a field that is not a string it is refused. */
    public void testCaseInsensitiveQueriesOnAStringField() {
        assertEquals("lower(\"level\") = lower('ErRor')", translate(QueryBuilders.termQuery("level", "ErRor").caseInsensitive(true)).where());
        assertEquals("lower(\"level\") LIKE lower('Er%')", translate(QueryBuilders.prefixQuery("level", "Er").caseInsensitive(true)).where());
        assertEquals("lower(\"level\") LIKE lower('E_r%')", translate(QueryBuilders.wildcardQuery("level", "E?r*").caseInsensitive(true)).where());
        assertEquals("regexp_like(\"level\", '^(?:Er.*)$', 'i')", translate(QueryBuilders.regexpQuery("level", "Er.*").caseInsensitive(true)).where());
        assertEquals("regexp_like(\"level\", '^(?:Er.*)$')", translate(QueryBuilders.regexpQuery("level", "Er.*")).where());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> translate(QueryBuilders.termQuery("status", "5").caseInsensitive(true))
        );
        assertEquals("[infino] a case_insensitive term on [status], which is not a string field, is not served", e.getMessage());
    }

    /** A backslash in a wildcard escapes the character after it: {@code \*} is a star, not any run. */
    public void testAnEscapedWildcardCharacterIsLiteral() {
        assertEquals("\"level\" LIKE '*'", translate(QueryBuilders.wildcardQuery("level", "\\*")).where());
        assertEquals("\"level\" LIKE '\\\\*'", translate(QueryBuilders.wildcardQuery("level", "\\\\\\*")).where());
        assertEquals("\"level\" LIKE 'a\\%%'", translate(QueryBuilders.wildcardQuery("level", "a%*")).where());
        assertEquals("a?b%", InfinoQueryTranslator.escapedWildcardToLike("a\\?b*"));
    }

    /** A plan of {@code t} under {@code sorts} and {@code searchAfter} (null for none). */
    private static InfinoSearch.Plan sortedPlan(Translation t, java.util.List<InfinoSearch.SortSpec> sorts, Object[] searchAfter) {
        return new InfinoSearch.Plan(TABLE, columns(), t, sorts, null, null, null, searchAfter, java.util.List.of(), InfinoAccess.NONE, NOW);
    }

    /** {@code _score} beside a field sort is a sort of its own, in its place; alone, or beside {@code _doc}, it orders nothing. */
    public void testAScoreBesideAFieldIsASort() {
        java.util.List<org.opensearch.search.sort.SortBuilder<?>> mixed = java.util.List.of(
            new org.opensearch.search.sort.ScoreSortBuilder(),
            org.opensearch.search.sort.SortBuilders.fieldSort("status")
        );
        java.util.List<InfinoSearch.SortSpec> sorts = InfinoSearch.sorts(mixed, columns(), java.util.Set.of(), NOW, true);
        assertEquals(2, sorts.size());
        assertTrue(sorts.get(0).isScore());
        assertTrue(sorts.get(0).desc());
        assertTrue(sorts.get(0).noneMissing());
        assertEquals("status", sorts.get(1).name());
        // Where no hit carries its score among its sort values, the score is no sort.
        assertEquals(java.util.List.of("status"), InfinoSearch.sorts(mixed, columns(), NOW).stream().map(InfinoSearch.SortSpec::name).toList());
        assertTrue(InfinoSearch.sorts(java.util.List.of(new org.opensearch.search.sort.ScoreSortBuilder()), columns(), java.util.Set.of(), NOW, true).isEmpty());
        assertTrue(
            InfinoSearch.sorts(
                java.util.List.of(new org.opensearch.search.sort.ScoreSortBuilder(), org.opensearch.search.sort.SortBuilders.fieldSort("_doc")),
                columns(),
                java.util.Set.of(),
                NOW,
                true
            ).isEmpty()
        );
        // The score orders no column: the plain sorted statement leaves it out.
        assertEquals(java.util.List.of("\"status\" ASC NULLS LAST"), InfinoSearch.orderBy(sorts));
    }

    /** Each hit hands out a sort value for {@code _score} beside a field, as stock's does, and none for it alone. */
    public void testTheScoreBesideAFieldIsAHandedOutSortValue() {
        org.opensearch.search.builder.SearchSourceBuilder source = new org.opensearch.search.builder.SearchSourceBuilder();
        assertEquals(0, InfinoSearch.requestedSortValues(source.sort(new org.opensearch.search.sort.ScoreSortBuilder())));
        assertEquals(0, InfinoSearch.requestedSortValues(source.sort("_doc")));
        assertEquals(2, InfinoSearch.requestedSortValues(new org.opensearch.search.builder.SearchSourceBuilder().sort(new org.opensearch.search.sort.ScoreSortBuilder()).sort("status")));
        assertEquals(2, InfinoSearch.requestedSortValues(new org.opensearch.search.builder.SearchSourceBuilder().sort("status").sort(new org.opensearch.search.sort.ScoreSortBuilder())));
        // A scroll's later pages keep the score beside the field, and whether the hits carry their scores.
        org.opensearch.search.builder.SearchSourceBuilder scroll = InfinoSearch.scrollSource(
            new org.opensearch.search.builder.SearchSourceBuilder().sort(new org.opensearch.search.sort.ScoreSortBuilder()).sort("status").trackScores(true)
        );
        assertEquals(2, scroll.sorts().size());
        assertTrue(scroll.sorts().get(0) instanceof org.opensearch.search.sort.ScoreSortBuilder);
        assertTrue(scroll.trackScores());
    }

    /**
     * A scored query sorted by the score then a field ranks a window of
     * candidates by its score and orders them by the score, then the field,
     * then the id; each hit carries the score, and the least score a cut
     * window holds.
     */
    public void testAScoredQuerySortedByScoreThenFieldIsRanked() {
        java.util.List<InfinoSearch.SortSpec> sorts = java.util.List.of(
            InfinoSearch.SortSpec.score(true),
            InfinoSearch.SortSpec.of(columns().get("status"), false),
            InfinoSearch.SortSpec.rowId()
        );
        InfinoSearch.Plan plan = sortedPlan(translate(QueryBuilders.matchQuery("message", "disk")), sorts, null);
        assertTrue(plan.ranksByScore());
        String sql = InfinoSearch.hitStatement(plan, 10, false);
        assertTrue(sql, sql.startsWith("SELECT osid, \"status\", \"_id\", score, MAX(infino_band_floor) OVER () AS infino_floor FROM ("));
        assertTrue(sql, sql.contains(", 10000, 'or')"));
        assertTrue(sql, sql.contains(", 0 AS infino_band"));
        assertTrue(sql, sql.contains("COUNT(*) OVER (PARTITION BY infino_band) >= 10000"));
        assertTrue(sql, sql.endsWith(" ORDER BY score DESC, \"status\" ASC NULLS LAST, \"_id\" ASC NULLS LAST LIMIT 10"));
        // The source is read by id once the page is chosen.
        assertFalse(sql, sql.startsWith("SELECT osid, source"));
        String wide = InfinoSearch.hitStatement(plan, 10, false, InfinoSearch.Keyset.WHOLE, 50_000);
        assertTrue(wide, wide.contains(", 50000, 'or')"));
        assertTrue(wide, wide.contains(">= 50000"));
    }

    /** A query whose hits all score alike, sorted by the score beside a field, is ordered by the field alone. */
    public void testAnUnscoredQuerySortedByScoreAndFieldIsOrderedByTheField() {
        java.util.List<InfinoSearch.SortSpec> sorts = java.util.List.of(
            InfinoSearch.SortSpec.score(true),
            InfinoSearch.SortSpec.of(columns().get("status"), false),
            InfinoSearch.SortSpec.rowId()
        );
        InfinoSearch.Plan plan = sortedPlan(translate(QueryBuilders.matchAllQuery()), sorts, null);
        assertFalse(plan.ranksByScore());
        String sql = InfinoSearch.hitStatement(plan, 10, false);
        assertFalse(sql, sql.contains("score"));
        assertTrue(sql, sql.endsWith(" ORDER BY \"status\" ASC NULLS LAST, \"_id\" ASC NULLS LAST LIMIT 10"));
    }

    /**
     * Resuming past a score every hit shares: no hit is past it in the sort's
     * direction unless the value is beyond the constant, and every hit ties
     * with it where it is the constant.
     */
    public void testASearchAfterTheConstantScoreComparesItWithTheConstant() {
        float constant = InfinoSearch.constantScore(0);
        java.util.List<InfinoSearch.SortSpec> asc = java.util.List.of(
            InfinoSearch.SortSpec.score(false),
            InfinoSearch.SortSpec.of(columns().get("status"), false),
            InfinoSearch.SortSpec.rowId()
        );
        String sql = InfinoSearch.hitStatement(sortedPlan(translate(QueryBuilders.matchAllQuery()), asc, new Object[] { constant, 200, "5" }), 10, false);
        assertTrue(sql, sql.contains("((1=0) OR (1=1 AND (\"status\" > 200 OR \"status\" IS NULL))"));
        java.util.List<InfinoSearch.SortSpec> desc = java.util.List.of(
            InfinoSearch.SortSpec.score(true),
            InfinoSearch.SortSpec.of(columns().get("status"), false),
            InfinoSearch.SortSpec.rowId()
        );
        sql = InfinoSearch.hitStatement(sortedPlan(translate(QueryBuilders.matchAllQuery()), desc, new Object[] { constant + 1, 200, "5" }), 10, false);
        assertTrue(sql, sql.contains("((1=1) OR (1=0 AND "));
    }

    public void testEveryRowSortedOnAColumnIsTheEnginesSortedSearch() {
        java.util.List<InfinoSearch.SortSpec> byStatus = java.util.List.of(
            InfinoSearch.SortSpec.of(columns().get("status"), true),
            InfinoSearch.SortSpec.rowId()
        );
        assertTrue(InfinoSearch.indexSorted(sortedPlan(QueryBuilders.matchAllQuery(), byStatus, null, null)));
        assertTrue(
            InfinoSearch.indexSorted(
                sortedPlan(
                    QueryBuilders.matchAllQuery(),
                    java.util.List.of(InfinoSearch.SortSpec.of(columns().get("@timestamp"), false), InfinoSearch.SortSpec.rowId()),
                    null,
                    null
                )
            )
        );
        // A predicate or a post_filter narrows the rows: the hit statement.
        assertFalse(InfinoSearch.indexSorted(sortedPlan(QueryBuilders.termQuery("level", "error"), byStatus, null, null)));
        assertFalse(InfinoSearch.indexSorted(sortedPlan(QueryBuilders.matchAllQuery(), byStatus, "\"level\" = 'error'", null)));
        // So does a text query, a page after given sort values, and a sort on the row id alone.
        assertFalse(InfinoSearch.indexSorted(sortedPlan(QueryBuilders.matchQuery("message", "error"), byStatus, null, null)));
        assertFalse(InfinoSearch.indexSorted(sortedPlan(QueryBuilders.matchAllQuery(), byStatus, null, new Object[] { 404, "7" })));
        assertFalse(
            InfinoSearch.indexSorted(
                sortedPlan(QueryBuilders.matchAllQuery(), java.util.List.of(InfinoSearch.SortSpec.rowId()), null, null)
            )
        );
        // missing: _first puts the rows without a value first, which the engine's sorted search does not.
        java.util.List<InfinoSearch.SortSpec> missingFirst = java.util.List.of(
            InfinoSearch.SortSpec.of(columns().get("status"), true).withMissing("_first", NOW),
            InfinoSearch.SortSpec.rowId()
        );
        assertFalse(InfinoSearch.indexSorted(sortedPlan(QueryBuilders.matchAllQuery(), missingFirst, null, null)));
        // A missing value standing in for the rows without one sorts on an expression: the hit statement.
        java.util.List<InfinoSearch.SortSpec> standIn = java.util.List.of(
            InfinoSearch.SortSpec.of(columns().get("status"), true).withMissing(0, NOW),
            InfinoSearch.SortSpec.rowId()
        );
        assertFalse(InfinoSearch.indexSorted(sortedPlan(QueryBuilders.matchAllQuery(), standIn, null, null)));
    }

    /** A plan over {@code query} sorted by {@code sorts}, with {@code postWhere} and continuing after {@code after}. */
    private static InfinoSearch.Plan sortedPlan(
        org.opensearch.index.query.QueryBuilder query,
        java.util.List<InfinoSearch.SortSpec> sorts,
        String postWhere,
        Object[] after
    ) {
        return new InfinoSearch.Plan(
            TABLE,
            columns(),
            translate(query),
            sorts,
            null,
            postWhere,
            null,
            after,
            java.util.List.of(),
            InfinoAccess.NONE,
            NOW
        );
    }

    /** The plan of an unsorted page for {@code t}, over {@code cols}. */
    private static InfinoSearch.Plan unsortedPlan(Translation t, Map<String, Column> cols) {
        return new InfinoSearch.Plan(TABLE, cols, t, List.of(), null, null, null, null, List.of(), InfinoAccess.NONE, NOW);
    }

    private static List<InfinoQueryTranslator.TextClause> clause(String column, String query) {
        return List.of(new InfinoQueryTranslator.TextClause(column, query, "or"));
    }

    public void testEveryMatchClauseOfABoolScores() {
        // Stock sums a bool's scoring clauses: two matches on one field each add their BM25.
        Map<String, Column> cols = columns();
        cols.put("title", new Column("title", "str", true, false));
        BoolQueryBuilder same = QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("message", "disk")).must(QueryBuilders.matchQuery("message", "full"));
        Translation t = InfinoQueryTranslator.translate(same, cols, TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches);
        assertEquals(new InfinoQueryTranslator.TextClause("message", "disk", "or"), t.ranked());
        InfinoQueryTranslator.Across across = t.scoring().across();
        assertNotNull(across);
        assertNull(across.match());
        assertEquals(List.of(clause("message", "disk"), clause("message", "full")), across.clauses());
        assertFalse(t.scoring().reorders());
        // One engine search sums both; the principal's predicate and the second clause's keep the rows both match.
        assertEquals(
            "SELECT osid, score, \"_id\" AS infino_row FROM bm25_search_fields('os_logs_0', "
                + "'[{\"query\":\"disk\",\"fields\":[\"message\"]},{\"query\":\"full\",\"fields\":[\"message\"]}]', 10, 'or')"
                + " WHERE (text_match(\"message\", 'disk', 'or')) AND ((1=1 AND text_match(\"message\", 'full', 'or')))"
                + " ORDER BY score DESC, infino_row ASC LIMIT 10",
            hits(t, null, 10)
        );
        // The engine's own BM25 call alone would rank by the principal: the page is no bare ranking.
        assertFalse(InfinoSearch.bareRanked(unsortedPlan(t, cols)));
        assertTrue(InfinoSearch.ranksAcross(unsortedPlan(t, cols)));

        // On different fields each clause scores its own column.
        BoolQueryBuilder apart = QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("message", "disk")).must(QueryBuilders.matchQuery("title", "full"));
        Translation a = InfinoQueryTranslator.translate(apart, cols, TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches);
        assertEquals(List.of(clause("message", "disk"), clause("title", "full")), a.scoring().across().clauses());
        assertTrue(
            hits(a, null, 10),
            hits(a, null, 10).contains(
                "'[{\"query\":\"disk\",\"fields\":[\"message\"]},{\"query\":\"full\",\"fields\":[\"title\"]}]', 10, 'or')"
                    + " WHERE (text_match(\"message\", 'disk', 'or')) AND ((1=1 AND text_match(\"title\", 'full', 'or')))"
            )
        );

        // A clause repeated scores twice, as stock sums each.
        BoolQueryBuilder twice = QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("message", "disk")).must(QueryBuilders.matchQuery("message", "disk"));
        assertEquals(List.of(clause("message", "disk"), clause("message", "disk")), InfinoQueryTranslator.translate(twice, cols, TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches).scoring().across().clauses());

        // An all-words clause scores in or: its own predicate, still all-words, keeps only the rows with every word.
        BoolQueryBuilder all = QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("message", "disk"))
            .must(QueryBuilders.matchQuery("title", "full error").operator(Operator.AND));
        Translation al = InfinoQueryTranslator.translate(all, cols, TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches);
        assertEquals(List.of(clause("message", "disk"), clause("title", "full error")), al.scoring().across().clauses());
        assertTrue(hits(al, null, 10), hits(al, null, 10).contains("AND ((1=1 AND text_match(\"title\", 'full error', 'and')))"));

        // An optional match scores where it matches; only the principal keeps the rows.
        BoolQueryBuilder optional = QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("message", "disk")).should(QueryBuilders.matchQuery("title", "full"));
        Translation o = InfinoQueryTranslator.translate(optional, cols, TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches);
        assertEquals(List.of(clause("message", "disk"), clause("title", "full")), o.scoring().across().clauses());
        String optionalHits = hits(o, null, 10);
        assertTrue(optionalHits, optionalHits.contains("10, 'or') WHERE (text_match(\"message\", 'disk', 'or')) AND "));
        assertFalse(optionalHits, optionalHits.contains("text_match(\"title\""));

        // A must range adds its constant and filters the ranking.
        BoolQueryBuilder ranged = QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("message", "disk"))
            .must(QueryBuilders.matchQuery("message", "full"))
            .must(QueryBuilders.rangeQuery("status").gte(500));
        Translation r = InfinoQueryTranslator.translate(ranged, cols, TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches);
        assertEquals(1.0, r.scoring().offset(), 0);
        assertEquals(2, r.scoring().across().clauses().size());
        assertTrue(hits(r, null, 10), hits(r, null, 10).contains("(\"status\" >= 500)"));
    }

    public void testMatchClausesScoredOtherwiseLeaveThePrincipalRanking() {
        // A clause the sum would score otherwise than stock leaves the principal's BM25 the score, as before.
        Map<String, Column> cols = columns();
        cols.put("title", new Column("title", "str", true, false));
        for (BoolQueryBuilder b : List.of(
            // A phrase, which the summed search does not read.
            QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("message", "disk")).must(QueryBuilders.matchPhraseQuery("title", "full error")),
            // A boost, which the summed search does not weigh.
            QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("message", "disk")).must(QueryBuilders.matchQuery("title", "full").boost(2f)),
            // A minimum_should_match, which the summed search does not apply.
            QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("message", "disk")).must(QueryBuilders.matchQuery("title", "full error").minimumShouldMatch("2")),
            // An optional all-words clause, which scores only where every word is.
            QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("message", "disk"))
                .should(QueryBuilders.matchQuery("title", "full error").operator(Operator.AND)),
            // An optional term, a constant the summed search does not add.
            QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("message", "disk"))
                .must(QueryBuilders.matchQuery("title", "full"))
                .should(QueryBuilders.termQuery("level", "error")),
            // A must query string, which is no one-column clause.
            QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("message", "disk")).must(QueryBuilders.queryStringQuery("full").field("title")),
            // A boosted inner bool, which weighs its clause's BM25.
            QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("message", "disk"))
                .must(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("title", "full")).boost(3f)),
            // A boosted bool, which weighs the whole sum.
            QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("message", "disk")).must(QueryBuilders.matchQuery("title", "full")).boost(2f)
        )) {
            Translation t = InfinoQueryTranslator.translate(b, cols, TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches);
            assertNull(b.toString(), t.scoring().across());
            assertEquals(b.toString(), new InfinoQueryTranslator.TextClause("message", "disk", "or"), t.ranked());
            assertFalse(b.toString(), InfinoSearch.ranksAcross(unsortedPlan(t, cols)));
        }
        // Two all-words clauses on one column join the principal, whose one search reads both.
        BoolQueryBuilder joined = QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("message", "disk").operator(Operator.AND))
            .must(QueryBuilders.matchQuery("message", "full").operator(Operator.AND));
        Translation j = InfinoQueryTranslator.translate(joined, cols, TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches);
        assertEquals(new InfinoQueryTranslator.TextClause("message", "disk full", "and"), j.ranked());
        assertNull(j.scoring().across());
        // A match in filter context scores nothing: the principal ranks alone.
        BoolQueryBuilder filtered = QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("message", "disk")).filter(QueryBuilders.matchQuery("title", "full"));
        assertNull(InfinoQueryTranslator.translate(filtered, cols, TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches).scoring().across());
        // A sorted page has no score to sum; an ascending score sort reads the hits unscored.
        BoolQueryBuilder same = QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("message", "disk")).must(QueryBuilders.matchQuery("message", "full"));
        Translation t = InfinoQueryTranslator.translate(same, cols, TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches);
        InfinoSearch.Plan sorted = new InfinoSearch.Plan(
            TABLE,
            cols,
            t,
            List.of(InfinoSearch.SortSpec.of(cols.get("status"), true)),
            null,
            null,
            null,
            null,
            List.of(),
            InfinoAccess.NONE,
            NOW
        );
        assertFalse(InfinoSearch.ranksAcross(sorted));
        assertNull(t.ascending().scoring().across());
    }

    private static Map<String, Column> typedColumns() {
        Map<String, Column> cols = columns();
        cols.put("f", new Column("f", "f64", false, false).typed(new InfinoNative.FieldType("float", 0)));
        cols.put("h", new Column("h", "f64", false, false).typed(new InfinoNative.FieldType("half_float", 0)));
        cols.put("s", new Column("s", "f64", false, false).typed(new InfinoNative.FieldType("scaled_float", 100)));
        cols.put("d", new Column("d", "f64", false, false));
        cols.put("ip", new Column("ip", "str", false, false).typed(new InfinoNative.FieldType("ip", 0)));
        return cols;
    }

    private static String typedWhere(org.opensearch.index.query.QueryBuilder qb) {
        return InfinoQueryTranslator.translate(qb, typedColumns(), TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches).where();
    }

    /** A half float's value, rounded to the nearest as the field keeps it. */
    private static double half(float v) {
        return (double) org.apache.lucene.sandbox.document.HalfFloatPoint.sortableShortToHalfFloat(
            org.apache.lucene.sandbox.document.HalfFloatPoint.halfFloatToSortableShort(v)
        );
    }

    /**
     * A float field keeps 0.3 as the 32-bit float nearest it, so a term compares that float and
     * a range's bounds are the first floats within it, as stock reads them; a double field's
     * number is its own.
     */
    public void testFloatLiteralsAreReadAtTheFieldsPrecision() {
        assertEquals("\"f\" = " + (double) 0.3f, typedWhere(QueryBuilders.termQuery("f", 0.3)));
        assertEquals("\"f\" = " + (double) 0.3f, typedWhere(QueryBuilders.termQuery("f", "0.3")));
        assertEquals("\"f\" IN (" + (double) 0.3f + ", 1.5)", typedWhere(QueryBuilders.termsQuery("f", List.of(0.3, 1.5))));
        assertEquals("(\"f\" >= " + (double) Math.nextUp(0.3f) + ")", typedWhere(QueryBuilders.rangeQuery("f").gt(0.3)));
        assertEquals("(\"f\" <= " + (double) 2.7f + ")", typedWhere(QueryBuilders.rangeQuery("f").lte(2.7)));
        assertEquals(
            "(\"f\" >= " + (double) 0.3f + " AND \"f\" <= " + (double) 0.3f + ")",
            typedWhere(QueryBuilders.rangeQuery("f").gte(0.3).lte(0.3))
        );
        assertEquals("(\"f\" <= " + (double) Math.nextDown(0.3f) + ")", typedWhere(QueryBuilders.rangeQuery("f").lt(0.3)));
        // Past the greatest float, nothing; below the least, every value.
        assertEquals("1=0", typedWhere(QueryBuilders.rangeQuery("f").gt(Float.MAX_VALUE)));
        assertEquals("(\"f\" >= 1)", typedWhere(QueryBuilders.rangeQuery("f").gte(1)));
        // A value the field cannot hold has no translation, as text on a double field has none.
        assertNull(InfinoQueryTranslator.translate(QueryBuilders.termQuery("f", "1e39"), typedColumns(), TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches));
        assertNull(InfinoQueryTranslator.translate(QueryBuilders.termQuery("f", "abc"), typedColumns(), TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches));
        assertNull(InfinoQueryTranslator.translate(QueryBuilders.termQuery("d", "abc"), typedColumns(), TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches));
        assertNull(InfinoQueryTranslator.translate(QueryBuilders.rangeQuery("f").gte("abc"), typedColumns(), TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches));
        assertNull(InfinoQueryTranslator.translate(QueryBuilders.rangeQuery("s").lt("abc"), typedColumns(), TABLE, NOW, InfinoQueryTranslatorTests::aRowMatches));
        // Text across a text and a float field reads the float field as it reads a double one: a
        // lenient query skips it, another is refused, as stock fails it.
        assertEquals(
            typedWhere(QueryBuilders.multiMatchQuery("disk", "message", "d").lenient(true)).replace("\"d\"", "\"f\""),
            typedWhere(QueryBuilders.multiMatchQuery("disk", "message", "f").lenient(true))
        );
        expectThrows(IllegalArgumentException.class, () -> typedWhere(QueryBuilders.multiMatchQuery("disk", "message", "f")));
        assertEquals("\"d\" = 0.3", typedWhere(QueryBuilders.termQuery("d", 0.3)));
        assertEquals("(\"d\" > 0.3)", typedWhere(QueryBuilders.rangeQuery("d").gt(0.3)));
    }

    /** A half_float field keeps the nearest 16-bit float: a term compares it, a range's bounds are the first halves within it. */
    public void testHalfFloatLiteralsAreReadAtTheFieldsPrecision() {
        assertEquals("\"h\" = " + half(0.3f), typedWhere(QueryBuilders.termQuery("h", 0.3)));
        // 0.3 is kept as its nearest half, which lies above 0.3: gte and gt 0.3 admit it, lte 0.3 does not.
        assertEquals("(\"h\" >= " + half(0.3f) + ")", typedWhere(QueryBuilders.rangeQuery("h").gte(0.3)));
        assertEquals("(\"h\" >= " + half(0.3f) + ")", typedWhere(QueryBuilders.rangeQuery("h").gt(0.3)));
        assertEquals(
            "(\"h\" <= " + (double) org.apache.lucene.sandbox.document.HalfFloatPoint.nextDown(0.3f) + ")",
            typedWhere(QueryBuilders.rangeQuery("h").lte(0.3))
        );
        // 1.5 is a half: gt 1.5 starts at the next one, lte 1.5 ends at it.
        assertEquals(
            "(\"h\" >= " + (double) org.apache.lucene.sandbox.document.HalfFloatPoint.nextUp(1.5f) + ")",
            typedWhere(QueryBuilders.rangeQuery("h").gt(1.5))
        );
        assertEquals("(\"h\" <= 1.5)", typedWhere(QueryBuilders.rangeQuery("h").lte(1.5)));
    }

    /** A scaled_float field keeps multiples of 1/scaling_factor: a term compares the rounded value, a range the first multiples within it. */
    public void testScaledFloatLiteralsAreReadAtTheFieldsPrecision() {
        assertEquals("\"s\" = 1.23", typedWhere(QueryBuilders.termQuery("s", 1.234)));
        assertEquals("\"s\" = 1.24", typedWhere(QueryBuilders.termQuery("s", 1.235)));
        assertEquals("(\"s\" >= 1.24)", typedWhere(QueryBuilders.rangeQuery("s").gt(1.234)));
        assertEquals("(\"s\" >= 1.24)", typedWhere(QueryBuilders.rangeQuery("s").gt(1.23)));
        assertEquals("(\"s\" >= 1.23)", typedWhere(QueryBuilders.rangeQuery("s").gte(1.23)));
        assertEquals("(\"s\" <= 1.24)", typedWhere(QueryBuilders.rangeQuery("s").lt(1.25)));
        // A bound is rounded to the nearest multiple first, so gte 1.234 admits the 1.23 that term 1.234 finds.
        assertEquals("(\"s\" >= 1.23)", typedWhere(QueryBuilders.rangeQuery("s").gte(1.234)));
        assertEquals("(\"s\" >= 1.24)", typedWhere(QueryBuilders.rangeQuery("s").gte(1.235)));
        assertEquals("(\"s\" <= 1.23)", typedWhere(QueryBuilders.rangeQuery("s").lte(1.234)));
        assertEquals("(\"s\" <= 1.26)", typedWhere(QueryBuilders.rangeQuery("s").lte(1.259)));
    }

    /** A CIDR block in a term on an ip field is the addresses it covers, a prefix of their text; an IPv6 block is refused. */
    public void testCidrTermsOnAnIpField() {
        assertEquals("\"ip\" LIKE '10.%'", typedWhere(QueryBuilders.termQuery("ip", "10.0.0.0/8")));
        assertEquals("\"ip\" LIKE '10.%'", typedWhere(QueryBuilders.termQuery("ip", "10.9.8.7/8")));
        assertEquals("\"ip\" LIKE '192.168.%'", typedWhere(QueryBuilders.termQuery("ip", "192.168.0.0/16")));
        assertEquals("\"ip\" = '192.168.1.9'", typedWhere(QueryBuilders.termQuery("ip", "192.168.1.9/32")));
        assertEquals("\"ip\" NOT LIKE '%:%'", typedWhere(QueryBuilders.termQuery("ip", "0.0.0.0/0")));
        StringBuilder sixteen = new StringBuilder();
        for (int i = 0; i < 16; i++) {
            sixteen.append(i == 0 ? "" : "|").append(i);
        }
        assertEquals("regexp_like(\"ip\", '^10\\.(?:" + sixteen + ")\\.')", typedWhere(QueryBuilders.termQuery("ip", "10.1.2.3/12")));
        assertEquals(
            "regexp_like(\"ip\", '^192\\.168\\.1\\.(?:8|9|10|11)$')",
            typedWhere(QueryBuilders.termQuery("ip", "192.168.1.9/30"))
        );
        assertEquals(
            "(\"ip\" LIKE '10.%' OR \"ip\" = '192.168.1.9')",
            typedWhere(QueryBuilders.termsQuery("ip", List.of("10.0.0.0/8", "192.168.1.9")))
        );
        assertEquals("\"ip\" = '10.0.0.1'", typedWhere(QueryBuilders.termQuery("ip", "10.0.0.1")));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> typedWhere(QueryBuilders.termQuery("ip", "2001:db8::/32"))
        );
        assertEquals("[infino] a term with the IPv6 CIDR block [2001:db8::/32] on [ip] is not served", e.getMessage());
        expectThrows(IllegalArgumentException.class, () -> typedWhere(QueryBuilders.termQuery("ip", "10.0.0.0/33")));
        // On a keyword field the text is the value.
        assertEquals("\"level\" = 'a/8'", typedWhere(QueryBuilders.termQuery("level", "a/8")));
    }

    /** A match asking for fuzziness is refused, naming it; a fuzziness of 0 asks for none and is served. */
    public void testFuzzyMatchIsRefused() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> translate(QueryBuilders.matchQuery("message", "dsk").fuzziness(org.opensearch.common.unit.Fuzziness.AUTO))
        );
        assertEquals("[infino] match on [message] asks for fuzziness [AUTO], which is not served", e.getMessage());
        expectThrows(
            IllegalArgumentException.class,
            () -> translate(QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("level", "eror").fuzziness(org.opensearch.common.unit.Fuzziness.ONE)))
        );
        e = expectThrows(
            IllegalArgumentException.class,
            () -> translate(QueryBuilders.multiMatchQuery("dsk", "message").fuzziness(org.opensearch.common.unit.Fuzziness.ONE))
        );
        assertEquals("[infino] multi_match on [message] asks for fuzziness [1], which is not served", e.getMessage());
        assertEquals(
            translate(QueryBuilders.matchQuery("message", "disk")).where(),
            translate(QueryBuilders.matchQuery("message", "disk").fuzziness(org.opensearch.common.unit.Fuzziness.ZERO)).where()
        );
    }
}
