/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.infino.InfinoNative.Column;
import org.opensearch.infino.InfinoQueryTranslator.Translation;
import org.opensearch.test.OpenSearchTestCase;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/** The SQL a query-DSL tree translates to, over a log table's columns. */
public class InfinoQueryTranslatorTests extends OpenSearchTestCase {

    private static final String TABLE = "os_logs_0";
    private static final long NOW = Instant.parse("2026-10-24T12:00:00Z").toEpochMilli();

    private static Map<String, Column> columns() {
        Map<String, Column> cols = new LinkedHashMap<>();
        cols.put("osid", new Column("osid", "str", false, false));
        cols.put("@timestamp", new Column("@timestamp", "ts", false, false));
        cols.put("level", new Column("level", "str", false, false));
        cols.put("message", new Column("message", "str", true, false));
        cols.put("status", new Column("status", "i64", false, false));
        cols.put("host.name", new Column("host.name", "str", false, false));
        return cols;
    }

    private static Translation translate(org.opensearch.index.query.QueryBuilder qb) {
        return InfinoQueryTranslator.translate(qb, columns(), TABLE, NOW);
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
        assertTrue(where, where.contains("token_match('os_logs_0', 'message', 'error', 'and')"));
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

    public void testEveryRowSortedOnAColumnIsTheEnginesSortedSearch() {
        InfinoSearch.Plan all = new InfinoSearch.Plan(
            TABLE,
            columns(),
            translate(QueryBuilders.matchAllQuery()),
            java.util.List.of(InfinoSearch.SortSpec.of(columns().get("status"), true), InfinoSearch.SortSpec.rowId()),
            null,
            null,
            null,
            null,
            java.util.List.of(),
            InfinoAccess.NONE,
            NOW
        );
        assertTrue(InfinoSearch.indexSorted(all));
        // Its sources are looked up by id once the page is read, not read with it.
        assertTrue(InfinoSearch.sourcesById(all));
        // A predicate narrows the rows: the hit statement.
        InfinoSearch.Plan filtered = new InfinoSearch.Plan(
            TABLE,
            columns(),
            translate(QueryBuilders.termQuery("level", "error")),
            all.sorts(),
            null,
            null,
            null,
            null,
            java.util.List.of(),
            InfinoAccess.NONE,
            NOW
        );
        assertFalse(InfinoSearch.indexSorted(filtered));
        // So does a page after given sort values, and a sort on the row id alone.
        InfinoSearch.Plan after = new InfinoSearch.Plan(
            TABLE,
            columns(),
            translate(QueryBuilders.matchAllQuery()),
            all.sorts(),
            null,
            null,
            null,
            new Object[] { 404, "7" },
            java.util.List.of(),
            InfinoAccess.NONE,
            NOW
        );
        assertFalse(InfinoSearch.indexSorted(after));
        InfinoSearch.Plan byRowId = new InfinoSearch.Plan(
            TABLE,
            columns(),
            translate(QueryBuilders.matchAllQuery()),
            java.util.List.of(InfinoSearch.SortSpec.rowId()),
            null,
            null,
            null,
            null,
            java.util.List.of(),
            InfinoAccess.NONE,
            NOW
        );
        assertFalse(InfinoSearch.indexSorted(byRowId));
    }

    /** A match_all sorted by {@code status} descending, then by the row id, continuing after {@code after}, under {@code collapse}. */
    private static InfinoSearch.Plan afterPlan(Object[] after, Column collapse) {
        return new InfinoSearch.Plan(
            TABLE,
            columns(),
            translate(QueryBuilders.matchAllQuery()),
            java.util.List.of(InfinoSearch.SortSpec.of(columns().get("status"), true), InfinoSearch.SortSpec.rowId()),
            null,
            null,
            collapse,
            after,
            java.util.List.of(),
            InfinoAccess.NONE,
            NOW
        );
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
                + "WHERE (1=1 AND \"level\" = 'error' AND (\"status\" >= 500)) ORDER BY score DESC LIMIT 10",
            hits(translate(b), null, 10)
        );
        // No filter: the page, as before.
        assertEquals(
            "SELECT osid, source, score FROM bm25_search('os_logs_0', 'message', 'disk', 10, 'or') WHERE 1=1 ORDER BY score DESC LIMIT 10",
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

    public void testRankedSearchKeepsTheWindowWhereRowsGoAfterTheRanking() {
        // A text clause on another column is a subquery joined above the
        // ranked search, so the search ranks more candidates than the page.
        Map<String, Column> cols = columns();
        cols.put("title", new Column("title", "str", true, false));
        BoolQueryBuilder b = QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("message", "disk"))
            .filter(QueryBuilders.matchQuery("title", "outage"));
        Translation t = InfinoQueryTranslator.translate(b, cols, TABLE, NOW);
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

    public void testTextUnderShouldIsASubquery() {
        BoolQueryBuilder b = QueryBuilders.boolQuery()
            .should(QueryBuilders.matchQuery("message", "disk"))
            .should(QueryBuilders.termQuery("level", "error"));
        Translation t = translate(b);
        assertNull(t.principal());
        assertEquals("\"os_logs_0\"", t.from());
        assertEquals("((osid IN (SELECT osid FROM token_match('os_logs_0', 'message', 'disk', 'or')) OR \"level\" = 'error'))", t.where());
    }

    public void testShouldIsOptionalBesideMust() {
        BoolQueryBuilder b = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("level", "error"))
            .should(QueryBuilders.termQuery("status", 500));
        assertEquals("(\"level\" = 'error')", translate(b).where());
        b.minimumShouldMatch(1);
        assertEquals("(\"level\" = 'error' AND (\"status\" = 500))", translate(b).where());
    }

    public void testQueryStringOverEveryTextColumn() {
        assertEquals("1=1", translate(QueryBuilders.queryStringQuery("*")).where());
        Translation t = translate(QueryBuilders.queryStringQuery("disk"));
        assertEquals("token_match('os_logs_0', 'message', 'disk', 'or')", t.from());
    }

    public void testQueryStringClausesReadAsLuceneReadsThem() {
        // Terms on the default fields go to the engine as one query with Lucene's occurrences as sigils.
        assertEquals("token_match('os_logs_0', 'message', '+disk +cache', 'or')", translate(QueryBuilders.queryStringQuery("disk AND cache")).from());
        assertEquals("token_match('os_logs_0', 'message', 'disk cache -full', 'or')", translate(QueryBuilders.queryStringQuery("disk cache -full")).from());
        assertEquals(
            "token_match('os_logs_0', 'message', '+disk +cache', 'or')",
            translate(QueryBuilders.queryStringQuery("disk cache").defaultOperator(org.opensearch.index.query.Operator.AND)).from()
        );
        // Field clauses are predicates; AND makes both sides required.
        assertEquals("(\"level\" = 'error' AND \"status\" = 500)", translate(QueryBuilders.queryStringQuery("level:error AND status:500")).where());
        // A group of terms stays one engine query; a prohibited field clause is a NOT.
        Translation t = translate(QueryBuilders.queryStringQuery("(disk OR cache) AND NOT level:info"));
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
        assertEquals("regexp_like(\"message\", '\\bdis[^\\s]*', 'i')", translate(QueryBuilders.queryStringQuery("dis*")).where());
        assertEquals("\"host.name\" LIKE 'n%'", translate(QueryBuilders.queryStringQuery("host.name:n*")).where());
        assertEquals("regexp_like(\"message\", '\\b(?:d.sk)\\b', 'i')", translate(QueryBuilders.queryStringQuery("message:/d.sk/")).where());
        assertEquals("regexp_like(\"level\", '^(?:err.*)$')", translate(QueryBuilders.queryStringQuery("level:/err.*/")).where());
        // Boosts change nothing; fuzziness and proximity are refused with the clause named; so is a bad string.
        assertEquals("token_match('os_logs_0', 'message', '+disk +cache', 'or')", translate(QueryBuilders.queryStringQuery("disk^2 AND cache")).from());
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
        String match = "osid IN (SELECT osid FROM token_match('os_logs_0', 'message', 'disk', 'or'))";
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
        Translation t = InfinoQueryTranslator.translate(b, cols, TABLE, NOW);
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
        assertSame(InfinoQueryTranslator.Scoring.NONE, translate(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("level", "error"))).scoring());
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
        assertSame(InfinoQueryTranslator.Scoring.NONE, translate(b).scoring());
        b = QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("message", "disk")).should(QueryBuilders.queryStringQuery("level:error OR status:500"));
        assertSame(InfinoQueryTranslator.Scoring.NONE, translate(b).scoring());
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
        assertSame(InfinoQueryTranslator.Scoring.NONE, InfinoQueryTranslator.translate(b, cols, TABLE, NOW, counts(new java.util.ArrayList<>(), 5, 3)).scoring());

        Map<String, Column> orders = orderColumns();
        orders.put("items.gift", new Column("items.gift", "bool", false, false));
        b = QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("items.sku", "a"))
            .should(QueryBuilders.nestedQuery("items", QueryBuilders.termQuery("items.gift", true), org.apache.lucene.search.join.ScoreMode.Avg));
        assertSame(
            InfinoQueryTranslator.Scoring.NONE,
            InfinoQueryTranslator.translate(b, orders, "os_orders_0", NOW, new Rows(orders(), true)).scoring()
        );
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
}
