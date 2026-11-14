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
        // A predicate's rows in no order may come from a range index; a sort, or every row, does not.
        InfinoSearch.Plan ranged = new InfinoSearch.Plan(
            TABLE,
            columns(),
            translate(QueryBuilders.rangeQuery("status").gte(500).lt(600)),
            java.util.List.of(),
            null,
            null,
            null,
            null,
            java.util.List.of(),
            InfinoAccess.NONE,
            NOW
        );
        assertTrue(InfinoSearch.intervalPage(ranged));
        assertFalse(InfinoSearch.intervalPage(filtered));
        InfinoSearch.Plan everyRow = new InfinoSearch.Plan(
            TABLE,
            columns(),
            translate(QueryBuilders.matchAllQuery()),
            java.util.List.of(),
            null,
            null,
            null,
            null,
            java.util.List.of(),
            InfinoAccess.NONE,
            NOW
        );
        assertFalse(InfinoSearch.intervalPage(everyRow));
        // Only a query of ranges alone can be one interval: a term beside one, or a should, is not asked for.
        assertTrue(InfinoSearch.onlyRanges(QueryBuilders.rangeQuery("status").gte(500)));
        assertTrue(InfinoSearch.onlyRanges(QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("status").gte(500)).filter(QueryBuilders.rangeQuery("status").lt(600))));
        assertFalse(InfinoSearch.onlyRanges(QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("status").gte(500)).must(QueryBuilders.termQuery("level", "error"))));
        assertFalse(InfinoSearch.onlyRanges(QueryBuilders.boolQuery().should(QueryBuilders.rangeQuery("status").gte(500))));
        assertFalse(InfinoSearch.onlyRanges(QueryBuilders.termQuery("level", "error")));
        assertFalse(InfinoSearch.onlyRanges(null));
        // A page after one present value of a column sort may come from its range index.
        InfinoSearch.Plan afterOne = new InfinoSearch.Plan(
            TABLE,
            columns(),
            translate(QueryBuilders.matchAllQuery()),
            all.sorts(),
            null,
            null,
            null,
            new Object[] { 404 },
            java.util.List.of(),
            InfinoAccess.NONE,
            NOW
        );
        assertTrue(InfinoSearch.afterInterval(afterOne));
        // Two values (the tiebreak's too) make the rows past them an OR, and no search_after is no page after one.
        assertFalse(InfinoSearch.afterInterval(after));
        assertFalse(InfinoSearch.afterInterval(all));
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
}
