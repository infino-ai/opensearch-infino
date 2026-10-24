/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.index.query.BoolQueryBuilder;
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

    public void testDateRangeUsesDateMath() {
        Translation t = translate(QueryBuilders.rangeQuery("@timestamp").gte("now-15m").lte("now"));
        assertEquals("(\"@timestamp\" >= '2026-10-24T11:45:00Z' AND \"@timestamp\" <= '2026-10-24T12:00:00Z')", t.where());
        // An exclusive bound on a day rounds past the day.
        t = translate(QueryBuilders.rangeQuery("@timestamp").gt("2026-10-23"));
        assertEquals("(\"@timestamp\" > '2026-10-23T23:59:59.999Z')", t.where());
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
        assertEquals(
            "(1=1 AND osid IN (SELECT osid FROM token_match('os_logs_0', 'message', 'node 3', 'and')) AND \"level\" = 'error')",
            t.where()
        );
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

    public void testMatchOnAKeywordIsAnEquality() {
        Translation t = translate(QueryBuilders.matchQuery("level", "error"));
        assertNull(t.principal());
        assertEquals("\"level\" = 'error'", t.where());
    }

    public void testUnknownFieldMatchesNothingAndUnknownShapeIsNull() {
        assertEquals("1=0", translate(QueryBuilders.termQuery("nope", 1)).where());
        assertNull(translate(QueryBuilders.fuzzyQuery("message", "dsk")));
    }
}
