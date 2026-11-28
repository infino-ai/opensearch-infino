/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.infino.InfinoAggregationProcessor.Source;
import org.opensearch.infino.InfinoNative.Column;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.test.OpenSearchTestCase;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The {@code _index} metadata field over a table: one value, the name of the
 * index the table backs, which a query matches on every row or none and an
 * aggregation reads as that value. Never read as an unmapped field, which
 * would match nothing and count 0.
 */
public class InfinoIndexFieldTests extends OpenSearchTestCase {

    /** The index served from {@link #TABLE}. */
    private static final String INDEX = "logs_0";
    private static final String TABLE = "os_logs_0";
    private static final long NOW = 0L;

    private static Map<String, Column> columns() {
        Map<String, Column> cols = new LinkedHashMap<>();
        cols.put("osid", new Column("osid", "str", false, false));
        cols.put("service", new Column("service", "str", false, false));
        cols.put("msg", new Column("msg", "str", true, false));
        return cols;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        IndexMetadata md = IndexMetadata.builder(INDEX)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_INDEX_UUID, "u1")
                    .put(InfinoPlugin.INFINO_ENABLED.getKey(), true)
                    .put(InfinoPlugin.INFINO_MODE.getKey(), InfinoPlugin.MODE_PROXY)
            )
            .build();
        assertEquals(List.of(TABLE), InfinoTables.tablesOf(md));
        InfinoPlugin.tables().observe(Metadata.builder().put(md, false).build());
    }

    @Override
    public void tearDown() throws Exception {
        InfinoPlugin.tables().observe(Metadata.EMPTY_METADATA);
        super.tearDown();
    }

    private static String where(QueryBuilder qb) {
        return InfinoQueryTranslator.translate(qb, columns(), TABLE, NOW).where();
    }

    public void testATermOnTheFieldMatchesEveryRowOrNone() {
        assertEquals("1=1", where(QueryBuilders.termQuery("_index", INDEX)));
        assertEquals("1=0", where(QueryBuilders.termQuery("_index", "logs_1")));
        assertEquals("1=1", where(QueryBuilders.termQuery("_index", "LOGS_0").caseInsensitive(true)));
        assertEquals("1=0", where(QueryBuilders.termQuery("_index", "LOGS_0")));
        assertEquals("1=1", where(QueryBuilders.termsQuery("_index", "logs_1", INDEX)));
        assertEquals("1=0", where(QueryBuilders.termsQuery("_index", "logs_1", "logs_2")));
        assertEquals("1=1", where(QueryBuilders.prefixQuery("_index", "logs")));
        assertEquals("1=1", where(QueryBuilders.wildcardQuery("_index", "l*_0")));
        assertEquals("1=0", where(QueryBuilders.wildcardQuery("_index", "metrics*")));
        assertEquals("1=1", where(QueryBuilders.matchQuery("_index", INDEX)));
        assertEquals("1=1", where(QueryBuilders.matchPhraseQuery("_index", INDEX)));
        assertEquals("(1=1 AND \"service\" = 'payments')", where(QueryBuilders.queryStringQuery("_index:logs_0 AND service:payments")));
        assertEquals("1=0", where(QueryBuilders.queryStringQuery("_index:metrics*")));
        // Beside other clauses, and negated.
        assertEquals(
            "(1=1 AND \"service\" = 'payments')",
            where(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("_index", INDEX)).filter(QueryBuilders.termQuery("service", "payments")))
        );
        assertEquals("(NOT COALESCE((1=1), FALSE))", where(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("_index", INDEX))));
        // A filters bucket or an aggregation's filter reads it the same way.
        assertEquals("1=1", InfinoQueryTranslator.predicate(QueryBuilders.termQuery("_index", INDEX), columns(), TABLE, NOW));
    }

    public void testAnyOtherQueryOnTheFieldIsRefused() {
        for (QueryBuilder qb : List.<QueryBuilder>of(
            QueryBuilders.rangeQuery("_index").gte("a"),
            QueryBuilders.regexpQuery("_index", "logs.*"),
            QueryBuilders.queryStringQuery("_index:/logs.*/")
        )) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> where(qb));
            assertTrue(e.getMessage(), e.getMessage().startsWith("[infino] "));
        }
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> where(QueryBuilders.matchQuery("_index", INDEX).fuzziness("AUTO")));
        assertTrue(e.getMessage(), e.getMessage().contains("asks for fuzziness"));
    }

    /**
     * A wildcard on fields that hold {@code _index}, as a query string reads
     * one without a field: on the field, the index's name matched against the
     * pattern; on the others, as before. It used to be refused there.
     */
    public void testAWildcardOverFieldsHoldingTheFieldMatchesTheIndexName() {
        assertEquals("1=1", where(QueryBuilders.queryStringQuery("logs*").defaultField("_index")));
        assertEquals("1=0", where(QueryBuilders.queryStringQuery("metrics*").defaultField("_index")));
        assertEquals("(1=0 OR regexp_like(\"msg\", '\\bsl[^\\s]*', 'i'))", where(QueryBuilders.queryStringQuery("sl*").field("msg").field("_index")));
        assertEquals("(1=1 OR regexp_like(\"msg\", '\\blogs_[^\\s]*', 'i'))", where(QueryBuilders.queryStringQuery("logs_*").field("msg").field("_index")));
        String onKeyword = where(QueryBuilders.queryStringQuery("sl*").field("service").field("_index"));
        assertTrue(onKeyword, onKeyword.startsWith("(1=0 OR "));
    }

    /** A script reading {@code doc['_index']} is refused: no column holds it, and read as unmapped it would be null where stock reads the name. */
    public void testAScriptReadingTheFieldIsRefused() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> where(QueryBuilders.scriptQuery(new Script("doc['_index'].value == 'logs_0'")))
        );
        assertEquals("[infino] script [doc['_index'].value == 'logs_0'] reads doc['_index'], which is not served", e.getMessage());
    }

    public void testATableNoIndexIsServedFromIsRefusedNotReadAsUnmapped() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> InfinoQueryTranslator.translate(QueryBuilders.termQuery("_index", INDEX), columns(), "os_gone", NOW)
        );
        assertEquals(
            "[infino] a query or aggregation on [_index] is not served: no index of the cluster is served from table [os_gone]",
            e.getMessage()
        );
        // Exists needs no name: every document has the field.
        assertEquals("1=1", InfinoQueryTranslator.translate(QueryBuilders.existsQuery("_index"), columns(), "os_gone", NOW).where());
    }

    public void testAnAggregationReadsTheIndexName() {
        Source terms = InfinoAggregationProcessor.Spec.sourceOf(AggregationBuilders.terms("t").field("_index"), columns(), TABLE, NOW);
        assertEquals("'logs_0'", terms.expr());
        assertNull(terms.column());
        assertEquals("'logs_0'", InfinoAggregationProcessor.Spec.sourceOf(AggregationBuilders.count("c").field("_index"), columns(), TABLE, NOW).expr());
        assertEquals("'logs_0'", InfinoAggregationProcessor.Spec.sourceOf(AggregationBuilders.cardinality("d").field("_index"), columns(), TABLE, NOW).expr());
        // Over several shards' tables read as one, each row's own index, from the union's column.
        Map<String, Column> union = new LinkedHashMap<>(columns());
        union.put("_infino_index", new Column("_infino_index", "str", false, false));
        assertEquals("\"_infino_index\"", InfinoAggregationProcessor.Spec.sourceOf(AggregationBuilders.terms("t").field("_index"), union, TABLE, NOW).expr());
    }

    public void testAnAggregationThatCannotReadTheNameIsRefused() {
        InfinoAggregationProcessor.Unsupported e = expectThrows(
            InfinoAggregationProcessor.Unsupported.class,
            () -> InfinoAggregationProcessor.Spec.sourceOf(AggregationBuilders.sum("s").field("_index"), columns(), TABLE, NOW)
        );
        assertEquals("[infino] aggregation [s] of type [sum] on [_index] is not served", e.getMessage());
        e = expectThrows(
            InfinoAggregationProcessor.Unsupported.class,
            () -> InfinoAggregationProcessor.Spec.sourceOf(
                AggregationBuilders.terms("t").field("_index").script(new Script("_value")),
                columns(),
                TABLE,
                NOW
            )
        );
        assertEquals("[infino] aggregation [t] has a script beside [_index], which is not served", e.getMessage());
    }
}
