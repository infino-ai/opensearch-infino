/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.infino.InfinoNative.Column;
import org.opensearch.infino.InfinoSearch.VectorRequest;
import org.opensearch.test.OpenSearchTestCase;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The k-NN plugin's {@code knn} query as the table reads it, through a
 * stand-in builder that renders and exposes its filter the way the k-NN
 * plugin's own builder does (the suites run the real one against a cluster).
 */
public class InfinoStockKnnTests extends OpenSearchTestCase {

    private static final String TABLE = "os_items";
    private static final long NOW = 0L;

    /** Renders as the k-NN plugin's builder does: {@code {"knn":{field:{vector,k,filter?,extra?,boost}}}}. */
    public static final class StandIn extends AbstractQueryBuilder<StandIn> {
        private final String field;
        private final float[] vector;
        private final int k;
        private final QueryBuilder filter;
        private final String extra;

        StandIn(String field, float[] vector, int k, QueryBuilder filter, String extra) {
            this.field = field;
            this.vector = vector;
            this.k = k;
            this.filter = filter;
            this.extra = extra;
        }

        public QueryBuilder getFilter() {
            return filter;
        }

        @Override
        protected void doWriteTo(StreamOutput out) {}

        @Override
        protected void doXContent(XContentBuilder b, Params params) throws java.io.IOException {
            b.startObject(InfinoStockKnn.NAME);
            b.startObject(field);
            b.array("vector", vector);
            b.field("k", k);
            if (filter != null) {
                b.field("filter", filter);
            }
            if (extra != null) {
                b.field(extra, 0.5f);
            }
            b.field(BOOST_FIELD.getPreferredName(), boost());
            b.endObject();
            b.endObject();
        }

        @Override
        protected Query doToQuery(QueryShardContext context) {
            return new MatchNoDocsQuery();
        }

        @Override
        protected boolean doEquals(StandIn o) {
            return k == o.k && Objects.equals(field, o.field);
        }

        @Override
        protected int doHashCode() {
            return Objects.hash(field, k);
        }

        @Override
        public String getWriteableName() {
            return InfinoStockKnn.NAME;
        }
    }

    private static Map<String, Column> columns() {
        Map<String, Column> cols = new LinkedHashMap<>();
        cols.put("osid", new Column("osid", "str", false, false));
        cols.put("tag", new Column("tag", "str", false, false));
        cols.put("title", new Column("title", "str", true, false));
        cols.put("n", new Column("n", "i64", false, false));
        cols.put("emb", new Column("emb", "vec", false, true));
        return cols;
    }

    public void testReadsFieldVectorKAndFilter() {
        QueryBuilder filter = QueryBuilders.termQuery("tag", "x");
        InfinoStockKnn.Query q = InfinoStockKnn.read(new StandIn("emb", new float[] { 1f, 0.5f }, 3, filter, null));
        assertEquals("emb", q.field());
        assertArrayEquals(new float[] { 1f, 0.5f }, q.vector(), 0f);
        assertEquals(3, q.k());
        assertSame(filter, q.filter());
        assertNull(InfinoStockKnn.read(new StandIn("emb", new float[] { 1f }, 2, null, null)).filter());
    }

    public void testRefusesWhatTheTableDoesNotServe() {
        for (String param : new String[] { "max_distance", "min_score", "rescore" }) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> InfinoStockKnn.read(new StandIn("emb", new float[] { 1f }, 2, null, param))
            );
            assertTrue(e.getMessage(), e.getMessage().contains("[" + param + "]"));
        }
        StandIn boosted = new StandIn("emb", new float[] { 1f }, 2, null, null);
        boosted.boost(2f);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> InfinoStockKnn.read(boosted));
        assertTrue(e.getMessage(), e.getMessage().contains("boost"));
    }

    public void testFilterIsAColumnPredicate() {
        String where = InfinoQueryTranslator.vectorFilter(
            QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("tag", "x")).filter(QueryBuilders.rangeQuery("n").gte(2)),
            columns(),
            TABLE,
            NOW
        );
        assertEquals("(\"tag\" = 'x' AND (\"n\" >= 2))", where);
    }

    public void testATextMatchInTheFilterIsRefusedByName() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> InfinoQueryTranslator.vectorFilter(QueryBuilders.matchQuery("title", "wool"), columns(), TABLE, NOW)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("[title]"));
    }

    public void testAKnnInsideAnotherClauseIsRefused() {
        QueryBuilder nested = QueryBuilders.boolQuery().must(new StandIn("emb", new float[] { 1f }, 2, null, null));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> InfinoQueryTranslator.translate(nested, columns(), TABLE, NOW)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("[knn]"));
        QueryBuilder ours = QueryBuilders.boolQuery().must(new InfinoKnnQueryBuilder("emb", new float[] { 1f }, 2));
        e = expectThrows(IllegalArgumentException.class, () -> InfinoQueryTranslator.translate(ours, columns(), TABLE, NOW));
        assertTrue(e.getMessage(), e.getMessage().contains("[infino_knn]"));
    }

    public void testScoresAreTheKnnPluginsForEachSpaceType() {
        // l2: the engine's squared distance, 1 / (1 + d).
        assertEquals(1.0f, InfinoStockKnn.score(InfinoStockKnn.L2, 0f), 0f);
        assertEquals(1.0f / 1.13f, InfinoStockKnn.score(InfinoStockKnn.L2, 0.13f), 1e-6f);
        // innerproduct: the engine's negated product; product + 1 when not negative, else 1 / (1 - product).
        assertEquals(3.0f, InfinoStockKnn.score(InfinoStockKnn.INNER_PRODUCT, -2f), 0f);
        assertEquals(1.0f / 1.5f, InfinoStockKnn.score(InfinoStockKnn.INNER_PRODUCT, 0.5f), 1e-6f);
        // cosine: the engine's 1 - similarity, (1 + similarity) / 2.
        assertEquals(1.0f, InfinoStockKnn.score(InfinoStockKnn.COSINE, 0f), 0f);
        assertEquals(0.0f, InfinoStockKnn.score(InfinoStockKnn.COSINE, 2f), 0f);
    }

    public void testSpaceTypeIsReadAsTheBridgeReadsIt() {
        Map<String, Object> emb = Map.of("type", "knn_vector", "dimension", 4, "method", Map.of("space_type", "l2"));
        Map<String, Object> top = Map.of("type", "knn_vector", "dimension", 4, "space_type", "innerproduct");
        Map<String, Object> bare = Map.of("type", "knn_vector", "dimension", 4);
        MappingMetadata mapping = new MappingMetadata(
            "_doc",
            Map.of("properties", Map.of("emb", emb, "top", top, "bare", bare, "doc", Map.of("properties", Map.of("emb", emb))))
        );
        assertEquals("l2", InfinoStockKnn.spaceType(mapping, "emb"));
        assertEquals("l2", InfinoStockKnn.spaceType(mapping, "doc.emb"));
        assertEquals("innerproduct", InfinoStockKnn.spaceType(mapping, "top"));
        assertEquals(InfinoStockKnn.COSINE, InfinoStockKnn.spaceType(mapping, "bare"));
        assertEquals(InfinoStockKnn.COSINE, InfinoStockKnn.spaceType(null, "emb"));
    }

    public void testFilteredStatementRanksAmongTheKeptRows() {
        VectorRequest v = new VectorRequest(false, null, null, "emb", new float[] { 1f, 0.5f }, 3, true, "\"tag\" = 'x'");
        assertEquals(
            "SELECT osid, score FROM vector_search('os_items', 'emb', '1.0,0.5', 3) WHERE \"tag\" = 'x' ORDER BY score ASC LIMIT 3",
            InfinoSearch.filteredKnnStatement(TABLE, v)
        );
    }
}
