/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

/**
 * The k-NN plugin's own {@code knn} query, read for the table without
 * compiling against the k-NN plugin.
 * <pre>{"query":{"knn":{"embedding":{"vector":[...],"k":10,"filter":{...}}}}}</pre>
 *
 * <p>The {@code knn_vector} mapping type is the k-NN plugin's, so where an
 * Infino-backed index has vectors the k-NN plugin runs, and its query
 * reaches the search filter already parsed into the k-NN plugin's builder.
 * The field, vector and k are read from the builder's own rendering, which
 * is the query DSL itself and the same in every release; the filter is
 * taken as the builder holds it, through its public {@code getFilter()}, so
 * it is the parsed query the plugin's translator reads everywhere else.
 *
 * <p>What the rendering carries beyond those (a radial search's
 * {@code max_distance} / {@code min_score}, {@code method_parameters},
 * {@code rescore}, {@code expand_nested_docs}, {@code ignore_unmapped}, a
 * boost, a query name) is refused with the parameter named: each changes
 * which documents come back or how they score, and the table does none of it.
 */
final class InfinoStockKnn {

    /** The name the k-NN plugin registers its query under. */
    static final String NAME = "knn";
    private static final String VECTOR = "vector";
    private static final String K = "k";
    private static final String FILTER = "filter";
    private static final String BOOST = "boost";
    /** The boost a query carries when the request names none; the rendering always prints it. */
    private static final float DEFAULT_BOOST = 1.0f;
    /** The k-NN builder's public accessor for the filter it parsed. */
    private static final String FILTER_GETTER = "getFilter";
    /** The space types whose distance the engine computes (see the bridge's {@code vector_metric}). */
    static final String L2 = "l2";
    static final String INNER_PRODUCT = "innerproduct";
    static final String COSINE = "cosinesimil";
    /** The space type the bridge declares a vector column with when the mapping names none. */
    private static final String BRIDGE_DEFAULT_SPACE = COSINE;

    /** A {@code knn} query as the table serves it; {@code filter} is null when the query has none. */
    record Query(String field, float[] vector, int k, QueryBuilder filter) {}

    private InfinoStockKnn() {}

    /** Whether {@code qb} is the k-NN plugin's query (this plugin registers no query by that name). */
    static boolean is(QueryBuilder qb) {
        return qb != null && NAME.equals(qb.getWriteableName());
    }

    /** The query's field, vector, k and filter; throws naming a parameter the table does not serve. */
    static Query read(QueryBuilder qb) {
        Map<String, Object> rendered = render(qb);
        Object body = rendered.get(NAME);
        if (!(body instanceof Map<?, ?> byField) || byField.size() != 1) {
            throw new IllegalArgumentException("[infino] knn query not served by the table: " + qb);
        }
        Map.Entry<?, ?> only = byField.entrySet().iterator().next();
        String field = String.valueOf(only.getKey());
        if (!(only.getValue() instanceof Map<?, ?> params)) {
            throw new IllegalArgumentException("[infino] knn query on [" + field + "] not served by the table: " + qb);
        }
        float[] vector = null;
        Integer k = null;
        boolean filtered = false;
        for (Map.Entry<?, ?> e : params.entrySet()) {
            String name = String.valueOf(e.getKey());
            Object value = e.getValue();
            switch (name) {
                case VECTOR -> vector = floats(value);
                case K -> k = value instanceof Number n ? n.intValue() : null;
                case FILTER -> filtered = value != null;
                case BOOST -> {
                    if (!(value instanceof Number n) || n.floatValue() != DEFAULT_BOOST) {
                        throw new IllegalArgumentException("[infino] knn on [" + field + "] with boost [" + value + "] is not served by the table");
                    }
                }
                default -> throw new IllegalArgumentException(
                    "[infino] knn on [" + field + "] with [" + name + "] is not served by the table; it serves vector, k and filter"
                );
            }
        }
        if (vector == null || vector.length == 0) {
            throw new IllegalArgumentException("[infino] knn on [" + field + "] names no vector");
        }
        if (k == null || k <= 0) {
            throw new IllegalArgumentException("[infino] knn on [" + field + "] needs a positive k");
        }
        QueryBuilder filter = filtered ? filter(qb) : null;
        return new Query(field, vector, k, filter);
    }

    /**
     * The space type of the vector {@code field} in the index's mapping, read
     * as the bridge reads it when it declares the column's metric: the
     * method's {@code space_type}, else the field's own, else the bridge's
     * default. The engine's distances are under that metric.
     */
    static String spaceType(MappingMetadata mapping, String field) {
        if (mapping == null) {
            return BRIDGE_DEFAULT_SPACE;
        }
        Object node = mapping.sourceAsMap();
        for (String part : field.split("\\.")) {
            Object props = node instanceof Map<?, ?> m ? m.get("properties") : null;
            node = props instanceof Map<?, ?> p ? p.get(part) : null;
        }
        if (!(node instanceof Map<?, ?> def)) {
            return BRIDGE_DEFAULT_SPACE;
        }
        Object space = def.get("method") instanceof Map<?, ?> method ? method.get("space_type") : null;
        if (space == null) {
            space = def.get("space_type");
        }
        return space == null ? BRIDGE_DEFAULT_SPACE : space.toString();
    }

    /**
     * The score the k-NN plugin gives a hit at the engine's distance under
     * {@code spaceType}: for {@code l2} the engine's distance is the squared
     * one and the score {@code 1 / (1 + d)}; for {@code innerproduct} the
     * distance is the negated product and the score is the product plus one
     * when it is not negative, else {@code 1 / (1 - product)}; for cosine
     * the distance is one minus the similarity and the score
     * {@code (1 + similarity) / 2}.
     */
    static float score(String spaceType, float distance) {
        return switch (spaceType) {
            case L2 -> 1.0f / (1.0f + distance);
            case INNER_PRODUCT -> distance <= 0 ? 1.0f - distance : 1.0f / (1.0f + distance);
            default -> 1.0f - distance / 2.0f;
        };
    }

    /** The builder as the query DSL it renders to: {@code {"knn": {field: {...}}}}. */
    private static Map<String, Object> render(QueryBuilder qb) {
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            qb.toXContent(b, ToXContent.EMPTY_PARAMS);
            return XContentHelper.convertToMap(BytesReference.bytes(b), true, MediaTypeRegistry.JSON).v2();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The parsed filter the k-NN builder holds. */
    private static QueryBuilder filter(QueryBuilder qb) {
        try {
            Object filter = qb.getClass().getMethod(FILTER_GETTER).invoke(qb);
            if (filter instanceof QueryBuilder f) {
                return f;
            }
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException("[infino] the knn query's filter could not be read from " + qb.getClass().getName(), e);
        }
        throw new IllegalStateException("[infino] the knn query renders a filter its builder does not hold");
    }

    private static float[] floats(Object value) {
        if (!(value instanceof List<?> list)) {
            return null;
        }
        float[] out = new float[list.size()];
        for (int i = 0; i < out.length; i++) {
            if (!(list.get(i) instanceof Number n)) {
                return null;
            }
            out[i] = n.floatValue();
        }
        return out;
    }
}
