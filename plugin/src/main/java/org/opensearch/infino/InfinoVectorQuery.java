/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import java.util.Arrays;
import java.util.Objects;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

/**
 * Marker Lucene query carrying a kNN or hybrid request for Infino. It is never
 * executed by Lucene: {@link InfinoQueryPhaseSearcher} recognizes it, routes to
 * Infino {@code vector_search} / {@code hybrid_search}, and fills the result. If
 * it ever reaches Lucene execution (e.g. nested where the Infino phase can't see
 * it), {@link #createWeight} fails loudly rather than returning wrong hits.
 */
public final class InfinoVectorQuery extends Query {
    final boolean hybrid;
    final String textColumn; // hybrid only
    final String textQuery;  // hybrid only
    final String vectorColumn;
    final float[] vector;
    final int k;

    InfinoVectorQuery(boolean hybrid, String textColumn, String textQuery, String vectorColumn, float[] vector, int k) {
        this.hybrid = hybrid;
        this.textColumn = textColumn;
        this.textQuery = textQuery;
        this.vectorColumn = vectorColumn;
        this.vector = vector;
        this.k = k;
    }

    @Override
    public String toString(String field) {
        return (hybrid ? "infino_hybrid" : "infino_knn")
            + "{col=" + vectorColumn + ",dim=" + (vector == null ? 0 : vector.length) + ",k=" + k + "}";
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        throw new IllegalStateException("InfinoVectorQuery must be served by the Infino query phase, not Lucene");
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof InfinoVectorQuery q)) return false;
        return hybrid == q.hybrid && k == q.k
            && Objects.equals(textColumn, q.textColumn)
            && Objects.equals(textQuery, q.textQuery)
            && Objects.equals(vectorColumn, q.vectorColumn)
            && Arrays.equals(vector, q.vector);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hybrid, textColumn, textQuery, vectorColumn, k, Arrays.hashCode(vector));
    }
}
