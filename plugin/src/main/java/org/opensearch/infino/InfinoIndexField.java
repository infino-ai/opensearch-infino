/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.regex.Regex;
import org.opensearch.core.common.Strings;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.index.query.SearchIndexNameMatcher;

/**
 * The {@code _index} metadata field over a table. A table holds the rows of
 * one index (see {@link InfinoTables}), so the field has one value over all
 * of them, the index's name, as stock's has one over a shard: a query on it
 * matches every row of the table or none, and an aggregation reads it as that
 * one value. A search over several indices (a data stream's backing indices,
 * a pattern) reads each index's table on its own, so each answers with its
 * own name, and the answers add up as stock's shards' do.
 */
final class InfinoIndexField {

    /** The field's name. */
    static final String NAME = IndexFieldMapper.NAME;

    private InfinoIndexField() {}

    /**
     * The name of the index {@code table} backs. Refused when no live index
     * is served from it: the field's value is not known, and reading it as
     * no value would answer a count of 0.
     */
    static String indexOf(String table) {
        InfinoTables tables = InfinoPlugin.tables();
        String index = tables == null ? null : tables.indexOf(table);
        if (index == null) {
            throw new IllegalArgumentException(
                "[infino] a query or aggregation on [" + NAME + "] is not served: no index of the cluster is served from table [" + table + "]"
            );
        }
        return index;
    }

    /**
     * Whether {@code value}, given to a term, terms, prefix or wildcard on
     * {@code _index}, matches the rows of {@code table}, as stock's field
     * matches a shard's documents: the value is an index expression (a name,
     * an alias, a data stream, a wildcard pattern) and matches when the index
     * the table backs is one of the open indices it resolves to. A
     * case-insensitive query lower-cases the value first, as stock does (an
     * index name is lower case). With no cluster at hand (a unit test) the
     * value is matched against the name alone, as a pattern.
     */
    static boolean matches(String table, String value, boolean caseInsensitive) {
        String index = indexOf(table);
        String pattern = caseInsensitive ? Strings.toLowercaseAscii(value) : value;
        ClusterService clusters = InfinoServices.clusterService();
        IndexNameExpressionResolver resolver = InfinoServices.indexNameExpressionResolver();
        if (clusters == null || resolver == null) {
            return Regex.simpleMatch(pattern, index);
        }
        // Stock's own matcher, for the local cluster (no cluster alias).
        return new SearchIndexNameMatcher(index, "", clusters, resolver).test(pattern);
    }
}
