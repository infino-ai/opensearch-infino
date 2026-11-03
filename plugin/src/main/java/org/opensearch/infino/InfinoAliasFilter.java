/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.infino.InfinoNative.Column;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Set;

/**
 * What the filtering aliases a request reached an index through keep of it,
 * as stock OpenSearch applies them to a search: any one alias's filter (a
 * document two aliases reach is kept when either keeps it), and nothing
 * when the request also reaches the index whole, by its name, a pattern or
 * an alias without a filter.
 *
 * <p>An alias's filter is checked against the index's table before the
 * search reads anything: one the table cannot answer (a geo, nested or span
 * query, say) is refused as a limit of the alias's filter, naming the
 * alias, never served as a rule the search would then drop, and never
 * mistaken for a document-level security rule the user does not have.
 */
final class InfinoAliasFilter {

    private InfinoAliasFilter() {}

    /**
     * The filter on {@code index} (whose table is {@code table}) for a
     * request naming {@code expressions}; null when it reads the index
     * whole. {@code nowMillis} is the time date math in a filter reads.
     */
    static QueryBuilder of(
        ClusterState state,
        IndexNameExpressionResolver resolver,
        IndexMetadata index,
        String[] expressions,
        String table,
        long nowMillis
    ) {
        Set<String> resolved = resolver.resolveExpressions(state, expressions);
        String[] aliases = resolver.filteringAliases(state, index.getIndex().getName(), resolved);
        if (aliases == null || aliases.length == 0) {
            return null;
        }
        QueryBuilder[] filters = new QueryBuilder[aliases.length];
        for (int i = 0; i < aliases.length; i++) {
            filters[i] = filterOf(index, aliases[i]);
            if (filters[i] == null) {
                return null; // one alias that filters nothing reads the index whole
            }
        }
        Map<String, Column> columns = InfinoNative.columns(table);
        if (aliases.length == 1) {
            return served(filters[0], aliases[0], columns, table, nowMillis);
        }
        BoolQueryBuilder any = QueryBuilders.boolQuery().minimumShouldMatch(1);
        for (int i = 0; i < aliases.length; i++) {
            any.should(served(filters[i], aliases[i], columns, table, nowMillis));
        }
        return any;
    }

    /**
     * {@code filter}, the filter of {@code alias}, when the table answers it; refused otherwise.
     *
     * <p>The filter is translated here only to check it, over every column of the table, and
     * translated again where the search reads it ({@link InfinoAccess#documentsWhere}), over the
     * columns the user may name: under a field rule, a field hidden from the user is unmapped there
     * and matches nothing. The two are different predicates for such a user, so the one made here
     * is never kept for the search.
     */
    private static QueryBuilder served(QueryBuilder filter, String alias, Map<String, Column> columns, String table, long nowMillis) {
        if (InfinoQueryTranslator.rulePredicate(filter, columns, table, nowMillis) == null) {
            throw new IllegalArgumentException(
                "[infino] the filter of alias [" + alias + "] uses a query an Infino-backed index does not serve in an alias filter: " + filter
            );
        }
        return filter;
    }

    /** The filter of {@code alias} on {@code index}, as the cluster stores it; null when it has none. */
    private static QueryBuilder filterOf(IndexMetadata index, String alias) {
        AliasMetadata am = index.getAliases().get(alias);
        if (am == null || am.filter() == null) {
            return null;
        }
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                InfinoServices.namedXContentRegistry(),
                LoggingDeprecationHandler.INSTANCE,
                am.filter().string()
            )
        ) {
            return AbstractQueryBuilder.parseInnerQueryBuilder(parser);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
