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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Set;

/**
 * What the filtering aliases a request reached an index through keep of it,
 * as stock OpenSearch applies them to a search: any one alias's filter (a
 * document two aliases reach is kept when either keeps it), and nothing
 * when the request also reaches the index whole, by its name, a pattern or
 * an alias without a filter.
 */
final class InfinoAliasFilter {

    private InfinoAliasFilter() {}

    /** The filter on {@code index} for a request naming {@code expressions}; null when it reads the index whole. */
    static QueryBuilder of(ClusterState state, IndexNameExpressionResolver resolver, IndexMetadata index, String[] expressions) {
        Set<String> resolved = resolver.resolveExpressions(state, expressions);
        String[] aliases = resolver.filteringAliases(state, index.getIndex().getName(), resolved);
        if (aliases == null || aliases.length == 0) {
            return null;
        }
        if (aliases.length == 1) {
            return filterOf(index, aliases[0]);
        }
        BoolQueryBuilder any = QueryBuilders.boolQuery().minimumShouldMatch(1);
        for (String alias : aliases) {
            QueryBuilder filter = filterOf(index, alias);
            if (filter == null) {
                return null;
            }
            any.should(filter);
        }
        return any;
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
