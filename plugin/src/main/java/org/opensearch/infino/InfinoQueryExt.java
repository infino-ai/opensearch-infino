/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchExtBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * The request's query as the client wrote it, carried to the shard in the
 * {@code ext} section of the search source.
 *
 * <p>Before the query phase, OpenSearch rewrites the query against the
 * shard's Lucene statistics: a range over a field the Lucene skeleton holds
 * no points for rewrites to {@code match_none}, since Lucene has no data
 * for an Infino index. The {@code ext} section is not rewritten, so the
 * translator reads the query from here.
 */
public final class InfinoQueryExt extends SearchExtBuilder {
    public static final String NAME = "infino_query";
    private static final String QUERY = "query";

    private final QueryBuilder query;

    public InfinoQueryExt(QueryBuilder query) {
        this.query = query;
    }

    public InfinoQueryExt(StreamInput in) throws IOException {
        this.query = in.readNamedWriteable(QueryBuilder.class);
    }

    public QueryBuilder query() {
        return query;
    }

    public static InfinoQueryExt fromXContent(XContentParser parser) throws IOException {
        QueryBuilder query = null;
        XContentParser.Token token;
        String current = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                current = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT && QUERY.equals(current)) {
                query = AbstractQueryBuilder.parseInnerQueryBuilder(parser);
            } else {
                parser.skipChildren();
            }
        }
        return new InfinoQueryExt(query);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(query);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(QUERY, query);
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(NAME, query);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof InfinoQueryExt other && Objects.equals(query, other.query);
    }
}
