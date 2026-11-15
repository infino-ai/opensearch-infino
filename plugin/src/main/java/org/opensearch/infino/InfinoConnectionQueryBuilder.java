/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;

import java.io.IOException;

/**
 * The query a default-mode search the shard request cache may keep is routed
 * with (see {@link InfinoSearchActionFilter}): every document, like the
 * {@code match_all} any other search is routed with, plus the shard node's
 * connection to the tables. The shard rewrites it to name its node's
 * connection before the cache keys the request, so a result read through
 * one connection is never served once the node reads through another (an
 * account taken into use or dropped, see {@link InfinoAccount}).
 */
public final class InfinoConnectionQueryBuilder extends AbstractQueryBuilder<InfinoConnectionQueryBuilder> {
    public static final String NAME = "infino_connection";
    private static final ParseField CONNECTION = new ParseField("connection");
    /** No connection named yet: the query as the coordinating node routes it. */
    static final long UNNAMED = -1L;

    private final long connection;

    public InfinoConnectionQueryBuilder() {
        this(UNNAMED);
    }

    InfinoConnectionQueryBuilder(long connection) {
        this.connection = connection;
    }

    public InfinoConnectionQueryBuilder(StreamInput in) throws IOException {
        super(in);
        connection = in.readLong();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeLong(connection);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(CONNECTION.getPreferredName(), connection);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static InfinoConnectionQueryBuilder fromXContent(XContentParser parser) throws IOException {
        long connection = UNNAMED;
        String current = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                current = parser.currentName();
            } else if (token.isValue() && CONNECTION.match(current, parser.getDeprecationHandler())) {
                connection = parser.longValue();
            }
        }
        return new InfinoConnectionQueryBuilder(connection);
    }

    /** The connection the shard's node reads through, once a shard has rewritten the query. */
    long connection() {
        return connection;
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext context) throws IOException {
        if (context.convertToShardContext() == null) {
            return this;
        }
        long current = InfinoRequestCache.connection();
        return current == connection ? this : new InfinoConnectionQueryBuilder(current);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) {
        return new MatchAllDocsQuery();
    }

    @Override
    protected boolean doEquals(InfinoConnectionQueryBuilder other) {
        return connection == other.connection;
    }

    @Override
    protected int doHashCode() {
        return Long.hashCode(connection);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
