/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;

import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryShardContext;

/**
 * {@code infino_knn} query: native vector search routed to Infino.
 * <pre>{"query":{"infino_knn":{"field":"embedding","vector":[...],"k":10}}}</pre>
 * The field must be a mapped {@code knn_vector}.
 */
public class InfinoKnnQueryBuilder extends AbstractQueryBuilder<InfinoKnnQueryBuilder> {
    public static final String NAME = "infino_knn";
    private static final ParseField FIELD = new ParseField("field");
    private static final ParseField VECTOR = new ParseField("vector");
    private static final ParseField K = new ParseField("k");

    private final String field;
    private final float[] vector;
    private final int k;

    public InfinoKnnQueryBuilder(String field, float[] vector, int k) {
        this.field = field;
        this.vector = vector;
        this.k = k;
    }

    public InfinoKnnQueryBuilder(StreamInput in) throws IOException {
        super(in);
        field = in.readString();
        vector = in.readFloatArray();
        k = in.readVInt();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeFloatArray(vector);
        out.writeVInt(k);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(FIELD.getPreferredName(), field);
        builder.array(VECTOR.getPreferredName(), vector);
        builder.field(K.getPreferredName(), k);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static InfinoKnnQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String field = null;
        int k = 10;
        List<Float> vec = new ArrayList<>();
        String current = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                current = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (VECTOR.match(current, parser.getDeprecationHandler())) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) vec.add(parser.floatValue());
                }
            } else if (token.isValue()) {
                if (FIELD.match(current, parser.getDeprecationHandler())) field = parser.text();
                else if (K.match(current, parser.getDeprecationHandler())) k = parser.intValue();
            }
        }
        float[] v = new float[vec.size()];
        for (int i = 0; i < v.length; i++) v[i] = vec.get(i);
        return new InfinoKnnQueryBuilder(field, v, k);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) {
        // Served from the table (at the action layer in proxy mode, by the
        // shard's query phase in default mode); Lucene never runs it.
        return new MatchNoDocsQuery(NAME + " is served by the Infino table");
    }

    String field() { return field; }
    float[] vector() { return vector; }
    int k() { return k; }

    @Override
    protected boolean doEquals(InfinoKnnQueryBuilder o) {
        return k == o.k && Objects.equals(field, o.field) && Arrays.equals(vector, o.vector);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, Arrays.hashCode(vector), k);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
