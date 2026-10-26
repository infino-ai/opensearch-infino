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
 * {@code infino_hybrid} query: native hybrid (BM25 + vector) search routed to
 * Infino's {@code hybrid_search}.
 * <pre>{"query":{"infino_hybrid":{"text_column":"title","query":"fox",
 *   "vector_column":"embedding","vector":[...],"k":10}}}</pre>
 * Both columns are named by the request: the text column must carry a text
 * index and the vector column must be a mapped {@code knn_vector}.
 */
public class InfinoHybridQueryBuilder extends AbstractQueryBuilder<InfinoHybridQueryBuilder> {
    public static final String NAME = "infino_hybrid";
    private static final ParseField TEXT_COLUMN = new ParseField("text_column");
    private static final ParseField QUERY = new ParseField("query");
    private static final ParseField VECTOR_COLUMN = new ParseField("vector_column");
    private static final ParseField VECTOR = new ParseField("vector");
    private static final ParseField K = new ParseField("k");

    private final String textColumn;
    private final String textQuery;
    private final String vectorColumn;
    private final float[] vector;
    private final int k;

    public InfinoHybridQueryBuilder(String textColumn, String textQuery, String vectorColumn, float[] vector, int k) {
        this.textColumn = textColumn;
        this.textQuery = textQuery;
        this.vectorColumn = vectorColumn;
        this.vector = vector;
        this.k = k;
    }

    public InfinoHybridQueryBuilder(StreamInput in) throws IOException {
        super(in);
        textColumn = in.readString();
        textQuery = in.readString();
        vectorColumn = in.readString();
        vector = in.readFloatArray();
        k = in.readVInt();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(textColumn);
        out.writeString(textQuery);
        out.writeString(vectorColumn);
        out.writeFloatArray(vector);
        out.writeVInt(k);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(TEXT_COLUMN.getPreferredName(), textColumn);
        builder.field(QUERY.getPreferredName(), textQuery);
        builder.field(VECTOR_COLUMN.getPreferredName(), vectorColumn);
        builder.array(VECTOR.getPreferredName(), vector);
        builder.field(K.getPreferredName(), k);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static InfinoHybridQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String textColumn = null;
        String textQuery = "";
        String vectorColumn = null;
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
                if (TEXT_COLUMN.match(current, parser.getDeprecationHandler())) textColumn = parser.text();
                else if (QUERY.match(current, parser.getDeprecationHandler())) textQuery = parser.text();
                else if (VECTOR_COLUMN.match(current, parser.getDeprecationHandler())) vectorColumn = parser.text();
                else if (K.match(current, parser.getDeprecationHandler())) k = parser.intValue();
            }
        }
        float[] v = new float[vec.size()];
        for (int i = 0; i < v.length; i++) v[i] = vec.get(i);
        return new InfinoHybridQueryBuilder(textColumn, textQuery, vectorColumn, v, k);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) {
        // Served at the action layer from the table; a shard never runs it.
        return new MatchNoDocsQuery(NAME + " is served by the Infino table");
    }

    String textColumn() { return textColumn; }
    String textQuery() { return textQuery; }
    String vectorColumn() { return vectorColumn; }
    float[] vector() { return vector; }
    int k() { return k; }

    @Override
    protected boolean doEquals(InfinoHybridQueryBuilder o) {
        return k == o.k && Objects.equals(textColumn, o.textColumn) && Objects.equals(textQuery, o.textQuery)
            && Objects.equals(vectorColumn, o.vectorColumn) && Arrays.equals(vector, o.vector);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(textColumn, textQuery, vectorColumn, Arrays.hashCode(vector), k);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
