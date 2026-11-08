/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * A caller's SQL statement and the indices it reads, as the bridge named
 * them (see {@link InfinoSql}). The security plugin authorizes the request
 * for those indices; the action checks that the statement reads no index
 * outside them before it runs.
 */
public final class InfinoSqlRequest extends ActionRequest implements IndicesRequest.Replaceable {

    /** How the named indices resolve: as the statement reads them (see {@link InfinoSql#OPTIONS}). */
    private static final IndicesOptions OPTIONS = InfinoSql.OPTIONS;

    private final String query;
    private String[] indices;

    public InfinoSqlRequest(String query, String[] indices) {
        this.query = query;
        this.indices = indices;
    }

    public InfinoSqlRequest(StreamInput in) throws IOException {
        super(in);
        query = in.readString();
        indices = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(query);
        out.writeStringArray(indices);
    }

    public String query() {
        return query;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return OPTIONS;
    }

    /** A data stream resolves to its backing indices, as for a search. */
    @Override
    public boolean includeDataStreams() {
        return true;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException e = null;
        if (query == null || query.isBlank()) {
            e = addValidationError("a statement is required", e);
        }
        if (indices == null || indices.length == 0) {
            e = addValidationError("the statement reads no index", e);
        }
        return e;
    }
}
