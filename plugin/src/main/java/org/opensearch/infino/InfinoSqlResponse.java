/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/** A statement's result as the bridge wrote it: {@code {"schema": [..], "rows": [..]}}. */
public final class InfinoSqlResponse extends ActionResponse {

    private final String json;

    public InfinoSqlResponse(String json) {
        this.json = json;
    }

    public InfinoSqlResponse(StreamInput in) throws IOException {
        super(in);
        json = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(json);
    }

    public String json() {
        return json;
    }
}
