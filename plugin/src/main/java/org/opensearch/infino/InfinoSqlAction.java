/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.action.ActionType;

/**
 * A caller's read-only SQL statement over Infino-backed indices (see
 * {@link InfinoSql}). The action is named under {@code indices:data/read},
 * and its request names the indices the statement reads, so the security
 * plugin authorizes it as it authorizes a search: the caller needs read on
 * each index, which a role's {@code read} action group grants, and the
 * audit log records it as a read of those indices.
 */
public final class InfinoSqlAction extends ActionType<InfinoSqlResponse> {

    public static final String NAME = "indices:data/read/infino/sql";
    public static final InfinoSqlAction INSTANCE = new InfinoSqlAction();

    private InfinoSqlAction() {
        super(NAME, InfinoSqlResponse::new);
    }
}
