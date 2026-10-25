/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.OpenSearchSecurityException;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.rest.RestStatus;

import java.util.Collection;
import java.util.List;

/**
 * The security plugin's per-request restrictions that the shard level
 * would enforce and the action level cannot: document-level security,
 * field-level security and field masking. The security plugin evaluates
 * them after its index-level check and hands them to the shards through
 * the thread context; a request they apply to is refused here with the
 * reason, since serving it from the table would hand back every row and
 * field. Index-level permissions are enforced before this filter runs.
 */
final class InfinoSecurity {
    private InfinoSecurity() {}

    /** The thread-context keys the security plugin sets when a restriction applies to the request. */
    private static final List<String> RESTRICTION_KEYS = List.of(
        "_opendistro_security_dls_query",
        "_opendistro_security_fls_fields",
        "_opendistro_security_masked_fields"
    );

    /** Refuse the request when a document- or field-level restriction is in force for it. */
    static void refuseIfRestricted(ThreadContext context, Collection<String> indices, String what) {
        if (context == null) {
            return;
        }
        for (String key : RESTRICTION_KEYS) {
            Object transientValue = context.getTransient(key);
            String header = context.getHeader(key);
            boolean set = (transientValue != null && !transientValue.toString().isEmpty()) || (header != null && !header.isEmpty());
            if (set) {
                throw new OpenSearchSecurityException(
                    "[infino] " + what + " over " + indices + " is restricted by document- or field-level security (" + key
                        + "), which the table cannot enforce; the request is refused rather than served without it",
                    RestStatus.FORBIDDEN
                );
            }
        }
    }
}
