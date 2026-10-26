/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.common.inject.Injector;
import org.opensearch.common.util.concurrent.ThreadContext;

import java.util.List;

/**
 * The security plugin's per-request restrictions on 2.x, whose security
 * plugin the plugin does not extend: document-level security, field-level
 * security and field masking. There the security plugin evaluates them
 * after its index-level check and hands them to the shards as
 * thread-context headers, so a request they apply to is refused with the
 * reason rather than served around them, and a request they do not apply
 * to is served. Index-level permissions are enforced before the plugin's
 * filters run.
 */
final class InfinoSecurity {
    private InfinoSecurity() {}

    /** The thread-context keys the 2.x security plugin sets when a restriction applies to the request. */
    private static final List<String> RESTRICTION_KEYS = List.of(
        "_opendistro_security_dls_query",
        "_opendistro_security_fls_fields",
        "_opendistro_security_masked_fields"
    );

    /** Nothing to take from the node here: the restrictions arrive with each request. */
    static void bind(Injector injector) {}

    /** What the request may see of {@code index}: everything, or nothing, refused with the reason. */
    static InfinoAccess access(String index, String what) {
        ThreadContext context = InfinoServices.threadContext();
        if (context == null) {
            return InfinoAccess.NONE;
        }
        for (String key : RESTRICTION_KEYS) {
            Object transientValue = context.getTransient(key);
            String header = context.getHeader(key);
            boolean set = (transientValue != null && !transientValue.toString().isEmpty()) || (header != null && !header.isEmpty());
            if (set) {
                throw InfinoAccess.refused(
                    what + " over [" + index + "] is restricted by document- or field-level security (" + key
                        + "), which the plugin cannot enforce on this release; the request is refused rather than served without it"
                );
            }
        }
        return InfinoAccess.NONE;
    }
}
