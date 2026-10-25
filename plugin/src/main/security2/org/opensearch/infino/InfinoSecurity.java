/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.Version;
import org.opensearch.common.inject.Injector;
import org.opensearch.common.util.concurrent.ThreadContext;

import java.util.List;

/**
 * The security plugin's per-request restrictions on the releases whose
 * security plugin the plugin cannot extend: document-level security,
 * field-level security and field masking. On 2.x the security plugin
 * evaluates them after its index-level check and hands them to the shards
 * as thread-context headers, so a request they apply to is refused with the
 * reason rather than served around them. On 3.0 to 3.5 it keeps the
 * evaluated rules where nothing outside it can read them and sets no
 * headers, so there any request from a user is refused; a request with no
 * user (the node's own, the security plugin's configuration reads) is
 * served. Index-level permissions are enforced before the plugin's filters
 * run on every release.
 */
final class InfinoSecurity {
    private InfinoSecurity() {}

    /** The thread-context keys the 2.x security plugin sets when a restriction applies to the request. */
    private static final List<String> RESTRICTION_KEYS = List.of(
        "_opendistro_security_dls_query",
        "_opendistro_security_fls_fields",
        "_opendistro_security_masked_fields"
    );
    /** The thread-context key the security plugin keeps the authenticated user under. */
    private static final String USER_KEY = "_opendistro_security_user";
    /** The header the security plugin's own configuration reads carry. */
    private static final String CONF_REQUEST_HEADER = "_opendistro_security_conf_request";
    /** The first major whose security plugin keeps its rules out of the thread context. */
    private static final int RULES_IN_OBJECTS_MAJOR = 3;

    /** Nothing to take from the node here: the restrictions arrive with each request, or cannot be read at all. */
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
        if (Version.CURRENT.major >= RULES_IN_OBJECTS_MAJOR
            && context.getTransient(USER_KEY) != null
            && !"true".equals(context.getHeader(CONF_REQUEST_HEADER))) {
            throw InfinoAccess.refused(
                what + " over [" + index + "] is refused: the security plugin of this release keeps its document- and field-level rules where the plugin cannot read them (3.6 and later register them)"
            );
        }
        return InfinoAccess.NONE;
    }
}
