/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.OpenSearchSecurityException;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/** A request the security plugin restricted by document or field is refused, never served around the restriction. */
public class InfinoSecurityTests extends OpenSearchTestCase {

    public void testUnrestrictedRequestPasses() {
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        InfinoSecurity.refuseIfRestricted(context, List.of("logs"), "search");
        InfinoSecurity.refuseIfRestricted(null, List.of("logs"), "search");
    }

    public void testDocumentFieldAndMaskRestrictionsRefuse() {
        for (String key : new String[] { "_opendistro_security_dls_query", "_opendistro_security_fls_fields", "_opendistro_security_masked_fields" }) {
            ThreadContext asTransient = new ThreadContext(Settings.EMPTY);
            asTransient.putTransient(key, "{\"logs\":[\"{}\"]}");
            OpenSearchSecurityException e = expectThrows(
                OpenSearchSecurityException.class,
                () -> InfinoSecurity.refuseIfRestricted(asTransient, List.of("logs"), "search")
            );
            assertEquals(RestStatus.FORBIDDEN, e.status());
            assertTrue(e.getMessage().contains(key));

            ThreadContext asHeader = new ThreadContext(Settings.EMPTY);
            asHeader.putHeader(key, "x");
            expectThrows(OpenSearchSecurityException.class, () -> InfinoSecurity.refuseIfRestricted(asHeader, List.of("logs"), "get"));
        }
    }
}
