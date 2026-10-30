/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/** A REST test against the security-enabled test cluster, as its admin user. */
public abstract class InfinoRestTestCase extends OpenSearchRestTestCase {

    /** The admin user of the test cluster's internal user database. */
    private static final String ADMIN_BASIC = "admin:admin";

    @Override
    protected Settings restClientSettings() {
        String token = Base64.getEncoder().encodeToString(ADMIN_BASIC.getBytes(StandardCharsets.UTF_8));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", "Basic " + token).build();
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        // A wildcard delete would reach the security plugin's own index.
        return true;
    }

    /** {@code method path} with a JSON body (null for none), as a map. */
    protected Map<String, Object> call(String method, String path, String body) throws IOException {
        return entityAsMap(perform(method, path, body));
    }

    /** {@code method path} with a JSON body (null for none), whose answer is a JSON array. */
    protected List<Object> callList(String method, String path, String body) throws IOException {
        return entityAsList(perform(method, path, body));
    }

    private Response perform(String method, String path, String body) throws IOException {
        Request request = new Request(method, path);
        if (body != null) {
            request.setJsonEntity(body);
        }
        return client().performRequest(request);
    }
}
