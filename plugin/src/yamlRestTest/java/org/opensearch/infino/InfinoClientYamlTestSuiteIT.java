/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.test.rest.yaml.ClientYamlTestCandidate;
import org.opensearch.test.rest.yaml.OpenSearchClientYamlSuiteTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Runs the YAML REST suites under {@code rest-api-spec/test} against a cluster
 * with the plugin and the security plugin installed. Requests go as the
 * admin user unless a test sends its own {@code Authorization} header; the
 * suites leave their indices behind, since a wildcard delete over the whole
 * cluster would reach the security plugin's own index.
 */
public class InfinoClientYamlTestSuiteIT extends OpenSearchClientYamlSuiteTestCase {

    /** The admin user of the test cluster's internal user database. */
    private static final String ADMIN_BASIC = "admin:admin";

    public InfinoClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return OpenSearchClientYamlSuiteTestCase.createParameters();
    }

    @Override
    protected Settings restClientSettings() {
        String token = Base64.getEncoder().encodeToString(ADMIN_BASIC.getBytes(StandardCharsets.UTF_8));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", "Basic " + token).build();
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }
}
