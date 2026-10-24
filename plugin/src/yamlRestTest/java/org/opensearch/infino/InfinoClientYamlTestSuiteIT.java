/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.opensearch.test.rest.yaml.ClientYamlTestCandidate;
import org.opensearch.test.rest.yaml.OpenSearchClientYamlSuiteTestCase;

/** Runs the YAML REST suites under {@code rest-api-spec/test} against a cluster with the plugin installed. */
public class InfinoClientYamlTestSuiteIT extends OpenSearchClientYamlSuiteTestCase {

    public InfinoClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return OpenSearchClientYamlSuiteTestCase.createParameters();
    }
}
