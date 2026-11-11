/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.junit.After;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.test.rest.yaml.ClientYamlTestCandidate;
import org.opensearch.test.rest.yaml.OpenSearchClientYamlSuiteTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Runs OpenSearch's own YAML REST suites (copied from the release's
 * {@code rest-api-spec} artifact) against a cluster with the plugin and the
 * security plugin installed. The stock suites expect a clean cluster and reuse
 * index names such as {@code test_1}, so every index a test leaves behind is
 * deleted after it, except the dot-prefixed system indices: a wildcard delete
 * would reach the security plugin's own index, which it refuses to drop.
 */
public class InfinoCoreYamlTestSuiteIT extends InfinoClientYamlTestSuiteIT {

    public InfinoCoreYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return OpenSearchClientYamlSuiteTestCase.createParameters();
    }

    /** Runs before the base class's cluster wipe, which leaves indices alone here. */
    @After
    public void deleteTestIndices() throws IOException {
        // Data streams first: their backing indices cannot be deleted directly.
        try {
            adminClient().performRequest(new Request("DELETE", "/_data_stream/*"));
        } catch (ResponseException e) {
            // None, or the API is absent: nothing to delete.
        }
        Request list = new Request("GET", "/_cat/indices");
        list.addParameter("h", "index");
        list.addParameter("format", "json");
        list.addParameter("expand_wildcards", "all");
        List<String> names = new ArrayList<>();
        for (Object row : entityAsList(adminClient().performRequest(list))) {
            String name = (String) ((Map<?, ?>) row).get("index");
            if (name.startsWith(".") == false) {
                names.add(name);
            }
        }
        if (names.isEmpty()) {
            return;
        }
        Request delete = new Request("DELETE", "/" + String.join(",", names));
        delete.addParameter("expand_wildcards", "all");
        delete.addParameter("ignore_unavailable", "true");
        adminClient().performRequest(delete);
    }
}
