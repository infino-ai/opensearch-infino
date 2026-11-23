/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.Version;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.infino.InfinoMultiGetActionFilter.Layout;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/** Which items of a multi-get the tables serve, and which fail alone as on the shards. */
public class InfinoMultiGetActionFilterTests extends OpenSearchTestCase {

    private static IndexMetadata index(String name, boolean infino, String... aliases) {
        IndexMetadata.Builder builder = IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_INDEX_UUID, name)
                    .put(InfinoPlugin.INFINO_ENABLED.getKey(), infino)
                    .put(InfinoPlugin.INFINO_MODE.getKey(), InfinoPlugin.MODE_PROXY)
            );
        for (String alias : aliases) {
            builder.putAlias(AliasMetadata.builder(alias));
        }
        return builder.build();
    }

    private static ClusterState state() {
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .put(index("test_1", true, "test_one"), false)
                    .put(index("test_2", true, "test_two_and_three"), false)
                    .put(index("test_3", true, "test_two_and_three"), false)
                    .put(index("plain", false), false)
            )
            .build();
    }

    private static Layout layout(String... indices) {
        IndexNameExpressionResolver names = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));
        List<MultiGetRequest.Item> items = Arrays.stream(indices).map(name -> new MultiGetRequest.Item(name, "1")).toList();
        return InfinoMultiGetActionFilter.layout(state(), names, items);
    }

    public void testAnAliasOfSeveralIndicesFailsItsItemAloneAndTheRestAreServed() {
        Layout layout = layout("test_1", "test_two_and_three", "test_one");
        assertNotNull(layout);
        assertEquals(Set.of("test_1", "test_one"), layout.indices().keySet());
        assertEquals("test_1", layout.indices().get("test_one").getIndex().getName());
        assertNull(layout.failed()[0]);
        assertNull(layout.failed()[2]);
        Exception failed = layout.failed()[1];
        assertTrue(String.valueOf(failed), failed instanceof IllegalArgumentException);
        assertTrue(
            failed.getMessage(),
            failed.getMessage().matches("alias \\[test_two_and_three\\] has more than one index associated with it \\[test_[23], test_[23]\\], can't execute a single index op")
        );
    }

    public void testAMissingIndexFailsItsItemAloneAsOnTheShards() {
        Layout layout = layout("no_such", "test_1");
        assertNotNull(layout);
        assertTrue(String.valueOf(layout.failed()[0]), layout.failed()[0] instanceof IndexNotFoundException);
        assertNull(layout.failed()[1]);
        assertEquals(Set.of("test_1"), layout.indices().keySet());
    }

    public void testARequestTheTablesCannotServeGoesOnToTheShards() {
        // An item on an index the plugin does not back sends the whole request on.
        assertNull(layout("test_1", "plain"));
        // So does one whose every item fails: the shards answer each the same way.
        assertNull(layout("test_two_and_three", "no_such"));
    }
}
