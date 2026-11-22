/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.ExceptionsHelper;
import org.opensearch.Version;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;
import java.util.Set;

/** How a bulk's items that require their index be an alias are answered. */
public class InfinoBulkActionFilterTests extends OpenSearchTestCase {

    private static IndexRequest write(String index, boolean requireAlias) {
        return new IndexRequest(index).id("1").source(Map.of("f", 1)).setRequireAlias(requireAlias);
    }

    public void testANameAnyItemRequiresBeAnAliasIsNeverCreated() {
        BulkRequest bulk = new BulkRequest().add(write("new_index_not_created", true))
            .add(write("new_index_created", false))
            .add(write("test_require_alias", true))
            .add(write("test_require_alias", false));
        assertEquals(Set.of("new_index_not_created", "test_require_alias"), InfinoBulkActionFilter.aliasRequired(bulk));
        // One item that requires an alias keeps the name from being created for the items that do not.
        BulkRequest mixed = new BulkRequest().add(write("logs", false)).add(write("logs", true));
        assertEquals(Set.of("logs"), InfinoBulkActionFilter.aliasRequired(mixed));
        assertEquals(Set.of(), InfinoBulkActionFilter.aliasRequired(new BulkRequest().add(write("logs", false))));
    }

    public void testAnItemRequiringAnAliasFailsOnAnythingButAnAlias() {
        IndexMetadata backing = IndexMetadata.builder("backing_index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .putAlias(AliasMetadata.builder("test_require_alias"))
            .build();
        Metadata metadata = Metadata.builder().put(backing, false).build();

        IndexNotFoundException missing = InfinoBulkActionFilter.notAnAlias(write("new_index_not_created", true), metadata);
        assertNotNull(missing);
        assertEquals(RestStatus.NOT_FOUND, ExceptionsHelper.status(missing));
        assertEquals(
            "no such index [new_index_not_created] and [require_alias] request flag is [true] and [new_index_not_created] is not an alias",
            missing.getMessage()
        );
        // An index that exists is still no alias.
        IndexNotFoundException concrete = InfinoBulkActionFilter.notAnAlias(write("backing_index", true), metadata);
        assertNotNull(concrete);
        assertEquals(
            "no such index [backing_index] and [require_alias] request flag is [true] and [backing_index] is not an alias",
            concrete.getMessage()
        );
        assertNull(InfinoBulkActionFilter.notAnAlias(write("test_require_alias", true), metadata));
        // An item that requires nothing passes wherever it writes.
        assertNull(InfinoBulkActionFilter.notAnAlias(write("new_index_created", false), metadata));
        assertNull(InfinoBulkActionFilter.notAnAlias(write("backing_index", false), metadata));
    }

    public void testAnItemToAMissingNameAnotherItemRequiresBeAnAliasFindsNoIndex() {
        IndexMetadata base = IndexMetadata.builder("base")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .putAlias(AliasMetadata.builder("base_alias"))
            .build();
        Metadata metadata = Metadata.builder().put(base, false).build();
        BulkRequest bulk = new BulkRequest().add(write("base", false))
            .add(write("new_index", true))
            .add(write("new_index", false))
            .add(write("base_alias", true))
            .add(write("base_alias", false))
            .add(write("other_new", false));
        Set<String> aliasRequired = InfinoBulkActionFilter.aliasRequired(bulk);

        // The flagless item to the name the flagged one requires be an alias finds no index, as stock answers it.
        IndexNotFoundException missing = InfinoBulkActionFilter.notCreated(write("new_index", false), aliasRequired, metadata);
        assertNotNull(missing);
        assertEquals(RestStatus.NOT_FOUND, ExceptionsHelper.status(missing));
        assertEquals("no such index [new_index]", missing.getMessage());
        // A name that exists, as an index or an alias, is written as usual.
        assertNull(InfinoBulkActionFilter.notCreated(write("base", false), aliasRequired, metadata));
        assertNull(InfinoBulkActionFilter.notCreated(write("base_alias", false), aliasRequired, metadata));
        // A missing name no item requires be an alias is created as usual.
        assertNull(InfinoBulkActionFilter.notCreated(write("other_new", false), aliasRequired, metadata));
    }
}
