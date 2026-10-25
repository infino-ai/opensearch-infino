/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.indices.TermsLookup;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The values a terms lookup names, read from the document in the
 * Infino-backed index it points at. Only a document-level security rule
 * reaches this: the rule is the security plugin's, so its lookup is read
 * from the table directly; a lookup a user writes into a query goes through
 * no authorization here and is not served.
 */
final class InfinoTermsLookup {
    private InfinoTermsLookup() {}

    static List<Object> values(TermsLookup lookup) {
        ClusterService clusters = InfinoServices.clusterService();
        if (clusters == null) {
            throw new IllegalArgumentException("[infino] terms lookup into [" + lookup.index() + "] cannot be served before the node is up");
        }
        IndexMetadata md = InfinoGetActionFilter.infinoIndex(clusters.state(), InfinoServices.indexNameExpressionResolver(), lookup.index());
        if (md == null) {
            throw new IllegalArgumentException("[infino] terms lookup names index [" + lookup.index() + "], which the plugin does not back");
        }
        String table = InfinoPlugin.tables().ensure(md);
        String source = InfinoNative.getSourceFresh(table, lookup.id());
        if (source == null || source.isEmpty()) {
            return List.of();
        }
        Map<String, Object> map = XContentHelper.convertToMap(new BytesArray(source), true, MediaTypeRegistry.JSON).v2();
        Object v = XContentMapValues.extractValue(lookup.path(), map);
        if (v == null) {
            return List.of();
        }
        return v instanceof List<?> list ? new ArrayList<>(list) : List.of(v);
    }
}
