/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.common.inject.Injector;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.transport.client.Client;

/**
 * The node client, for the two things the plugin asks the cluster to do:
 * create an index a write names before it exists, as a stock bulk would, and
 * store the hosted account in the persistent cluster settings. The client's
 * package moved in 3.0, so this class has a copy per major.
 */
final class InfinoClient {
    private static volatile Client client;

    private InfinoClient() {}

    static void bind(Injector injector) {
        client = injector.getInstance(Client.class);
    }

    /**
     * Write {@code persistent} into the cluster's persistent settings, as the
     * caller: the request runs under the caller's own identity, so whoever may
     * not change cluster settings may not store or forget the account either.
     */
    static void updateSettings(Settings persistent, ActionListener<Void> listener) {
        Client c = client;
        if (c == null) {
            listener.onFailure(new IllegalStateException("[infino] the node client is not available yet"));
            return;
        }
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest().persistentSettings(persistent);
        c.admin().cluster().updateSettings(request, ActionListener.wrap(response -> listener.onResponse(null), listener::onFailure));
    }

    /** Create {@code index} with the cluster's defaults; one that appeared meanwhile counts as created. */
    static void createIndex(String index, ActionListener<Void> listener) {
        Client c = client;
        if (c == null) {
            listener.onFailure(new IllegalStateException("[infino] the node client is not available yet"));
            return;
        }
        c.admin().indices().create(new CreateIndexRequest(index), ActionListener.wrap(response -> listener.onResponse(null), e -> {
            if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                listener.onResponse(null);
            } else {
                listener.onFailure(e);
            }
        }));
    }
}
