/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.common.inject.Injector;
import org.opensearch.core.action.ActionListener;
import org.opensearch.transport.client.Client;

/**
 * The node client, for the one thing the filters ask the cluster to do:
 * create an index a write names before it exists, as a stock bulk would.
 * The client's package moved in 3.0, so this class has a copy per major.
 */
final class InfinoClient {
    private static volatile Client client;

    private InfinoClient() {}

    static void bind(Injector injector) {
        client = injector.getInstance(Client.class);
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
