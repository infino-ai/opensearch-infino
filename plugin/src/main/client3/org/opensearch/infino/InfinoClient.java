/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.common.inject.Injector;
import org.opensearch.core.action.ActionListener;
import org.opensearch.transport.client.Client;

/**
 * The node client, for the one thing the plugin asks the cluster to do:
 * create an index — one a write names before it exists, as a stock bulk
 * would, or one the registry names on a fresh cluster. The client's package
 * moved in 3.0, so this class has a copy per major.
 */
final class InfinoClient {
    private static volatile Client client;

    private InfinoClient() {}

    static void bind(Injector injector) {
        client = injector.getInstance(Client.class);
    }

    /** Create {@code index} with the cluster's defaults; one that appeared meanwhile counts as created. */
    static void createIndex(String index, ActionListener<Void> listener) {
        createIndex(new CreateIndexRequest(index), listener);
    }

    /**
     * Whether the caller may manage the cluster, asked of the cluster itself:
     * a reroute that is a dry run and carries no command runs under the
     * caller's own identity, so the security plugin authorizes it as the
     * cluster-management action it is ({@code cluster:admin/reroute}) and
     * refuses it (403) for a user without that right, and it changes nothing.
     * Without the security plugin every caller has the right. (A settings
     * update with nothing in it would say the same, but the cluster rejects
     * it as invalid before any authorization runs.)
     */
    static void requireClusterManagement(ActionListener<Void> listener) {
        Client c = client;
        if (c == null) {
            listener.onFailure(new IllegalStateException("[infino] the node client is not available yet"));
            return;
        }
        c.admin().cluster().reroute(new ClusterRerouteRequest().dryRun(true), ActionListener.wrap(response -> listener.onResponse(null), listener::onFailure));
    }

    /**
     * Run {@code request} as a bulk on this node, as the caller, through the
     * cluster's bulk action and its filters: the part of a bulk whose items
     * write to indices Infino does not back, which the stock bulk serves, and
     * the writes an update or an adopted skeleton turns into.
     */
    static void bulk(BulkRequest request, ActionListener<BulkResponse> listener) {
        Client c = client;
        if (c == null) {
            listener.onFailure(new IllegalStateException("[infino] the node client is not available yet"));
            return;
        }
        c.bulk(request, listener);
    }

    /** Create the index {@code request} describes; one that appeared meanwhile counts as created. */
    static void createIndex(CreateIndexRequest request, ActionListener<Void> listener) {
        Client c = client;
        if (c == null) {
            listener.onFailure(new IllegalStateException("[infino] the node client is not available yet"));
            return;
        }
        c.admin().indices().create(request, ActionListener.wrap(response -> listener.onResponse(null), e -> {
            if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                listener.onResponse(null);
            } else {
                listener.onFailure(e);
            }
        }));
    }
}
