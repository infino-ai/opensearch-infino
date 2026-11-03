/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.ingest.IngestService;

import java.util.function.BiConsumer;
import java.util.function.IntConsumer;

/**
 * The node's run of a bulk's ingest pipelines. On 2.x the call also takes
 * the bulk itself, so this class has a copy per major.
 */
final class InfinoIngest {

    private InfinoIngest() {}

    /** Run the pipelines resolved onto {@code bulk}'s documents on {@code executor}, as the stock bulk runs them. */
    static void executeBulkRequest(
        IngestService ingest,
        BulkRequest bulk,
        BiConsumer<Integer, Exception> onFailure,
        BiConsumer<Thread, Exception> onCompletion,
        IntConsumer onDropped,
        String executor
    ) {
        ingest.executeBulkRequest(bulk.numberOfActions(), bulk.requests(), onFailure, onCompletion, onDropped, executor);
    }
}
