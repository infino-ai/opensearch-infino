/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;

import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.fetch.FetchSubPhaseProcessor;

import java.nio.charset.StandardCharsets;

/**
 * Serves each hit's {@code _source} from Infino during the fetch phase, so an
 * Infino-enabled index can store zero user data in Lucene (mapping
 * {@code _source.enabled=false}). For non-Infino indices it returns a null
 * processor and the stock Lucene source loading is used unchanged.
 */
public class InfinoFetchSubPhase implements FetchSubPhase {

    private static final Logger log = LogManager.getLogger(InfinoFetchSubPhase.class);

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) throws IOException {
        boolean enabled;
        try {
            enabled = fetchContext.getIndexSettings().getValue(InfinoPlugin.INFINO_ENABLED);
        } catch (Exception e) {
            enabled = false;
        }
        if (!enabled) {
            return null; // stock fetch
        }
        final String table = InfinoNative.tableFor(
            fetchContext.getIndexName(), fetchContext.getQueryShardContext().getShardId());

        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) {
                // Infino is keyed by _id, not docid; nothing per-segment to prepare.
            }

            @Override
            public void process(HitContext hitContext) {
                String id = hitContext.hit().getId();
                if (id == null) return;
                try {
                    String src = InfinoNative.getSource(table, id);
                    if (src != null && !src.isEmpty()) {
                        hitContext.hit().sourceRef(new BytesArray(src.getBytes(StandardCharsets.UTF_8)));
                    }
                } catch (Throwable t) {
                    log.warn("[infino] source fetch failed for id {} in {}", id, table, t);
                }
            }
        };
    }
}
