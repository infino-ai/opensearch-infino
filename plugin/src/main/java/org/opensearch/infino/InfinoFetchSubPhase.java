/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.common.document.DocumentField;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.fetch.FetchSubPhaseProcessor;
import org.opensearch.search.fetch.subphase.FetchSourceContext;

import java.util.Map;

/**
 * The fetch phase of an engine-mode shard: each hit gets what its query
 * phase built for it from the table ({@link InfinoQueryPhaseSearcher}) —
 * its source, filtered and masked as the action layer serves it, and the
 * requested fields, script fields and collapse value — since the shard's
 * Lucene skeleton keeps no source and no values. A hit whose kept copy is
 * gone (it waited past its time) is read from the table again, under the
 * same rules. Other shards get no processor.
 */
public class InfinoFetchSubPhase implements FetchSubPhase {

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext context) {
        if (!InfinoPlugin.atEngine(context.getIndexSettings().getSettings())) {
            return null;
        }
        if (!(context.getSearchExt(InfinoQueryExt.NAME) instanceof InfinoQueryExt carried)) {
            return null;
        }
        ShardId shardId = new ShardId(context.getIndexSettings().getIndex(), context.getQueryShardContext().getShardId());
        String table = InfinoPlugin.tables().ensure(context.getIndexSettings().getIndexMetadata(), shardId.id());
        SearchSourceBuilder written = carried.source();
        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) {
                // The table is keyed by id, not by Lucene document: nothing per segment.
            }

            @Override
            public void process(HitContext hit) {
                String id = hit.hit().getId();
                if (id == null) {
                    return;
                }
                SearchHit kept = InfinoQueryPhaseSearcher.take(carried.request(), shardId, id);
                if (kept == null) {
                    kept = reread(table, shardId, id, written);
                }
                if (kept.getSourceRef() != null) {
                    hit.hit().sourceRef(kept.getSourceRef());
                }
                for (Map.Entry<String, DocumentField> field : kept.getFields().entrySet()) {
                    hit.hit().setDocumentField(field.getKey(), field.getValue());
                }
            }
        };
    }

    /** A hit read from the table again, as the query phase would have built it: the source as the user may see it, filtered, and the requested fields. */
    private static SearchHit reread(String table, ShardId shardId, String id, SearchSourceBuilder written) {
        SearchHit out = new SearchHit(0, id, Map.of(), Map.of());
        InfinoAccess access = InfinoSecurity.access(shardId.getIndexName(), InfinoSecurityCalls.SEARCH);
        String src = access.read(id, InfinoEngine.currentSource(table, id));
        if (src == null || src.isEmpty()) {
            return out;
        }
        FetchSourceContext fetch = written.fetchSource();
        if (fetch == null || fetch.fetchSource()) {
            boolean filtered = fetch != null && (fetch.includes().length > 0 || fetch.excludes().length > 0);
            out.sourceRef(filtered ? InfinoSearch.filtered(src, fetch) : new BytesArray(src));
        }
        InfinoSearch.fillFields(
            out,
            src,
            InfinoSearch.requestedFields(written.docValueFields(), written.fetchFields()),
            access.columns(InfinoNative.columns(table))
        );
        return out;
    }
}
