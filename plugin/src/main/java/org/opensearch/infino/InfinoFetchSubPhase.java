/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.common.document.DocumentField;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.fetch.FetchSubPhaseProcessor;
import org.opensearch.search.fetch.subphase.FieldAndFormat;

import java.util.List;
import java.util.Map;

/**
 * The fetch phase of a default-mode shard: each hit gets what its query
 * phase built for it from the table ({@link InfinoQueryPhaseSearcher}) —
 * its source, filtered and masked as cloud mode serves it, and the
 * requested fields, script fields and collapse value — since the shard's
 * Lucene skeleton keeps no source and no values. A hit whose kept copy is
 * gone (it waited past its time) is read from the table again by the same
 * hit statement, under the same rules. Other shards get no processor.
 */
public class InfinoFetchSubPhase implements FetchSubPhase {

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext context) {
        if (!InfinoPlugin.inDefaultMode(context.getIndexSettings().getSettings())) {
            return null;
        }
        if (!(context.getSearchExt(InfinoQueryExt.NAME) instanceof InfinoQueryExt carried)) {
            return null;
        }
        ShardId shardId = new ShardId(context.getIndexSettings().getIndex(), context.getQueryShardContext().getShardId());
        String table = InfinoPlugin.tables().ensure(context.getIndexSettings().getIndexMetadata(), shardId.id());
        SearchSourceBuilder written = carried.source();
        long now = carried.nowMillis();
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
                    kept = reread(table, shardId, id, written, now);
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

    /**
     * A hit read from the table again, as the query phase built it: the
     * request's hit statement over this one id, so the source comes filtered
     * and masked and the requested fields and script fields come with it.
     */
    private static SearchHit reread(String table, ShardId shardId, String id, SearchSourceBuilder written, long now) {
        SearchSourceBuilder one = new SearchSourceBuilder().query(QueryBuilders.idsQuery().addIds(id)).size(1);
        if (written.fetchSource() != null) {
            one.fetchSource(written.fetchSource());
        }
        if (written.scriptFields() != null) {
            for (SearchSourceBuilder.ScriptField sf : written.scriptFields()) {
                one.scriptField(sf.fieldName(), sf.script(), sf.ignoreFailure());
            }
        }
        if (written.docValueFields() != null) {
            for (FieldAndFormat f : written.docValueFields()) {
                one.docValueField(f.field, f.format);
            }
        }
        if (written.fetchFields() != null) {
            for (FieldAndFormat f : written.fetchFields()) {
                one.fetchField(f.field, f.format);
            }
        }
        InfinoAccess access = InfinoSecurity.access(shardId.getIndexName(), InfinoSecurityCalls.SEARCH);
        InfinoSearch.Plan plan = InfinoSearch.plan(table, one, now, access);
        List<SearchHit> hits = InfinoSearch.page(shardId.getIndex(), plan, one, false).hits();
        return hits.isEmpty() ? new SearchHit(0, id, Map.of(), Map.of()) : hits.get(0);
    }
}
