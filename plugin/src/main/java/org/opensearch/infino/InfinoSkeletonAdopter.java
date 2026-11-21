/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Rebuilds the Lucene skeleton of a default-mode index the registry
 * recreated over a table that holds rows ({@link InfinoPlugin#INFINO_REBUILT}).
 * A search hit names a document of the skeleton, so until every row has
 * one, the index's searches miss the rows written before the rebuild.
 *
 * <p>When the shard's primary starts (or a replica takes over as primary),
 * the shard's table is read a page at a time in id order, and every row the
 * skeleton lacks is created through the cluster's own write path, from the
 * row's own source, so each of the shard's copies gets it as it gets any
 * write, routed by the routing value the row was written with when it was
 * written with one. The table already holds these rows, so the primary's
 * engine leaves the table alone for them (the request carries
 * {@link #SKELETON_ONLY}).
 * Each is a create, so a client's write of the same id wins; a row a client
 * deleted while its create was in flight is deleted again through the
 * shard, so the skeleton and the translog end where the table is. Until a
 * shard's skeleton is whole, its engine looks ids up in the table
 * ({@link InfinoEngine#skeletonComplete}).
 *
 * <p>While {@link #PAUSED} is set, a rebuild waits before it reads the
 * table: the index's searches miss the rows written before the rebuild, and
 * its engine goes on looking ids up in the table.
 */
final class InfinoSkeletonAdopter {

    private static final Logger log = LogManager.getLogger(InfinoSkeletonAdopter.class);

    /** The header a skeleton-only write carries, so the primary's engine writes Lucene alone. */
    static final String SKELETON_ONLY = "_infino_skeleton_only";
    /** Rows read from the table per page. */
    private static final int PAGE_ROWS = 10_000;
    /** The count's alias in the row-count statement. */
    private static final String COUNT_ALIAS = "n";

    /**
     * Hold every skeleton rebuild that has not begun reading its table until
     * the setting is cleared, as {@code PUT _cluster/settings} sets it: an
     * operator keeps a rebuild's writes off a busy cluster, and a test holds
     * a skeleton incomplete while it writes.
     */
    static final Setting<Boolean> PAUSED = Setting.boolSetting(
        "infino.skeleton_rebuild.paused",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    /** How often a paused rebuild looks whether it may go on. */
    private static final long PAUSE_POLL_MILLIS = 200;

    /** The shards whose skeleton is being rebuilt on this node, so a second start does not run beside the first. */
    private static final Set<ShardId> RUNNING = ConcurrentHashMap.newKeySet();

    private InfinoSkeletonAdopter() {}

    /** Rebuild {@code shard}'s skeleton from {@code table} on the generic pool, unless it is being rebuilt already. */
    static void start(IndexShard shard, String table) {
        ThreadPool threadPool = InfinoServices.threadPool();
        ShardId shardId = shard.shardId();
        if (threadPool == null || !RUNNING.add(shardId)) {
            return;
        }
        threadPool.generic().execute(() -> {
            try {
                adopt(shard, table);
            } catch (Exception e) {
                log.warn("[infino] the skeleton of {} could not be rebuilt from table {}", shardId, table, e);
            } finally {
                RUNNING.remove(shardId);
            }
        });
    }

    private static void adopt(IndexShard shard, String table) throws IOException {
        ShardId shardId = shard.shardId();
        if (!waitWhilePaused(shard)) {
            return;
        }
        IndexMetadata md = shard.indexSettings().getIndexMetadata();
        long rows = tableRows(table);
        long docs;
        try (Engine.Searcher searcher = shard.acquireSearcher("infino_skeleton")) {
            docs = searcher.getIndexReader().numDocs();
        }
        if (docs >= rows) {
            InfinoEngine.skeletonComplete(shardId);
            return;
        }
        log.info("[infino] rebuilding the skeleton of {}: {} documents for {} rows of table {}", shardId, docs, rows, table);
        // A row written with a routing value keeps it; a table that never
        // had one has no column for it.
        boolean routed = InfinoNative.columns(table).containsKey(InfinoQueryTranslator.ROUTING);
        long created = 0;
        long unroutable = 0;
        String after = null;
        while (true) {
            List<Map<String, Object>> page = InfinoNative.sqlRows(pageSql(table, after, routed));
            Map<String, Row> missing = new LinkedHashMap<>();
            try (Engine.Searcher searcher = shard.acquireSearcher("infino_skeleton")) {
                for (Map<String, Object> row : page) {
                    Object id = row.get(InfinoQueryTranslator.OSID);
                    Object source = row.get(InfinoQueryTranslator.SOURCE);
                    if (id == null) {
                        continue;
                    }
                    after = id.toString();
                    Object r = routed ? row.get(InfinoQueryTranslator.ROUTING) : null;
                    String routing = r == null ? null : r.toString();
                    if (OperationRouting.generateShardId(md, after, routing) != shardId.id()) {
                        // Written with a routing value before the table kept them: there
                        // is no routing that brings it back to this shard.
                        unroutable++;
                        continue;
                    }
                    if (source != null && searcher.count(new TermQuery(new Term(IdFieldMapper.NAME, Uid.encodeId(after)))) == 0) {
                        missing.put(after, new Row(source.toString(), routing));
                    }
                }
            }
            if (!missing.isEmpty()) {
                created += createMissing(md.getIndex().getName(), table, missing);
            }
            if (page.size() < PAGE_ROWS) {
                break;
            }
        }
        InfinoEngine.skeletonComplete(shardId);
        if (unroutable > 0) {
            log.warn("[infino] {} rows of table {} route to another shard of {} by their id and keep no routing value; they are left out of its skeleton", unroutable, table, shardId);
        }
        log.info("[infino] rebuilt the skeleton of {}: created {} documents from table {}", shardId, created, table);
    }

    /**
     * Wait while {@link #PAUSED} is set. False when the shard closed (or
     * the node stops the wait) meanwhile: the shard's next start rebuilds it.
     */
    private static boolean waitWhilePaused(IndexShard shard) {
        boolean logged = false;
        while (paused()) {
            if (shard.state() == IndexShardState.CLOSED) {
                return false;
            }
            if (!logged) {
                log.info("[infino] the skeleton of {} waits to be rebuilt: {} is set", shard.shardId(), PAUSED.getKey());
                logged = true;
            }
            try {
                Thread.sleep(PAUSE_POLL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return shard.state() != IndexShardState.CLOSED;
    }

    /** Whether the cluster's settings pause rebuilds now. */
    private static boolean paused() {
        ClusterService clusterService = InfinoServices.clusterService();
        return clusterService != null && PAUSED.get(clusterService.state().metadata().settings());
    }

    /** A row the skeleton lacks: its source, and the routing value it was written with (null for none). */
    private record Row(String source, String routing) {}

    /** The table's rows. */
    private static long tableRows(String table) {
        List<Map<String, Object>> out = InfinoNative.sqlRows("SELECT COUNT(*) AS " + COUNT_ALIAS + " FROM " + InfinoQueryTranslator.quoteIdent(table));
        return out.isEmpty() ? 0 : InfinoAggregationProcessor.asLong(out.get(0).get(COUNT_ALIAS));
    }

    /** One page of the table's ids, sources and (when {@code routed}) routing values in id order, after {@code after} (null for the first). */
    private static String pageSql(String table, String after, boolean routed) {
        String osid = InfinoQueryTranslator.quoteIdent(InfinoQueryTranslator.OSID);
        return "SELECT "
            + osid
            + ", "
            + InfinoQueryTranslator.quoteIdent(InfinoQueryTranslator.SOURCE)
            + (routed ? ", " + InfinoQueryTranslator.quoteIdent(InfinoQueryTranslator.ROUTING) : "")
            + " FROM "
            + InfinoQueryTranslator.quoteIdent(table)
            + (after == null ? "" : " WHERE " + osid + " > " + InfinoQueryTranslator.literal(after))
            + " ORDER BY "
            + osid
            + " LIMIT "
            + PAGE_ROWS;
    }

    /**
     * Create a skeleton document for each id, as a skeleton-only write; then
     * delete through the shard any whose row the table lost meanwhile.
     * Returns the documents created.
     */
    private static long createMissing(String index, String table, Map<String, Row> missing) {
        BulkRequest creates = new BulkRequest();
        for (Map.Entry<String, Row> row : missing.entrySet()) {
            creates.add(
                new IndexRequest(index).id(row.getKey())
                    .routing(row.getValue().routing())
                    .source(row.getValue().source(), XContentType.JSON)
                    .opType(DocWriteRequest.OpType.CREATE)
            );
        }
        List<String> created = new ArrayList<>();
        for (BulkItemResponse item : run(creates, true).getItems()) {
            if (!item.isFailed()) {
                created.add(item.getId());
            }
        }
        if (created.isEmpty()) {
            return 0;
        }
        Set<String> kept = InfinoNative.existingOsids(table, created);
        BulkRequest deletes = new BulkRequest();
        for (String id : created) {
            if (!kept.contains(id)) {
                deletes.add(new DeleteRequest(index, id).routing(missing.get(id).routing()));
            }
        }
        if (deletes.numberOfActions() > 0) {
            run(deletes, false);
        }
        return created.size();
    }

    /** Run {@code bulk} as the node, skeleton-only when asked, and wait for it. */
    private static BulkResponse run(BulkRequest bulk, boolean skeletonOnly) {
        ThreadContext context = InfinoServices.threadPool().getThreadContext();
        try (ThreadContext.StoredContext ignored = context.stashContext()) {
            if (skeletonOnly) {
                context.putHeader(SKELETON_ONLY, Boolean.TRUE.toString());
            }
            PlainActionFuture<BulkResponse> response = PlainActionFuture.newFuture();
            InfinoClient.bulk(bulk, response);
            return response.actionGet();
        }
    }
}
