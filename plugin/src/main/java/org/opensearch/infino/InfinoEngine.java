/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineCreationFailureException;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ParseContext.Document;
import org.opensearch.index.mapper.SourceFieldMapper;

import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.MediaType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keeps InternalEngine's write path (seqno / translog / ack) and mirrors each
 * document to Infino.
 *
 * <p>The Infino table is declared from the index mapping, re-applied whenever
 * the mapping moves (a dynamic field detected as a date, a new mapped
 * field), so columns carry their mapped types. A document the declared
 * columns cannot hold is refused here, synchronously, which OpenSearch
 * reports as that item's bulk error; nothing reaches Infino for it.
 *
 * <p>Writes are batched: new primary creates accumulate and flush as one
 * Infino append when the batch reaches a document or byte threshold, or on
 * refresh / flush, so Infino rows become visible exactly when Lucene's do
 * and are durable before the translog is trimmed. Updates and recovery
 * replays are upserts, batched too: the pending creates go first, then one
 * tombstone mutation for the upserted ids, then one append. Deletes are
 * batched the same way. A force-merge compacts the Infino table.
 */
public class InfinoEngine extends InternalEngine {
    private static final Logger log = LogManager.getLogger(InfinoEngine.class);
    /** Documents per Infino append before the batch flushes on its own. */
    private static final int MAX_BATCH_DOCS = Integer.getInteger("infino.batch.docs", 50_000);
    /** Source bytes per Infino append before the batch flushes on its own. */
    private static final long MAX_BATCH_BYTES = Long.getLong("infino.batch.bytes", 64L << 20);

    /** The live engines by shard, for the plugin's scheduled compaction; the shard itself hides its engine. */
    private static final Map<ShardId, InfinoEngine> ENGINES = new ConcurrentHashMap<>();

    private final ShardId shardId;
    private final String table;
    private final MapperService mapperService;
    private volatile DocumentMapper appliedMapping;

    private final Object batchLock = new Object();
    private final List<String> pendOsids = new ArrayList<>();
    private final List<String> pendSources = new ArrayList<>();
    private long pendBytes = 0;
    /** Upserts keyed by id: a second update to the same id in one batch replaces the first. */
    private final Map<String, String> pendUpserts = new LinkedHashMap<>();
    private final List<String> pendDeletes = new ArrayList<>();

    public InfinoEngine(EngineConfig config) {
        super(config);
        this.shardId = config.getShardId();
        this.table = InfinoNative.tableFor(shardId.getIndexName(), shardId.id());
        this.mapperService = config.getMapperService();
        try {
            ensureMapping();
        } catch (Throwable t) {
            // A bridge that cannot load or a table that cannot be declared fails
            // this shard; as an Error it would take the whole node down.
            throw new EngineCreationFailureException(shardId, "[infino] cannot declare table " + table, t instanceof Exception e ? e : new RuntimeException(t));
        }
        ENGINES.put(shardId, this);
        log.info("[infino] engine attached for shard table {}", table);
    }

    /** The live engine of a shard, or null when the shard has none. */
    static InfinoEngine forShard(ShardId shardId) {
        return ENGINES.get(shardId);
    }

    @Override
    public void close() throws IOException {
        ENGINES.remove(shardId, this);
        super.close();
    }

    private static String sourceAsJson(Index index) throws IOException {
        if (index.source() == null) {
            return "{}";
        }
        MediaType type = index.parsedDoc() == null ? null : index.parsedDoc().getMediaType();
        if (type == null || type == XContentType.JSON) {
            return index.source().utf8ToString();
        }
        return XContentHelper.convertToJson(index.source(), false, type);
    }

    /** Declare the table from the mapping, and re-declare it whenever the mapping has moved. */
    private void ensureMapping() {
        if (mapperService == null) {
            return;
        }
        DocumentMapper current = mapperService.documentMapper();
        if (current == null || current == appliedMapping) {
            return;
        }
        synchronized (this) {
            if (current == appliedMapping) {
                return;
            }
            String mapping = current.mappingSource() == null ? "{}" : current.mappingSource().string();
            InfinoNative.ensureTable(table, mapping);
            appliedMapping = current;
        }
    }

    @Override
    public IndexResult index(Index index) throws IOException {
        ensureMapping();
        // The source arrives in whatever content type the client sent (JSON, SMILE,
        // CBOR, YAML); the bridge reads JSON.
        String src = sourceAsJson(index);

        // A document the declared columns cannot hold is refused before anything is
        // written, as OpenSearch refuses a document its mapping cannot parse. Only a
        // primary write gets refused: a replay re-applies what a primary accepted.
        if (index.origin() == Operation.Origin.PRIMARY) {
            String why = InfinoNative.checkDoc(table, src);
            if (why != null) {
                return new IndexResult(new IllegalArgumentException("[infino] " + why), index.version());
            }
        }

        // Stop the dual-write of user DATA: strip every user field, and the stored
        // _source, from the Lucene document so Lucene holds only the _id + seqno
        // skeleton the Engine contract needs (searcher, translog, docid mapping).
        // All content lives in Infino; search hits and GET read it from there.
        for (Document d : index.docs()) {
            d.getFields().removeIf(f -> !f.name().startsWith("_") || SourceFieldMapper.NAME.equals(f.name()));
        }
        IndexResult r = super.index(index); // Lucene path: seqno, translog, ack (skeleton only)
        if (r.getResultType() != Result.Type.SUCCESS) {
            return r;
        }
        // A replica never writes the table: the primary did, and a table has one writer.
        if (index.origin() == Operation.Origin.REPLICA) {
            return r;
        }
        try {
            // Recovery replay or update to an existing id ⇒ upsert (idempotent); a
            // brand-new primary create is batch-appended.
            boolean upsert = index.origin().isRecovery() || !r.isCreated();
            synchronized (batchLock) {
                if (upsert) {
                    pendUpserts.put(index.id(), src);
                } else {
                    pendOsids.add(index.id());
                    pendSources.add(src);
                }
                pendBytes += src.length();
                if (pendOsids.size() + pendUpserts.size() >= MAX_BATCH_DOCS || pendBytes >= MAX_BATCH_BYTES) {
                    flushLocked();
                }
            }
        } catch (Throwable t) {
            log.warn("[infino] mirror of {} failed", index.id(), t);
        }
        return r;
    }

    @Override
    public DeleteResult delete(Delete delete) throws IOException {
        DeleteResult r = super.delete(delete);
        if (r.getResultType() != Result.Type.SUCCESS || delete.origin() == Operation.Origin.REPLICA) {
            return r;
        }
        synchronized (batchLock) {
            pendUpserts.remove(delete.id());
            pendDeletes.add(delete.id());
        }
        return r;
    }

    /** Write everything pending, in order: creates, then upsert tombstones + rows, then deletes. Caller holds batchLock. */
    private void flushLocked() {
        if (!pendOsids.isEmpty()) {
            append(pendOsids, pendSources);
            pendOsids.clear();
            pendSources.clear();
        }
        if (!pendUpserts.isEmpty()) {
            List<String> ids = new ArrayList<>(pendUpserts.keySet());
            List<String> sources = new ArrayList<>(pendUpserts.values());
            tombstone(ids);
            append(ids, sources);
            pendUpserts.clear();
        }
        if (!pendDeletes.isEmpty()) {
            tombstone(pendDeletes);
            pendDeletes.clear();
        }
        pendBytes = 0;
    }

    private void append(List<String> ids, List<String> sources) {
        int n = ids.size();
        InfinoNative.BatchResult result = InfinoNative.indexBatch(
            table,
            ids.toArray(new String[0]),
            sources.toArray(new String[0]),
            n
        );
        if (!result.failed().isEmpty()) {
            InfinoNative.Failure first = result.failed().get(0);
            log.warn(
                "[infino] {} of {} documents refused by {} (first: id {}: {})",
                result.failed().size(),
                n,
                table,
                ids.get(first.index()),
                first.reason()
            );
        }
    }

    private void tombstone(List<String> ids) {
        int rc = InfinoNative.deleteOsids(table, ids.toArray(new String[0]), ids.size());
        if (rc != 0) {
            log.warn("[infino] delete of {} ids from {} returned {}", ids.size(), table, rc);
        }
    }

    private void flushBatch() {
        synchronized (batchLock) {
            flushLocked();
        }
    }

    @Override
    public void refresh(String source) throws EngineException {
        // Make pending Infino rows visible exactly when Lucene's refresh does.
        try {
            flushBatch();
        } catch (Throwable t) {
            log.warn("[infino] batch flush on refresh failed", t);
        }
        super.refresh(source);
    }

    @Override
    public void flush(boolean force, boolean waitIfOngoing) throws EngineException {
        // Flush before Lucene commits + trims the translog, so committed docs are
        // durable in Infino too (not left only in an un-flushed in-memory batch).
        try {
            flushBatch();
        } catch (Throwable t) {
            log.warn("[infino] batch flush on flush failed", t);
        }
        super.flush(force, waitIfOngoing);
    }

    @Override
    public void forceMerge(
        final boolean flush,
        int maxNumSegments,
        boolean onlyExpungeDeletes,
        final boolean upgrade,
        final boolean upgradeOnlyAncientSegments,
        final String forceMergeUUID
    ) throws EngineException, IOException {
        // A force-merge is the operator asking for fewer, larger files: compact the table.
        optimize();
        super.forceMerge(flush, maxNumSegments, onlyExpungeDeletes, upgrade, upgradeOnlyAncientSegments, forceMergeUUID);
    }

    /** Flush what is pending and compact the table; also run on a schedule by the plugin. */
    void optimize() {
        try {
            flushBatch();
            int rc = InfinoNative.optimize(table);
            if (rc != 0) {
                log.warn("[infino] optimize of {} returned {}", table, rc);
            }
        } catch (Throwable t) {
            log.warn("[infino] optimize of {} failed", table, t);
        }
    }
}
