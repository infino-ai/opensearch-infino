package org.opensearch.infino;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.mapper.ParseContext.Document;

/**
 * Spike engine: keeps InternalEngine's write path (seqno/translog/ack) and mirrors
 * each doc to Infino (dynamic typed columns; see {@link InfinoNative}).
 *
 * <p>Writes are <b>batched</b> — Infino wants batched appends, and one append per
 * document fragments the table badly at scale. New primary creates accumulate and
 * flush as a single Infino append on a size threshold or on refresh/flush (so the
 * Infino rows become visible exactly when Lucene's do). Updates and recovery
 * replays are upserts (delete-then-append) applied immediately after flushing the
 * pending batch, which keeps the write path idempotent.
 */
public class InfinoEngine extends InternalEngine {
    private static final Logger log = LogManager.getLogger(InfinoEngine.class);
    private static final int MAX_BATCH = Integer.getInteger("infino.batch.max", 10_000);

    private final String table;
    private volatile boolean created = false;

    private final Object batchLock = new Object();
    private final List<Long> pendIds = new ArrayList<>();
    private final List<String> pendOsids = new ArrayList<>();
    private final List<String> pendSources = new ArrayList<>();

    public InfinoEngine(EngineConfig config) {
        super(config);
        this.table = InfinoNative.tableFor(config.getShardId().getIndexName(), config.getShardId().id());
        log.info("[infino] engine attached for shard table {}", table);
    }

    @Override
    public IndexResult index(Index index) throws IOException {
        // Stop the dual-write of user DATA: strip every user field from the Lucene
        // document so Lucene stores only the _id + seqno skeleton the Engine contract
        // needs (searcher, translog, docid mapping). All content lives in Infino, fed
        // from index.source() below (which carries the full JSON even with _source off).
        for (Document d : index.docs()) {
            d.getFields().removeIf(f -> !f.name().startsWith("_"));
        }
        IndexResult r = super.index(index); // Lucene path: seqno, translog, ack (skeleton only)
        try {
            String src = index.source() != null ? index.source().utf8ToString() : "{}";
            ensureCreated(src);
            long id = (long) index.id().hashCode();
            // Recovery replay or update to an existing id ⇒ upsert (idempotent); a
            // brand-new primary create is batch-appended.
            boolean upsert = index.origin().isRecovery() || !r.isCreated();
            synchronized (batchLock) {
                if (upsert) {
                    flushLocked();
                    InfinoNative.deleteOsid(table, index.id());
                    InfinoNative.indexSource(table, id, index.id(), src);
                } else {
                    pendIds.add(id);
                    pendOsids.add(index.id());
                    pendSources.add(src);
                    if (pendIds.size() >= MAX_BATCH) flushLocked();
                }
            }
        } catch (Throwable t) {
            log.warn("[infino] dual-write failed", t);
        }
        return r;
    }

    private void ensureCreated(String src) {
        if (!created) {
            synchronized (this) {
                if (!created) {
                    InfinoNative.createFromSource(table, src); // schema from first doc (idempotent)
                    created = true;
                }
            }
        }
    }

    /** Append the pending batch as one Infino write. Caller holds batchLock. */
    private void flushLocked() {
        int n = pendIds.size();
        if (n == 0) return;
        long[] ids = new long[n];
        String[] osids = new String[n];
        String[] sources = new String[n];
        for (int i = 0; i < n; i++) {
            ids[i] = pendIds.get(i);
            osids[i] = pendOsids.get(i);
            sources[i] = pendSources.get(i);
        }
        pendIds.clear();
        pendOsids.clear();
        pendSources.clear();
        int rc = InfinoNative.indexBatch(table, ids, osids, sources, n);
        if (rc != 0) log.warn("[infino] index_batch rc={} n={}", rc, n);
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
}
