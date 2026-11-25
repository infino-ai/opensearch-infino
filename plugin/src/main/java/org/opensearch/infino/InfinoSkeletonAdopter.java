/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
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
import org.opensearch.common.Randomness;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

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
 * written with one. The table already holds these rows, so no copy's
 * engine writes them to the table: each create's source carries
 * {@link #MARK} (see {@link #isMarked}), which the shard's translog and its
 * history keep with the document, so a translog replay after a restart and
 * a copy taking over as primary leave the table alone for them too.
 * Each is a create, so a client's write of the same id wins; a row a client
 * deleted while its create was in flight is deleted again through the
 * shard, so the skeleton and the translog end where the table is. Until a
 * shard's skeleton is whole, its engine looks ids up in the table
 * ({@link InfinoEngine#skeletonComplete}).
 *
 * <p>While {@link #PAUSED} is set, a rebuild waits before it reads the
 * table: the index's searches miss the rows written before the rebuild, and
 * its engine goes on looking ids up in the table.
 *
 * <p>A request that fails for a reason that passes (a rate limit, a server
 * error, a transport error, a shard or thread pool that is busy) is tried
 * again, after a wait that grows to {@link #MAX_BACKOFF_MILLIS}, until it
 * goes through ({@link Retrier}): a hosted service answers a long rebuild
 * with some of these, and a rebuild that stopped at the first would leave
 * the index's searches short until the node starts again. Only a refusal
 * (a key or a statement the service refuses) stops it, with an error; the
 * shard closing (the index deleted, the node stopping) stops it quietly.
 */
final class InfinoSkeletonAdopter {

    private static final Logger log = LogManager.getLogger(InfinoSkeletonAdopter.class);

    /**
     * The comment a skeleton-only create's JSON source opens with, right
     * after its first brace, so every engine that applies it writes Lucene
     * alone. It is part of the operation itself, not of the request around
     * it: a thread-context header does not survive the hop to the primary's
     * shard (the security plugin's transport interceptor forwards its own
     * headers only), and an operation replayed from the translog or read
     * from the shard's history never carries one. The mapper reads JSON with
     * comments allowed, so the document is parsed as if it were not there,
     * and the content type is still detected from the leading brace.
     */
    static final String MARK = "/*infino:skeleton*/";
    /** The bytes a marked source opens with. */
    private static final byte[] MARKED_PREFIX = ("{" + MARK).getBytes(StandardCharsets.UTF_8);
    /** The pipeline name that runs no ingest pipeline: the table's row was written through them already. */
    private static final String NO_PIPELINE = "_none";
    /**
     * The skeleton-only creates this node's rebuilds have in flight, by
     * shard, id and marked source: a primary takes a marked create as
     * skeleton-only only when it is one of these (see {@link #claim}), so a
     * client cannot send the mark to keep its write out of the table.
     */
    private static final Map<ShardId, Map<String, String>> IN_FLIGHT = new ConcurrentHashMap<>();
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

    /** The first wait after a request fails. */
    static final long FIRST_BACKOFF_MILLIS = 500;
    /**
     * The longest wait between two tries of a request: the wait doubles
     * after each failure up to this, so a rate limit that lasts minutes is
     * tried about twice a minute.
     */
    static final long MAX_BACKOFF_MILLIS = 30_000;
    /** How often a rebuild whose requests keep failing says so, after it says so for the first failure. */
    static final long RETRY_LOG_INTERVAL_MILLIS = 60_000;

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
            } catch (Stopped e) {
                log.info("[infino] the skeleton rebuild of {} stopped: the shard closed or the node is stopping", shardId);
            } catch (Exception e) {
                if (shard.state() == IndexShardState.CLOSED) {
                    log.info("[infino] the skeleton rebuild of {} stopped: the shard closed", shardId, e);
                    return;
                }
                log.error(
                    "[infino] the skeleton of {} could not be rebuilt from table {}: a request failed in a way no retry changes, so the "
                        + "rebuild stops and the index's searches miss the rows written before it until the shard starts again",
                    shardId,
                    table,
                    e
                );
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
        Retrier retrier = new Retrier(
            shardId + " from table " + table,
            () -> shard.state() == IndexShardState.CLOSED || Thread.currentThread().isInterrupted(),
            Thread::sleep,
            System::currentTimeMillis
        );
        IndexMetadata md = shard.indexSettings().getIndexMetadata();
        long rows = retrier.call("counting its rows", () -> tableRows(table));
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
        boolean routed = retrier.call("reading its columns", () -> columnsOf(table)).containsKey(InfinoQueryTranslator.ROUTING);
        long created = 0;
        long unroutable = 0;
        String after = null;
        while (true) {
            String sql = pageSql(table, after, routed);
            List<Map<String, Object>> page = retrier.call("reading a page of its rows", () -> InfinoNative.sqlRows(sql));
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
                created += createMissing(shardId, table, missing, retrier);
            }
            if (page.size() < PAGE_ROWS) {
                break;
            }
        }
        InfinoEngine.skeletonComplete(shardId);
        if (unroutable > 0) {
            log.warn("[infino] {} rows of table {} route to another shard of {} by their id and keep no routing value; they are left out of its skeleton", unroutable, table, shardId);
        }
        log.info(
            "[infino] rebuilt the skeleton of {}: created {} documents from table {}, trying {} failed requests again",
            shardId,
            created,
            table,
            retrier.retried()
        );
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

    /**
     * The table's columns. The bridge answers none when it cannot read
     * them, as it does for a table that does not exist; a table the rebuild
     * reads holds rows, so none means the read failed, and it is tried again.
     */
    private static Map<String, InfinoNative.Column> columnsOf(String table) {
        Map<String, InfinoNative.Column> columns = InfinoNative.columns(table);
        if (columns.isEmpty()) {
            throw new InfinoNative.InfinoException("the columns of table " + table + " could not be read", RestStatus.SERVICE_UNAVAILABLE);
        }
        return columns;
    }

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
     * Returns the documents created. A create that fails for a reason that
     * passes (a full write pool, a shard not ready) is sent again; one a
     * client's write of the same id beat is not.
     */
    private static long createMissing(ShardId shardId, String table, Map<String, Row> missing, Retrier retrier) {
        Map<String, IndexRequest> pending = new LinkedHashMap<>();
        Map<String, String> sources = new LinkedHashMap<>();
        for (Map.Entry<String, Row> row : missing.entrySet()) {
            String source = marked(row.getValue().source());
            if (source == null) {
                log.warn("[infino] the row of {} in table {} holds no JSON object; it is left out of the skeleton of {}", row.getKey(), table, shardId);
                continue;
            }
            sources.put(row.getKey(), source);
            pending.put(
                row.getKey(),
                new IndexRequest(shardId.getIndexName()).id(row.getKey())
                    .routing(row.getValue().routing())
                    .source(source, XContentType.JSON)
                    .opType(DocWriteRequest.OpType.CREATE)
                    .setPipeline(NO_PIPELINE)
            );
        }
        if (pending.isEmpty()) {
            return 0;
        }
        List<String> created = new ArrayList<>();
        Map<String, String> inFlight = new ConcurrentHashMap<>();
        IN_FLIGHT.put(shardId, inFlight);
        try {
            while (true) {
                BulkRequest creates = new BulkRequest();
                for (Map.Entry<String, IndexRequest> create : pending.entrySet()) {
                    // A claim is taken once: a create sent again is claimable again.
                    inFlight.put(create.getKey(), sources.get(create.getKey()));
                    creates.add(create.getValue());
                }
                BulkItemResponse.Failure again = null;
                for (BulkItemResponse item : retrier.call("creating documents", () -> run(creates)).getItems()) {
                    if (!item.isFailed()) {
                        created.add(item.getId());
                        pending.remove(item.getId());
                    } else if (retryable(item.getFailure().getStatus())) {
                        again = item.getFailure();
                    } else {
                        pending.remove(item.getId());
                    }
                }
                if (again == null) {
                    break;
                }
                retrier.failed("creating documents", again.getCause());
            }
        } finally {
            IN_FLIGHT.remove(shardId, inFlight);
        }
        if (created.isEmpty()) {
            return 0;
        }
        // As of the table's latest commit: a row another node deleted within the
        // engine's staleness window would otherwise read as kept, and its
        // skeleton stay.
        Set<String> kept = retrier.call("reading which of its rows are kept", () -> InfinoNative.existingOsidsLatest(table, created));
        BulkRequest deletes = new BulkRequest();
        for (String id : created) {
            if (!kept.contains(id)) {
                deletes.add(new DeleteRequest(shardId.getIndexName(), id).routing(missing.get(id).routing()));
            }
        }
        if (deletes.numberOfActions() > 0) {
            retrier.call("deleting documents whose rows were deleted", () -> run(deletes));
        }
        return created.size();
    }

    /**
     * {@code source} with {@link #MARK} after its opening brace, or null when
     * it is no JSON object.
     */
    static String marked(String source) {
        String s = source.strip();
        if (!s.startsWith("{")) {
            return null;
        }
        return "{" + MARK + s.substring(1);
    }

    /**
     * Whether {@code source} is a skeleton-only create's (see {@link #MARK}).
     * A replayed operation or one of a shard's history that is was taken by
     * a primary as one of its rebuild's creates ({@link #claim}), whose row
     * the table holds.
     */
    static boolean isMarked(BytesReference source) {
        if (source == null || source.length() < MARKED_PREFIX.length) {
            return false;
        }
        for (int i = 0; i < MARKED_PREFIX.length; i++) {
            if (source.get(i) != MARKED_PREFIX[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether a marked create of {@code id} the primary of {@code shard}
     * applies is one this node's rebuild of the shard sent, with this very
     * source; it is claimed once. The rebuild runs on the node of the
     * shard's primary, so its creates reach this node's engine. One that is
     * not (a client's write that carries the mark) is refused by the engine.
     */
    static boolean claim(ShardId shard, String id, BytesReference source) {
        Map<String, String> inFlight = IN_FLIGHT.get(shard);
        if (inFlight == null || source == null) {
            return false;
        }
        String expected = inFlight.get(id);
        return expected != null && expected.equals(source.utf8ToString()) && inFlight.remove(id, expected);
    }

    /** Run {@code bulk} as the node and wait for it. */
    private static BulkResponse run(BulkRequest bulk) {
        ThreadContext context = InfinoServices.threadPool().getThreadContext();
        try (ThreadContext.StoredContext ignored = context.stashContext()) {
            PlainActionFuture<BulkResponse> response = PlainActionFuture.newFuture();
            InfinoClient.bulk(bulk, response);
            return response.actionGet();
        }
    }

    /**
     * A rebuild stopped without being refused: its shard closed (the index
     * deleted or moved, the node stopping) or its thread was interrupted.
     * The shard's next start rebuilds it.
     */
    static final class Stopped extends RuntimeException {
        Stopped() {
            super("the shard closed or the node is stopping", null, false, false);
        }
    }

    /**
     * Whether a request that failed with {@code status} may go through when
     * tried again: a rate limit, or a server error. The bridge reports a
     * transport error (a timeout, a connection refused or reset) as a 500,
     * and a write pool that is full or a shard that is not ready answer 429
     * and 503. Any other status is a refusal that no retry changes.
     */
    static boolean retryable(RestStatus status) {
        return status == RestStatus.TOO_MANY_REQUESTS || status.getStatus() >= RestStatus.INTERNAL_SERVER_ERROR.getStatus();
    }

    /**
     * Whether {@code e} may pass when its request is tried again
     * ({@link #retryable(RestStatus)}). Only a failure that carries a status
     * (and a full thread pool, which answers 429) is the answer of the
     * service or the cluster; any other is a fault of the rebuild itself,
     * which a retry would only repeat.
     */
    static boolean retryable(Throwable e) {
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        return (cause instanceof OpenSearchException || cause instanceof OpenSearchRejectedExecutionException)
            && retryable(ExceptionsHelper.status(cause));
    }

    /**
     * Tries the requests of one rebuild until each goes through. After a
     * failure that may pass ({@link #retryable(Throwable)}) it waits and
     * tries again: the wait starts at {@link #FIRST_BACKOFF_MILLIS}, doubles
     * up to {@link #MAX_BACKOFF_MILLIS}, is drawn between half and all of
     * that so the shards of a node do not try again together, and halves
     * after each request that goes through, so a service that keeps
     * answering 429 is not sent the next request at once. It says so for
     * the first failure and then at most once per
     * {@link #RETRY_LOG_INTERVAL_MILLIS}, with the failures since. A refusal
     * is thrown as it is. Once {@code stopped} answers true it throws
     * {@link Stopped}: it asks before each try and every
     * {@link #PAUSE_POLL_MILLIS} of a wait, so a closed shard or a stopping
     * node ends the loop within that.
     */
    static final class Retrier {

        /** Waits, as {@link Thread#sleep} does. */
        interface Sleeper {
            void sleep(long millis) throws InterruptedException;
        }

        private final String what;
        private final BooleanSupplier stopped;
        private final Sleeper sleeper;
        private final LongSupplier clock;
        private long backoff = FIRST_BACKOFF_MILLIS;
        private long retried;
        private long failedSinceLog;
        private long loggedAt;
        private boolean logged;

        /** For the rebuild of {@code what}, as the log names it; {@code clock} answers milliseconds. */
        Retrier(String what, BooleanSupplier stopped, Sleeper sleeper, LongSupplier clock) {
            this.what = what;
            this.stopped = stopped;
            this.sleeper = sleeper;
            this.clock = clock;
        }

        /** What {@code call} answers, tried until it goes through; {@code request} names it in the log. */
        <T> T call(String request, Supplier<T> call) {
            while (true) {
                stopIfStopped();
                T out;
                try {
                    out = call.get();
                } catch (RuntimeException e) {
                    // The shard closing under a request fails it too: that is no refusal.
                    stopIfStopped();
                    if (!retryable(e)) {
                        throw e;
                    }
                    failed(request, e);
                    continue;
                }
                backoff = Math.max(FIRST_BACKOFF_MILLIS, backoff / 2);
                return out;
            }
        }

        /** Count a failure of {@code request} that may pass, with {@code cause}, and wait before it is tried again. */
        void failed(String request, Throwable cause) {
            retried++;
            failedSinceLog++;
            long wait = backoff / 2 + Randomness.get().nextLong(backoff / 2 + 1);
            long now = clock.getAsLong();
            if (!logged || now - loggedAt >= RETRY_LOG_INTERVAL_MILLIS) {
                log.warn(
                    "[infino] the skeleton rebuild of {} tries again in {} ms: {} failed, {} failures since the last report, the last: {}",
                    what,
                    wait,
                    request,
                    failedSinceLog,
                    String.valueOf(cause)
                );
                logged = true;
                loggedAt = now;
                failedSinceLog = 0;
            }
            pause(wait);
            backoff = Math.min(backoff * 2, MAX_BACKOFF_MILLIS);
        }

        /** The failures tried again so far. */
        long retried() {
            return retried;
        }

        /** Wait {@code millis}, asking every {@link #PAUSE_POLL_MILLIS} whether to stop. */
        private void pause(long millis) {
            long until = clock.getAsLong() + millis;
            for (long left = millis; left > 0; left = until - clock.getAsLong()) {
                stopIfStopped();
                try {
                    sleeper.sleep(Math.min(left, PAUSE_POLL_MILLIS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new Stopped();
                }
            }
            stopIfStopped();
        }

        private void stopIfStopped() {
            if (stopped.getAsBoolean()) {
                throw new Stopped();
            }
        }
    }
}
