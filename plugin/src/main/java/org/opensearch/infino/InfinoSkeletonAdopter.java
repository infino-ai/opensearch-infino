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
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
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
 * written with one. No ingest pipeline runs on them, a final or a system
 * one included: the row went through those when it was written. The table
 * already holds these rows, so no copy's engine writes them to the table.
 * The primary's engine takes a create as the rebuild's when this node's
 * rebuild of the shard has it in flight with the same content ({@link
 * #claim}), compared as bytes and then as a parsed document, because a
 * plugin may write the source anew before the engine sees it (the k-NN
 * plugin's derived source does for an index with a vector field). Each
 * create's source also carries {@link #MARK} (see {@link #isMarked}), which
 * the primary puts back when such a plugin dropped it ({@link #kind}), so
 * the primary's translog and its history keep it: a translog replay after a
 * restart and a copy taking over as primary leave the table alone for them
 * too. Where a plugin writes the source anew, it may do so again on a
 * replica and on a translog replay after the node restarts, which then lack
 * the mark: a replica promoted to primary, or the replay, writes those rows
 * to the table once more, as a replace with the same content.
 * Each is a create, so a client's write of the same id wins; a row a client
 * deleted while its create was in flight is deleted again through the
 * shard, so the skeleton and the translog end where the table is. While a
 * shard's skeleton may lack a row, its engine looks ids up in the table
 * ({@link InfinoEngine#looksUpIds}).
 *
 * <p>While {@link #PAUSED} is set, a rebuild waits before it reads the
 * table: the index's searches miss the rows written before the rebuild, and
 * its engine goes on looking ids up in the table.
 *
 * <p>A request that fails for a reason that passes (a rate limit, the
 * hosted service not reached or not answering, a shard or thread pool that
 * is busy: see {@link #retryable(Throwable)}) is tried again, after a wait
 * that grows to {@link #MAX_BACKOFF_MILLIS} ({@link Retrier}): a hosted
 * service answers a long rebuild with some of these, and a rebuild that
 * stopped at the first would leave the index's searches short until the
 * node starts again. Any other failure, or one that passes but goes on for
 * {@link #GIVE_UP_MILLIS} with no request going through, stops the rebuild
 * with an error; the shard closing (the index deleted, the node stopping)
 * stops it quietly.
 *
 * <p>A row the rebuild cannot create a document for (a create the shard
 * refuses, a conflict aside, which is a client's write that got there first;
 * a row with no source, or one that is no JSON object) is left out: it is
 * counted and logged, with the first few by id and why. The rebuild still
 * ends once it has walked the whole table ({@link InfinoEngine#rebuildWalked}),
 * but the skeleton stays incomplete, so the shard's writes go on looking ids
 * up in the table and a client's write of a left-out id replaces its row
 * rather than adding a second one. Each time the shard's primary starts
 * again, it walks the table again, and only a walk that leaves nothing out
 * makes the skeleton complete ({@link InfinoEngine#skeletonComplete}). A row
 * that routes to another shard and keeps no routing value, or that has no
 * id, is logged too: no write of this shard can name it.
 */
final class InfinoSkeletonAdopter {

    private static final Logger log = LogManager.getLogger(InfinoSkeletonAdopter.class);

    /**
     * The comment a skeleton-only create's JSON source opens with, right
     * after its first brace, so an engine that replays it, or writes the
     * table from the shard's history, leaves the table alone for it. It is
     * part of the operation itself, not of the request around it: a
     * thread-context header does not survive the hop to the primary's shard
     * (the security plugin's transport interceptor forwards its own headers
     * only), and an operation replayed from the translog or read from the
     * shard's history never carries one. The mapper reads JSON with comments
     * allowed, so the document is parsed as if it were not there, and the
     * content type is still detected from the leading brace. The primary
     * does not trust it: it claims a create by content ({@link #claim}).
     */
    static final String MARK = "/*infino:skeleton*/";
    /** The bytes a marked source opens with. */
    private static final byte[] MARKED_PREFIX = ("{" + MARK).getBytes(StandardCharsets.UTF_8);
    /** The pipeline name that runs no ingest pipeline: the table's row was written through them already. */
    private static final String NO_PIPELINE = "_none";
    /**
     * {@code IndexRequest#setSystemIngestPipeline} on the releases that run
     * a system ingest pipeline (3.1 and later), else null: a request whose
     * pipelines are resolved already runs that one too unless it names none.
     */
    private static final Method SET_SYSTEM_PIPELINE = systemPipelineSetter();
    /**
     * The skeleton-only creates this node's rebuilds have in flight, by
     * shard, id and source as sent: a primary takes a create as
     * skeleton-only only when it is one of these with the same content (see
     * {@link #claim}), so a client's write never is.
     */
    private static final Map<ShardId, Map<String, String>> IN_FLIGHT = new ConcurrentHashMap<>();
    /** Rows read from the table per page. */
    private static final int PAGE_ROWS = 10_000;
    /** The count's alias in the row-count statement. */
    private static final String COUNT_ALIAS = "n";
    /** Rows of each kind a walk names in its log, beyond which it only counts them. */
    static final int NAMED_ROWS = 10;

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
    /**
     * How long a rebuild's requests may go on failing, each for a reason
     * that passes, with none going through, before the rebuild stops with an
     * error: a service that answers none of them for this long is down or
     * refusing, not busy, and a rebuild left trying would hold its shard's
     * place and a generic thread with no end.
     */
    static final long GIVE_UP_MILLIS = 30 * 60_000L;

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
                    "[infino] the skeleton of {} could not be rebuilt from table {}: a request was refused, or failed for longer than the "
                        + "rebuild tries again; the rebuild stops and the index's searches miss the rows written before it until the shard "
                        + "starts again (close and open the index, or restart the node)",
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
            InfinoEngine.rebuildWalked(shardId);
            InfinoEngine.skeletonComplete(shardId);
            return;
        }
        log.info("[infino] rebuilding the skeleton of {}: {} documents for {} rows of table {}", shardId, docs, rows, table);
        // A row written with a routing value keeps it; a table that never
        // had one has no column for it.
        boolean routed = retrier.call("reading its columns", () -> columnsOf(table)).containsKey(InfinoQueryTranslator.ROUTING);
        long created = 0;
        // Rows of this shard the skeleton has no document for, which a write of this shard may name.
        Named leftOut = new Named();
        // Rows no write of this shard can name: no id, or an id that routes to another shard.
        Named unreachable = new Named();
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
                        unreachable.add("a row", "has no id");
                        continue;
                    }
                    after = id.toString();
                    Object r = routed ? row.get(InfinoQueryTranslator.ROUTING) : null;
                    String routing = r == null ? null : r.toString();
                    if (OperationRouting.generateShardId(md, after, routing) != shardId.id()) {
                        // Written with a routing value before the table kept them: there
                        // is no routing that brings it back to this shard.
                        unreachable.add(after, "routes to another shard by its id and keeps no routing value");
                        continue;
                    }
                    if (searcher.count(new TermQuery(new Term(IdFieldMapper.NAME, Uid.encodeId(after)))) > 0) {
                        continue;
                    }
                    if (source == null) {
                        leftOut.add(after, "has no source");
                    } else {
                        missing.put(after, new Row(source.toString(), routing));
                    }
                }
            }
            if (!missing.isEmpty()) {
                created += createMissing(
                    shardId,
                    table,
                    missing,
                    leftOut,
                    retrier,
                    InfinoSkeletonAdopter::run,
                    ids -> InfinoNative.existingOsidsLatest(table, ids)
                );
            }
            if (page.size() < PAGE_ROWS) {
                break;
            }
        }
        // The whole table is walked. A row left out keeps the skeleton
        // incomplete: taken as whole, it would let a client's write of a
        // left-out row's id be appended as a new row beside the table's.
        InfinoEngine.rebuildWalked(shardId);
        if (leftOut.count > 0) {
            log.error(
                "[infino] the skeleton of {} has no document for {} rows of table {}: {}. The index's searches miss those rows; the "
                    + "shard's writes go on looking ids up in the table, so a write of one of those ids replaces its row. Fix the cause "
                    + "(or delete those ids): each time the shard's primary starts again (close and open the index, or restart the node) "
                    + "it walks the table again, and a walk that leaves nothing out ends the lookups",
                shardId,
                leftOut.count,
                table,
                leftOut
            );
        } else {
            InfinoEngine.skeletonComplete(shardId);
        }
        if (unreachable.count > 0) {
            log.warn(
                "[infino] the skeleton of {} has no document for {} rows of table {} that no write of this shard can name, which the "
                    + "index's searches miss: {}",
                shardId,
                unreachable.count,
                table,
                unreachable
            );
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
    record Row(String source, String routing) {}

    /**
     * Rows counted, the first {@link #NAMED_ROWS} by id and why: what a walk
     * left out, for its log.
     */
    static final class Named {
        long count;
        final List<String> named = new ArrayList<>();

        void add(String id, String why) {
            count++;
            if (named.size() < NAMED_ROWS) {
                named.add(id + " (" + why + ")");
            }
        }

        @Override
        public String toString() {
            long more = count - named.size();
            return String.join(", ", named) + (more > 0 ? ", and " + more + " more" : "");
        }
    }

    /**
     * The table's columns. The bridge answers none when it cannot read
     * them, as it does for a table that does not exist. A table that does
     * not exist fails the row count first, which is read just before; so
     * none here is a read that failed (on a hosted table, a request to the
     * service), and it is tried again, for no longer than
     * {@link #GIVE_UP_MILLIS}.
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
     * {@code bulk} runs a bulk as the node and {@code kept} says which of
     * some ids the table holds as of its latest commit. A create that fails
     * for a reason that passes (a full write pool, a shard not ready) is sent
     * again. One that meets a document of its id (a conflict) is done: a
     * client's write got there first, or, when it was sent before, this very
     * create did and its answer was lost, so it is checked against the table
     * as a created one is. Any other refusal leaves the row out of the
     * skeleton, in {@code leftOut}. Returns the documents created (or found
     * created by an earlier send of the same create).
     */
    static long createMissing(
        ShardId shardId,
        String table,
        Map<String, Row> missing,
        Named leftOut,
        Retrier retrier,
        Function<BulkRequest, BulkResponse> bulk,
        Function<List<String>, Set<String>> kept
    ) {
        Map<String, IndexRequest> creates = new LinkedHashMap<>();
        Map<String, String> sources = new LinkedHashMap<>();
        for (Map.Entry<String, Row> row : missing.entrySet()) {
            String source = marked(row.getValue().source());
            if (source == null) {
                leftOut.add(row.getKey(), "holds no JSON object");
                continue;
            }
            sources.put(row.getKey(), source);
            creates.put(row.getKey(), createOf(shardId.getIndexName(), row.getKey(), row.getValue().routing(), source));
        }
        Map<String, Integer> sends = new HashMap<>();
        Map<String, String> inFlight = new ConcurrentHashMap<>();
        Map<String, BulkItemResponse.Failure> failed;
        track(shardId, inFlight);
        try {
            failed = sendAll("creating documents", creates, retrier, bulk, ids -> {
                for (String id : ids) {
                    // A claim is taken once: a create sent again is claimable again.
                    inFlight.put(id, sources.get(id));
                    sends.merge(id, 1, Integer::sum);
                }
            });
        } finally {
            untrack(shardId, inFlight);
        }
        List<String> created = new ArrayList<>();
        for (String id : creates.keySet()) {
            BulkItemResponse.Failure failure = failed.get(id);
            if (failure == null || (failure.getStatus() == RestStatus.CONFLICT && sends.getOrDefault(id, 0) > 1)) {
                created.add(id);
            } else if (failure.getStatus() != RestStatus.CONFLICT) {
                leftOut.add(id, "refused: " + failure.getMessage());
                log.debug("[infino] the skeleton document of {} in {} was refused", id, shardId, failure.getCause());
            }
        }
        if (created.isEmpty()) {
            return 0;
        }
        // As of the table's latest commit: a row another node deleted within the
        // engine's staleness window would otherwise read as kept, and its
        // skeleton stay.
        Set<String> keptIds = retrier.call("reading which of its rows are kept", () -> kept.apply(created));
        Map<String, DeleteRequest> deletes = new LinkedHashMap<>();
        for (String id : created) {
            if (!keptIds.contains(id)) {
                deletes.put(id, new DeleteRequest(shardId.getIndexName(), id).routing(missing.get(id).routing()));
            }
        }
        if (!deletes.isEmpty()) {
            Map<String, BulkItemResponse.Failure> notDeleted = sendAll("deleting documents whose rows were deleted", deletes, retrier, bulk, ids -> {});
            if (!notDeleted.isEmpty()) {
                BulkItemResponse.Failure first = notDeleted.values().iterator().next();
                log.warn(
                    "[infino] {} documents of {} whose rows table {} deleted during the rebuild could not be deleted (the first: {}); "
                        + "a search may name them until they are",
                    notDeleted.size(),
                    shardId,
                    table,
                    first.getMessage()
                );
            }
        }
        return created.size();
    }

    /**
     * The create of {@code id} from the table's row: no ingest pipeline runs
     * on it, not even the index's final or system one, which the table's row
     * went through when it was written. A pipeline would run again on it,
     * and write the source anew without the mark.
     */
    static IndexRequest createOf(String index, String id, String routing, String source) {
        IndexRequest create = new IndexRequest(index).id(id)
            .routing(routing)
            .source(source, XContentType.JSON)
            .opType(DocWriteRequest.OpType.CREATE)
            .setPipeline(NO_PIPELINE)
            .setFinalPipeline(NO_PIPELINE);
        if (SET_SYSTEM_PIPELINE != null) {
            try {
                SET_SYSTEM_PIPELINE.invoke(create, NO_PIPELINE);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new IllegalStateException("[infino] a rebuild's create could not be kept from the system ingest pipeline", e);
            }
        }
        // Resolved already, so the bulk does not read the pipelines from the index's settings.
        return create.isPipelineResolved(true);
    }

    private static Method systemPipelineSetter() {
        try {
            return IndexRequest.class.getMethod("setSystemIngestPipeline", String.class);
        } catch (NoSuchMethodException e) {
            return null; // a release with no system ingest pipeline
        }
    }

    /**
     * Send {@code requests} (by id) as bulks until each has an answer that is
     * not a failure that passes: the items that fail for a reason that passes
     * are sent again together after the retrier's wait. Returns the failure
     * each id ended with; an id that went through is absent. {@code sending}
     * is told the ids of each bulk before it is sent, a bulk sent again after
     * a lost answer included.
     */
    static Map<String, BulkItemResponse.Failure> sendAll(
        String request,
        Map<String, ? extends DocWriteRequest<?>> requests,
        Retrier retrier,
        Function<BulkRequest, BulkResponse> bulk,
        Consumer<Collection<String>> sending
    ) {
        Map<String, DocWriteRequest<?>> pending = new LinkedHashMap<>(requests);
        Map<String, BulkItemResponse.Failure> failed = new HashMap<>();
        while (!pending.isEmpty()) {
            BulkRequest sent = new BulkRequest();
            pending.values().forEach(sent::add);
            BulkResponse response = retrier.attempt(request, () -> {
                sending.accept(pending.keySet());
                return bulk.apply(sent);
            });
            boolean answered = false;
            BulkItemResponse.Failure again = null;
            for (BulkItemResponse item : response.getItems()) {
                if (item.isFailed() && retryable(item.getFailure().getStatus())) {
                    again = item.getFailure();
                    continue;
                }
                if (item.isFailed()) {
                    failed.put(item.getId(), item.getFailure());
                }
                pending.remove(item.getId());
                answered = true;
            }
            if (answered) {
                retrier.progress();
            }
            if (again == null) {
                break;
            }
            retrier.failed(request, again.getCause());
        }
        return failed;
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
     * a primary as one of its rebuild's creates ({@link #kind}), whose row
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

    /** Whose write an index operation is, as the engine of {@link #kind} decides it. */
    enum Kind {
        /** A client's write, which the engine writes to the table. */
        CLIENT,
        /** A skeleton rebuild's create, whose row the table holds already. */
        REBUILD,
        /** A write that carries the rebuild's mark but is no create of a rebuild: refused. */
        FORGED
    }

    /**
     * Whose write {@code index}, applied by an engine of {@code shard}, is.
     * On the primary, a create is the rebuild's when this node's rebuild of
     * the shard has it in flight with the same content ({@link #claim}); the
     * mark is then put back on its source when a plugin dropped it, so the
     * translog and the shard's history keep it. A primary write that carries
     * the mark but is not claimed is forged: a client cannot keep a write out
     * of the table with it. Any other copy takes the mark as the primary
     * left it: a replay, a replica, a recovery.
     */
    static Kind kind(ShardId shard, Engine.Index index) {
        boolean marked = isMarked(index.source());
        if (index.origin() != Engine.Operation.Origin.PRIMARY) {
            return marked ? Kind.REBUILD : Kind.CLIENT;
        }
        MediaType type = index.parsedDoc() == null ? null : index.parsedDoc().getMediaType();
        if (index.version() == Versions.MATCH_DELETED && claim(shard, index.id(), index.source(), type)) {
            if (!marked && (type == null || type == MediaTypeRegistry.JSON)) {
                String source = marked(index.source().utf8ToString());
                if (source != null) {
                    index.parsedDoc().setSource(new BytesArray(source), XContentType.JSON);
                }
            }
            return Kind.REBUILD;
        }
        return marked ? Kind.FORGED : Kind.CLIENT;
    }

    /**
     * Whether a create of {@code id} that the primary of {@code shard}
     * applies is one this node's rebuild of the shard has in flight, with the
     * same content, so its row is in the table already; it is claimed once.
     * The rebuild runs on the node of the shard's primary, so its creates
     * reach this node's engine. The content is compared as bytes first, then
     * as parsed documents ({@code type} is the source's, null for JSON): a
     * plugin may have written the source anew. A create with other content,
     * or one the rebuild does not have in flight, is a client's write.
     */
    static boolean claim(ShardId shard, String id, BytesReference source, MediaType type) {
        Map<String, String> inFlight = IN_FLIGHT.get(shard);
        if (inFlight == null || source == null) {
            return false;
        }
        String sent = inFlight.get(id);
        return sent != null && sameContent(sent, source, type) && inFlight.remove(id, sent);
    }

    /** Whether {@code source} (of media type {@code type}, null for JSON) holds the JSON document {@code sent}. */
    static boolean sameContent(String sent, BytesReference source, MediaType type) {
        BytesArray sentBytes = new BytesArray(sent);
        if ((type == null || type == MediaTypeRegistry.JSON) && sentBytes.equals(source)) {
            return true;
        }
        try {
            return XContentHelper.convertToMap(sentBytes, false, MediaTypeRegistry.JSON)
                .v2()
                .equals(XContentHelper.convertToMap(source, false, type == null ? MediaTypeRegistry.JSON : type).v2());
        } catch (RuntimeException e) {
            return false; // a source that does not parse is no rebuild's
        }
    }

    /** Hold {@code inFlight} (source by id) as the creates the rebuild of {@code shard} has in flight, for {@link #claim}. */
    static void track(ShardId shard, Map<String, String> inFlight) {
        IN_FLIGHT.put(shard, inFlight);
    }

    /** Let go of {@code inFlight}, once its creates have been answered. */
    static void untrack(ShardId shard, Map<String, String> inFlight) {
        IN_FLIGHT.remove(shard, inFlight);
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
     * Whether a request answered {@code status} may go through when tried
     * again: a rate limit or a full thread pool (429), or a service, a shard
     * or a gateway not ready or not answering (502, 503, 504). Any other
     * status, a 500 among them, says the request failed, and trying it again
     * would fail it again.
     */
    static boolean retryable(RestStatus status) {
        return status == RestStatus.TOO_MANY_REQUESTS
            || status == RestStatus.BAD_GATEWAY
            || status == RestStatus.SERVICE_UNAVAILABLE
            || status == RestStatus.GATEWAY_TIMEOUT;
    }

    /**
     * Whether {@code e} may pass when its request is tried again. A bridge
     * error passes when the bridge marks it as the hosted service not reached
     * or not answering ({@link InfinoNative.InfinoException#unavailable}: a
     * transport failure, or a 502, 503 or 504), or when its status is one
     * that passes ({@link #retryable(RestStatus)}), as the service's 429 is;
     * one without a status reads as a 500 and does not. A failure of the
     * cluster passes when it is a full thread pool or carries a status that
     * passes. Anything else is a fault of the request or of the rebuild,
     * which a retry would only repeat.
     */
    static boolean retryable(Throwable e) {
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        if (cause instanceof InfinoNative.InfinoException infino) {
            return infino.unavailable() || retryable(infino.status());
        }
        return cause instanceof OpenSearchRejectedExecutionException
            || (cause instanceof OpenSearchException && retryable(ExceptionsHelper.status(cause)));
    }

    /**
     * A rebuild whose requests went on failing, each for a reason that
     * passes, for {@link #GIVE_UP_MILLIS} with none going through: it stops
     * with an error, as a refusal does.
     */
    static final class GaveUp extends RuntimeException {
        GaveUp(String what, String request, long millis, Throwable last) {
            super("the skeleton rebuild of " + what + " gave up after " + millis + " ms with no request going through; " + request + " failed last", last);
        }
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
     * is thrown as it is; failures that pass but go on for its give-up time
     * ({@link #GIVE_UP_MILLIS}) with nothing going through ({@link #progress})
     * throw {@link GaveUp}. Once {@code stopped} answers true it throws
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
        private final long giveUpMillis;
        private long backoff = FIRST_BACKOFF_MILLIS;
        private long retried;
        private long failedSinceLog;
        private long loggedAt;
        private boolean logged;
        /** When the failures since the last request that went through began, or -1 for none. */
        private long failingSince = -1;

        /** For the rebuild of {@code what}, as the log names it; {@code clock} answers milliseconds. */
        Retrier(String what, BooleanSupplier stopped, Sleeper sleeper, LongSupplier clock) {
            this(what, stopped, sleeper, clock, GIVE_UP_MILLIS);
        }

        /** As above, giving up after {@code giveUpMillis} of failures with nothing going through. */
        Retrier(String what, BooleanSupplier stopped, Sleeper sleeper, LongSupplier clock, long giveUpMillis) {
            this.what = what;
            this.stopped = stopped;
            this.sleeper = sleeper;
            this.clock = clock;
            this.giveUpMillis = giveUpMillis;
        }

        /** What {@code call} answers, tried until it goes through, which counts as {@link #progress}; {@code request} names it in the log. */
        <T> T call(String request, Supplier<T> call) {
            T out = attempt(request, call);
            progress();
            return out;
        }

        /**
         * What {@code call} answers, tried until it goes through, with no
         * {@link #progress} counted: for a bulk whose answer may still hold
         * failures of its items, whose caller says whether it got anywhere.
         */
        <T> T attempt(String request, Supplier<T> call) {
            while (true) {
                stopIfStopped();
                try {
                    return call.get();
                } catch (RuntimeException e) {
                    // The shard closing under a request fails it too: that is no refusal.
                    stopIfStopped();
                    if (!retryable(e)) {
                        throw e;
                    }
                    failed(request, e);
                }
            }
        }

        /** A request went through: the give-up time starts again, and the wait halves. */
        void progress() {
            failingSince = -1;
            backoff = Math.max(FIRST_BACKOFF_MILLIS, backoff / 2);
        }

        /**
         * Count a failure of {@code request} that may pass, with {@code cause},
         * and wait before it is tried again; throws {@link GaveUp} when the
         * failures since the last {@link #progress} have gone on for the
         * give-up time.
         */
        void failed(String request, Throwable cause) {
            long now = clock.getAsLong();
            if (failingSince < 0) {
                failingSince = now;
            } else if (now - failingSince >= giveUpMillis) {
                throw new GaveUp(what, request, now - failingSince, cause);
            }
            retried++;
            failedSinceLog++;
            long wait = backoff / 2 + Randomness.get().nextLong(backoff / 2 + 1);
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
