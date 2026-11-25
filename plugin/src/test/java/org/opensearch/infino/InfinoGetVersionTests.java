/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.action.UnavailableShardsException;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.get.MultiGetItemResponse;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.replication.ClusterStateCreationUtils;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * A default-mode get reports the version, sequence number and primary term
 * the shard's skeleton holds for the document, as a stock get does, with the
 * row those numbers belong to: never the sequence number the table stored
 * beside the row, which a replace leaves at the first write's. It waits for
 * a write the skeleton found to be recorded before it reads the row,
 * answers 503 for a document it cannot tell is missing while a skeleton is
 * rebuilt, checks a version it is asked for, and is served by the node
 * holding the shard's primary.
 */
public class InfinoGetVersionTests extends OpenSearchTestCase {

    private static final String INDEX = "logs";
    private static final String ID = "d5";
    /** The primary term a fresh shard writes under. */
    private static final long TERM = 1L;
    /** The sequence number of the write that replaced the document, as the shard gave it. */
    private static final long REPLACED_SEQ = 300_000L;
    /** The version that replace took the document to. */
    private static final long REPLACED_VERSION = 2L;
    /** The sequence number the table stores beside a row a replace rewrote: the stamp of the first write, or none. */
    private static final long STORED_SEQ = 0L;
    /** How long a test waits for a read on another thread to reach a point. */
    private static final long WAIT_SECONDS = 10;

    /** What the skeleton holds of a document's latest write at {@code seq} and {@code version}. */
    private static InfinoEngine.WrittenNumbers found(long seq, long version) {
        return new InfinoEngine.WrittenNumbers(version, seq, TERM);
    }

    /** A row of the document holding {@code source}, with the sequence number the table stored beside it. */
    private static InfinoNative.Doc row(String source) {
        return new InfinoNative.Doc(source, STORED_SEQ, null);
    }

    /**
     * A shard whose skeleton answers each realtime read of the document with
     * the next of its answers, the last repeated once they run out (null for
     * none), and each read that is not realtime with {@link #refreshed};
     * {@link #knowsIds} says whether a miss is one the table holds no row
     * for either. Each wait for a write's record is noted in {@link #events}.
     */
    private static final class FakeShard implements InfinoEngine.ShardReads {
        final List<InfinoEngine.WrittenNumbers> realtime;
        InfinoEngine.WrittenNumbers refreshed;
        boolean knowsIds = true;
        final List<String> events = new ArrayList<>();

        FakeShard(InfinoEngine.WrittenNumbers... answers) {
            this.realtime = new ArrayList<>(Arrays.asList(answers));
            this.refreshed = answers[0];
        }

        @Override
        public InfinoEngine.WrittenNumbers written(String id, boolean asRealtime) {
            if (!asRealtime) {
                return refreshed;
            }
            return realtime.size() > 1 ? realtime.remove(0) : realtime.get(0);
        }

        @Override
        public void awaitRecorded(String id) {
            events.add("await " + id);
        }

        @Override
        public boolean knowsIds() {
            return knowsIds;
        }
    }

    /** Reads of the document's row, one source per call, the last repeated; each call's ids are noted in {@code events}. */
    private static Function<Collection<String>, Map<String, InfinoNative.Doc>> rowsAnswering(List<String> events, String... sources) {
        Deque<String> left = new ArrayDeque<>(List.of(sources));
        return ids -> {
            events.add("rows " + ids);
            String source = left.size() > 1 ? left.poll() : left.peek();
            Map<String, InfinoNative.Doc> out = new HashMap<>();
            for (String id : ids) {
                out.put(id, row(source));
            }
            return out;
        };
    }

    /** The calls that read rows among {@code events}. */
    private static List<String> rowReads(List<String> events) {
        return events.stream().filter(e -> e.startsWith("rows ")).toList();
    }

    private static InfinoGetActionFilter.Read read(FakeShard shard, boolean realtime, Function<Collection<String>, Map<String, InfinoNative.Doc>> rows) {
        return InfinoGetActionFilter.reads(shard, List.of(ID), realtime, rows).get(ID);
    }

    /** The get result a read answers with, from what the skeleton holds and its row. */
    private static GetResult answer(InfinoGetActionFilter.Read read) {
        return InfinoGetActionFilter.result(INDEX, ID, read.doc(), read.written(), InfinoAccess.NONE, InfinoMappingSource.WHOLE, null, null, List.of());
    }

    /**
     * After a replace, the get reported version 1 and sequence number 0, the
     * numbers stored beside the row, so a write conditioned on them
     * conflicted. It reports the skeleton's.
     */
    public void testAReplacedDocumentReportsTheShardsVersionAndSequenceNumber() {
        FakeShard shard = new FakeShard(found(REPLACED_SEQ, REPLACED_VERSION));
        GetResult got = answer(read(shard, true, rowsAnswering(shard.events, "{\"n\":10}")));
        assertTrue(got.isExists());
        assertEquals("{\"n\":10}", got.sourceAsString());
        assertEquals(REPLACED_SEQ, got.getSeqNo());
        assertEquals(REPLACED_VERSION, got.getVersion());
        assertEquals(TERM, got.getPrimaryTerm());
        assertEquals(List.of("rows [" + ID + "]"), rowReads(shard.events));
    }

    /** A proxy-mode document has no skeleton: it carries the sequence number the table stored, at version 1. */
    public void testAProxyDocumentReportsTheStoredSequenceNumber() {
        GetResult got = InfinoGetActionFilter.result(INDEX, ID, row("{\"n\":1}"), null, InfinoAccess.NONE, InfinoMappingSource.WHOLE, null, null, List.of());
        assertEquals(STORED_SEQ, got.getSeqNo());
        assertEquals(InfinoDocumentVersion.VERSION, got.getVersion());
        assertEquals(InfinoDocumentVersion.PRIMARY_TERM, got.getPrimaryTerm());
    }

    /** A document the skeleton does not hold is missing, and its row is not read. */
    public void testADocumentTheShardDoesNotHoldIsMissing() {
        FakeShard shard = new FakeShard((InfinoEngine.WrittenNumbers) null);
        InfinoGetActionFilter.Read read = read(shard, true, rowsAnswering(shard.events, "{}"));
        assertNull(read.failure());
        assertNull(read.doc());
        GetResult got = answer(read);
        assertFalse(got.isExists());
        assertEquals(SequenceNumbers.UNASSIGNED_SEQ_NO, got.getSeqNo());
        assertEquals(Versions.NOT_FOUND, got.getVersion());
        assertEquals(List.of(), shard.events);
    }

    /**
     * While a rebuilt index's skeleton is rebuilt, a document it does not
     * hold may be a row the table holds: the read answers 503, which the
     * client tries again, rather than "not found", which a client that
     * creates what is missing would act on. A document it holds is read.
     */
    public void testAMissWhileTheSkeletonIsRebuiltIsUnavailable() {
        FakeShard shard = new FakeShard((InfinoEngine.WrittenNumbers) null);
        shard.knowsIds = false;
        InfinoGetActionFilter.Read read = read(shard, true, rowsAnswering(shard.events, "{}"));
        assertNotNull(read.failure());
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ((InfinoNative.InfinoException) read.failure()).status());
        assertTrue(read.failure().getMessage(), read.failure().getMessage().endsWith(ID));
        assertEquals(List.of(), shard.events);

        FakeShard holding = new FakeShard(found(REPLACED_SEQ, REPLACED_VERSION));
        holding.knowsIds = false;
        InfinoGetActionFilter.Read found = read(holding, true, rowsAnswering(holding.events, "{\"n\":10}"));
        assertNull(found.failure());
        assertEquals("{\"n\":10}", found.doc().source());
    }

    /**
     * Each document the skeleton found waits for its writes to be recorded
     * for the table before its row is read, so the row is never older than
     * the numbers; a document it did not find waits for nothing.
     */
    public void testTheRowIsReadOnlyOnceEveryFoundWriteIsRecorded() {
        Map<String, InfinoEngine.WrittenNumbers> numbers = new LinkedHashMap<>();
        numbers.put("a", found(1, 1));
        numbers.put("gone", null);
        numbers.put("b", found(2, 1));
        List<String> events = new ArrayList<>();
        InfinoEngine.ShardReads shard = new InfinoEngine.ShardReads() {
            @Override
            public InfinoEngine.WrittenNumbers written(String id, boolean realtime) {
                return numbers.get(id);
            }

            @Override
            public void awaitRecorded(String id) {
                events.add("await " + id);
            }

            @Override
            public boolean knowsIds() {
                return true;
            }
        };
        Map<String, InfinoGetActionFilter.Read> reads = InfinoGetActionFilter.reads(shard, numbers.keySet(), true, rowsAnswering(events, "{}"));
        assertEquals(List.of("await a", "await b", "rows [a, b]"), events);
        assertNull(reads.get("gone").doc());
        assertNotNull(reads.get("a").doc());
    }

    /**
     * A write the skeleton holds and has not recorded for the table yet: it
     * holds the id's lock from its apply to its record. The read waits for
     * that lock before it reads the row, so it reads the write's row with
     * the write's numbers. Without the wait it would read the row before,
     * with the numbers of the write after it, and a write conditioned on
     * those numbers would replace a document the client never saw.
     */
    public void testAReadWaitsForAWriteTheSkeletonHoldsToReachItsRow() throws Exception {
        ReentrantLock idLock = new ReentrantLock();
        AtomicReference<InfinoEngine.WrittenNumbers> skeleton = new AtomicReference<>(found(1, 1));
        Map<String, String> table = new ConcurrentHashMap<>(Map.of(ID, "{\"n\":1}"));
        InfinoEngine.ShardReads shard = new InfinoEngine.ShardReads() {
            @Override
            public InfinoEngine.WrittenNumbers written(String id, boolean realtime) {
                return skeleton.get();
            }

            @Override
            public void awaitRecorded(String id) {
                idLock.lock();
                idLock.unlock();
            }

            @Override
            public boolean knowsIds() {
                return true;
            }
        };
        Function<Collection<String>, Map<String, InfinoNative.Doc>> rows = ids -> Map.of(ID, row(table.get(ID)));

        // The write: applied to the skeleton under the id's lock, not yet recorded.
        idLock.lock();
        skeleton.set(found(2, 2));
        AtomicReference<InfinoGetActionFilter.Read> got = new AtomicReference<>();
        Thread reader = new Thread(() -> got.set(InfinoGetActionFilter.reads(shard, List.of(ID), true, rows).get(ID)));
        try {
            reader.start();
            assertBusy(() -> assertTrue("the read waits on the id's lock", idLock.hasQueuedThread(reader)), WAIT_SECONDS, TimeUnit.SECONDS);
            table.put(ID, "{\"n\":2}");
        } finally {
            idLock.unlock();
        }
        reader.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
        assertEquals(2, got.get().written().seqNo());
        assertEquals("{\"n\":2}", got.get().doc().source());
    }

    /**
     * A write that lands between the skeleton's read and the row's read: the row
     * read is the newer write's, so the document is read again, and the
     * numbers reported are the ones that row belongs to.
     */
    public void testAWriteLandingWhileTheDocumentIsReadIsReadAgain() {
        FakeShard shard = new FakeShard(found(5, 2), found(6, 3));
        GetResult got = answer(read(shard, true, rowsAnswering(shard.events, "{\"n\":3}")));
        assertEquals(6, got.getSeqNo());
        assertEquals(3, got.getVersion());
        assertEquals("{\"n\":3}", got.sourceAsString());
        assertEquals(2, rowReads(shard.events).size());
    }

    /**
     * Writes that keep landing: the last read stands, its row read after its
     * numbers, so never older than them; a write conditioned on them meets
     * the newer write as a conflict.
     */
    public void testWritesThatKeepLandingLeaveTheLastRead() {
        FakeShard shard = new FakeShard(found(5, 2), found(6, 3), found(7, 4), found(8, 5));
        InfinoGetActionFilter.Read read = read(shard, true, rowsAnswering(shard.events, "{\"n\":3}", "{\"n\":4}", "{\"n\":5}"));
        assertEquals(7, read.written().seqNo());
        assertEquals("{\"n\":5}", read.doc().source());
        assertEquals(3, rowReads(shard.events).size());
    }

    /** A read that is not realtime misses a document the latest refresh did not make visible, and reads no row. */
    public void testAReadThatIsNotRealtimeMissesWhatNoRefreshMadeVisible() {
        FakeShard shard = new FakeShard(found(1, 1));
        shard.refreshed = null;
        InfinoGetActionFilter.Read read = read(shard, false, rowsAnswering(shard.events, "{\"n\":1}"));
        assertNull(read.doc());
        assertNull(read.written());
        assertEquals(List.of(), rowReads(shard.events));
    }

    /**
     * A read that is not realtime, of a document updated since the latest
     * refresh: the table keeps no row older than its latest commit, so the
     * row read may already be the update's. The numbers reported are the
     * update's too, never the refreshed write's beside the update's source.
     */
    public void testAReadThatIsNotRealtimeReportsTheNumbersOfTheRowItReads() {
        FakeShard shard = new FakeShard(found(2, 2));
        shard.refreshed = found(1, 1);
        InfinoGetActionFilter.Read read = read(shard, false, rowsAnswering(shard.events, "{\"n\":2}"));
        assertEquals(found(2, 2), read.written());
        assertEquals("{\"n\":2}", read.doc().source());

        // Deleted since the refresh: missing, as the row is.
        FakeShard deleted = new FakeShard((InfinoEngine.WrittenNumbers) null);
        deleted.refreshed = found(1, 1);
        assertNull(read(deleted, false, rowsAnswering(deleted.events, "{}")).doc());
    }

    /**
     * A read of a shard whose engine is not open on this node (still opening,
     * closing or being reset while the cluster state names this node as the
     * primary's) answers 503, which the client tries again, not a 500; so
     * does a read through a document rule that would commit its writes.
     */
    public void testAReadOfAShardThatIsNotOpenHereIsUnavailable() {
        InfinoNative.InfinoException notOpen = expectThrows(InfinoNative.InfinoException.class, () -> InfinoEngine.shardReads("os_not_open"));
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, notOpen.status());
        InfinoNative.InfinoException ruleRead = expectThrows(
            InfinoNative.InfinoException.class,
            () -> InfinoEngine.commitForRead("os_not_open", List.of(ID))
        );
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ruleRead.status());
    }

    /** A get that asks for a version meets a conflict, as a stock get does, unless the document is at it. */
    public void testAGetThatAsksForAVersionIsChecked() {
        ShardId shard = new ShardId(INDEX, "_na_", 0);
        assertNull(InfinoGetActionFilter.versionConflict(shard, ID, VersionType.INTERNAL, Versions.MATCH_ANY, REPLACED_VERSION));
        assertNull(InfinoGetActionFilter.versionConflict(shard, ID, VersionType.INTERNAL, REPLACED_VERSION, REPLACED_VERSION));
        Exception conflict = InfinoGetActionFilter.versionConflict(shard, ID, VersionType.INTERNAL, 1, REPLACED_VERSION);
        assertTrue(conflict instanceof VersionConflictEngineException);
        assertEquals(RestStatus.CONFLICT, ((VersionConflictEngineException) conflict).status());
        assertNull(InfinoGetActionFilter.versionConflict(shard, ID, VersionType.EXTERNAL, REPLACED_VERSION, REPLACED_VERSION));
        assertNotNull(InfinoGetActionFilter.versionConflict(shard, ID, VersionType.EXTERNAL, 10, REPLACED_VERSION));
        assertNotNull(InfinoGetActionFilter.versionConflict(shard, ID, VersionType.EXTERNAL_GTE, 1, REPLACED_VERSION));
    }

    /** A cluster state of one index of one shard, whose primary is on this node or another, in {@code primary}'s state. */
    private static ClusterState stateWithPrimary(boolean local, ShardRoutingState primary) {
        return ClusterStateCreationUtils.state(INDEX, local, primary);
    }

    private static ShardId shardOf(ClusterState state) {
        return new ShardId(state.metadata().index(INDEX).getIndex(), 0);
    }

    /**
     * A default-mode read is served where the shard's primary is: here, on
     * the node holding it, or sent on to that node; a shard with no active
     * primary answers 503.
     */
    public void testAReadGoesToThePrimarysNode() {
        ClusterState here = stateWithPrimary(true, ShardRoutingState.STARTED);
        InfinoGetActionFilter.Route local = InfinoGetActionFilter.route(here, shardOf(here));
        assertNull(local.node());
        assertNull(local.failure());

        ClusterState elsewhere = stateWithPrimary(false, ShardRoutingState.STARTED);
        InfinoGetActionFilter.Route sent = InfinoGetActionFilter.route(elsewhere, shardOf(elsewhere));
        assertNull(sent.failure());
        String primary = elsewhere.routingTable().index(INDEX).shard(0).primaryShard().currentNodeId();
        assertEquals(primary, sent.node().getId());
        assertNotEquals(elsewhere.nodes().getLocalNodeId(), sent.node().getId());

        ClusterState none = stateWithPrimary(false, ShardRoutingState.UNASSIGNED);
        InfinoGetActionFilter.Route unassigned = InfinoGetActionFilter.route(none, shardOf(none));
        assertTrue(unassigned.failure() instanceof UnavailableShardsException);
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ((UnavailableShardsException) unassigned.failure()).status());
    }

    /**
     * A read another node sent here because this node held the primary,
     * which has moved on since: it answers 503, which the client tries
     * again, rather than being sent on again, which could bounce between
     * nodes for as long as their cluster states disagree.
     */
    public void testAForwardedReadWhosePrimaryMovedIsUnavailable() {
        ThreadPool pool = new TestThreadPool(getTestName());
        try {
            InfinoServices.bind(null, pool);
            ClusterState elsewhere = stateWithPrimary(false, ShardRoutingState.STARTED);
            pool.getThreadContext().putHeader(InfinoGetActionFilter.FORWARDED, "true");
            InfinoGetActionFilter.Route route = InfinoGetActionFilter.route(elsewhere, shardOf(elsewhere));
            assertNull(route.node());
            assertTrue(route.failure() instanceof UnavailableShardsException);
            assertEquals(RestStatus.SERVICE_UNAVAILABLE, ((UnavailableShardsException) route.failure()).status());
        } finally {
            InfinoServices.bind(null, null);
            terminate(pool);
        }
    }

    /** A read that must go to the primary's node, from a node whose transport is not up: 503, as the other answers that cannot reach the primary. */
    public void testAReadThatCannotReachThePrimaryIsUnavailable() {
        ClusterState elsewhere = stateWithPrimary(false, ShardRoutingState.STARTED);
        DiscoveryNode node = InfinoGetActionFilter.route(elsewhere, shardOf(elsewhere)).node();
        PlainActionFuture<GetResponse> answer = new PlainActionFuture<>();
        InfinoGetActionFilter.forward(node, GetAction.INSTANCE, new GetRequest(INDEX, ID), answer);
        ExecutionException failed = expectThrows(ExecutionException.class, answer::get);
        assertTrue(failed.getCause() instanceof InfinoNative.InfinoException);
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ((InfinoNative.InfinoException) failed.getCause()).status());
    }

    /**
     * A multi-get split between this node and two others: each node's items
     * fill their own slots, in the request's order, and a node that fails
     * fails its own items alone, with its failure.
     */
    public void testAMultiGetSplitAcrossNodesFailsOnlyTheFailingNodesItems() throws Exception {
        ClusterState state = ClusterStateCreationUtils.state(INDEX, true, ShardRoutingState.STARTED, ShardRoutingState.STARTED, ShardRoutingState.STARTED);
        List<DiscoveryNode> others = new ArrayList<>();
        for (DiscoveryNode n : state.nodes()) {
            if (!n.getId().equals(state.nodes().getLocalNodeId())) {
                others.add(n);
            }
        }
        DiscoveryNode answering = others.get(0);
        DiscoveryNode failing = others.get(1);
        MultiGetRequest mget = new MultiGetRequest();
        for (String id : List.of("here", "there1", "down", "there2")) {
            mget.add(INDEX, id);
        }
        MultiGetItemResponse[] responses = new MultiGetItemResponse[mget.getItems().size()];
        responses[0] = item("here");
        Map<DiscoveryNode, List<Integer>> elsewhere = new LinkedHashMap<>();
        elsewhere.put(answering, List.of(1, 3));
        elsewhere.put(failing, List.of(2));
        Map<DiscoveryNode, List<String>> sent = new HashMap<>();
        PlainActionFuture<MultiGetResponse> answer = new PlainActionFuture<>();
        InfinoMultiGetActionFilter.gather(mget, responses, elsewhere, (node, part, listener) -> {
            List<String> ids = part.getItems().stream().map(MultiGetRequest.Item::id).toList();
            sent.put(node, ids);
            answerPart(node == failing, ids, listener);
        }, answer);
        MultiGetResponse got = answer.get(WAIT_SECONDS, TimeUnit.SECONDS);
        assertEquals(List.of("there1", "there2"), sent.get(answering));
        assertEquals(List.of("down"), sent.get(failing));
        MultiGetItemResponse[] items = got.getResponses();
        assertEquals("here", items[0].getResponse().getId());
        assertEquals("there1", items[1].getResponse().getId());
        assertEquals("there2", items[3].getResponse().getId());
        assertNull(items[2].getResponse());
        assertEquals("down", items[2].getFailure().getId());
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ((InfinoNative.InfinoException) items[2].getFailure().getFailure()).status());
    }

    /** A found item of {@code id}. */
    private static MultiGetItemResponse item(String id) {
        return new MultiGetItemResponse(new GetResponse(new GetResult(INDEX, id, 0, TERM, 1, true, null, Map.of(), Map.of())), null);
    }

    /** A node's answer to its part of a multi-get: an item per id, or a failure of the whole part. */
    private static void answerPart(boolean fails, List<String> ids, ActionListener<MultiGetResponse> listener) {
        if (fails) {
            listener.onFailure(new InfinoNative.InfinoException("the node stopped", RestStatus.SERVICE_UNAVAILABLE));
            return;
        }
        listener.onResponse(new MultiGetResponse(ids.stream().map(InfinoGetVersionTests::item).toArray(MultiGetItemResponse[]::new)));
    }
}
