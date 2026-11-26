/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateHelper;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.mapper.RoutingFieldMapper;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.script.ScriptService;
import org.opensearch.script.UpdateScript;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Runs an update's script against the document it targets, as the stock
 * update does: through OpenSearch's own {@link UpdateHelper}, so the script
 * sees the same {@code ctx} ({@code _source}, {@code op}, {@code _now},
 * {@code _index}, {@code _id}, {@code _version}, {@code _routing}) and its
 * {@code params}, an inline or a stored script runs alike, and an upsert,
 * a scripted upsert and a script that fails are answered as stock answers
 * them. The stock update reads the document from the shard, which holds no
 * source for an Infino-backed index; here the caller hands in the source it
 * read from the table, and writes what the script made of it.
 */
final class InfinoScriptedUpdate {

    private InfinoScriptedUpdate() {}

    /** What the script asks for. */
    enum Op {
        /** Write {@link Change#document}: the script's {@code ctx._source}, or the upsert document. */
        INDEX,
        /** Delete the document ({@code ctx.op = "delete"}). */
        DELETE,
        /** Write nothing ({@code ctx.op = "noop"} or {@code "none"}); {@link Change#noop} is the answer. */
        NOOP
    }

    /**
     * What the script made of the document: the document to write for
     * {@link Op#INDEX}, the stock answer for {@link Op#NOOP}.
     */
    record Change(Op op, String document, UpdateResponse noop) {}

    /** {@link UpdateHelper}'s preparation from a document read elsewhere than the shard. */
    private static final class Helper extends UpdateHelper {
        Helper(ScriptService scripts) {
            super(scripts);
        }

        Result run(ShardId shardId, UpdateRequest update, GetResult current, LongSupplier now) {
            return prepare(shardId, update, current, now);
        }
    }

    /**
     * Compile {@code update}'s script ahead of its run, outside any lock: the
     * node's script service keeps what it compiles, so the run finds it
     * compiled. Null when it compiled, or there is no script to compile;
     * else what the compile threw (a script that does not parse, a script
     * type or context the node does not allow, the compile rate exceeded),
     * which the run raises where stock compiles the script ({@link #compiles}).
     */
    static RuntimeException compileAhead(UpdateRequest update) {
        return compileAhead(InfinoServices.scriptService(), update);
    }

    /** {@link #compileAhead(UpdateRequest)} with the given script service. */
    static RuntimeException compileAhead(ScriptService scripts, UpdateRequest update) {
        if (update.script() == null || scripts == null) {
            return null; // without the service the run refuses the update itself
        }
        try {
            scripts.compile(update.script(), UpdateScript.CONTEXT);
            return null;
        } catch (RuntimeException e) {
            return e;
        }
    }

    /**
     * Whether stock's update compiles {@code update}'s script, given whether
     * its document {@code exists}: it runs the script over a document that
     * exists, and over a missing one only for a scripted upsert. A missing
     * document is otherwise written from the upsert, or refused as missing,
     * without the script.
     */
    static boolean compiles(UpdateRequest update, boolean exists) {
        return update.script() != null
            && (exists || (update.scriptedUpsert() && (update.upsertRequest() != null || update.docAsUpsert())));
    }

    /**
     * Run {@code update}'s script against {@code current}, the document's
     * JSON source (null or empty when there is none), written under
     * {@code seqNo} and {@code primaryTerm} at {@code version}, with the
     * node's script service and clock.
     */
    static Change run(ShardId shardId, UpdateRequest update, String current, long seqNo, long primaryTerm, long version) {
        ScriptService scripts = InfinoServices.scriptService();
        if (scripts == null) {
            // Without it the stock helper would skip the script and write the document unchanged.
            throw new IllegalStateException("[infino] the script service is not ready");
        }
        ThreadPool pools = InfinoServices.threadPool();
        LongSupplier now = pools == null ? System::currentTimeMillis : pools::absoluteTimeInMillis;
        return run(scripts, now, shardId, update, current, seqNo, primaryTerm, version);
    }

    /** {@link #run(ShardId, UpdateRequest, String, long, long, long)} with the given script service and clock. */
    static Change run(
        ScriptService scripts,
        LongSupplier now,
        ShardId shardId,
        UpdateRequest update,
        String current,
        long seqNo,
        long primaryTerm,
        long version
    ) {
        UpdateHelper.Result result = new Helper(scripts).run(shardId, update, asGetResult(shardId, update, current, seqNo, primaryTerm, version), now);
        Writeable action = result.action();
        if (action instanceof UpdateResponse noop) {
            return new Change(Op.NOOP, null, noop);
        }
        if (action instanceof DeleteRequest) {
            return new Change(Op.DELETE, null, null);
        }
        IndexRequest written = (IndexRequest) action;
        String document = InfinoBulkActionFilter.sourceAsJson(written.source(), written.getContentType());
        String unreadable = InfinoBulkActionFilter.unreadable(document);
        if (unreadable != null) {
            throw new MapperParsingException(unreadable);
        }
        return new Change(Op.INDEX, document, null);
    }

    /**
     * What stock's update answers under {@code get} when it asks for the
     * source ({@code _source} on the update): the document it wrote,
     * {@code written} (its JSON source), filtered as the update asks, under
     * the sequence number, primary term and version the write took. A
     * document the script deleted ({@code written} null) is answered without
     * a source, as stock answers it. Null when the update asks for no source.
     */
    static GetResult writtenGetResult(UpdateRequest update, String index, long seqNo, long primaryTerm, long version, String written) {
        if (update.fetchSource() == null || !update.fetchSource().fetchSource()) {
            return null;
        }
        if (written == null) {
            return new GetResult(index, update.id(), seqNo, primaryTerm, version, true, null, Map.of(), Map.of());
        }
        BytesReference bytes = new BytesArray(written);
        Map<String, Object> source = XContentHelper.convertToMap(bytes, true, MediaTypeRegistry.JSON).v2();
        return UpdateHelper.extractGetResult(update, index, seqNo, primaryTerm, version, source, MediaTypeRegistry.JSON, bytes);
    }

    /**
     * The document as the stock update reads it from the shard: its source,
     * sequence number, primary term, version and routing (the update's own,
     * the routing the document was found under), or a missing document.
     */
    private static GetResult asGetResult(ShardId shardId, UpdateRequest update, String current, long seqNo, long primaryTerm, long version) {
        String index = shardId.getIndexName();
        if (current == null || current.isEmpty()) {
            return new GetResult(index, update.id(), SequenceNumbers.UNASSIGNED_SEQ_NO, SequenceNumbers.UNASSIGNED_PRIMARY_TERM, Versions.NOT_FOUND, false, null, null, null);
        }
        boolean numbered = seqNo >= 0 && primaryTerm > SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
        Map<String, DocumentField> meta = update.routing() == null
            ? Map.of()
            : Map.of(RoutingFieldMapper.NAME, new DocumentField(RoutingFieldMapper.NAME, List.<Object>of(update.routing())));
        return new GetResult(
            index,
            update.id(),
            numbered ? seqNo : SequenceNumbers.UNASSIGNED_SEQ_NO,
            numbered ? primaryTerm : SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            version,
            true,
            new BytesArray(current),
            Map.of(),
            meta
        );
    }
}
