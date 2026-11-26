/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.engine.DocumentMissingException;
import org.opensearch.index.get.GetResult;
import org.opensearch.script.MockScriptEngine;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptModule;
import org.opensearch.script.ScriptService;
import org.opensearch.script.ScriptType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/** How an update's script is run against a document read from the table. */
public class InfinoScriptedUpdateTests extends OpenSearchTestCase {

    private static final ShardId SHARD = new ShardId("logs", "_na_", 0);
    /** The clock the scripts see as {@code ctx._now}. */
    private static final long NOW = 1_700_000_000_000L;
    private static final long SEQ_NO = 7L;
    private static final long PRIMARY_TERM = 2L;
    private static final long VERSION = 3L;

    private static ScriptService scripts() {
        Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
        scripts.put("add_tag", vars -> {
            List<Object> tags = sourceList(vars, "tags");
            tags.add(((Map<?, ?>) vars.get("params")).get("tag"));
            return null;
        });
        scripts.put("remove_field", vars -> source(vars).remove("gone"));
        scripts.put("set_object", vars -> source(vars).put("owner", new HashMap<>(Map.of("name", "ann", "team", "x"))));
        scripts.put("noop", vars -> ctx(vars).put("op", "noop"));
        scripts.put("delete", vars -> ctx(vars).put("op", "delete"));
        scripts.put("fail", vars -> { throw new IllegalStateException("boom"); });
        scripts.put("ctx_fields", vars -> {
            Map<String, Object> ctx = ctx(vars);
            Map<String, Object> source = source(vars);
            for (String key : List.of("_index", "_id", "_version", "_routing", "_now")) {
                source.put("seen" + key, ctx.get(key));
            }
            return null;
        });
        scripts.put("count", vars -> {
            Map<String, Object> source = source(vars);
            source.put("count", ((Number) source.getOrDefault("count", 0)).intValue() + 1);
            return null;
        });
        MockScriptEngine engine = new MockScriptEngine(MockScriptEngine.NAME, scripts, Map.of());
        return new ScriptService(Settings.EMPTY, Map.of(engine.getType(), engine), ScriptModule.CORE_CONTEXTS);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> ctx(Map<String, Object> vars) {
        return (Map<String, Object>) vars.get("ctx");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> source(Map<String, Object> vars) {
        return (Map<String, Object>) ctx(vars).get("_source");
    }

    @SuppressWarnings("unchecked")
    private static List<Object> sourceList(Map<String, Object> vars, String field) {
        Map<String, Object> source = source(vars);
        List<Object> list = (List<Object>) source.get(field);
        if (list == null) {
            list = new ArrayList<>();
            source.put(field, list);
        }
        return list;
    }

    private static Script script(String name) {
        return script(name, Map.of());
    }

    private static Script script(String name, Map<String, Object> params) {
        return new Script(ScriptType.INLINE, MockScriptEngine.NAME, name, params);
    }

    private static InfinoScriptedUpdate.Change run(UpdateRequest update, String current) {
        return InfinoScriptedUpdate.run(scripts(), () -> NOW, SHARD, update, current, SEQ_NO, PRIMARY_TERM, VERSION);
    }

    private static Map<String, Object> asMap(String json) {
        return XContentHelper.convertToMap(new BytesArray(json), true, MediaTypeRegistry.JSON).v2();
    }

    public void testAScriptAddsATagWithItsParams() {
        UpdateRequest update = new UpdateRequest("logs", "1").script(script("add_tag", Map.of("tag", "b")));
        InfinoScriptedUpdate.Change change = run(update, "{\"tags\":[\"a\"],\"n\":1}");
        assertEquals(InfinoScriptedUpdate.Op.INDEX, change.op());
        assertEquals(Map.of("tags", List.of("a", "b"), "n", 1), asMap(change.document()));
    }

    public void testAScriptRemovesAFieldAndSetsAnObject() {
        assertEquals(Map.of("kept", 1), asMap(run(new UpdateRequest("logs", "1").script(script("remove_field")), "{\"kept\":1,\"gone\":2}").document()));
        assertEquals(
            Map.of("kept", 1, "owner", Map.of("name", "ann", "team", "x")),
            asMap(run(new UpdateRequest("logs", "1").script(script("set_object")), "{\"kept\":1,\"owner\":{\"name\":\"bob\"}}").document())
        );
    }

    public void testAScriptSeesTheDocumentsContextAsStockGivesIt() {
        UpdateRequest update = new UpdateRequest("logs", "1").routing("r1").script(script("ctx_fields"));
        Map<String, Object> doc = asMap(run(update, "{\"a\":1}").document());
        assertEquals("logs", doc.get("seen_index"));
        assertEquals("1", doc.get("seen_id"));
        assertEquals((int) VERSION, ((Number) doc.get("seen_version")).intValue());
        assertEquals("r1", doc.get("seen_routing"));
        assertEquals(NOW, ((Number) doc.get("seen_now")).longValue());
    }

    public void testANoopScriptWritesNothingAndAnswersNoop() {
        InfinoScriptedUpdate.Change change = run(new UpdateRequest("logs", "1").script(script("noop")), "{\"a\":1}");
        assertEquals(InfinoScriptedUpdate.Op.NOOP, change.op());
        assertNull(change.document());
        assertEquals(DocWriteResponse.Result.NOOP, change.noop().getResult());
        assertEquals(SEQ_NO, change.noop().getSeqNo());
        assertEquals(PRIMARY_TERM, change.noop().getPrimaryTerm());
        assertEquals(VERSION, change.noop().getVersion());
    }

    public void testADeleteScriptDeletes() {
        assertEquals(InfinoScriptedUpdate.Op.DELETE, run(new UpdateRequest("logs", "1").script(script("delete")), "{\"a\":1}").op());
    }

    public void testAFailingScriptIsABadRequest() {
        Exception e = expectThrows(Exception.class, () -> run(new UpdateRequest("logs", "1").script(script("fail")), "{\"a\":1}"));
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(e));
        assertTrue(e.getMessage(), e.getMessage().contains("failed to execute script"));
    }

    public void testAMissingDocumentWithoutAnUpsertIsMissing() {
        Exception e = expectThrows(DocumentMissingException.class, () -> run(new UpdateRequest("logs", "1").script(script("count")), null));
        assertEquals(RestStatus.NOT_FOUND, ExceptionsHelper.status(e));
    }

    public void testAnUpsertIsWrittenAsItIsWhenTheDocumentIsMissing() {
        UpdateRequest update = new UpdateRequest("logs", "1").script(script("count")).upsert("{\"count\":5}", MediaTypeRegistry.JSON);
        InfinoScriptedUpdate.Change change = run(update, null);
        assertEquals(InfinoScriptedUpdate.Op.INDEX, change.op());
        assertEquals(Map.of("count", 5), asMap(change.document()));
        // With a document, the script runs and the upsert is ignored.
        assertEquals(Map.of("count", 2), asMap(run(update, "{\"count\":1}").document()));
    }

    public void testAScriptedUpsertRunsTheScriptOnTheUpsert() {
        UpdateRequest update = new UpdateRequest("logs", "1").script(script("count")).upsert("{\"count\":5}", MediaTypeRegistry.JSON).scriptedUpsert(true);
        assertEquals(Map.of("count", 6), asMap(run(update, null).document()));
    }

    /**
     * A script is compiled as its update is staged, outside the table's lock;
     * the node's script service keeps it, so the run compiles nothing more.
     * A script that does not compile comes back as the failure, for the run
     * to raise; an update with no script compiles nothing.
     */
    public void testAScriptCompiledAheadIsNotCompiledAgainByTheRun() {
        ScriptService scripts = scripts();
        UpdateRequest update = new UpdateRequest("logs", "1").script(script("count"));
        assertNull(InfinoScriptedUpdate.compileAhead(scripts, update));
        assertEquals(1, scripts.stats().getCompilations());
        InfinoScriptedUpdate.Change change = InfinoScriptedUpdate.run(scripts, () -> NOW, SHARD, update, "{\"count\":1}", SEQ_NO, PRIMARY_TERM, VERSION);
        assertEquals(Map.of("count", 2), asMap(change.document()));
        assertEquals("the run found it compiled", 1, scripts.stats().getCompilations());

        RuntimeException failed = InfinoScriptedUpdate.compileAhead(scripts, new UpdateRequest("logs", "1").script(script("no_such_script")));
        assertNotNull(failed);
        assertNull(InfinoScriptedUpdate.compileAhead(scripts, new UpdateRequest("logs", "1").doc("{\"a\":1}", MediaTypeRegistry.JSON)));
    }

    /**
     * Stock compiles an update's script only to run it: over a document that
     * exists, or a missing one in a scripted upsert. A missing document is
     * otherwise written from its upsert, or refused as missing, and a script
     * that does not compile changes neither answer.
     */
    public void testAScriptIsCompiledWhereStockRunsIt() {
        UpdateRequest scripted = new UpdateRequest("logs", "1").script(script("count"));
        assertTrue(InfinoScriptedUpdate.compiles(scripted, true));
        assertFalse("missing, no upsert: refused as missing", InfinoScriptedUpdate.compiles(scripted, false));
        UpdateRequest upsert = new UpdateRequest("logs", "1").script(script("count")).upsert("{\"count\":5}", MediaTypeRegistry.JSON);
        assertFalse("missing: written from the upsert", InfinoScriptedUpdate.compiles(upsert, false));
        assertTrue(InfinoScriptedUpdate.compiles(upsert, true));
        assertTrue("a scripted upsert runs the script", InfinoScriptedUpdate.compiles(upsert.scriptedUpsert(true), false));
        assertFalse(InfinoScriptedUpdate.compiles(new UpdateRequest("logs", "1").doc("{\"a\":1}", MediaTypeRegistry.JSON), true));
    }

    /**
     * An update that asks for its source is answered with the document it
     * wrote under {@code get}, filtered as it asks, as stock answers; one its
     * script deleted is answered found without a source; one that asks for
     * none has no {@code get}.
     */
    public void testAnUpdateThatAsksForItsSourceGetsTheDocumentItWrote() {
        String written = "{\"n\":2,\"tags\":[\"a\"]}";
        UpdateRequest asks = new UpdateRequest("logs", "1").script(script("count")).fetchSource(true);
        GetResult get = InfinoScriptedUpdate.writtenGetResult(asks, "logs", SEQ_NO, PRIMARY_TERM, VERSION, written);
        assertTrue(get.isExists());
        assertEquals(Map.of("n", 2, "tags", List.of("a")), get.sourceAsMap());
        assertEquals(SEQ_NO, get.getSeqNo());
        assertEquals(PRIMARY_TERM, get.getPrimaryTerm());

        UpdateRequest filtered = new UpdateRequest("logs", "1").script(script("count")).fetchSource(new String[] { "n" }, null);
        assertEquals(Map.of("n", 2), InfinoScriptedUpdate.writtenGetResult(filtered, "logs", SEQ_NO, PRIMARY_TERM, VERSION, written).sourceAsMap());

        GetResult deleted = InfinoScriptedUpdate.writtenGetResult(asks, "logs", SEQ_NO, PRIMARY_TERM, VERSION, null);
        assertTrue(deleted.isExists());
        assertNull(deleted.sourceRef());

        assertNull(InfinoScriptedUpdate.writtenGetResult(new UpdateRequest("logs", "1").script(script("count")), "logs", SEQ_NO, PRIMARY_TERM, VERSION, written));
    }
}
