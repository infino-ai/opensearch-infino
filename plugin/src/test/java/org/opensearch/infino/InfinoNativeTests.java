/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.infino.InfinoNative.Mapping;
import org.opensearch.test.OpenSearchTestCase;

import com.sun.jna.Memory;
import com.sun.jna.Pointer;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/** What a mapping declares beyond the columns, read as the cluster keeps it. */
public class InfinoNativeTests extends OpenSearchTestCase {
    /** A local storage URI for the storage check's tests. */
    private static final String LOCAL = "file:///var/lib/infino/data";
    /** How long a storage check that never answers is waited for in a test. */
    private static final long SHORT_WAIT_MILLIS = 200;

    /** A mapping with an alias at the root and one inside an object, multi-fields, dates with and without a format. */
    private static final String MAPPING = """
        {"_doc":{"properties":{
          "timestamp":{"type":"date"},
          "@timestamp":{"type":"alias","path":"timestamp"},
          "when":{"type":"date_nanos","format":"yyyy-MM-dd HH:mm:ss||strict_date_optional_time"},
          "agent":{"type":"text","fields":{"keyword":{"type":"keyword"}}},
          "geo":{"properties":{"src":{"type":"keyword"},"origin":{"type":"alias","path":"geo.src"}}}
        }}}""";

    public void testEveryPathAliasAndDateFormatIsRead() {
        Mapping m = Mapping.of(MAPPING);
        assertEquals(
            Set.of("timestamp", "@timestamp", "when", "agent", "agent.keyword", "geo", "geo.src", "geo.origin"),
            m.paths()
        );
        assertEquals(Map.of("@timestamp", "timestamp", "geo.origin", "geo.src"), m.aliases());
        assertEquals(Map.of("agent.keyword", "agent"), m.keywordFields());
        assertEquals(Map.of("when", "yyyy-MM-dd HH:mm:ss||strict_date_optional_time"), m.dateFormats());
    }

    /** The float, half_float, scaled_float and ip fields carry their type, which their columns do not; a malformed scaling factor none. */
    public void testFieldTypesTheColumnsDoNotCarryAreRead() {
        Mapping m = Mapping.of("""
            {"properties":{
              "f":{"type":"float"},"h":{"type":"half_float"},"d":{"type":"double"},
              "s":{"type":"scaled_float","scaling_factor":100},"t":{"type":"scaled_float","scaling_factor":"10"},
              "bad":{"type":"scaled_float","scaling_factor":"x"},
              "obj":{"properties":{"ip":{"type":"ip"}}}}}""");
        assertEquals(
            Map.of(
                "f",
                new InfinoNative.FieldType("float", 0),
                "h",
                new InfinoNative.FieldType("half_float", 0),
                "s",
                new InfinoNative.FieldType("scaled_float", 100),
                "t",
                new InfinoNative.FieldType("scaled_float", 10),
                "obj.ip",
                new InfinoNative.FieldType("ip", 0)
            ),
            m.fieldTypes()
        );
    }

    /**
     * A keyword multi-field is read by its own name, whatever the mapping calls it (the logs
     * track's {@code request.raw}); one of another type indexes the value differently and is
     * not read as its field.
     */
    public void testAKeywordMultiFieldOfAnyNameReadsItsField() {
        Mapping m = Mapping.of("""
            {"properties":{"request":{"type":"match_only_text","fields":{
              "raw":{"type":"keyword","ignore_above":256},
              "english":{"type":"text","analyzer":"english"}}}}}""");
        assertEquals(Set.of("request", "request.raw", "request.english"), m.paths());
        assertEquals(Map.of("request.raw", "request"), m.keywordFields());
    }

    /** A keyword field, or a keyword multi-field, mapped with use_similarity is read as one whose terms Lucene scores by BM25. */
    public void testUseSimilarityKeywordFieldsAreRead() {
        Mapping m = Mapping.of("""
            {"properties":{
              "tag":{"type":"keyword","use_similarity":true},
              "plain":{"type":"keyword"},
              "off":{"type":"keyword","use_similarity":false},
              "title":{"type":"text","fields":{"raw":{"type":"keyword","use_similarity":"true"}}}}}""");
        assertEquals(Set.of("tag", "title.raw"), m.similarityFields());
    }

    /**
     * A wildcard multi-field reads its field's value whole, as a keyword one; one with a
     * normalizer, and a keyword or wildcard field with one, are normalized, not read as written.
     */
    public void testWildcardMultiFieldsAndNormalizedFieldsAreRead() {
        Mapping m = Mapping.of("""
            {"properties":{
              "w":{"type":"wildcard","fields":{
                "lower":{"type":"wildcard","normalizer":"lowercase"},
                "dv":{"type":"wildcard","doc_values":true},
                "num":{"type":"long"}}},
              "k":{"type":"keyword","normalizer":"lowercase"},
              "t":{"type":"text","fields":{"keyword":{"type":"keyword","normalizer":"lowercase"}}}}}""");
        assertEquals(Map.of("w.dv", "w"), m.keywordFields());
        assertEquals(Set.of("w.lower", "w.dv", "w.num", "t.keyword"), m.multiFields());
        assertEquals(Set.of("w.lower", "k", "t.keyword"), m.normalized());
    }

    public void testAMappingWithoutTheTypeNameIsReadTheSame() {
        Mapping m = Mapping.of("{\"properties\":{\"a\":{\"type\":\"alias\",\"path\":\"b\"},\"b\":{\"type\":\"long\"}}}");
        assertEquals(Set.of("a", "b"), m.paths());
        assertEquals(Map.of("a", "b"), m.aliases());
    }

    public void testAnEmptyMappingDeclaresNothing() {
        Mapping m = Mapping.of("{}");
        assertTrue(m.paths().isEmpty());
        assertTrue(m.aliases().isEmpty());
        assertTrue(m.keywordFields().isEmpty());
        assertTrue(m.dateFormats().isEmpty());
    }

    /**
     * The id probe asks for plain terms: a leading + or - (a phone number, a
     * negative number) and a double quote are clause syntax to the engine,
     * and with a required clause the union found only one id's rows.
     */
    public void testIdsAreAskedForAsPlainTerms() {
        assertEquals("15551234 15559876", InfinoNative.idTerms(List.of("+15551234", "+15559876")));
        assertEquals("7 a b x y z", InfinoNative.idTerms(List.of("-7", "a\"b", "x y", "--z")));
        assertEquals("", InfinoNative.idTerms(List.of("---", "+")));
    }

    public void testAnIdWithNoLetterOrDigitHasNoTerm() {
        assertFalse(InfinoNative.hasTerm("---"));
        assertFalse(InfinoNative.hasTerm("+/-"));
        assertTrue(InfinoNative.hasTerm("+1"));
        assertTrue(InfinoNative.hasTerm("é"));
    }

    /**
     * A table with no row has performed no operation, so its first write is
     * given 0 as stock's first is; one whose rows were all written without a
     * number reports them at the untracked number, so the next is past it.
     */
    public void testTheHighestSequenceNumberTellsAnEmptyTableFromOneAtZero() {
        assertEquals(SequenceNumbers.NO_OPS_PERFORMED, InfinoNative.highestSeq(null, () -> false));
        assertEquals(InfinoDocumentVersion.UNTRACKED_SEQ_NO, InfinoNative.highestSeq(null, () -> true));
        assertEquals(0L, InfinoNative.highestSeq(0L, () -> { throw new AssertionError("a stored number needs no row check"); }));
        assertEquals(41L, InfinoNative.highestSeq(41, () -> { throw new AssertionError("a stored number needs no row check"); }));
    }

    /** A storage the node can use passes the check: nothing is logged as an error and the node starts. */
    public void testAUsableStoragePasses() {
        withBridge(new InfinoNative.Target(LOCAL, null, false, LOCAL), Map.of("infino_check_storage", () -> cString("")), () -> {
            assertNull(InfinoNative.checkStorage());
        });
    }

    /**
     * A directory the node may not write will not become writable on its
     * own: the node stops, with one message that names the storage, the
     * reason and what to check.
     */
    public void testARefusedDirectoryStopsTheNodeSayingWhy() {
        String reason = "/var/lib/infino/data: Permission denied (os error 13)";
        String answer = "{\"error\":\"" + reason + "\",\"permanent\":true}";
        withBridge(new InfinoNative.Target(LOCAL, null, false, LOCAL), Map.of("infino_check_storage", () -> cString(answer)), () -> {
            IllegalStateException e = expectThrows(IllegalStateException.class, InfinoNative::checkStorage);
            assertTrue(e.getMessage(), e.getMessage().contains("cannot use its storage at " + LOCAL + ": " + reason));
            assertTrue(e.getMessage(), e.getMessage().contains("may read and write it"));
            assertTrue(e.getMessage(), e.getMessage().contains("The node stops"));
        });
    }

    /** A store that fails for now (a 5xx, a dropped connection) is logged, and the node keeps starting. */
    public void testAStorageThatMayRecoverIsLoggedAndTheNodeKeepsStarting() {
        String answer = "{\"error\":\"io: transient error after retry: 503 Slow Down\",\"permanent\":false}";
        InfinoNative.Target s3 = new InfinoNative.Target("s3://bucket/prefix", null, false, "s3://bucket/prefix");
        withBridge(s3, Map.of("infino_check_storage", () -> cString(answer)), () -> {
            String logged = InfinoNative.checkStorage();
            assertNotNull("the failure is logged", logged);
            assertTrue(logged, logged.contains("503 Slow Down"));
            assertTrue(logged, logged.contains("hop limit to 2"));
            assertTrue(logged, logged.contains("The node keeps starting"));
        });
    }

    /** No answer from the bridge (it failed) fails the check rather than passing it. */
    public void testNoAnswerFromTheBridgeFailsTheCheck() {
        withBridge(new InfinoNative.Target(LOCAL, null, false, LOCAL), Map.of("infino_check_storage", () -> null), () -> {
            String logged = InfinoNative.checkStorage();
            assertNotNull("no answer is not a pass", logged);
            assertTrue(logged, logged.contains("no answer"));
        });
    }

    /**
     * A hosted key the service refuses will not be taken however often it is
     * sent: the node stops, says to check the platform URL, names the service
     * once and never shows the key, even where the service's reason repeats it.
     */
    public void testARefusedKeyStopsAHostedNodeWithoutShowingTheKey() {
        String key = "ik_live_5ecret";
        InfinoNative.Target hosted = InfinoNative.Target.hosted("https://service.example", "logs", key);
        String why = "{\"error\":\"list_tables: server returned 401 Unauthorized: key " + key + " is not valid\",\"status\":401}";
        Map<String, Supplier<Object>> bridge = Map.of("infino_open_with", () -> null, "infino_open_error", () -> cString(why));
        withBridge(hosted, bridge, () -> {
            IllegalStateException e = expectThrows(IllegalStateException.class, InfinoNative::checkStorage);
            String message = e.getMessage();
            assertTrue(message, message.contains("A key belongs to one platform URL"));
            assertTrue(message, message.contains(InfinoPlugin.PLATFORM_URL.getKey()));
            assertFalse(message, message.contains(key));
            assertEquals(message, 1, message.split(Pattern.quote(hosted.name()), -1).length - 1);
        });
    }

    /**
     * A hosted service that fails for now (here a 503 while it deploys) lets
     * the node start, and the reason is no refused key just because the
     * database's name holds the digits 401.
     */
    public void testAHostedServiceThatFailsForNowLetsTheNodeStart() {
        InfinoNative.Target hosted = InfinoNative.Target.hosted("https://service.example:4013", "logs-2401", "ik_x");
        String why = "{\"error\":\"list_tables: server returned 503 Service Unavailable\",\"status\":503}";
        Map<String, Supplier<Object>> bridge = Map.of("infino_open_with", () -> null, "infino_open_error", () -> cString(why));
        withBridge(hosted, bridge, () -> {
            String logged = InfinoNative.checkStorage();
            assertNotNull(logged);
            assertTrue(logged, logged.contains("can reach the service"));
            assertFalse(logged, logged.contains("refused the key"));
        });
    }

    /** A storage that does not answer in time is reported, and the node's start does not wait for it. */
    public void testACheckThatDoesNotAnswerInTimeLetsTheNodeStart() {
        CountDownLatch release = new CountDownLatch(1);
        Supplier<Object> stuck = () -> {
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return cString("");
        };
        try {
            withBridge(new InfinoNative.Target(LOCAL, null, false, LOCAL), Map.of("infino_check_storage", stuck), () -> {
                String logged = InfinoNative.checkStorage(TimeValue.timeValueMillis(SHORT_WAIT_MILLIS));
                assertNotNull(logged);
                assertTrue(logged, logged.contains("did not answer within"));
            });
        } finally {
            release.countDown();
        }
    }

    /**
     * A bridge that does not link (a wrong {@code -Dinfino.lib}) stops the
     * node with the loader's error: no retry fixes it, so it is not logged as
     * a storage that may recover.
     */
    public void testABridgeThatDoesNotLinkStopsTheNode() {
        String why = "Unable to load library '/wrong/libinfino_capi.so'";
        Supplier<Object> unlinked = () -> { throw new UnsatisfiedLinkError(why); };
        withBridge(new InfinoNative.Target(LOCAL, null, false, LOCAL), Map.of("infino_open_with", unlinked), () -> {
            UnsatisfiedLinkError e = expectThrows(UnsatisfiedLinkError.class, InfinoNative::checkStorage);
            assertEquals(why, e.getMessage());
        });
    }

    /** A node in memory has nothing to check, however its URI is spelled. */
    public void testANodeInMemoryChecksNothing() {
        for (String uri : List.of("memory://", "memory:", "memory")) {
            withBridge(new InfinoNative.Target(uri, null, false, uri), Map.of(), () -> assertNull(InfinoNative.checkStorage()));
        }
    }

    /** What a node that cannot use its storage says to check depends on the storage. */
    public void testWhatToCheckFollowsTheStorage() {
        InfinoNative.StorageFailure failed = new InfinoNative.StorageFailure("failed", true);
        InfinoNative.Target s3 = new InfinoNative.Target("s3://bucket/prefix", null, false, "s3://bucket/prefix");
        assertTrue(InfinoNative.storageHint(s3, failed).contains("hop limit to 2"));
        for (String path : List.of("file:///data/infino", "/data/infino", "./data")) {
            InfinoNative.Target local = new InfinoNative.Target(path, null, false, path);
            assertTrue(path, InfinoNative.storageHint(local, failed).contains("may read and write it"));
        }
        InfinoNative.Target gcs = new InfinoNative.Target("gs://bucket", null, false, "gs://bucket");
        assertTrue(InfinoNative.storageHint(gcs, failed).contains("credentials for this storage"));
    }

    /** A user and password in a storage URI never reach the message, wherever the URI appears in it. */
    public void testAPasswordInTheUriIsNeverShown() {
        String uri = "s3://user:s3cret@host/bucket";
        InfinoNative.Target s3 = new InfinoNative.Target(uri, null, false, uri);
        String message = InfinoNative.unusableStorage(s3, new InfinoNative.StorageFailure("not found: " + uri + "/catalog.json", false));
        assertFalse(message, message.contains("s3cret"));
        assertTrue(message, message.contains("s3://***@host/bucket"));
        assertEquals("file:///data", InfinoNative.scrubbed(s3, "file:///data"));
    }

    /**
     * Run {@code test} with a stand-in bridge configured for {@code target}:
     * each named call answers from {@code answers}, the open succeeds unless
     * an answer says otherwise, and every other call answers null.
     */
    static void withBridge(InfinoNative.Target target, Map<String, Supplier<Object>> answers, Runnable test) {
        InfinoLib bridge = (InfinoLib) Proxy.newProxyInstance(InfinoLib.class.getClassLoader(), new Class<?>[] { InfinoLib.class }, (p, m, a) -> {
            Supplier<Object> answer = answers.get(m.getName());
            if (answer != null) {
                return answer.get();
            }
            return m.getName().equals("infino_open_with") ? new Memory(1) : null;
        });
        InfinoNative.useLib(bridge);
        InfinoNative.configure(target);
        try {
            test.run();
        } finally {
            InfinoNative.configure(InfinoNative.Target.node());
            InfinoNative.useLib(null);
        }
    }

    static Pointer cString(String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        Memory m = new Memory(bytes.length + 1);
        m.write(0, bytes, 0, bytes.length);
        m.setByte(bytes.length, (byte) 0);
        return m;
    }
}
