/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.infino.InfinoNative.Mapping;
import org.opensearch.test.OpenSearchTestCase;

import com.sun.jna.Memory;
import com.sun.jna.Pointer;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** What a mapping declares beyond the columns, read as the cluster keeps it. */
public class InfinoNativeTests extends OpenSearchTestCase {

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

    /**
     * A node whose own storage does not answer stops at start with one
     * message that names the storage, the reason and what to check.
     */
    public void testANodeThatCannotUseItsStorageStopsSayingWhy() {
        String uri = "file:///var/lib/infino/data";
        String reason = "list_tables: Permission denied (os error 13)";
        InfinoLib bridge = (InfinoLib) Proxy.newProxyInstance(InfinoLib.class.getClassLoader(), new Class<?>[] { InfinoLib.class }, (p, m, a) -> switch (m.getName()) {
            case "infino_open_with" -> new Memory(1);
            case "infino_check_storage" -> cString("{\"error\":\"" + reason + "\"}");
            default -> null;
        });
        InfinoNative.useLib(bridge);
        InfinoNative.configure(new InfinoNative.Target(uri, null, false, uri));
        try {
            IllegalStateException e = expectThrows(IllegalStateException.class, InfinoNative::checkStorage);
            assertTrue(e.getMessage(), e.getMessage().contains("cannot use its storage at " + uri + ": " + reason));
            assertTrue(e.getMessage(), e.getMessage().contains("may read and write it"));
        } finally {
            InfinoNative.configure(InfinoNative.Target.node());
            InfinoNative.useLib(null);
        }
    }

    /** What a stopped node says to check depends on its storage, and never shows a password in its URI. */
    public void testWhatToCheckFollowsTheStorage() {
        InfinoNative.Target s3 = new InfinoNative.Target("s3://bucket/prefix", null, false, "s3://bucket/prefix");
        assertTrue(InfinoNative.storageHint(s3, "no credentials").contains("hop limit to 2"));
        InfinoNative.Target hosted = InfinoNative.Target.hosted("https://service.example", "db", "ik_x");
        String refused = InfinoNative.unusableStorage(hosted, "cannot open: list_tables: server returned 401");
        assertTrue(refused, refused.contains("A key belongs to one platform URL"));
        assertTrue(refused, refused.contains(InfinoPlugin.PLATFORM_URL.getKey()));
        assertFalse(refused, refused.contains("ik_x"));
        assertEquals("s3://***@host/bucket", InfinoNative.redacted("s3://user:secret@host/bucket"));
        assertEquals("file:///data", InfinoNative.redacted("file:///data"));
    }

    private static Pointer cString(String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        Memory m = new Memory(bytes.length + 1);
        m.write(0, bytes, 0, bytes.length);
        m.setByte(bytes.length, (byte) 0);
        return m;
    }
}
