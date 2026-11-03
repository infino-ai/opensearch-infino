/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.infino.InfinoNative.Mapping;
import org.opensearch.test.OpenSearchTestCase;

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
}
