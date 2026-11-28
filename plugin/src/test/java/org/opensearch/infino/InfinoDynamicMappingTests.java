/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.MapperServiceTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Which documents a dynamic mapping is taken from, and the document it is
 * taken from, without the fields the table already holds unmapped.
 */
public class InfinoDynamicMappingTests extends MapperServiceTestCase {

    /** Whether {@code json} is parsed for a mapping update under {@code mappers}. */
    private static boolean parsed(MapperService mappers, String json) {
        return !InfinoDynamicMapping.indexesNothing(mappers)
            && InfinoDynamicMapping.addsField(new BytesArray(json), MediaTypeRegistry.JSON, mappers, InfinoDynamicMapping.Unmapped.NONE);
    }

    /**
     * A field {@code dynamic: false} leaves out of the mapping stays out, so a
     * document whose only new fields are such is not parsed: it would be on
     * every bulk that carries it. A new field where the mapping maps one is.
     */
    public void testAFieldDynamicFalseLeavesOutIsNotParsedFor() throws IOException {
        MapperService mappers = createMapperService(topMapping(b -> {
            b.field("dynamic", "false");
            b.startObject("properties");
            b.startObject("tag").field("type", "keyword").endObject();
            b.startObject("open").field("type", "object").field("dynamic", "true").endObject();
            b.startObject("body").field("type", "object").field("enabled", false).endObject();
            b.endObject();
        }));
        assertFalse(parsed(mappers, "{\"tag\":\"a\",\"data\":{\"x\":1},\"note\":\"n\",\"body\":\"t\"}"));
        assertFalse(parsed(mappers, "{\"items\":[{\"a\":1},{\"a\":[2]}]}"));
        // The object that sets dynamic: true maps a new field under it.
        assertTrue(parsed(mappers, "{\"tag\":\"a\",\"open\":{\"fresh\":1}}"));

        // false_allow_templates maps only what a template matches: no
        // template, no new field; with one, the document is parsed.
        MapperService templated = createMapperService(topMapping(b -> {
            b.field("dynamic", "false_allow_templates");
            b.startArray("dynamic_templates");
            b.startObject().startObject("strings").field("match_mapping_type", "string");
            b.startObject("mapping").field("type", "keyword").endObject();
            b.endObject().endObject();
            b.endArray();
        }));
        assertTrue(parsed(templated, "{\"fresh\":\"s\"}"));
        MapperService untemplated = createMapperService(topMapping(b -> b.field("dynamic", "false_allow_templates")));
        assertFalse(parsed(untemplated, "{\"fresh\":\"s\"}"));

        // The default maps a new field.
        assertTrue(parsed(createMapperService(mapping(b -> b.startObject("tag").field("type", "keyword").endObject())), "{\"fresh\":1}"));
    }

    /** A mapping with enabled: false at its root maps no field, so no document is parsed for one. */
    public void testAMappingThatIndexesNothingParsesNoDocument() throws IOException {
        MapperService mappers = createMapperService(topMapping(b -> b.field("enabled", false)));
        assertFalse(parsed(mappers, "{\"anything\":{\"at\":\"all\"}}"));
    }

    private static Map<String, Object> without(String json, Set<String> paths) throws Exception {
        return XContentHelper.convertToMap(InfinoDynamicMapping.without(new BytesArray(json), MediaTypeRegistry.JSON, paths), true, MediaTypeRegistry.JSON)
            .v2();
    }

    public void testRemovesHeldFieldsWrittenAsObjectsOrDottedNames() throws Exception {
        String doc = "{\"host\":{\"name\":\"a\",\"ip\":\"b\"},\"data_stream.dataset\":\"c\",\"fresh\":\"d\"}";
        assertEquals(
            Map.of("host", Map.of("ip", "b"), "fresh", "d"),
            without(doc, Set.of("host.name", "data_stream.dataset"))
        );
    }

    public void testDropsAnObjectLeftEmpty() throws Exception {
        assertEquals(Map.of("fresh", 1), without("{\"host\":{\"name\":\"a\"},\"fresh\":1}", Set.of("host.name")));
    }

    public void testLooksIntoObjectsInArrays() throws Exception {
        assertEquals(
            Map.of("items", java.util.List.of(Map.of("b", 2))),
            without("{\"items\":[{\"a\":1,\"b\":2}]}", Set.of("items.a"))
        );
    }
}
