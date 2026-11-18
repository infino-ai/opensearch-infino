/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;
import java.util.Set;

/** The document a dynamic mapping is taken from, without the fields the table already holds unmapped. */
public class InfinoDynamicMappingTests extends OpenSearchTestCase {

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
