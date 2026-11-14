/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

/** What a response carries of a document's source under the mapping's {@code _source}. */
public class InfinoMappingSourceTests extends OpenSearchTestCase {

    private static final String DOC = "{\"rank\":1,\"note\":\"a\",\"items\":[{\"name\":\"size\"}]}";

    private static InfinoMappingSource of(Map<String, Object> source) {
        return InfinoMappingSource.of(new MappingMetadata("_doc", Map.of("_source", source, "properties", Map.of())));
    }

    /** The source shown, as a map: the filters keep fields, not their order. */
    private static Map<String, Object> shown(InfinoMappingSource stored, FetchSourceContext request) {
        BytesReference b = stored.shown(DOC, request);
        return b == null ? null : XContentHelper.convertToMap(b, false, MediaTypeRegistry.JSON).v2();
    }

    private static Map<String, Object> json(String s) {
        return XContentHelper.convertToMap(new BytesArray(s), false, MediaTypeRegistry.JSON).v2();
    }

    public void testAMappingThatSaysNothingKeepsTheSourceWhole() {
        assertSame(InfinoMappingSource.WHOLE, InfinoMappingSource.of(null));
        assertSame(InfinoMappingSource.WHOLE, InfinoMappingSource.of(new MappingMetadata("_doc", Map.of("properties", Map.of()))));
        assertSame(InfinoMappingSource.WHOLE, of(Map.of("enabled", true)));
        assertEquals(json(DOC), shown(InfinoMappingSource.WHOLE, null));
    }

    public void testADisabledSourceIsNeverShown() {
        InfinoMappingSource off = of(Map.of("enabled", false));
        assertNull(shown(off, null));
        off.refuseFilters("i", null);
        off.refuseFilters("i", new FetchSourceContext(true));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> off.refuseFilters("i", new FetchSourceContext(true, new String[] { "rank" }, null))
        );
        assertEquals("unable to fetch fields from _source field: _source is disabled in the mappings for index [i]", e.getMessage());
        InfinoMappingSource.WHOLE.refuseFilters("i", new FetchSourceContext(true, new String[] { "rank" }, null));
    }

    public void testExcludesAndIncludesApplyBeforeTheRequestsFilter() {
        InfinoMappingSource ex = of(Map.of("excludes", List.of("items")));
        assertEquals(json("{\"rank\":1,\"note\":\"a\"}"), shown(ex, null));
        assertEquals(json("{\"rank\":1}"), shown(ex, new FetchSourceContext(true, new String[] { "rank", "items" }, null)));

        InfinoMappingSource in = of(Map.of("includes", List.of("rank", "items")));
        assertEquals(json("{\"rank\":1,\"items\":[{\"name\":\"size\"}]}"), shown(in, null));
        assertEquals(json("{\"items\":[{\"name\":\"size\"}]}"), shown(in, new FetchSourceContext(true, null, new String[] { "rank" })));
    }

    public void testASingleStringNamesOneField() {
        assertEquals(json("{\"rank\":1,\"note\":\"a\"}"), shown(of(Map.of("excludes", "items")), null));
    }
}
