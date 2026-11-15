/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.search.SearchHit;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.fetch.subphase.FieldAndFormat;
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

    public void testAMappingIsReadOnceAndAChangedOneAfresh() {
        MappingMetadata excluding = new MappingMetadata("_doc", Map.of("_source", Map.of("excludes", List.of("items")), "properties", Map.of()));
        InfinoMappingSource first = InfinoMappingSource.of(excluding);
        assertSame("the same mapping is not read again", first, InfinoMappingSource.of(excluding));
        MappingMetadata including = new MappingMetadata("_doc", Map.of("_source", Map.of("includes", List.of("rank")), "properties", Map.of()));
        InfinoMappingSource changed = InfinoMappingSource.of(including);
        assertNotSame("a changed mapping is read afresh", first, changed);
        assertArrayEquals(new String[] { "rank" }, changed.includes());
        assertArrayEquals(new String[] { "items" }, InfinoMappingSource.of(excluding).excludes());
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

    public void testFieldsAreRefusedWhenTheSourceIsDisabled() {
        InfinoMappingSource off = of(Map.of("enabled", false));
        off.refuseFields("i", List.of());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> off.refuseFields("i", List.of(new FieldAndFormat("rank", null)))
        );
        assertEquals("Unable to retrieve the requested [fields] since _source is disabled in the mappings for index [i]", e.getMessage());
        of(Map.of("excludes", List.of("items"))).refuseFields("i", List.of(new FieldAndFormat("rank", null)));
    }

    public void testAFetchedHitIsRefusedWhenItsSourceIsDisabledAndTheRequestReadsIt() {
        InfinoMappingSource off = of(Map.of("enabled", false));
        List<FieldAndFormat> rank = List.of(new FieldAndFormat("rank", null));
        FetchSourceContext filtered = new FetchSourceContext(true, new String[] { "rank" }, null);
        expectThrows(IllegalArgumentException.class, () -> off.refuse("i", filtered, List.of()));
        expectThrows(IllegalArgumentException.class, () -> off.refuse("i", null, rank));
        expectThrows(IllegalArgumentException.class, () -> off.refuse("i", new FetchSourceContext(false), rank));
        // Nothing read from the source: the whole source, none, or a filter on a source not returned.
        off.refuse("i", null, List.of());
        off.refuse("i", new FetchSourceContext(true), List.of());
        off.refuse("i", new FetchSourceContext(false, new String[] { "rank" }, null), List.of());
        // A source the mapping keeps, whole or in part.
        InfinoMappingSource.WHOLE.refuse("i", filtered, rank);
        of(Map.of("excludes", List.of("items"))).refuse("i", filtered, rank);
    }

    public void testKeptIsWhatTheMappingKeeps() {
        assertSame(DOC, InfinoMappingSource.WHOLE.kept(DOC));
        assertEquals(json("{\"rank\":1,\"note\":\"a\"}"), json(of(Map.of("excludes", List.of("items"))).kept(DOC)));
        assertEquals(json("{\"rank\":1}"), json(of(Map.of("includes", List.of("rank"))).kept(DOC)));
    }

    public void testDocValueFieldsReadTheDocumentAndFieldsReadTheKeptSource() {
        InfinoMappingSource ex = of(Map.of("excludes", List.of("note")));
        List<FieldAndFormat> note = List.of(new FieldAndFormat("note", null));

        SearchHit fields = new SearchHit(0, "d1", Map.of(), Map.of());
        InfinoSearch.fillRequested(fields, DOC, ex, List.of(), note, Map.of());
        assertNull(fields.field("note"));

        SearchHit docValues = new SearchHit(0, "d1", Map.of(), Map.of());
        InfinoSearch.fillRequested(docValues, DOC, ex, note, List.of(), Map.of());
        assertEquals(List.of("a"), docValues.field("note").getValues());

        // Asked for both ways, the doc value stays when the source leaves the field out.
        SearchHit both = new SearchHit(0, "d1", Map.of(), Map.of());
        InfinoSearch.fillRequested(both, DOC, ex, note, note, Map.of());
        assertEquals(List.of("a"), both.field("note").getValues());

        // A whole source, read once for both: each group keeps its own fields.
        SearchHit whole = new SearchHit(0, "d1", Map.of(), Map.of());
        InfinoSearch.fillRequested(whole, DOC, InfinoMappingSource.WHOLE, note, List.of(new FieldAndFormat("rank", null)), Map.of());
        assertEquals(List.of("a"), whole.field("note").getValues());
        assertEquals(List.of(1), whole.field("rank").getValues());
        assertNull(whole.field("items.name"));

        SearchHit kept = new SearchHit(0, "d1", Map.of(), Map.of());
        InfinoSearch.fillRequested(kept, DOC, ex, List.of(), List.of(new FieldAndFormat("rank", null)), Map.of());
        assertEquals(List.of(1), kept.field("rank").getValues());

        InfinoMappingSource off = of(Map.of("enabled", false));
        // A disabled source fills no fields; the hits a response carries are refused (see refuse).
        SearchHit offFields = new SearchHit(0, "d1", Map.of(), Map.of());
        InfinoSearch.fillRequested(offFields, DOC, off, List.of(), note, Map.of());
        assertNull(offFields.field("note"));
        SearchHit offDocValues = new SearchHit(0, "d1", Map.of(), Map.of());
        InfinoSearch.fillRequested(offDocValues, DOC, off, note, List.of(), Map.of());
        assertEquals(List.of("a"), offDocValues.field("note").getValues());
    }
}
