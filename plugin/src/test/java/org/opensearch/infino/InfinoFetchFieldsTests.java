/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.document.DocumentField;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.StoredFieldsContext;
import org.opensearch.search.fetch.subphase.FieldAndFormat;
import org.opensearch.test.OpenSearchTestCase;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** What a search over a table asks by fields, docvalue_fields and stored_fields, and what each hit shows of it. */
public class InfinoFetchFieldsTests extends OpenSearchTestCase {

    /** A token_count field at the root, one under an object, a multi-field, one without doc values, and one under a nested object. */
    private static final Map<String, Object> PROPERTIES = Map.of(
        "count",
        Map.of("type", "token_count", "analyzer", "standard"),
        "count_without_dv",
        Map.of("type", "token_count", "analyzer", "standard", "doc_values", false),
        "title",
        Map.of("type", "text", "fields", Map.of("length", Map.of("type", "token_count", "analyzer", "standard"))),
        "meta",
        Map.of("properties", Map.of("words", Map.of("type", "token_count", "analyzer", "standard"))),
        "parts",
        Map.of("type", "nested", "properties", Map.of("words", Map.of("type", "token_count", "analyzer", "standard"))),
        "name",
        Map.of("type", "keyword")
    );

    private static FieldAndFormat field(String name) {
        return new FieldAndFormat(name, null);
    }

    public void testTokenCountFieldsAreFoundThroughObjectsAndMultiFieldsButNotNested() {
        Map<String, Boolean> expected = new LinkedHashMap<>();
        expected.put("count", true);
        expected.put("count_without_dv", false);
        expected.put("title.length", true);
        expected.put("meta.words", true);
        assertEquals(expected, new LinkedHashMap<>(InfinoTokenCounts.fields(PROPERTIES)));
    }

    public void testRequestedFieldsMatchByPatternAndTakeTheFieldsFormat() {
        List<String> fields = List.of("count", "title.length", "meta.words");
        Map<String, String> requested = InfinoTokenCounts.requested(
            fields,
            List.of(new FieldAndFormat("count", "0.00"), new FieldAndFormat("meta.*", "0.0")),
            List.of(new FieldAndFormat("count", "0.0"), field("name"))
        );
        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("count", "0.0");
        expected.put("meta.words", "0.0");
        assertEquals(expected, requested);
        assertTrue(InfinoTokenCounts.requested(fields, List.of(field("name")), List.of(field("other*"))).isEmpty());
    }

    public void testValuesAreTheDocValuesAscendingAsLongsOrInTheFormat() {
        ParseContext.Document doc = new ParseContext.Document();
        doc.add(new SortedNumericDocValuesField("count", 4));
        doc.add(new SortedNumericDocValuesField("count", 1));
        doc.add(new StoredField("count", 9));
        doc.add(new SortedNumericDocValuesField("other", 7));
        assertEquals(List.of(1L, 4L), InfinoTokenCounts.values(doc, "count", null));
        assertEquals(List.of(1L, 4L), InfinoTokenCounts.values(doc, "count", InfinoSearch.USE_FIELD_MAPPING));
        assertEquals(List.of("1.00", "4.00"), InfinoTokenCounts.values(doc, "count", "0.00"));
        assertTrue(InfinoTokenCounts.values(doc, "missing", null).isEmpty());
    }

    public void testDocValueFieldsOnATokenCountWithoutDocValuesIsRefused() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> InfinoSearchChecks.checkTokenCountDocValues(new SearchSourceBuilder().docValueField("count_*"), PROPERTIES)
        );
        assertEquals(
            "Can't load fielddata on [count_without_dv] because fielddata is unsupported on fields of type [token_count]. Use doc values instead.",
            e.getMessage()
        );
        InfinoSearchChecks.checkTokenCountDocValues(new SearchSourceBuilder().docValueField("count"), PROPERTIES);
        InfinoSearchChecks.checkTokenCountDocValues(new SearchSourceBuilder().fetchField("count_without_dv"), PROPERTIES);
    }

    public void testNoStoredFieldsRefusesSourceAndFieldsAsStockDoes() {
        StoredFieldsContext none = StoredFieldsContext.fromList(List.of(StoredFieldsContext._NONE_));
        IllegalArgumentException source = expectThrows(
            IllegalArgumentException.class,
            () -> InfinoSearchChecks.checkNoStoredFields(new SearchSourceBuilder().storedFields(none).fetchSource(true))
        );
        assertEquals("[stored_fields] cannot be disabled if [_source] is requested", source.getMessage());
        IllegalArgumentException fields = expectThrows(
            IllegalArgumentException.class,
            () -> InfinoSearchChecks.checkNoStoredFields(new SearchSourceBuilder().storedFields(none).fetchField("name"))
        );
        assertEquals("[stored_fields] cannot be disabled when using the [fields] option", fields.getMessage());
        InfinoSearchChecks.checkNoStoredFields(new SearchSourceBuilder().storedFields(none).docValueField("count"));
        InfinoSearchChecks.checkNoStoredFields(new SearchSourceBuilder().storedFields(none).fetchSource(false).docValueField("count"));
        InfinoSearchChecks.checkNoStoredFields(new SearchSourceBuilder().fetchSource(true).fetchField("name"));
    }

    public void testWithoutIdsDropsOnlyTheId() {
        SearchHit h = new SearchHit(3, "doc-1", Map.of("count", new DocumentField("count", List.of(2L))), Map.of());
        h.score(1.5f);
        h.version(4);
        h.setSeqNo(7);
        h.setPrimaryTerm(1);
        h.sourceRef(new BytesArray("{\"name\":\"a\"}"));
        h.sortValues(new Object[] { 10L, "b" }, new DocValueFormat[] { DocValueFormat.RAW, DocValueFormat.RAW });
        SearchHits hits = new SearchHits(new SearchHit[] { h }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.5f);

        SearchHits bare = InfinoSearch.withoutIds(hits);

        assertEquals(1, bare.getHits().length);
        SearchHit b = bare.getHits()[0];
        assertNull(b.getId());
        assertEquals(1.5f, b.getScore(), 0f);
        assertEquals(4, b.getVersion());
        assertEquals(7, b.getSeqNo());
        assertEquals(1, b.getPrimaryTerm());
        assertEquals(h.getSourceRef(), b.getSourceRef());
        assertArrayEquals(new Object[] { 10L, "b" }, b.getSortValues());
        assertEquals(List.of(2L), b.field("count").getValues());
        assertEquals(hits.getTotalHits(), bare.getTotalHits());
        assertEquals(1.5f, bare.getMaxScore(), 0f);
    }
}
