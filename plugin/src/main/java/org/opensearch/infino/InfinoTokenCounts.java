/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.regex.Regex;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHit;
import org.opensearch.search.fetch.subphase.FieldAndFormat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The values of the {@code token_count} fields a search asks for by
 * {@code fields} or {@code docvalue_fields}, as stock reads them: the number
 * of tokens the field's analyzer makes of each of the document's values, from
 * its doc values, so in ascending order. The table keeps no column for such a
 * field and the count is the analyzer's to make, so each hit's source, as
 * the user may see it and cut to the fields the counts are made of, is
 * parsed by the index's mapper, as the shard parsed it when it was written,
 * and the counts read from the parsed document. A field without doc values
 * gives no values, as stock's do not.
 */
final class InfinoTokenCounts {

    static final String TYPE = "token_count";

    private final Index index;
    /** Each token_count field the request names, by full path, with the format it is asked in (null for none). */
    private final Map<String, String> requested;
    /** The paths of the requested fields that keep doc values. */
    private final List<String> withDocValues;
    /** The source the counts are made from: only the fields that feed them, so the rest of the source is not parsed. */
    private final InfinoMappingSource feeding;
    private final List<SearchHit> hits = new ArrayList<>();
    private final List<String> ids = new ArrayList<>();
    private final List<String> sources = new ArrayList<>();

    private InfinoTokenCounts(Index index, Map<String, String> requested, List<String> withDocValues, String[] fedBy) {
        this.index = index;
        this.requested = requested;
        this.withDocValues = withDocValues;
        this.feeding = new InfinoMappingSource(true, fedBy, new String[0]);
    }

    /** A token_count field: whether it keeps doc values, and the source field its count is made of. */
    private record Field(boolean docValues, String fedBy) {}

    /**
     * The token counts a search over {@code index}, mapped by {@code mapping},
     * asks for; null when it asks for none.
     */
    static InfinoTokenCounts of(Index index, MappingMetadata mapping, List<FieldAndFormat> docValueFields, List<FieldAndFormat> fetchFields) {
        Map<String, Field> fields = new LinkedHashMap<>();
        collect("", null, InfinoSearchChecks.properties(mapping), fields);
        if (fields.isEmpty()) {
            return null;
        }
        Map<String, String> requested = requested(fields.keySet(), docValueFields, fetchFields);
        if (requested.isEmpty()) {
            return null;
        }
        List<String> withDocValues = requested.keySet().stream().filter(path -> fields.get(path).docValues()).toList();
        String[] fedBy = withDocValues.stream().map(path -> fields.get(path).fedBy()).distinct().toArray(String[]::new);
        return new InfinoTokenCounts(index, requested, withDocValues, fedBy);
    }

    /**
     * The token_count fields of a mapping's {@code properties} by full path,
     * through objects and multi-fields, each with whether it keeps doc
     * values. A nested object's fields are left out: their values are not
     * the root document's.
     */
    static Map<String, Boolean> fields(Map<String, Object> properties) {
        Map<String, Field> fields = new LinkedHashMap<>();
        collect("", null, properties, fields);
        Map<String, Boolean> out = new LinkedHashMap<>();
        fields.forEach((path, field) -> out.put(path, field.docValues()));
        return out;
    }

    /**
     * Collects the token_count fields under {@code prefix}; {@code parent} is
     * the field whose multi-fields {@code properties} are, which a multi-field
     * is made of, else null.
     */
    @SuppressWarnings("unchecked")
    private static void collect(String prefix, String parent, Map<String, Object> properties, Map<String, Field> out) {
        for (Map.Entry<String, Object> e : properties.entrySet()) {
            if (!(e.getValue() instanceof Map<?, ?> m)) {
                continue;
            }
            Map<String, Object> field = (Map<String, Object>) m;
            String path = prefix + e.getKey();
            if (TYPE.equals(field.get("type"))) {
                out.put(path, new Field(!"false".equals(String.valueOf(field.get("doc_values"))), parent == null ? path : parent));
            }
            if (field.get("fields") instanceof Map<?, ?> multi) {
                collect(path + ".", path, (Map<String, Object>) multi, out);
            }
            if (!"nested".equals(field.get("type")) && field.get("properties") instanceof Map<?, ?> children) {
                collect(path + ".", null, (Map<String, Object>) children, out);
            }
        }
    }

    /**
     * Which of the token_count {@code fields} a request names, by name or
     * pattern, each with its format: a field asked for both ways takes the
     * {@code fields} format, as stock's fields phase runs after its doc
     * values phase, and of several entries naming it the last.
     */
    static Map<String, String> requested(Iterable<String> fields, List<FieldAndFormat> docValueFields, List<FieldAndFormat> fetchFields) {
        Map<String, String> out = new LinkedHashMap<>();
        for (List<FieldAndFormat> asked : List.of(docValueFields, fetchFields)) {
            for (FieldAndFormat f : asked) {
                for (String path : fields) {
                    if (Regex.simpleMatch(f.field, path)) {
                        out.put(path, f.format);
                    }
                }
            }
        }
        return out;
    }

    /** Count {@code source}, the source of the hit {@code h} with id {@code id}, when {@link #fill} runs. */
    void add(SearchHit h, String id, String source) {
        hits.add(h);
        ids.add(id);
        sources.add(source);
    }

    /**
     * Gives each added hit the values of each requested field in place of any
     * it carries: the mapper of the index on this node parses the sources,
     * else one built from the index's metadata for the purpose.
     */
    void fill() {
        if (hits.isEmpty()) {
            return;
        }
        if (withDocValues.isEmpty()) {
            // Only fields without doc values are asked for: each hit shows none of them, with nothing to count.
            hits.forEach(this::clear);
            return;
        }
        MapperService local = InfinoMappedValues.mappers(index);
        if (local != null) {
            fill(local);
            return;
        }
        IndicesService indices = InfinoServices.indicesService();
        ClusterService clusters = InfinoServices.clusterService();
        IndexMetadata md = clusters == null ? null : clusters.state().metadata().index(index);
        if (indices == null || md == null) {
            throw new IllegalStateException("[infino] no mapping of [" + index.getName() + "] to count the tokens of its token_count fields");
        }
        try (MapperService mappers = indices.createIndexMapperService(md)) {
            mappers.merge(md, MapperService.MergeReason.MAPPING_RECOVERY);
            fill(mappers);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void fill(MapperService mappers) {
        DocumentMapper mapper = mappers.documentMapper();
        for (int i = 0; i < hits.size(); i++) {
            SearchHit h = hits.get(i);
            String fed = feeding.kept(sources.get(i));
            ParseContext.Document doc = mapper.parse(
                new SourceToParse(index.getName(), ids.get(i), new BytesArray(fed), MediaTypeRegistry.JSON, null)
            ).rootDoc();
            clear(h);
            for (String path : withDocValues) {
                List<Object> values = values(doc, path, requested.get(path));
                if (!values.isEmpty()) {
                    h.setDocumentField(path, new DocumentField(path, values));
                }
            }
        }
    }

    /** Takes off {@code h} what it was given of each requested field from the source, which holds the text, not its count. */
    private void clear(SearchHit h) {
        for (String path : requested.keySet()) {
            if (h.getDocumentFields().containsKey(path)) {
                h.removeDocumentField(path);
            }
        }
    }

    /**
     * The doc values of {@code path} in {@code doc}, ascending, as longs or,
     * with a {@code format}, as that decimal format gives them.
     */
    static List<Object> values(ParseContext.Document doc, String path, String format) {
        List<Long> counts = new ArrayList<>();
        for (IndexableField f : doc.getFields(path)) {
            if (f.fieldType().docValuesType() == DocValuesType.SORTED_NUMERIC && f.numericValue() != null) {
                counts.add(f.numericValue().longValue());
            }
        }
        counts.sort(null);
        DocValueFormat shown = format == null || InfinoSearch.USE_FIELD_MAPPING.equals(format) ? DocValueFormat.RAW : new DocValueFormat.Decimal(format);
        List<Object> out = new ArrayList<>(counts.size());
        for (long c : counts) {
            out.add(shown.format(c));
        }
        return out;
    }
}
