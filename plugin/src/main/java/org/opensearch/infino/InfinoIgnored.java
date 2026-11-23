/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.lucene.index.IndexableField;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexService;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.IgnoredFieldMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.indices.IndicesService;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * The fields of a document that OpenSearch's mapper ignored because their
 * value was malformed and the field, or the index, sets
 * {@code ignore_malformed}. The mapper is the judge: a document is parsed
 * by it, and what it lists under {@code _ignored} is left out of the row,
 * kept by path in the {@link InfinoQueryTranslator#IGNORED} column and
 * served as {@code _ignored}, while the source keeps the value whole.
 * Parsing costs a second pass over the document, so only an index whose
 * settings or mapping name {@code ignore_malformed} pays it.
 */
final class InfinoIgnored {

    /** What separates the paths handed to the bridge: no field name holds it. */
    static final String SEPARATOR = "\n";
    /** The index setting that makes every field that supports it ignore a malformed value. */
    static final String SETTING = "index.mapping.ignore_malformed";
    private static final String OPTION = "ignore_malformed";

    private InfinoIgnored() {}

    /** The paths as the bridge takes them. */
    static String join(List<String> paths) {
        return String.join(SEPARATOR, paths);
    }

    /** The paths the mapper ignored in {@code doc}, each once, in the order it listed them. */
    static List<String> of(ParsedDocument doc) {
        if (doc == null || doc.rootDoc() == null) {
            return List.of();
        }
        IndexableField[] fields = doc.rootDoc().getFields(IgnoredFieldMapper.NAME);
        if (fields.length == 0) {
            return List.of();
        }
        Set<String> paths = new LinkedHashSet<>();
        for (IndexableField field : fields) {
            String path = field.stringValue();
            if (path != null && !path.isEmpty()) {
                paths.add(path);
            }
        }
        return List.copyOf(paths);
    }

    /**
     * Whether a document of the index {@code mappers} maps may have a field
     * the mapper ignores: its settings or its mapping name
     * {@code ignore_malformed}, at the index or on a field or template.
     */
    static boolean mayIgnore(MapperService mappers) {
        if (mappers == null) {
            return false;
        }
        if (mappers.getIndexSettings().getSettings().getAsBoolean(SETTING, false)) {
            return true;
        }
        DocumentMapper mapper = mappers.documentMapper();
        return mapper != null && mapper.mappingSource().string().contains(OPTION);
    }

    /**
     * The mapper of {@code index} on this node when its documents may have a
     * field the mapper ignores ({@link #mayIgnore}); null when they cannot,
     * or when the index has no service here to parse against.
     */
    static MapperService ignoring(Index index) {
        IndicesService indices = InfinoServices.indicesService();
        IndexService service = indices == null || index == null ? null : indices.indexService(index);
        MapperService mappers = service == null ? null : service.mapperService();
        return mayIgnore(mappers) ? mappers : null;
    }

    /**
     * The paths the mapper ignores in {@code source} (JSON), parsed as the
     * shard would parse it; empty when {@code mappers} is null. Throws what
     * the mapper throws for a document it refuses, such as a malformed value
     * in a field that does not ignore one.
     */
    static List<String> parse(MapperService mappers, String index, String id, String source, String routing) {
        if (mappers == null || source == null) {
            return List.of();
        }
        DocumentMapper mapper = mappers.documentMapperWithAutoCreate().getDocumentMapper();
        return of(mapper.parse(new SourceToParse(index, id, new BytesArray(source), MediaTypeRegistry.JSON, routing)));
    }
}
