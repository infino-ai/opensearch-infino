/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * What a document's {@code _source} keeps under its index's mapping, as
 * stock stores it: nothing when the mapping disables {@code _source}, else
 * what its {@code includes} and {@code excludes} keep. The table keeps each
 * document as it was indexed, which nested queries read their entries from,
 * so the mapping is applied where a hit, a get or an mget hands the source
 * out, and the request's own source filter then applies to what it keeps.
 */
record InfinoMappingSource(boolean enabled, String[] includes, String[] excludes) {

    /** A mapping that keeps the source whole, as one that says nothing of it does. */
    static final InfinoMappingSource WHOLE = new InfinoMappingSource(true, new String[0], new String[0]);

    /**
     * Each mapping's {@code _source}, read once. Reading it parses the whole
     * mapping, which a search, get or mget would otherwise do on every
     * request. Keyed by the mapping, so a changed mapping is read afresh, and
     * weakly, so a mapping the cluster state no longer holds is let go.
     */
    private static final Map<MappingMetadata, InfinoMappingSource> READ = Collections.synchronizedMap(new WeakHashMap<>());

    /** The {@code _source} of {@code mapping}; {@link #WHOLE} when there is none or it says nothing. */
    static InfinoMappingSource of(MappingMetadata mapping) {
        if (mapping == null) {
            return WHOLE;
        }
        InfinoMappingSource kept = READ.get(mapping);
        if (kept == null) {
            kept = read(mapping);
            READ.put(mapping, kept);
        }
        return kept;
    }

    /** {@link #of}, read from the mapping itself. */
    private static InfinoMappingSource read(MappingMetadata mapping) {
        if (!(mapping.sourceAsMap().get(SourceFieldMapper.NAME) instanceof Map<?, ?> source)) {
            return WHOLE;
        }
        boolean enabled = XContentMapValues.nodeBooleanValue(source.get("enabled"), "_source.enabled", true);
        String[] includes = strings(source.get("includes"));
        String[] excludes = strings(source.get("excludes"));
        return enabled && includes.length == 0 && excludes.length == 0 ? WHOLE : new InfinoMappingSource(enabled, includes, excludes);
    }

    private static String[] strings(Object node) {
        if (node instanceof List<?> list) {
            return list.stream().map(String::valueOf).toArray(String[]::new);
        }
        return node == null ? new String[0] : new String[] { String.valueOf(node) };
    }

    /** Whether the mapping keeps every field of the source. */
    boolean whole() {
        return enabled && includes.length == 0 && excludes.length == 0;
    }

    /**
     * Refuses {@code request} when it filters the source of a hit of
     * {@code index} and the mapping disables {@code _source}, as stock's
     * fetch phase does.
     */
    void refuseFilters(String index, FetchSourceContext request) {
        if (!enabled && filters(request)) {
            throw new IllegalArgumentException(
                "unable to fetch fields from _source field: _source is disabled in the mappings for index [" + index + "]"
            );
        }
    }

    /** Whether {@code request} names includes or excludes. */
    private static boolean filters(FetchSourceContext request) {
        return request != null
            && ((request.includes() != null && request.includes().length > 0)
                || (request.excludes() != null && request.excludes().length > 0));
    }

    /**
     * Refuses {@code fields} (the request's {@code fields}, read from the
     * source) on a hit of {@code index} when the mapping disables
     * {@code _source}, as stock's fetch fields phase does.
     */
    void refuseFields(String index, List<?> fields) {
        if (!enabled && !fields.isEmpty()) {
            throw new IllegalArgumentException(
                "Unable to retrieve the requested [fields] since _source is disabled in the mappings for index [" + index + "]"
            );
        }
    }

    /**
     * The source {@code src} as the mapping keeps it, which stock's
     * {@code fields} read: what its includes and excludes keep (the whole
     * of {@code src} when it names none). Not called when the mapping
     * disables {@code _source}: {@link #refuseFields} refuses that first.
     */
    String kept(String src) {
        if (whole()) {
            return src;
        }
        Map<String, Object> map = XContentMapValues.filter(XContentHelper.convertToMap(new BytesArray(src), true, MediaTypeRegistry.JSON).v2(), includes, excludes);
        return json(map).utf8ToString();
    }

    /**
     * The source {@code src} as a response carries it: null when the mapping
     * disables {@code _source}, else what the mapping keeps of it, filtered
     * by {@code request}'s includes and excludes when it names any.
     */
    BytesReference shown(String src, FetchSourceContext request) {
        if (!enabled) {
            return null;
        }
        boolean requestFilters = filters(request);
        if (whole() && !requestFilters) {
            return new BytesArray(src);
        }
        Map<String, Object> map = XContentHelper.convertToMap(new BytesArray(src), true, MediaTypeRegistry.JSON).v2();
        if (!whole()) {
            map = XContentMapValues.filter(map, includes, excludes);
        }
        if (requestFilters) {
            map = XContentMapValues.filter(map, request.includes(), request.excludes());
        }
        return json(map);
    }

    private static BytesReference json(Map<String, Object> map) {
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.map(map);
            return BytesReference.bytes(b);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
