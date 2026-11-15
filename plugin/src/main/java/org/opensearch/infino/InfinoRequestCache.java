/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.indices.IndicesRequestCache;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The responses of repeated searches over Infino-backed indices, kept the way
 * OpenSearch's shard request cache keeps them for a Lucene index. The plugin
 * answers an Infino-backed search before it reaches a shard, so the shard
 * request cache never sees it; this cache stands in for it, under the same
 * settings.
 *
 * <p>What is kept follows the shard request cache: a request whose
 * {@code request_cache} parameter is true, or, when the parameter is absent, a
 * request asking for no hits ({@code size: 0}) on indices whose
 * {@code index.requests.cache.enable} is on. Never a scroll, a profiled
 * request, one whose answer depends on the time it runs ({@code now} in date
 * math), on a script or on a random score ({@link #changesWithoutWrites}),
 * or one the security plugin narrows for its user: a document- or
 * field-level rule, or a masked field. A search through a filtering alias
 * is kept under its alias filters (see {@link #key}).
 *
 * <p>A response is kept under its tables' write generations: every write
 * this node makes to a table moves the table's generation, before the write
 * and after it, so a response read before or during a write is never served
 * after it. Writes this node does not see (another node's, or the hosted
 * service's) are bounded the way a Lucene index bounds them: a response is
 * served for at most the index's refresh interval, the delay after which a
 * Lucene index shows a write.
 *
 * <p>The cache's size is the shard request cache's own,
 * {@code indices.requests.cache.size}, taken beside it: a search over an
 * Infino-backed index never reaches the shard request cache, so a node
 * serving only such indices holds that much, and one serving Lucene indices
 * too can hold up to twice it in request caches.
 */
final class InfinoRequestCache {

    /** A kept response and when it stops being served. */
    private record Entry(SearchResponse response, long expiresAtNanos) {}

    /** What date math reads as the time a request runs. */
    private static final String NOW = "now";
    /** What may follow {@code now} in date math: an added or taken unit, or a rounding. */
    private static final String DATE_MATH_OPERATIONS = "+-/";
    /** The key a script is given by, alone or within a longer key. */
    private static final String SCRIPT = "script";
    /** A score drawn at random for each request. */
    private static final String RANDOM_SCORE = "random_score";

    /** Writes this node made to each table, counted. */
    private static final Map<String, AtomicLong> GENERATIONS = new ConcurrentHashMap<>();
    /** Moves when the connection is reopened elsewhere, which changes every table at once. */
    private static final AtomicLong CONNECTION = new AtomicLong();
    /**
     * For each table another node wrote on this node's behalf, until when (in
     * {@link System#nanoTime} terms) a response read here is not kept (see
     * {@link #writtenElsewhere}).
     */
    private static final Map<String, Long> UNSETTLED = new ConcurrentHashMap<>();

    private static volatile InfinoRequestCache node;

    private final Cache<String, Entry> responses;

    private InfinoRequestCache(long maxBytes) {
        this.responses = CacheBuilder.<String, Entry>builder()
            .setMaximumWeight(maxBytes)
            .weigher((key, entry) -> key.length() + weight(entry.response()))
            .build();
    }

    /** Size the node's cache from its settings; called once, when the plugin is constructed. */
    static void configure(Settings settings) {
        long bytes = IndicesRequestCache.INDICES_CACHE_QUERY_SIZE.get(settings).getBytes();
        node = bytes > 0 ? new InfinoRequestCache(bytes) : null;
    }

    /** The node's cache, or null when it has no room. */
    static InfinoRequestCache node() {
        return node;
    }

    /** {@code table} is about to change, or has: responses read from it before now are not served again. */
    static void changed(String table) {
        GENERATIONS.computeIfAbsent(table, t -> new AtomicLong()).incrementAndGet();
    }

    /**
     * {@code table} was just written by another node for a write this node
     * took and sent there (see {@link InfinoBulkActionFilter#dispatch}). This
     * node reads another node's commit a moment after it lands, so a response
     * read here soon after could be from before the write; nothing read from
     * the table is kept until {@code lifetime} (the index's refresh interval,
     * the bound a kept response has anyway) has passed.
     */
    static void writtenElsewhere(String table, TimeValue lifetime) {
        changed(table);
        UNSETTLED.merge(table, System.nanoTime() + lifetime.nanos(), (a, b) -> b - a > 0 ? b : a);
    }

    /** Whether a write another node made for this node may not show here yet (see {@link #writtenElsewhere}). */
    private static boolean unsettled(String table) {
        Long until = UNSETTLED.get(table);
        if (until == null) {
            return false;
        }
        if (System.nanoTime() - until < 0) {
            return true;
        }
        UNSETTLED.remove(table, until);
        return false;
    }

    /** Every table changed at once: the connection now reads from somewhere else. */
    static void changedAll() {
        CONNECTION.incrementAndGet();
    }

    /** Which connection the node reads through: moves on every reopen (see {@link InfinoNative#init}). */
    static long connection() {
        return CONNECTION.get();
    }

    private static long generation(String table) {
        AtomicLong g = GENERATIONS.get(table);
        return g == null ? 0 : g.get();
    }

    /**
     * The key {@code search} is kept under, over {@code tables} (one per index
     * of {@code metadata}, in the same order), or null when the request is not
     * one to keep. Taken before the search runs, so a write that lands while
     * it runs moves the key away from what it reads.
     *
     * <p>{@code access} is what the security plugin lets the user see of each
     * index, and {@code filters} what the filtering aliases the request
     * reached each index through keep of it (null for one read whole). A
     * filter is part of what is read, so it is part of the key: two aliases
     * over one index with the same body but different filters answer
     * differently, and must never be served each other's response.
     */
    static String key(
        SearchRequest search,
        SearchSourceBuilder source,
        List<IndexMetadata> metadata,
        List<String> tables,
        Collection<InfinoAccess> access,
        List<QueryBuilder> filters
    ) {
        if (search.scroll() != null || source.profile()) {
            return null;
        }
        Boolean asked = search.requestCache();
        if (Boolean.FALSE.equals(asked)) {
            return null;
        }
        if (asked == null) {
            if (source.size() != 0) {
                return null;
            }
            for (IndexMetadata md : metadata) {
                if (!IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.get(md.getSettings())) {
                    return null;
                }
            }
        }
        for (InfinoAccess a : access) {
            if (!a.unrestricted()) {
                return null;
            }
        }
        for (String table : tables) {
            if (unsettled(table)) {
                return null;
            }
        }
        String body = source.toString();
        if (changesWithoutWrites(XContentHelper.convertToMap(new BytesArray(body), false, MediaTypeRegistry.JSON).v2())) {
            return null;
        }
        StringBuilder key = new StringBuilder().append(CONNECTION.get());
        for (int i = 0; i < tables.size(); i++) {
            String table = tables.get(i);
            key.append('\u0000').append(table).append('\u0000').append(generation(table));
            QueryBuilder filter = filters.get(i);
            if (filter != null) {
                String kept = filter.toString();
                // A filter whose date math reads `now` keeps different rows as time passes.
                if (changesWithoutWrites(XContentHelper.convertToMap(new BytesArray(kept), false, MediaTypeRegistry.JSON).v2())) {
                    return null;
                }
                key.append('\u0000').append(kept);
            }
        }
        return key.append('\u0000').append(body).toString();
    }

    /**
     * Whether a request (its body as parsed, {@code node} a part of it) can
     * answer differently with nothing written in between, so it is never
     * kept: a date that is {@code now} or counts from it, a script, or a
     * random score. The shard request cache asks the parsed query the same
     * thing. A word that merely holds {@code now} or {@code script} (a value
     * like "known", a field like "transcript") changes nothing; a value that
     * is the word {@code now} itself, which date math would read as the
     * time, keeps the request out.
     */
    static boolean changesWithoutWrites(Object node) {
        if (node instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                String name = String.valueOf(entry.getKey());
                if (scriptKey(name) || RANDOM_SCORE.equals(name) || changesWithoutWrites(entry.getValue())) {
                    return true;
                }
            }
            return false;
        }
        if (node instanceof List<?> list) {
            for (Object item : list) {
                if (changesWithoutWrites(item)) {
                    return true;
                }
            }
            return false;
        }
        return node instanceof String value && nowDateMath(value);
    }

    /**
     * A key that holds a script: {@code script} itself, and the keys that
     * name one ({@code script_score}, {@code script_fields}, {@code _script}
     * in a sort, {@code init_script}, {@code bucket_script}, ...).
     */
    private static boolean scriptKey(String name) {
        return name.equals(SCRIPT) || name.startsWith(SCRIPT + "_") || name.endsWith("_" + SCRIPT);
    }

    /** A date math expression that counts from the time it runs: {@code now}, alone or followed by its operations. */
    private static boolean nowDateMath(String value) {
        return value.startsWith(NOW) && (value.length() == NOW.length() || DATE_MATH_OPERATIONS.indexOf(value.charAt(NOW.length())) >= 0);
    }

    /** How long a response over {@code metadata} is served: the shortest refresh interval among the indices. */
    static TimeValue lifetime(List<IndexMetadata> metadata) {
        long millis = Long.MAX_VALUE;
        for (IndexMetadata md : metadata) {
            TimeValue refresh = IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.get(md.getSettings());
            // A disabled refresh (-1) would serve a response forever; the
            // default interval bounds it instead.
            long m = refresh.millis() > 0 ? refresh.millis() : IndexSettings.DEFAULT_REFRESH_INTERVAL.millis();
            millis = Math.min(millis, m);
        }
        return TimeValue.timeValueMillis(millis);
    }

    /** The response kept under {@code key}, its took the time this request has taken, or null. */
    SearchResponse get(String key, long startNanos) {
        Entry entry = responses.get(key);
        if (entry == null) {
            return null;
        }
        if (System.nanoTime() - entry.expiresAtNanos() >= 0) {
            responses.invalidate(key);
            return null;
        }
        SearchResponse kept = entry.response();
        return new SearchResponse(
            kept.getInternalResponse(),
            null,
            kept.getTotalShards(),
            kept.getSuccessfulShards(),
            kept.getSkippedShards(),
            (System.nanoTime() - startNanos) / 1_000_000L,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
    }

    /** Keep {@code response} under {@code key} for {@code lifetime}. */
    void put(String key, SearchResponse response, TimeValue lifetime) {
        responses.put(key, new Entry(response, System.nanoTime() + lifetime.nanos()));
    }

    /** About what a kept response holds: the length of its JSON. */
    private static long weight(SearchResponse response) {
        return Strings.toString(MediaTypeRegistry.JSON, response).length();
    }
}
