/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.common.UUIDs;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The scrolls open on this node. A scroll over Infino-backed indices is a
 * keyset cursor, not a held reader: the first page runs as a sorted search
 * (the request's sorts, the id last; the id alone when it names none) and
 * each later page is the same search after the last sort values handed out
 * for each index, so no page is scanned twice and nothing is pinned. What a
 * scroll keeps is the request as its pages run (query, sorts, fields,
 * size), the cursor per index, the total of the first page (repeated on
 * every page, as a stock scroll does), what the user may see of each index
 * as evaluated when it opened, and who opened it: a scroll is continued
 * only by that user, as the security plugin binds a stock scroll to its
 * user.
 *
 * <p>A scroll expires when its keep-alive passes without a page; a sweep
 * drops expired ones, and a node holds at most {@link #MAX_OPEN} at once.
 */
final class InfinoScrolls {
    private static final Logger log = LogManager.getLogger(InfinoScrolls.class);
    /** Scroll ids the node issues start with this, so another node's or OpenSearch's own are left alone. */
    static final String ID_PREFIX = "infino_";
    /** Scrolls a node holds at once: OpenSearch's own default for open scroll contexts. */
    static final int MAX_OPEN = 500;
    /** How often expired scrolls are dropped. */
    private static final TimeValue SWEEP_INTERVAL = TimeValue.timeValueSeconds(30);
    /** Keep-alive when a request names none, as OpenSearch's own default. */
    static final TimeValue DEFAULT_KEEP_ALIVE = TimeValue.timeValueMinutes(5);
    /** The id a clear request names to close every scroll. */
    static final String ALL = "_all";

    private InfinoScrolls() {}

    /** One open scroll. {@code after} is the cursor per index: the sort values of the last hit handed out from it. */
    static final class Scroll {
        final String id;
        final String principal;
        final List<Index> indices;
        final SearchSourceBuilder source;
        final Map<Index, InfinoAccess> access;
        final TotalHits total;
        volatile Map<Index, Object[]> after;
        volatile long expiresAtMillis;

        Scroll(
            String id,
            String principal,
            List<Index> indices,
            SearchSourceBuilder source,
            Map<Index, InfinoAccess> access,
            TotalHits total,
            Map<Index, Object[]> after,
            long expiresAtMillis
        ) {
            this.id = id;
            this.principal = principal;
            this.indices = indices;
            this.source = source;
            this.access = access;
            this.total = total;
            this.after = after;
            this.expiresAtMillis = expiresAtMillis;
        }
    }

    private static final Map<String, Scroll> OPEN = new ConcurrentHashMap<>();
    private static volatile Scheduler.Cancellable sweep;

    /** Start the sweep of expired scrolls; once per node. */
    static synchronized void start(ThreadPool threadPool) {
        if (sweep == null && threadPool != null) {
            sweep = threadPool.scheduleWithFixedDelay(InfinoScrolls::dropExpired, SWEEP_INTERVAL, ThreadPool.Names.GENERIC);
        }
    }

    static synchronized void stop() {
        if (sweep != null) {
            sweep.cancel();
            sweep = null;
        }
        OPEN.clear();
    }

    /** Whether an id is one of this node's. */
    static boolean owns(String scrollId) {
        return scrollId != null && scrollId.startsWith(ID_PREFIX);
    }

    /** An id for a scroll about to open; refused when the node holds its limit. */
    static String newId() {
        dropExpired();
        if (OPEN.size() >= MAX_OPEN) {
            throw new IllegalArgumentException("[infino] this node holds " + MAX_OPEN + " open scrolls; clear some before opening another");
        }
        return ID_PREFIX + UUIDs.base64UUID();
    }

    /** Register a scroll under the id {@link #newId} gave, after its first page ran. */
    static void register(
        String id,
        String principal,
        List<Index> indices,
        SearchSourceBuilder source,
        Map<Index, InfinoAccess> access,
        TotalHits total,
        Map<Index, Object[]> after,
        TimeValue keepAlive
    ) {
        OPEN.put(id, new Scroll(id, principal, indices, source, access, total, after, expiry(keepAlive)));
    }

    /**
     * The scroll an id names, for {@code principal}, its keep-alive extended
     * by {@code keepAlive}; an unknown or expired id is reported as OpenSearch
     * reports a missing search context, another user's as forbidden.
     */
    static Scroll next(String id, String principal, TimeValue keepAlive) {
        Scroll scroll = OPEN.get(id);
        if (scroll == null || scroll.expiresAtMillis < System.currentTimeMillis()) {
            OPEN.remove(id);
            throw new ResourceNotFoundException("[infino] scroll [" + id + "] is unknown or has expired");
        }
        if (!scroll.principal.equals(principal)) {
            throw new OpenSearchSecurityException("[infino] scroll [" + id + "] was opened by another user", RestStatus.FORBIDDEN);
        }
        scroll.expiresAtMillis = expiry(keepAlive);
        return scroll;
    }

    /** Close the scrolls named (every one of this node's for {@link #ALL}); the number closed. */
    static int close(Collection<String> ids) {
        int freed = 0;
        for (String id : ids) {
            if (ALL.equals(id)) {
                freed += OPEN.size();
                OPEN.clear();
                continue;
            }
            if (OPEN.remove(id) != null) {
                freed++;
            }
        }
        return freed;
    }

    static int openCount() {
        return OPEN.size();
    }

    private static long expiry(TimeValue keepAlive) {
        return System.currentTimeMillis() + (keepAlive == null ? DEFAULT_KEEP_ALIVE : keepAlive).millis();
    }

    private static void dropExpired() {
        long now = System.currentTimeMillis();
        List<String> expired = new ArrayList<>();
        for (Scroll s : OPEN.values()) {
            if (s.expiresAtMillis < now) {
                expired.add(s.id);
            }
        }
        for (String id : expired) {
            OPEN.remove(id);
        }
        if (!expired.isEmpty()) {
            log.debug("[infino] dropped {} expired scroll(s)", expired.size());
        }
    }
}
