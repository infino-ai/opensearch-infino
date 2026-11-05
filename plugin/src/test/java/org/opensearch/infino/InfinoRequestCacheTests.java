/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.Version;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.functionscore.ScoreFunctionBuilders;
import org.opensearch.indices.IndicesRequestCache;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;

/** Which searches the request cache keeps, under what key, and for how long. */
public class InfinoRequestCacheTests extends OpenSearchTestCase {

    /** A table name no other test moves. */
    private static final String TABLE = "request_cache_tests";
    /**
     * Time the kept response's took was measured as, which a hit replaces: a
     * minute, so the hit's own took (the time the lookup itself took, a few
     * milliseconds on a cold test JVM) is told from it whatever the machine.
     */
    private static final long KEPT_TOOK_MILLIS = 60_000;

    private static IndexMetadata index(Settings extra) {
        return IndexMetadata.builder("i")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).put(extra))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }

    private static String key(SearchRequest search, SearchSourceBuilder source, Settings indexSettings, InfinoAccess access) {
        return key(search, source, indexSettings, access, null);
    }

    private static String key(SearchRequest search, SearchSourceBuilder source, Settings indexSettings, InfinoAccess access, QueryBuilder filter) {
        return InfinoRequestCache.key(
            search,
            source,
            List.of(index(indexSettings)),
            List.of(TABLE),
            List.of(access),
            Collections.singletonList(filter)
        );
    }

    private static String key(SearchSourceBuilder source) {
        return key(new SearchRequest(), source, Settings.EMPTY, InfinoAccess.NONE);
    }

    private static SearchSourceBuilder aggregationOnly() {
        return new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.sum("s").field("price"));
    }

    public void testAnAggregationOnlyRequestIsKeptAndOneWithHitsIsNot() {
        assertNotNull(key(aggregationOnly()));
        assertNull(key(new SearchSourceBuilder().size(10).query(QueryBuilders.matchAllQuery())));
        // Unset size is OpenSearch's default page, so hits.
        assertNull(key(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())));
    }

    public void testTheRequestParameterDecidesOverSizeAndTheIndexSetting() {
        SearchRequest asked = new SearchRequest().requestCache(true);
        assertNotNull(key(asked, new SearchSourceBuilder().size(10), Settings.EMPTY, InfinoAccess.NONE));
        SearchRequest refused = new SearchRequest().requestCache(false);
        assertNull(key(refused, aggregationOnly(), Settings.EMPTY, InfinoAccess.NONE));
        Settings off = Settings.builder().put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), false).build();
        assertNull(key(new SearchRequest(), aggregationOnly(), off, InfinoAccess.NONE));
        assertNotNull(key(asked, aggregationOnly(), off, InfinoAccess.NONE));
    }

    public void testTimeDependentScriptedProfiledAndScrolledRequestsAreNotKept() {
        assertNull(key(aggregationOnly().query(QueryBuilders.rangeQuery("ts").gte("now-1d"))));
        assertNull(key(aggregationOnly().query(QueryBuilders.scriptQuery(new Script("doc['n'].value > 1")))));
        assertNull(key(aggregationOnly().profile(true)));
        SearchRequest scroll = new SearchRequest().scroll(TimeValue.timeValueMinutes(1));
        assertNull(key(scroll, aggregationOnly(), Settings.EMPTY, InfinoAccess.NONE));
        // now alone, rounded, and as the bound of a date aggregation.
        assertNull(key(aggregationOnly().query(QueryBuilders.rangeQuery("ts").lte("now"))));
        assertNull(key(aggregationOnly().query(QueryBuilders.rangeQuery("ts").gte("now/d"))));
        assertNull(key(new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.dateRange("r").field("ts").addUnboundedTo("now+1h"))));
        // A script under any of the keys that carry one, and a random score.
        assertNull(key(aggregationOnly().scriptField("f", new Script("doc['n'].value"))));
        assertNull(key(new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.terms("t").script(new Script("doc['n'].value")))));
        assertNull(key(aggregationOnly().query(QueryBuilders.functionScoreQuery(ScoreFunctionBuilders.randomFunction()))));
    }

    /**
     * A value or a field that only holds the letters of {@code now} or
     * {@code script} changes nothing, so the request is kept: "unknown" and
     * "snowfall" are words, not the time, and "transcript" is a field.
     */
    public void testAWordHoldingNowOrScriptIsKept() {
        assertNotNull(key(aggregationOnly().query(QueryBuilders.termQuery("status", "unknown"))));
        assertNotNull(key(aggregationOnly().query(QueryBuilders.termQuery("weather", "snowfall"))));
        assertNotNull(key(aggregationOnly().query(QueryBuilders.matchQuery("transcript", "now playing"))));
        assertNotNull(key(new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.terms("known").field("description_scripted"))));
    }

    public void testARestrictedUserIsNotKept() {
        InfinoAccess hidden = new InfinoAccess(null, field -> !field.equals("secret"), null);
        assertNull(key(new SearchRequest(), aggregationOnly(), Settings.EMPTY, hidden));
        InfinoAccess documents = new InfinoAccess(List.of(QueryBuilders.termQuery("tag", "x")), null, null);
        assertNull(key(new SearchRequest(), aggregationOnly(), Settings.EMPTY, documents));
    }

    /**
     * A search through a filtering alias is kept, under its filter: two
     * aliases over the index with the same body never share a response, nor
     * does either with the index read whole.
     */
    public void testAFilteredAliasIsKeptUnderItsFilter() {
        SearchRequest search = new SearchRequest();
        String whole = key(search, aggregationOnly(), Settings.EMPTY, InfinoAccess.NONE, null);
        String tagX = key(search, aggregationOnly(), Settings.EMPTY, InfinoAccess.NONE, QueryBuilders.termQuery("tag", "x"));
        String tagY = key(search, aggregationOnly(), Settings.EMPTY, InfinoAccess.NONE, QueryBuilders.termQuery("tag", "y"));
        assertNotNull(tagX);
        assertNotEquals(whole, tagX);
        assertNotEquals(tagX, tagY);
        assertEquals(tagX, key(search, aggregationOnly(), Settings.EMPTY, InfinoAccess.NONE, QueryBuilders.termQuery("tag", "x")));
        // A filter whose date math reads the time keeps different rows as time passes.
        assertNull(key(search, aggregationOnly(), Settings.EMPTY, InfinoAccess.NONE, QueryBuilders.rangeQuery("ts").gte("now-1d")));
        // A user the security plugin narrows is still not kept, alias or not.
        InfinoAccess documents = new InfinoAccess(List.of(QueryBuilders.termQuery("tag", "x")), null, null);
        assertNull(key(search, aggregationOnly(), Settings.EMPTY, documents, QueryBuilders.termQuery("tag", "x")));
    }

    public void testAWriteMovesTheKeyAwayFromWhatWasReadBefore() {
        String before = key(aggregationOnly());
        assertEquals(before, key(aggregationOnly()));
        InfinoRequestCache.changed(TABLE);
        assertNotEquals(before, key(aggregationOnly()));
        String afterWrite = key(aggregationOnly());
        InfinoRequestCache.changedAll();
        assertNotEquals(afterWrite, key(aggregationOnly()));
    }

    public void testALifetimeIsTheShortestRefreshIntervalAndADisabledRefreshTheDefault() {
        Settings fast = Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "200ms").build();
        Settings off = Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "-1").build();
        assertEquals(TimeValue.timeValueMillis(200), InfinoRequestCache.lifetime(List.of(index(Settings.EMPTY), index(fast))));
        assertEquals(IndexSettings.DEFAULT_REFRESH_INTERVAL, InfinoRequestCache.lifetime(List.of(index(off))));
    }

    public void testAKeptResponseIsServedWithItsOwnTookUntilItsLifetimeEnds() {
        InfinoRequestCache.configure(Settings.EMPTY);
        InfinoRequestCache cache = InfinoRequestCache.node();
        assertNotNull(cache);
        SearchResponse response = new SearchResponse(
            InternalSearchResponse.empty(),
            null,
            1,
            1,
            0,
            KEPT_TOOK_MILLIS,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
        String key = key(aggregationOnly());
        cache.put(key, response, TimeValue.timeValueMinutes(1));
        SearchResponse hit = cache.get(key, System.nanoTime());
        assertNotNull(hit);
        assertSame(response.getInternalResponse(), hit.getInternalResponse());
        assertTrue(hit.getTook().millis() < KEPT_TOOK_MILLIS);
        cache.put(key, response, TimeValue.ZERO);
        assertNull(cache.get(key, System.nanoTime()));
    }
}
