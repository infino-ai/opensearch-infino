/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.lucene.search.TotalHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;

/** The hits.total a proxy search reports, as a coordinator reports it under track_total_hits. */
public class InfinoSearchTotalsTests extends OpenSearchTestCase {

    private static final int CAP = SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO;

    public void testASearchThatNamesNoLimitCountsUpToTheDefaultCap() {
        SearchSourceBuilder source = new SearchSourceBuilder();
        assertEquals(new TotalHits(CAP, TotalHits.Relation.EQUAL_TO), InfinoSearch.reported(CAP, true, source));
        assertEquals(new TotalHits(CAP, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), InfinoSearch.reported(CAP + 1, true, source));
    }

    public void testTrackingEveryHitCountsPastTheDefaultCap() {
        SearchSourceBuilder source = new SearchSourceBuilder().trackTotalHits(true);
        assertEquals(new TotalHits(CAP + 1, TotalHits.Relation.EQUAL_TO), InfinoSearch.reported(CAP + 1, true, source));
    }
}
