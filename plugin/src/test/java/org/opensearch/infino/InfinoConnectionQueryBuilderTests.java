/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** A cacheable default-mode search's routed query names the shard node's connection, so its key moves with it. */
public class InfinoConnectionQueryBuilderTests extends OpenSearchTestCase {

    private static QueryRewriteContext atShard() {
        QueryRewriteContext context = mock(QueryRewriteContext.class);
        when(context.convertToShardContext()).thenReturn(mock(QueryShardContext.class));
        return context;
    }

    public void testTheCoordinatingNodeLeavesTheConnectionUnnamed() throws Exception {
        InfinoConnectionQueryBuilder routed = new InfinoConnectionQueryBuilder();
        assertSame(routed, routed.rewrite(mock(QueryRewriteContext.class)));
    }

    public void testAShardNamesItsConnectionAndAReopenMovesIt() throws Exception {
        QueryBuilder before = new InfinoConnectionQueryBuilder().rewrite(atShard());
        assertEquals(InfinoRequestCache.connection(), ((InfinoConnectionQueryBuilder) before).connection());
        // A repeat on the same connection rewrites to the same query, so it keys the same.
        assertSame(before, before.rewrite(atShard()));
        assertEquals(before, new InfinoConnectionQueryBuilder().rewrite(atShard()));

        InfinoRequestCache.changedAll();
        QueryBuilder after = new InfinoConnectionQueryBuilder().rewrite(atShard());
        assertNotEquals(before, after);
        assertNotEquals(before, before.rewrite(atShard()));
    }

    public void testTheNamedConnectionCrossesTheWire() throws Exception {
        InfinoConnectionQueryBuilder named = new InfinoConnectionQueryBuilder(7);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            named.writeTo(out);
            assertEquals(named, new InfinoConnectionQueryBuilder(out.bytes().streamInput()));
        }
    }
}
