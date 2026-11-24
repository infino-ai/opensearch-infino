/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

/**
 * The mark a skeleton rebuild's creates carry in their source: it is found
 * where it is and nowhere else, and the document reads as if it were not
 * there, so a shard's mapper, its translog replay and its history all take
 * the same document the table holds.
 */
public class InfinoSkeletonAdopterTests extends OpenSearchTestCase {

    private static final String ROW = "{\"n\":1,\"msg\":\"a /*not a mark*/ value\",\"tags\":[\"x\"]}";

    public void testAMarkedSourceReadsAsTheRowItWasMadeFrom() {
        String marked = InfinoSkeletonAdopter.marked(ROW);
        assertEquals("{" + InfinoSkeletonAdopter.MARK + ROW.substring(1), marked);
        BytesArray bytes = new BytesArray(marked);
        assertEquals("the content type is still found from the leading brace", XContentType.JSON, MediaTypeRegistry.xContentType(bytes));
        Map<String, Object> read = XContentHelper.convertToMap(bytes, false, XContentType.JSON).v2();
        assertEquals(XContentHelper.convertToMap(new BytesArray(ROW), false, XContentType.JSON).v2(), read);
    }

    public void testOnlyASourceOpeningWithTheMarkIsMarked() {
        assertTrue(InfinoSkeletonAdopter.isMarked(new BytesArray(InfinoSkeletonAdopter.marked(ROW))));
        assertTrue("leading blanks are dropped first", InfinoSkeletonAdopter.isMarked(new BytesArray(InfinoSkeletonAdopter.marked("  \n" + ROW))));
        assertFalse(InfinoSkeletonAdopter.isMarked(new BytesArray(ROW)));
        assertFalse("the mark inside a value is no mark", InfinoSkeletonAdopter.isMarked(new BytesArray("{\"m\":\"" + InfinoSkeletonAdopter.MARK + "\"}")));
        assertFalse("nor one after a blank", InfinoSkeletonAdopter.isMarked(new BytesArray(" {" + InfinoSkeletonAdopter.MARK + "\"n\":1}")));
        assertFalse(InfinoSkeletonAdopter.isMarked(new BytesArray("{")));
        assertFalse(InfinoSkeletonAdopter.isMarked(null));
        assertNull("a row that is no JSON object is not marked", InfinoSkeletonAdopter.marked("[1]"));
    }
}
