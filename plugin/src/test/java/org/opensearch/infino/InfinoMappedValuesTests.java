/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.action.update.UpdateRequest;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.MapperServiceTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** The values OpenSearch's mappers read from a document's date and integer fields. */
public class InfinoMappedValuesTests extends MapperServiceTestCase {

    private MapperService logs() throws IOException {
        return createMapperService(mapping(b -> {
            b.startObject("status").field("type", "integer").endObject();
            b.startObject("strict").field("type", "integer").field("coerce", false).endObject();
            b.startObject("t").field("type", "date").field("format", "dd/MMM/yyyy:HH:mm:ss Z").endObject();
            b.startObject("plain").field("type", "date").field("format", "yyyy-MM-dd HH:mm:ss").endObject();
            b.startObject("s").field("type", "date").field("format", "epoch_second").endObject();
            b.startObject("n").field("type", "date_nanos").endObject();
            b.startObject("msg").field("type", "keyword").endObject();
            b.startObject("ratio").field("type", "double").endObject();
        }));
    }

    private static Map<String, Object> values(String json) {
        assertNotNull(json);
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, json, false);
    }

    /** Each date as the instant its own format reads, each integer as the field coerces it; other fields carry none. */
    public void testDatesAreReadByTheirFormatAndIntegersAsCoerced() throws IOException {
        String source = "{\"status\":200.7,\"t\":\"23/Nov/2026:10:30:00 +0000\",\"plain\":\"2026-11-23 10:30:15\","
            + "\"s\":1795429815,\"n\":\"2026-11-23T10:30:15.123456789Z\",\"msg\":\"x\",\"ratio\":0.5}";
        Map<String, Object> v = values(InfinoMappedValues.parse(logs(), "logs", "1", source, null));
        assertEquals(List.of(200), v.get("status"));
        assertEquals(List.of(1_795_429_800_000L), v.get("t"));
        assertEquals(List.of(1_795_429_815_000L), v.get("plain"));
        assertEquals(List.of(1_795_429_815_000L), v.get("s"));
        assertEquals(List.of(1_795_429_815_123L), v.get("n"));
        assertEquals(Set.of("status", "t", "plain", "s", "n"), v.keySet());
    }

    /** An array keeps each value, in the order the source gives them. */
    public void testAnArrayKeepsItsValuesInOrder() throws IOException {
        Map<String, Object> v = values(InfinoMappedValues.parse(logs(), "logs", "1", "{\"status\":[3,1.9,2]}", null));
        assertEquals(List.of(3, 1, 2), v.get("status"));
    }

    /** An integer field that does not coerce refuses a fractional number, as the shard would. */
    public void testAFieldThatDoesNotCoerceRefusesAFraction() throws IOException {
        MapperService m = logs();
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> InfinoMappedValues.parse(m, "logs", "1", "{\"strict\":1.5}", null)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("[strict]"));
    }

    /** A value its field's format cannot read is refused, naming the field. */
    public void testADateItsFormatCannotReadIsRefused() throws IOException {
        MapperService m = logs();
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> InfinoMappedValues.parse(m, "logs", "1", "{\"t\":\"2026-11-23\"}", null)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("[t]"));
    }

    public void testADocumentWithoutDateOrIntegerFieldsHasNoValues() throws IOException {
        assertNull(InfinoMappedValues.parse(logs(), "logs", "1", "{\"msg\":\"x\",\"ratio\":1.5}", null));
        assertNull(InfinoMappedValues.parse(null, "logs", "1", "{\"status\":1}", null));
        assertNull(InfinoMappedValues.of(null, logs()));
    }

    public void testTheValuesFollowTheSource() {
        assertEquals("{\"a\":1}" + InfinoMappedValues.SEPARATOR + "{\"a\":[1]}", InfinoMappedValues.framed("{\"a\":1}", "{\"a\":[1]}"));
        assertEquals("{\"a\":1}", InfinoMappedValues.framed("{\"a\":1}", null));
        assertNull(InfinoMappedValues.framed(null, "{\"a\":[1]}"));
    }

    /**
     * A client's source that holds the separator unescaped is no JSON
     * document, and is refused before it reaches the bridge, where what
     * follows it would be read as the mapper's values; escaped, it is text.
     */
    public void testASourceHoldingTheSeparatorIsRefused() {
        String raw = "{\"a\":\"x\"}" + InfinoMappedValues.SEPARATOR + "{\"status\":[1]}";
        assertNotNull(InfinoBulkActionFilter.unreadable(raw));
        assertNull(InfinoBulkActionFilter.unreadable("{\"a\":\"x\\u001e\"}"));
        assertNull(InfinoBulkActionFilter.unreadable(null));
        UpdateRequest upsert = new UpdateRequest("logs", "1").doc(
            "{\"a\":1}",
            XContentType.JSON
        ).upsert(raw, XContentType.JSON);
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> InfinoBulkActionFilter.merge(null, upsert));
        assertEquals(RestStatus.BAD_REQUEST, e.status());
    }
}
