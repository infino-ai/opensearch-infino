/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

/** Which mappings make a write pay the mapper's parse for the fields it ignores. */
public class InfinoIgnoredTests extends OpenSearchTestCase {

    public void testAFieldThatIgnoresMalformedValuesIsFound() {
        assertTrue(InfinoIgnored.namesIgnoring(Map.of("properties", Map.of("n", Map.of("type", "integer", "ignore_malformed", true)))));
    }

    public void testAMultiFieldOrTemplateThatIgnoresMalformedValuesIsFound() {
        Map<String, Object> multi = Map.of(
            "properties",
            Map.of("t", Map.of("type", "text", "fields", Map.of("n", Map.of("type", "integer", "ignore_malformed", true))))
        );
        assertTrue(InfinoIgnored.namesIgnoring(multi));
        Map<String, Object> template = Map.of(
            "dynamic_templates",
            List.of(Map.of("longs", Map.of("match_mapping_type", "long", "mapping", Map.of("type", "long", "ignore_malformed", true))))
        );
        assertTrue(InfinoIgnored.namesIgnoring(template));
    }

    public void testIgnoreMalformedTurnedOffOrOnlyNamedIsNotFound() {
        assertFalse(InfinoIgnored.namesIgnoring(Map.of("properties", Map.of("n", Map.of("type", "integer", "ignore_malformed", false)))));
        assertFalse(InfinoIgnored.namesIgnoring(Map.of("_meta", Map.of("note", "ignore_malformed is off"), "properties", Map.of())));
        assertFalse(InfinoIgnored.namesIgnoring(Map.of("properties", Map.of("ignore_malformed", Map.of("type", "keyword")))));
    }

    public void testKeywordIgnoreAboveAloneIsNotFound() {
        assertFalse(InfinoIgnored.namesIgnoring(Map.of("properties", Map.of("k", Map.of("type", "keyword", "ignore_above", 5)))));
    }
}
