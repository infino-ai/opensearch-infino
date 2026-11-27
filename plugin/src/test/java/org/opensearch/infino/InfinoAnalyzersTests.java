/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

/** The text fields a table cannot analyze as their mapping asks, which creation and a mapping update refuse. */
public class InfinoAnalyzersTests extends OpenSearchTestCase {

    /** The analyzer an application's test suite maps its host and URI token fields with: the pattern tokenizer, lowercased. */
    private static final Settings WORD_SPLIT = Settings.builder()
        .put("index.analysis.analyzer.wordSplit.type", "custom")
        .put("index.analysis.analyzer.wordSplit.tokenizer", "pattern")
        .putList("index.analysis.analyzer.wordSplit.filter", "lowercase")
        .build();

    private static Map<String, Object> json(String source) {
        return XContentHelper.convertToMap(new BytesArray(source), true, MediaTypeRegistry.JSON).v2();
    }

    private static String refusal(String mapping, Settings settings) {
        return InfinoAnalyzers.refusal(json(mapping), settings);
    }

    public void testAFieldWithThePatternTokenizerIsRefusedByNameAndAnalyzer() {
        String refused = refusal("{\"properties\":{\"direct\":{\"type\":\"text\",\"norms\":false,\"analyzer\":\"wordSplit\"}}}", WORD_SPLIT);
        assertNotNull(refused);
        assertTrue(refused, refused.contains("field [direct] uses analyzer [wordSplit], which splits text with a pattern tokenizer"));
        assertTrue(refused, refused.contains("[index.infino.enabled: false]"));
        // The same under the _doc wrapper a mapping may come in.
        assertNotNull(refusal("{\"_doc\":{\"properties\":{\"direct\":{\"type\":\"text\",\"analyzer\":\"wordSplit\"}}}}", WORD_SPLIT));
    }

    public void testATargetOfCopyToWithThePatternTokenizerIsRefused() {
        String refused = refusal(
            "{\"properties\":{\"host\":{\"type\":\"keyword\",\"copy_to\":\"tokens\"},"
                + "\"tokens\":{\"type\":\"text\",\"analyzer\":\"wordSplit\"}}}",
            WORD_SPLIT
        );
        assertNotNull(refused);
        assertTrue(refused, refused.contains("field [tokens]"));
    }

    public void testEveryWayToNameAPatternTokenizerIsRefused() {
        String direct = "{\"properties\":{\"t\":{\"type\":\"text\",\"analyzer\":\"a\"}}}";
        // The built-in pattern analyzer, by name.
        assertNotNull(refusal("{\"properties\":{\"t\":{\"type\":\"text\",\"analyzer\":\"pattern\"}}}", Settings.EMPTY));
        // An analyzer of the pattern type, with its own pattern.
        assertNotNull(
            refusal(direct, Settings.builder().put("index.analysis.analyzer.a.type", "pattern").put("index.analysis.analyzer.a.pattern", ",").build())
        );
        // A custom analyzer over a tokenizer of the index's own, of each pattern type.
        for (String type : new String[] { "pattern", "simple_pattern", "simple_pattern_split" }) {
            Settings settings = Settings.builder()
                .put("index.analysis.analyzer.a.tokenizer", "split")
                .put("index.analysis.tokenizer.split.type", type)
                .put("index.analysis.tokenizer.split.pattern", "[.]")
                .build();
            assertNotNull(type, refusal(direct, settings));
        }
        // match_only_text and search_as_you_type take a text index too.
        assertNotNull(refusal("{\"properties\":{\"t\":{\"type\":\"match_only_text\",\"analyzer\":\"pattern\"}}}", Settings.EMPTY));
        assertNotNull(refusal("{\"properties\":{\"t\":{\"type\":\"search_as_you_type\",\"analyzer\":\"pattern\"}}}", Settings.EMPTY));
    }

    public void testAFieldUnderAnObjectIsRefusedByItsFullPath() {
        String refused = refusal("{\"properties\":{\"http\":{\"properties\":{\"uri\":{\"type\":\"text\",\"analyzer\":\"wordSplit\"}}}}}", WORD_SPLIT);
        assertNotNull(refused);
        assertTrue(refused, refused.contains("field [http.uri]"));
        // Nested too.
        assertNotNull(
            refusal("{\"properties\":{\"n\":{\"type\":\"nested\",\"properties\":{\"uri\":{\"type\":\"text\",\"analyzer\":\"wordSplit\"}}}}}", WORD_SPLIT)
        );
    }

    public void testADynamicTemplateWithThePatternTokenizerIsRefused() {
        String refused = refusal(
            "{\"dynamic_templates\":[{\"tokens\":{\"match\":\"*_tokens\",\"mapping\":{\"type\":\"text\",\"analyzer\":\"wordSplit\"}}}],"
                + "\"properties\":{\"plain\":{\"type\":\"text\"}}}",
            WORD_SPLIT
        );
        assertNotNull(refused);
        assertTrue(refused, refused.contains("dynamic template [tokens] uses analyzer [wordSplit]"));
    }

    public void testAnalyzersTheTableStandsInForAreLetThrough() {
        Settings settings = Settings.builder()
            .put(WORD_SPLIT)
            .put("index.analysis.analyzer.spaces.type", "custom")
            .put("index.analysis.analyzer.spaces.tokenizer", "whitespace")
            // An analyzer of the index's own named pattern is that analyzer, not the built-in one.
            .put("index.analysis.analyzer.pattern.type", "standard")
            .build();
        assertNull(refusal("{\"properties\":{\"t\":{\"type\":\"text\"}}}", settings));
        assertNull(refusal("{\"properties\":{\"t\":{\"type\":\"text\",\"analyzer\":\"standard\"}}}", settings));
        assertNull(refusal("{\"properties\":{\"t\":{\"type\":\"text\",\"analyzer\":\"whitespace\"}}}", settings));
        assertNull(refusal("{\"properties\":{\"t\":{\"type\":\"text\",\"analyzer\":\"spaces\"}}}", settings));
        assertNull(refusal("{\"properties\":{\"t\":{\"type\":\"text\",\"analyzer\":\"pattern\"}}}", settings));
        // A search analyzer alone does not change what the table indexes.
        assertNull(refusal("{\"properties\":{\"t\":{\"type\":\"text\",\"search_analyzer\":\"wordSplit\"}}}", settings));
        // A keyword field, a text field the mapping does not index, and one under an object that is not indexed get no text index.
        assertNull(refusal("{\"properties\":{\"k\":{\"type\":\"keyword\"}}}", settings));
        assertNull(refusal("{\"properties\":{\"t\":{\"type\":\"text\",\"index\":false,\"analyzer\":\"wordSplit\"}}}", settings));
        assertNull(
            refusal(
                "{\"properties\":{\"o\":{\"type\":\"object\",\"enabled\":false,\"properties\":{\"t\":{\"type\":\"text\",\"analyzer\":\"wordSplit\"}}}}}",
                settings
            )
        );
        assertNull(refusal("{}", settings));
    }

    public void testADefaultAnalyzerWithAPatternTokenizerIsRefused() {
        Settings pattern = Settings.builder().put("index.analysis.analyzer.default.type", "pattern").build();
        String refused = InfinoAnalyzers.defaultRefusal(pattern);
        assertNotNull(refused);
        assertTrue(refused, refused.contains("default analyzer [index.analysis.analyzer.default] splits text with a pattern tokenizer"));
        // A text field without an analyzer takes it, so a mapping update that adds one is refused too.
        assertNotNull(refusal("{\"properties\":{\"t\":{\"type\":\"text\"}}}", pattern));
        assertNull(InfinoAnalyzers.defaultRefusal(Settings.EMPTY));
        assertNull(InfinoAnalyzers.defaultRefusal(Settings.builder().put("index.analysis.analyzer.default.type", "standard").build()));
        assertNull(InfinoAnalyzers.defaultRefusal(WORD_SPLIT));
    }

    public void testAMappingUpdateThatAddsSuchAFieldIsRefusedOnAnInfinoBackedIndexOnly() {
        IndexMetadata infino = index("logs", "u1", true);
        IndexMetadata stock = index("stock", "u2", false);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(infino, false).put(stock, false)).build();
        String adds = "{\"properties\":{\"direct\":{\"type\":\"text\",\"analyzer\":\"wordSplit\"}}}";
        String refused = InfinoAttachedMappingActionFilter.analyzerRefusal(state, null, new PutMappingRequest("logs").source(adds, MediaTypeRegistry.JSON));
        assertNotNull(refused);
        assertTrue(refused, refused.contains("field [direct] uses analyzer [wordSplit]"));
        assertNotNull(
            InfinoAttachedMappingActionFilter.analyzerRefusal(state, null, new PutMappingRequest("stock", "logs").source(adds, MediaTypeRegistry.JSON))
        );
        // A stock index takes it, and a field the table can analyze is let through.
        assertNull(InfinoAttachedMappingActionFilter.analyzerRefusal(state, null, new PutMappingRequest("stock").source(adds, MediaTypeRegistry.JSON)));
        assertNull(
            InfinoAttachedMappingActionFilter.analyzerRefusal(
                state,
                null,
                new PutMappingRequest("logs").source("{\"properties\":{\"plain\":{\"type\":\"text\"}}}", MediaTypeRegistry.JSON)
            )
        );
        // An index the cluster does not hold is the cluster's to report.
        assertNull(InfinoAttachedMappingActionFilter.analyzerRefusal(state, null, new PutMappingRequest("absent").source(adds, MediaTypeRegistry.JSON)));
    }

    /** An index with {@link #WORD_SPLIT} among its analyzers, Infino-backed or not. */
    private static IndexMetadata index(String name, String uuid, boolean infino) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_INDEX_UUID, uuid)
                    .put(InfinoPlugin.INFINO_ENABLED.getKey(), infino)
                    .put(WORD_SPLIT)
            )
            .build();
    }
}
