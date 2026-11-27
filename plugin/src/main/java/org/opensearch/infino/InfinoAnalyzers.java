/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.MapperService;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The text analyzers a table cannot apply, and the mappings that ask for one.
 *
 * <p>The table indexes every text field with the engine's {@code standard}
 * analyzer, whatever analyzer the mapping names: it splits text into words
 * at Unicode word boundaries and lowercases them. For most analyzers a
 * search then still finds roughly what stock finds. A pattern tokenizer is
 * different: it splits text at a regular expression, so {@code www.Example.com}
 * becomes {@code www}, {@code example} and {@code com} on stock, while the
 * standard analyzer keeps {@code www.example.com} as one word. A match on any
 * of those tokens then finds nothing in the table, with no error. So an
 * index whose text field would be analyzed that way is refused instead: at
 * creation, and when a mapping update adds such a field.
 */
final class InfinoAnalyzers {

    /** The tokenizer types that split text at a regular expression, as {@code analysis-common} names them. */
    private static final Set<String> PATTERN_TOKENIZERS = Set.of("pattern", "simple_pattern", "simple_pattern_split");
    /** The built-in analyzer type that tokenizes with a pattern tokenizer. */
    private static final String PATTERN_ANALYZER = "pattern";
    /** The name an index's own default analyzer is defined under, and what a text field without an analyzer uses. */
    private static final String DEFAULT_ANALYZER = "default";
    /** The mapping types the table gives a text index. */
    private static final Set<String> TEXT_TYPES = Set.of("text", "match_only_text", "search_as_you_type");
    /** The settings prefix of an index's own analyzers. */
    private static final String ANALYZER_PREFIX = "index.analysis.analyzer.";
    /** The settings prefix of an index's own tokenizers. */
    private static final String TOKENIZER_PREFIX = "index.analysis.tokenizer.";

    private InfinoAnalyzers() {}

    /**
     * Refuse the index being created when its merged mapping (the request's
     * and its templates') has a text field the table cannot analyze as the
     * mapping asks ({@link #refusal}), or when its default analyzer is one
     * ({@link #defaultRefusal}). Called with the mapping as the index will
     * hold it, after the templates are applied.
     */
    static void validate(MapperService mapperService, IndexSettings indexSettings) {
        Settings settings = indexSettings.getSettings();
        if (!settings.getAsBoolean(InfinoPlugin.INFINO_ENABLED.getKey(), false)) {
            return;
        }
        String refused = defaultRefusal(settings);
        if (refused == null) {
            DocumentMapper mapper = mapperService.documentMapper();
            refused = refusal(mapper == null ? Map.of() : new MappingMetadata(mapper).sourceAsMap(), settings);
        }
        if (refused != null) {
            throw new IllegalArgumentException(refused);
        }
    }

    /**
     * Why an index with these settings cannot be served when its default
     * analyzer splits text with a pattern tokenizer: every text field that
     * names no analyzer takes it, those a document brings later too. Null
     * when the default analyzer is one the table can stand in for.
     */
    static String defaultRefusal(Settings settings) {
        if (settings.get(ANALYZER_PREFIX + DEFAULT_ANALYZER + ".type") == null
            && settings.get(ANALYZER_PREFIX + DEFAULT_ANALYZER + ".tokenizer") == null) {
            return null;
        }
        if (!splitsByPattern(DEFAULT_ANALYZER, settings)) {
            return null;
        }
        return "[infino] the index's default analyzer ["
            + ANALYZER_PREFIX
            + DEFAULT_ANALYZER
            + "] splits text with a pattern tokenizer, which the table does not apply: it indexes text with the standard analyzer only, "
            + "so a search on a text field would miss what stock finds. Use another default analyzer, or create the index with ["
            + InfinoPlugin.INFINO_ENABLED.getKey()
            + ": false]";
    }

    /**
     * Why a mapping cannot be served over a table: the first text field it
     * maps (or a dynamic template it holds) whose analyzer splits text with
     * a pattern tokenizer, with the index's {@code settings} resolving the
     * analyzer's name. Null when there is none.
     *
     * @param mapping a mapping's source, with or without its {@code _doc} wrapper
     */
    static String refusal(Map<String, Object> mapping, Settings settings) {
        Map<String, Object> root = rootOf(mapping);
        Field field = patternField(asMap(root.get("properties")), "", settings);
        if (field != null) {
            return fieldRefusal("field [" + field.path() + "]", field.analyzer());
        }
        if (root.get("dynamic_templates") instanceof List<?> templates) {
            for (Object entry : templates) {
                for (Map.Entry<String, Object> template : asMap(entry).entrySet()) {
                    Map<String, Object> def = asMap(asMap(template.getValue()).get("mapping"));
                    if (def.get("analyzer") instanceof String analyzer && splitsByPattern(analyzer, settings)) {
                        return fieldRefusal("dynamic template [" + template.getKey() + "]", analyzer);
                    }
                }
            }
        }
        return null;
    }

    private static String fieldRefusal(String what, String analyzer) {
        return "[infino] "
            + what
            + " uses analyzer ["
            + analyzer
            + "], which splits text with a pattern tokenizer. The table does not apply it: it indexes text with the standard analyzer only, "
            + "so a search on the field would miss what stock finds. Map the field with another analyzer, or create the index with ["
            + InfinoPlugin.INFINO_ENABLED.getKey()
            + ": false]";
    }

    /** A text field by its full path, and the name of the analyzer it is indexed with. */
    private record Field(String path, String analyzer) {}

    /**
     * The first indexed text field under {@code props} whose analyzer splits
     * by pattern; null when there is none. Objects and
     * nested fields are walked as the bridge walks them for columns, and an
     * object with {@code enabled: false} holds no column.
     */
    private static Field patternField(Map<String, Object> props, String prefix, Settings settings) {
        for (Map.Entry<String, Object> e : props.entrySet()) {
            Map<String, Object> def = asMap(e.getValue());
            String path = prefix.isEmpty() ? e.getKey() : prefix + "." + e.getKey();
            Object type = def.get("type");
            if (type == null || "object".equals(type) || "nested".equals(type)) {
                if (Boolean.FALSE.equals(flag(def.get("enabled")))) {
                    continue;
                }
                Field inner = patternField(asMap(def.get("properties")), path, settings);
                if (inner != null) {
                    return inner;
                }
            } else if (TEXT_TYPES.contains(type) && !Boolean.FALSE.equals(flag(def.get("index")))) {
                String analyzer = def.get("analyzer") instanceof String name ? name : DEFAULT_ANALYZER;
                if (splitsByPattern(analyzer, settings)) {
                    return new Field(path, analyzer);
                }
            }
        }
        return null;
    }

    /**
     * Whether the analyzer named {@code name} splits text with a pattern
     * tokenizer: the index's own analyzer of that name, of type
     * {@code pattern} or built on such a tokenizer (the index's own of that
     * name, or the built-in one), or else the built-in {@code pattern}
     * analyzer.
     */
    static boolean splitsByPattern(String name, Settings settings) {
        String type = settings.get(ANALYZER_PREFIX + name + ".type");
        String tokenizer = settings.get(ANALYZER_PREFIX + name + ".tokenizer");
        if (type == null && tokenizer == null) {
            return PATTERN_ANALYZER.equals(name);
        }
        if (PATTERN_ANALYZER.equals(type)) {
            return true;
        }
        if (tokenizer == null) {
            return false;
        }
        String tokenizerType = settings.get(TOKENIZER_PREFIX + tokenizer + ".type");
        return PATTERN_TOKENIZERS.contains(tokenizerType == null ? tokenizer : tokenizerType);
    }

    /** The root object of a mapping, past the {@code _doc} wrapper it may come in. */
    private static Map<String, Object> rootOf(Map<String, Object> mapping) {
        if (!mapping.containsKey("properties") && !mapping.containsKey("dynamic_templates") && mapping.size() == 1) {
            Object only = mapping.values().iterator().next();
            if (only instanceof Map<?, ?>) {
                return asMap(only);
            }
        }
        return mapping;
    }

    /** A mapping's boolean setting as OpenSearch reads it: a boolean or its string; null when absent or neither. */
    private static Boolean flag(Object value) {
        if (value instanceof Boolean b) {
            return b;
        }
        if ("true".equals(value) || "false".equals(value)) {
            return Boolean.parseBoolean((String) value);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object value) {
        return value instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
    }
}
