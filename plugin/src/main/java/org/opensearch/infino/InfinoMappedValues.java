/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.lucene.document.DoubleRange;
import org.apache.lucene.document.FloatRange;
import org.apache.lucene.document.IntRange;
import org.apache.lucene.document.LongRange;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexService;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.RangeFieldMapper;
import org.opensearch.index.mapper.RangeType;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.indices.IndicesService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The values OpenSearch's mappers read from a document's date and integer
 * fields, as the instants (epoch milliseconds) and whole numbers its index
 * holds. The mapper is the judge of what a value means: a date in any format
 * the field's mapping names, and a fractional number in an integer field
 * that coerces it (truncated), are read as the mapper reads them, where the
 * bridge reads only OpenSearch's built-in date formats and whole numbers.
 * The values travel with the document's source to the bridge
 * ({@link #framed}), which puts them in the columns of those fields in place
 * of what the source holds there; the source itself is kept whole.
 *
 * <p>Reading them costs a document a pass through the mapper in proxy mode
 * (default mode has the parsed document at hand), so only a document the
 * bridge refuses without them is given them.
 */
final class InfinoMappedValues {

    /**
     * What separates a document's source from its values when the two cross
     * to the bridge: JSON text never holds it unescaped.
     */
    static final char SEPARATOR = '\u001e';

    private InfinoMappedValues() {}

    /** The source with the values the bridge reads in place of its own (none when {@code values} is null). */
    static String framed(String source, String values) {
        return source == null || values == null ? source : source + SEPARATOR + values;
    }

    /**
     * The values of the date and integer fields of {@code doc}, as a JSON
     * object of each field's values in the order the document gives them;
     * null when it has none. A field of a nested object is read from the
     * nested documents when the root document does not hold it.
     */
    static String of(ParsedDocument doc, MapperService mappers) {
        if (doc == null || mappers == null || doc.rootDoc() == null) {
            return null;
        }
        Map<String, List<Number>> values = new LinkedHashMap<>();
        collect(doc.rootDoc(), mappers, values, Map.of());
        collectRanges(doc.rootDoc(), mappers, values, Map.of());
        Map<String, List<Number>> root = Map.copyOf(values);
        for (ParseContext.Document nested : doc.docs()) {
            if (nested != doc.rootDoc()) {
                collect(nested, mappers, values, root);
                collectRanges(nested, mappers, values, root);
            }
        }
        if (values.isEmpty()) {
            return null;
        }
        try (XContentBuilder b = JsonXContent.contentBuilder()) {
            b.startObject();
            for (Map.Entry<String, List<Number>> e : values.entrySet()) {
                b.startArray(e.getKey());
                for (Number v : e.getValue()) {
                    if (v instanceof Double d) {
                        b.value(d.doubleValue());
                    } else {
                        b.value(v.longValue());
                    }
                }
                b.endArray();
            }
            b.endObject();
            return BytesReference.bytes(b).utf8ToString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Each date or integer field's values in one Lucene document, added to
     * {@code into}; a field {@code skip} holds is left as it is. A value is
     * read from the field's doc values, else its point, else its stored
     * field: the one kind each value is indexed as once.
     */
    private static void collect(ParseContext.Document d, MapperService mappers, Map<String, List<Number>> into, Map<String, List<Number>> skip) {
        Map<String, List<IndexableField>> byName = new LinkedHashMap<>();
        for (IndexableField f : d.getFields()) {
            if (!skip.containsKey(f.name()) && f.numericValue() != null) {
                byName.computeIfAbsent(f.name(), n -> new ArrayList<>()).add(f);
            }
        }
        for (Map.Entry<String, List<IndexableField>> e : byName.entrySet()) {
            MappedFieldType type = mappers.fieldType(e.getKey());
            boolean nanos;
            if (type instanceof DateFieldMapper.DateFieldType date) {
                nanos = date.resolution() == DateFieldMapper.Resolution.NANOSECONDS;
            } else if (type instanceof NumberFieldMapper.NumberFieldType number && integral(number.numberType())) {
                nanos = false;
            } else {
                continue;
            }
            List<IndexableField> fields = e.getValue();
            List<IndexableField> chosen = of(fields, Kind.DOC_VALUES);
            if (chosen.isEmpty()) {
                chosen = of(fields, Kind.POINT);
            }
            if (chosen.isEmpty()) {
                chosen = of(fields, Kind.STORED);
            }
            if (chosen.isEmpty()) {
                continue;
            }
            List<Number> out = into.computeIfAbsent(e.getKey(), n -> new ArrayList<>());
            for (IndexableField f : chosen) {
                long v = f.numericValue().longValue();
                out.add(nanos ? Math.floorDiv(v, 1_000_000L) : v);
            }
        }
    }

    /**
     * Each range field's ranges in one Lucene document, added to
     * {@code into} as the lowest and highest value of each, both inclusive,
     * one after the other: the bridge keeps a range as those two ends. A
     * range is read from the field the mapper indexes it as, one per range in
     * the order the document gives them, else from its doc values. A bound
     * left out is the type's least or greatest value; a float or double
     * one's infinity is the greatest finite double, as the bridge keeps it.
     * An {@code ip_range} has no columns, and a field {@code skip} holds is
     * left as it is.
     */
    private static void collectRanges(ParseContext.Document d, MapperService mappers, Map<String, List<Number>> into, Map<String, List<Number>> skip) {
        Map<String, List<IndexableField>> byName = new LinkedHashMap<>();
        for (IndexableField f : d.getFields()) {
            if (!skip.containsKey(f.name())
                && mappers.fieldType(f.name()) instanceof RangeFieldMapper.RangeFieldType range
                && range.rangeType() != RangeType.IP) {
                byName.computeIfAbsent(f.name(), n -> new ArrayList<>()).add(f);
            }
        }
        for (Map.Entry<String, List<IndexableField>> e : byName.entrySet()) {
            List<Number> ends = new ArrayList<>();
            for (IndexableField f : e.getValue()) {
                if (f instanceof LongRange r) {
                    ends.add(r.getMin(0));
                    ends.add(r.getMax(0));
                } else if (f instanceof IntRange r) {
                    ends.add((long) r.getMin(0));
                    ends.add((long) r.getMax(0));
                } else if (f instanceof FloatRange r) {
                    ends.add(finite(r.getMin(0)));
                    ends.add(finite(r.getMax(0)));
                } else if (f instanceof DoubleRange r) {
                    ends.add(finite(r.getMin(0)));
                    ends.add(finite(r.getMax(0)));
                }
            }
            if (ends.isEmpty()) {
                RangeType type = ((RangeFieldMapper.RangeFieldType) mappers.fieldType(e.getKey())).rangeType();
                for (IndexableField f : e.getValue()) {
                    if (f.fieldType().docValuesType() == DocValuesType.BINARY && f.binaryValue() != null) {
                        try {
                            for (RangeFieldMapper.Range r : type.decodeRanges(f.binaryValue())) {
                                ends.add(end(r.getFrom()));
                                ends.add(end(r.getTo()));
                            }
                        } catch (IOException ex) {
                            throw new UncheckedIOException(ex);
                        }
                    }
                }
            }
            if (!ends.isEmpty()) {
                into.put(e.getKey(), ends);
            }
        }
    }

    /** One end of a range from its doc values, as {@link #collectRanges} gives it. */
    private static Number end(Object v) {
        return v instanceof Double || v instanceof Float ? finite(((Number) v).doubleValue()) : (Number) ((Number) v).longValue();
    }

    /** {@code v}, an infinity kept as the greatest finite double of its sign. */
    private static Double finite(double v) {
        return Double.isInfinite(v) ? Math.copySign(Double.MAX_VALUE, v) : v;
    }

    /** The integer types: an {@code unsigned_long} past the signed range is no value a column of longs holds. */
    private static boolean integral(NumberFieldMapper.NumberType type) {
        return switch (type) {
            case BYTE, SHORT, INTEGER, LONG -> true;
            default -> false;
        };
    }

    private enum Kind {
        DOC_VALUES,
        POINT,
        STORED
    }

    private static List<IndexableField> of(List<IndexableField> fields, Kind kind) {
        List<IndexableField> out = new ArrayList<>(fields.size());
        for (IndexableField f : fields) {
            boolean is = switch (kind) {
                case DOC_VALUES -> f.fieldType().docValuesType() == DocValuesType.SORTED_NUMERIC;
                case POINT -> f.fieldType().pointDimensionCount() == 1;
                case STORED -> f.fieldType().stored();
            };
            if (is) {
                out.add(f);
            }
        }
        return out;
    }

    /** The mapper of {@code index} on this node; null when the index has no service here. */
    static MapperService mappers(Index index) {
        IndicesService indices = InfinoServices.indicesService();
        IndexService service = indices == null || index == null ? null : indices.indexService(index);
        return service == null ? null : service.mapperService();
    }

    /**
     * The values of {@code source} (JSON), parsed as the shard would parse
     * it ({@link #of}); null when {@code mappers} is null or the document has
     * none. Throws what the mapper throws for a document it refuses, such as
     * a fractional number in an integer field with {@code coerce: false}.
     */
    static String parse(MapperService mappers, String index, String id, String source, String routing) {
        if (mappers == null || source == null) {
            return null;
        }
        ParsedDocument doc = mappers.documentMapperWithAutoCreate()
            .getDocumentMapper()
            .parse(new SourceToParse(index, id, new BytesArray(source), MediaTypeRegistry.JSON, routing));
        return of(doc, mappers);
    }
}
