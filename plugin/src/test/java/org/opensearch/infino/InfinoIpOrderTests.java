/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.common.network.NetworkAddress;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * An ip field's addresses, read through the engine, in the order stock
 * compares them: as numbers, by their 16-byte form, an IPv4 address as its
 * IPv4-mapped IPv6 address. The table holds each address as written, so
 * text order would put {@code 1.0.179.100} before {@code 1.0.6.192}, and
 * {@code 2001:db8::1} before {@code 255.0.0.1}. Ranges, sorts and search_after
 * pages must follow the address order for every form the table accepts.
 */
public class InfinoIpOrderTests extends OpenSearchTestCase {

    /** Every address below, one a document, and a document with none: sorted by address. */
    private static final String TABLE = "ip_order";
    /** The same, and a document with several addresses: ranged, since a sort over it is refused. */
    private static final String SEVERAL_TABLE = "ip_order_several";

    /**
     * Addresses in every written form: IPv4, IPv6 compressed, in full, in
     * upper case, with leading zeros, with an IPv4 tail, and IPv4-mapped.
     */
    private static final List<String> ADDRESSES = List.of(
        "1.0.179.100",
        "1.0.207.111",
        "1.0.6.192",
        "1.0.61.206",
        "9.255.255.255",
        "10.0.0.0",
        "10.0.0.1",
        "10.255.255.255",
        "11.0.0.0",
        "100.0.0.0",
        "100.0.0.1",
        "99.255.255.255",
        "255.255.255.255",
        "0.0.0.0",
        "::",
        "::1",
        "::2",
        "::ffff:10.1.2.3",
        "::FFFF:100.200.0.1",
        "::1.2.3.4",
        "64:ff9b::1.2.3.4",
        "2001:db8::",
        "2001:db8::1",
        "2001:DB8:0:0:0:0:0:2",
        "2001:0db8:0000:0000:0000:0000:0000:0003",
        "2001:db8:1::",
        "2001:db9::",
        "fe80::1",
        "1::",
        "1:2:3:4:5:6:7::",
        "1:2:3:4:5:6::8",
        "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"
    );

    /** A document with several addresses: it sorts by its least ascending and its greatest descending. */
    private static final List<String> SEVERAL = List.of("200.1.1.1", "3.3.3.3", "2001:db8::5");

    /** The id of the document with {@link #SEVERAL}, and of the one with no address. */
    private static final String SEVERAL_ID = "several";
    private static final String NONE_ID = "none";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        load(TABLE, false);
        load(SEVERAL_TABLE, true);
    }

    private static void load(String table, boolean several) {
        InfinoNative.dropTable(table);
        InfinoNative.ensureTable(table, "{\"properties\":{\"addr\":{\"type\":\"ip\"}}}");
        List<String> ids = new ArrayList<>();
        List<String> sources = new ArrayList<>();
        for (int i = 0; i < ADDRESSES.size(); i++) {
            ids.add(Integer.toString(i));
            sources.add("{\"addr\":\"" + ADDRESSES.get(i) + "\"}");
        }
        if (several) {
            ids.add(SEVERAL_ID);
            sources.add("{\"addr\":[" + SEVERAL.stream().map(a -> "\"" + a + "\"").collect(Collectors.joining(",")) + "]}");
        }
        ids.add(NONE_ID);
        sources.add("{}");
        int n = ids.size();
        assertEquals(n, InfinoNative.indexBatch(table, ids.toArray(new String[0]), sources.toArray(new String[0]), n).ok());
    }

    @Override
    public void tearDown() throws Exception {
        InfinoNative.dropTable(TABLE);
        InfinoNative.dropTable(SEVERAL_TABLE);
        super.tearDown();
    }

    /** An address's 16-byte form, the order stock compares addresses in. */
    private static BytesRef encoded(String address) {
        return new BytesRef(InetAddressPoint.encode(InetAddresses.forString(address)));
    }

    /** An address as stock prints it. */
    private static String shown(String address) {
        return NetworkAddress.format(InetAddresses.forString(address));
    }

    private static InfinoSearch.Plan plan(String table, SearchSourceBuilder source) {
        return InfinoSearch.plan(table, source, System.currentTimeMillis());
    }

    private static List<Map<String, Object>> rows(SearchSourceBuilder source) {
        return InfinoNative.sqlRows(InfinoSearch.hitStatement(plan(SEVERAL_TABLE, source), ADDRESSES.size() + 10, false));
    }

    /** The ids of the documents the range keeps, as stock reads it: any of a document's addresses in the range. */
    private static List<String> expectedIds(String from, boolean includeLower, String to, boolean includeUpper) {
        List<String> out = new ArrayList<>();
        for (int i = 0; i < ADDRESSES.size(); i++) {
            if (inRange(ADDRESSES.get(i), from, includeLower, to, includeUpper)) {
                out.add(Integer.toString(i));
            }
        }
        if (SEVERAL.stream().anyMatch(a -> inRange(a, from, includeLower, to, includeUpper))) {
            out.add(SEVERAL_ID);
        }
        out.sort(Comparator.naturalOrder());
        return out;
    }

    private static boolean inRange(String address, String from, boolean includeLower, String to, boolean includeUpper) {
        BytesRef x = encoded(address);
        if (from != null) {
            int c = x.compareTo(encoded(from));
            if (includeLower ? c < 0 : c <= 0) {
                return false;
            }
        }
        if (to != null) {
            int c = x.compareTo(encoded(to));
            if (includeUpper ? c > 0 : c >= 0) {
                return false;
            }
        }
        return true;
    }

    private static List<String> ids(List<Map<String, Object>> rows) {
        List<String> out = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            out.add(row.get(InfinoQueryTranslator.OSID).toString());
        }
        return out;
    }

    private void assertRange(String from, boolean includeLower, String to, boolean includeUpper) {
        RangeQueryBuilder r = QueryBuilders.rangeQuery("addr");
        if (from != null) {
            r.from(from, includeLower);
        }
        if (to != null) {
            r.to(to, includeUpper);
        }
        List<String> got = ids(rows(new SearchSourceBuilder().query(r)));
        got.sort(Comparator.naturalOrder());
        assertEquals(r.toString(), expectedIds(from, includeLower, to, includeUpper), got);
    }

    /** The key the statement computes for every written form is the hex of its 16-byte form. */
    public void testTheKeyOfEveryWrittenFormIsItsAddress() {
        String key = InfinoIp.key(InfinoQueryTranslator.quoteIdent("addr"));
        List<Map<String, Object>> rows = InfinoNative.sqlRows(
            "SELECT " + InfinoQueryTranslator.OSID + ", addr, " + key + " AS k FROM " + InfinoQueryTranslator.quoteIdent(TABLE)
        );
        assertEquals(ADDRESSES.size() + 1, rows.size());
        for (Map<String, Object> row : rows) {
            if (row.get("addr") == null) {
                // No address, no key.
                assertNull(row.get("k"));
                continue;
            }
            String address = row.get("addr").toString();
            assertEquals(address, InfinoIp.keyOf(address), row.get("k"));
            assertEquals(address, shown(address), InfinoIp.shown(row.get("k")));
        }
    }

    /**
     * The ranges the issue found wrong, and their v6 kin: each keeps
     * the documents whose address lies in it as a number, not as text (the
     * text range 10.0.0.0 to 10.255.255.255 would also keep 10.0.0.1's
     * neighbour 100.0.0.0, and miss ::ffff:10.1.2.3).
     */
    public void testARangeComparesAddressesAsNumbers() {
        assertRange("10.0.0.0", true, "10.255.255.255", true);
        assertRange("100.0.0.0", false, null, false);
        assertRange(null, false, "100.0.0.0", false);
        assertRange("10.0.0.0", false, "10.255.255.255", false);
        assertRange("::1", true, null, false);
        assertRange("::1", false, "::ffff:0:0", false);
        assertRange("2001:db8::", true, "2001:db9::", false);
        assertRange("2001:DB8::", true, "2001:db8:ffff:ffff:ffff:ffff:ffff:ffff", true);
        assertRange("0.0.0.0", true, "255.255.255.255", true);
        assertRange("255.255.255.255", false, null, false);
        assertRange("::", true, null, false);
        assertRange("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", false, null, false);
        assertRange("1.0.6.192", true, "1.0.61.206", true);
    }

    /** A bound that is not an address is refused with stock's message. */
    public void testARangeBoundThatIsNotAnAddressIsRefused() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> rows(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("addr").gte("10.0.0")))
        );
        assertEquals("'10.0.0' is not an IP string literal.", e.getMessage());
    }

    /** Every document's address in stock's order, the one with none last. */
    private static List<String> expectedOrder(boolean desc) {
        List<String[]> docs = new ArrayList<>();
        for (int i = 0; i < ADDRESSES.size(); i++) {
            docs.add(new String[] { Integer.toString(i), ADDRESSES.get(i) });
        }
        Comparator<String[]> order = Comparator.comparing((String[] d) -> encoded(d[1]));
        if (desc) {
            order = order.reversed();
        }
        // Equal addresses written apart tie; the id breaks the tie here and in the request.
        docs.sort(order.thenComparing(d -> d[0]));
        List<String> out = new ArrayList<>();
        for (String[] d : docs) {
            out.add(d[0] + "=" + shown(d[1]));
        }
        out.add(NONE_ID + "=null");
        return out;
    }

    private static List<String> sorted(SearchSourceBuilder source) {
        InfinoSearch.Plan plan = plan(TABLE, source);
        InfinoSearch.SortSpec sort = plan.sorts().get(0);
        assertTrue(sort.ip());
        List<String> out = new ArrayList<>();
        for (Map<String, Object> row : InfinoNative.sqlRows(InfinoSearch.hitStatement(plan, ADDRESSES.size() + 10, false))) {
            out.add(row.get(InfinoQueryTranslator.OSID) + "=" + sort.orStandIn(row.get(sort.name())));
        }
        return out;
    }

    private static SearchSourceBuilder sortedBy(SortOrder order) {
        return new SearchSourceBuilder().sort(new FieldSortBuilder("addr").order(order)).sort(new FieldSortBuilder("_id").order(SortOrder.ASC));
    }

    /**
     * A sort on the field orders the documents by address, each hit carrying
     * the address as stock prints it: the issue's 1.0.6.192 before
     * 1.0.61.206 before 1.0.179.100, every IPv4 address before 2001:db8::.
     */
    public void testASortOrdersAddressesAsNumbers() {
        assertEquals(expectedOrder(false), sorted(sortedBy(SortOrder.ASC)));
        assertEquals(expectedOrder(true), sorted(sortedBy(SortOrder.DESC)));
    }

    /** A page after a hit's sort values continues in address order, from the address as the hit printed it. */
    public void testSearchAfterContinuesInAddressOrder() {
        List<String> all = expectedOrder(false);
        for (int at : new int[] { 0, 3, 12, 20, all.size() - 3 }) {
            String[] last = all.get(at).split("=", 2);
            List<String> rest = sorted(sortedBy(SortOrder.ASC).searchAfter(new Object[] { last[1], last[0] }));
            assertEquals("after " + all.get(at), all.subList(at + 1, all.size()), rest);
        }
    }

    /**
     * A sort on an ip field some documents give several values is refused:
     * such a document sorts by its least or greatest address, which the
     * engine's least or greatest of the list, by text, is not.
     */
    public void testASortOverSeveralAddressesIsRefused() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> plan(SEVERAL_TABLE, sortedBy(SortOrder.ASC)));
        assertEquals("[infino] sort on [addr], an ip field some documents give several values, is not served", e.getMessage());
    }

    /**
     * A missing value on an ip sort is refused: stock compares its text with
     * the addresses' 16-byte form, so it does not stand in as the address it
     * names. {@code _first} and {@code _last} are served.
     */
    public void testAMissingAddressOnASortIsRefused() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> plan(TABLE, new SearchSourceBuilder().sort(new FieldSortBuilder("addr").missing("1.2.3.4")))
        );
        assertEquals("[infino] sort missing value [1.2.3.4] on [addr], an ip field, is not served", e.getMessage());
        List<String> first = sorted(new SearchSourceBuilder().sort(new FieldSortBuilder("addr").missing("_first")).sort(new FieldSortBuilder("_id")));
        assertEquals(NONE_ID + "=null", first.get(0));
        assertEquals(expectedOrder(false).subList(0, expectedOrder(false).size() - 1), first.subList(1, first.size()));
    }
}
