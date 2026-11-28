/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.lucene.document.InetAddressPoint;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.common.network.NetworkAddress;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.HexFormat;

/**
 * An {@code ip} field's addresses, compared as stock compares them. Stock
 * reads every spelling of an address ({@code 2001:db8::2},
 * {@code 2001:DB8:0:0:0:0:0:2}, {@code 2001:0db8::0002}; an IPv4 address or
 * its IPv4-mapped form {@code ::ffff:a.b.c.d}) as the same 16 bytes, so a
 * term in any spelling finds the document written in another.
 *
 * <p>The table keeps each address in the one spelling stock prints it in
 * ({@link #canonical}), so a term or terms compares that spelling. A CIDR
 * block compares a key the statement computes from the text ({@link #key}):
 * the 16 bytes as 32 lowercase hex digits, which compare as text in the
 * order the bytes compare as numbers, so a block is a range of keys.
 */
final class InfinoIp {
    private InfinoIp() {}

    /** What an IPv4 address's key starts with: the IPv4-mapped prefix {@code ::ffff:0:0/96}. */
    private static final String MAPPED_PREFIX = "00000000000000000000ffff";

    /** The groups of an IPv6 address written in full. */
    private static final int GROUPS = 8;

    /** The bits of an IPv4 address's 16-byte form before its own 32: its prefix length there is 96 more. */
    private static final int MAPPED_BITS = 96;

    private static final HexFormat HEX = HexFormat.of();

    /**
     * The address {@code value} (text, as a query writes it) in the spelling
     * stock prints it in and the table keeps it in: an IPv4 address,
     * IPv4-mapped ones included, in dotted decimal, any other IPv6 address
     * in lowercase hex with its longest run of zero groups compressed. Null
     * for text that is not an address.
     */
    static String canonical(Object value) {
        InetAddress address = address(value);
        return address == null ? null : NetworkAddress.format(address);
    }

    /** Whether the texts {@code a} and {@code b} spell one address; false when either is no address. */
    static boolean sameAddress(Object a, Object b) {
        InetAddress x = address(a);
        InetAddress y = address(b);
        return x != null && y != null && Arrays.equals(InetAddressPoint.encode(x), InetAddressPoint.encode(y));
    }

    /**
     * Whether the address text {@code address} lies in the CIDR block
     * {@code block}, compared by their 16-byte forms as stock compares them,
     * so an IPv4 address is in an IPv6 block over the IPv4-mapped range;
     * false for text that is not an address.
     */
    static boolean inBlock(Object address, String block) {
        InetAddress a = address(address);
        if (a == null) {
            return false;
        }
        String key = HEX.formatHex(InetAddressPoint.encode(a));
        String[] range = blockKeys(block);
        return key.compareTo(range[0]) >= 0 && key.compareTo(range[1]) <= 0;
    }

    /**
     * The least and greatest keys of the addresses the CIDR block
     * {@code block} covers, its host bits ignored, as stock reads the block.
     * A malformed block is refused with stock's message.
     */
    static String[] blockKeys(String block) {
        Tuple<InetAddress, Integer> parsed = InetAddresses.parseCidr(block);
        byte[] low = InetAddressPoint.encode(parsed.v1());
        byte[] high = low.clone();
        int bits = parsed.v2() + (parsed.v1() instanceof Inet4Address ? MAPPED_BITS : 0);
        for (int i = 0; i < low.length; i++, bits -= Byte.SIZE) {
            int keep = bits >= Byte.SIZE ? 0xff : bits <= 0 ? 0 : 0xff << (Byte.SIZE - bits) & 0xff;
            low[i] = (byte) (low[i] & keep);
            high[i] = (byte) (high[i] | ~keep);
        }
        return new String[] { HEX.formatHex(low), HEX.formatHex(high) };
    }

    /**
     * The key of the address text {@code v} (an SQL expression), as an SQL
     * expression: 32 hex digits, or null where {@code v} is null. The text is
     * any form the table holds an address in, so a row written before the
     * table kept one spelling is keyed too: IPv4 dotted decimal, or IPv6 in
     * any case, with or without leading zeros, {@code ::} or an IPv4 tail.
     */
    static String key(String v) {
        return "CASE WHEN strpos(" + v + ", ':') = 0 THEN " + v4Key(v) + " ELSE " + v6Key("lower(" + v + ")") + " END";
    }

    /** The key of IPv4 text {@code t}: the mapped prefix, then the address as 8 hex digits. */
    private static String v4Key(String t) {
        String number = "(" + octet(t, 1) + " * 16777216 + " + octet(t, 2) + " * 65536 + " + octet(t, 3) + " * 256 + " + octet(t, 4) + ")";
        return "('" + MAPPED_PREFIX + "' || lpad(to_hex(" + number + "), 8, '0'))";
    }

    /**
     * Octet {@code i} (from 1) of the dotted-decimal text {@code t}, as a
     * number. A try: the engine may evaluate a CASE arm over rows the arm
     * does not answer for, where the text has no such octet.
     */
    private static String octet(String t, int i) {
        return "TRY_CAST(split_part(" + t + ", '.', " + i + ") AS BIGINT)";
    }

    /**
     * The key of lowercase IPv6 text {@code a}, in four steps: an IPv4 tail
     * becomes two hex groups; a leading or trailing {@code ::} gets a zero
     * group beside it, so {@code ::} always sits between two groups; the
     * {@code ::} becomes as many zero groups as the address lacks; and each
     * of the eight groups is padded to four digits and the colons dropped.
     * Text is joined with {@code ||}, which keeps a null address null, where
     * {@code concat} would read it as empty text.
     */
    private static String v6Key(String a) {
        String tail = "regexp_replace(" + a + ", '^.*:', '')";
        String high = "(" + octet(tail, 1) + " * 256 + " + octet(tail, 2) + ")";
        String low = "(" + octet(tail, 3) + " * 256 + " + octet(tail, 4) + ")";
        String hexTail = "CASE WHEN strpos(" + a + ", '.') = 0 THEN " + a + " ELSE (regexp_replace(" + a + ", '[0-9.]+$', '') || to_hex(" + high
            + ") || ':' || to_hex(" + low + ")) END";
        String inner = "regexp_replace(regexp_replace(" + hexTail + ", '^::', '0::'), '::$', '::0')";
        String colons = "(length(" + inner + ") - length(replace(" + inner + ", ':', '')))";
        String full = "CASE WHEN strpos(" + inner + ", '::') = 0 THEN " + inner + " ELSE replace(" + inner + ", '::', (':' || repeat('0:', " + GROUPS
            + " - " + colons + "))) END";
        return "regexp_replace(regexp_replace((':' || " + full + "), ':', ':000', 'g'), ':[0-9a-f]*([0-9a-f]{4})', '\\1', 'g')";
    }

    /** The address {@code value} spells, as stock reads one in a query; null for text that is not one. */
    private static InetAddress address(Object value) {
        if (value == null) {
            return null;
        }
        try {
            return InetAddresses.forString(value.toString());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
