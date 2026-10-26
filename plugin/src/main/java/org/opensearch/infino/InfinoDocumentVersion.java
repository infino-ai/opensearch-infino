/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.index.VersionType;
import org.opensearch.index.seqno.SequenceNumbers;

/**
 * What the table reports as a document's version, sequence number and
 * primary term. The table keeps none of them, so every document reports
 * the same constants, and they never move. Clients that read them back as
 * a condition on a later write (Dashboards saves every object it has read
 * that way, the reindex family deletes and updates what it scrolled) send
 * exactly these values, which the table serves as it serves an
 * unconditioned write: it detects no conflict, the last write wins. A
 * condition on any other value cannot match and is refused, as is one on
 * an external version.
 */
final class InfinoDocumentVersion {
    private InfinoDocumentVersion() {}

    /** The version every document reports. */
    static final long VERSION = 1L;
    /** The sequence number every document reports. */
    static final long SEQ_NO = 0L;
    /** The primary term every document reports. */
    static final long PRIMARY_TERM = 1L;

    /** Why a write's condition cannot be served, or null when it is unconditioned or on the values the table reports. */
    static String conditioned(DocWriteRequest<?> item) {
        boolean seqNoGiven = item.ifSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO || item.ifPrimaryTerm() != SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
        if (seqNoGiven && (item.ifSeqNo() != SEQ_NO || item.ifPrimaryTerm() != PRIMARY_TERM)) {
            return "if_seq_no " + item.ifSeqNo() + " / if_primary_term " + item.ifPrimaryTerm() + " cannot match: the table reports every document at " + SEQ_NO + " / "
                + PRIMARY_TERM + " and keeps no sequence numbers";
        }
        // An op_type create carries the "match deleted" marker as its version; that is the create, not a condition.
        boolean unversioned = item.version() == Versions.MATCH_ANY || item.version() == Versions.MATCH_DELETED || item.version() == VERSION;
        if (!unversioned || item.versionType() != VersionType.INTERNAL) {
            return "version " + item.version() + " (" + item.versionType() + ") cannot match: the table reports every document at version " + VERSION
                + " and keeps no per-document version";
        }
        return null;
    }
}
