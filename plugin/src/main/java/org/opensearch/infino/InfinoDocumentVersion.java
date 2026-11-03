/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.seqno.SequenceNumbers;

/**
 * What the table reports as a document's version, sequence number and
 * primary term, and how a write conditioned on them is checked.
 *
 * <p>The plugin hands out sequence numbers itself: each write is given the
 * next number of its table when its batch is written, and the table stores
 * it beside the row. A write conditioned on {@code if_seq_no} /
 * {@code if_primary_term} (Dashboards saves every object it has read that
 * way, and alerting, index management and the job scheduler take their
 * locks that way) is checked against the stored number then, and refused
 * with the conflict stock reports when another write came between. The
 * primary term is always 1, since the table has no primary to fail over.
 * The table keeps no per-document version: every document reports version
 * 1, and a condition on any other version, or on an external one, is
 * refused.
 */
final class InfinoDocumentVersion {
    private InfinoDocumentVersion() {}

    /** The version every document reports. */
    static final long VERSION = 1L;
    /** The primary term every document reports. */
    static final long PRIMARY_TERM = 1L;
    /** The sequence number of a row written without one: loaded straight into the table, or written before the plugin kept them. */
    static final long UNTRACKED_SEQ_NO = 0L;
    /** The primary term stock reports for a document a conditional write finds missing. */
    private static final long NO_DOCUMENT_TERM = 0L;

    /** Why a write's version condition cannot be served, or null when it has none (sequence-number conditions are checked when it is written). */
    static String conditioned(DocWriteRequest<?> item) {
        // An op_type create carries the "match deleted" marker as its version; that is the create, not a condition.
        boolean unversioned = item.version() == Versions.MATCH_ANY || item.version() == Versions.MATCH_DELETED || item.version() == VERSION;
        if (!unversioned || item.versionType() != VersionType.INTERNAL) {
            return "version " + item.version() + " (" + item.versionType() + ") cannot match: the table reports every document at version " + VERSION
                + " and keeps no per-document version";
        }
        return null;
    }

    /** Whether a write is conditioned on a sequence number and primary term. */
    static boolean seqConditioned(DocWriteRequest<?> item) {
        return item.ifSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO || item.ifPrimaryTerm() != SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
    }

    /**
     * The conflict a write's sequence-number condition meets, or null when
     * it holds or there is none: {@code current} is the sequence number of
     * the document as it stands when the write is made, null when there is
     * no document.
     */
    static VersionConflictEngineException conflict(ShardId shardId, DocWriteRequest<?> item, Long current) {
        if (!seqConditioned(item)) {
            return null;
        }
        if (current == null) {
            return new VersionConflictEngineException(shardId, item.id(), item.ifSeqNo(), item.ifPrimaryTerm(), SequenceNumbers.UNASSIGNED_SEQ_NO, NO_DOCUMENT_TERM);
        }
        if (current != item.ifSeqNo() || item.ifPrimaryTerm() != PRIMARY_TERM) {
            return new VersionConflictEngineException(shardId, item.id(), item.ifSeqNo(), item.ifPrimaryTerm(), current, PRIMARY_TERM);
        }
        return null;
    }
}
