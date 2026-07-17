package ch.admin.bit.jeap.messageexchange.domain.database;

import ch.admin.bit.jeap.messageexchange.domain.InboundMessage;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.ScanStatus;

import java.util.Optional;
import java.util.UUID;

public interface InboundMessageRepository {

    Optional<InboundMessage> findByBpIdAndMessageId(String bpId, UUID messageId);

    /**
     * Finds the newest inbound message row with the given messageId - greatest createdAt, ties broken by
     * sequenceId. Every store and re-store of a message refreshes the row's createdAt, so the newest row is
     * the one owning the current S3 object.
     */
    Optional<InboundMessage> findLatestByMessageId(UUID messageId);

    /**
     * Atomically sets the scan status on the newest row (see {@link #findLatestByMessageId}) for the given
     * messageId and returns the pre-update state of that row, i.e. {@link InboundMessage#getScanStatus()} of the
     * returned message is the <em>previous</em> scan status while all other fields reflect the current row state.
     *
     * @return the pre-update row state, or empty if no row exists for the messageId
     */
    Optional<InboundMessage> updateScanStatusReturningPreviousState(UUID messageId, ScanStatus newScanStatus);

    /**
     * Sets the scan status on the newest row (see {@link #findLatestByMessageId}) for the given messageId, but
     * only while that row's scan status is still pending (NULL or SCAN_PENDING). Used by the legacy tag fallback
     * to heal scan results that were processed by MES &lt; 11.0.0 instances during a rolling deployment.
     * LEGACY-TAG-FALLBACK: remove with the contract story (JEAP-7252).
     *
     * @return true if a pending row was updated, false if the row is absent or already carries a terminal status
     */
    boolean updateScanStatusIfPending(UUID messageId, ScanStatus newScanStatus);

    /**
     * Inserts the inbound message or, if a row with the same (bpId, messageId) already exists, updates its
     * messageType, partnerTopic, partnerExternalReference, contentType, scanStatus, contentLength and createdAt.
     * Updating createdAt keeps the housekeeping retention aligned with the S3 object lifetime when a message is
     * stored again (the object's lifecycle expiration restarts with the new upload).
     */
    void upsertScanStatusAndMetadata(InboundMessage inboundMessage);

    /**
     * Like {@link #upsertScanStatusAndMetadata(InboundMessage)}, but never downgrades a terminal scan status
     * (see {@link ScanStatus#isTerminal()}): if the existing row already carries a scan verdict, that verdict
     * is kept and only the metadata is updated. Used when storing a new message, where the scan result of the
     * just-stored object may have been processed (and backfilled by the legacy tag fallback) before this
     * upsert. Re-storing an existing message must use {@link #upsertScanStatusAndMetadata(InboundMessage)}
     * instead: its replaced content is scanned anew, so a previous verdict must be reset.
     */
    void upsertScanStatusAndMetadataKeepingTerminalStatus(InboundMessage inboundMessage);

    /**
     * Deletes expired messages older than the given amount of days. Deletes at most the given limit amount of messages.
     * Creates a new transaction for the call to avoid large delete
     *
     * @return true if messages were deleted, false otherwise
     */
    boolean deleteExpiredMessages(int olderThanDays, int limit);
}
