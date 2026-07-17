package ch.admin.bit.jeap.messageexchange.domain.legacy;

import ch.admin.bit.jeap.messageexchange.domain.InboundMessage;
import ch.admin.bit.jeap.messageexchange.domain.database.InboundMessageRepository;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.ScanStatus;
import ch.admin.bit.jeap.messageexchange.domain.objectstore.BucketType;
import ch.admin.bit.jeap.messageexchange.domain.objectstore.ObjectStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.springframework.stereotype.Component;

/**
 * Read-only S3 tag fallback for messages stored by MES &lt; 11.0.0, whose metadata and scan status live in
 * S3 object tags instead of the database. The fallback itself never writes S3 tags; the only tag write after
 * object creation is the transitional scanStatus tag update for 10.x instances (see MessageExchangeService),
 * gated by the legacy tag compatibility property - the database stays authoritative either way. The database
 * row is backfilled from the tags so that all subsequent reads are served from the database.
 * LEGACY-TAG-FALLBACK: remove with the contract story (JEAP-7252).
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class LegacyS3TagFallbackService {

    private final ObjectStore objectStore;
    private final InboundMessageRepository inboundMessageRepository;
    private final LegacyS3ObjectTagsParser tagsParser;

    /**
     * Handles a malware scan result for a message without scan metadata in the database: reads the legacy
     * metadata tags from the S3 object, persists the new scan status (plus the metadata from the tags) in the
     * database, and returns the tag data needed to publish the message received event.
     *
     * @return the parsed legacy tags (whose {@link S3ObjectTags#scanStatus()} is the <em>previous</em> scan
     * status) and the object's content type, or empty if the scan result was dropped because the object
     * carries no metadata tags at all (upload crashed between storing the object and the database record)
     */
    public Optional<LegacyScanResultData> handleScanResult(UUID messageId, String bucketName, ScanStatus newScanStatus) {
        String objectKey = messageId.toString();
        log.info("No scan metadata found in the database for message {} - falling back to the legacy S3 object tags", messageId);
        Map<String, String> tagMap = objectStore.getObjectTags(BucketType.PARTNER, objectKey);
        if (tagsParser.getTagsfromMap(tagMap).hasNoMetadataTags()) {
            // with the legacy tag compatibility disabled, an upload that crashed between storing the object
            // and the database record leaves an object without any metadata. The message is undeliverable
            // (the delivery gate is fail-closed), so the scan result is dropped instead of failing forever.
            log.warn("Dropping the malware scan result for message {} - the object carries no metadata tags and " +
                    "no database record exists (upload crashed between storing the object and the database record)", messageId);
            return Optional.empty();
        }
        S3ObjectTags tags = tagsParser.getTagsfromMapAndValidate(bucketName, objectKey, tagMap);
        ObjectHead objectHead = objectStore.getObjectHead(BucketType.PARTNER, objectKey)
                .orElseThrow(() -> new IllegalStateException("S3 object not found for legacy message " + objectKey + " in bucket " + bucketName));

        inboundMessageRepository.upsertScanStatusAndMetadata(InboundMessage.builder()
                .messageId(messageId)
                .bpId(tags.bpId())
                .contentLength(objectHead.contentLength())
                // saveTimeInMillis is the original save time, keeping the housekeeping retention correct.
                // If absent, now() only extends the row's retention, which is the safe direction
                .overrideCreatedAt(toLocalDateTime(tags.saveTimeInMillis()))
                .messageType(tags.messageType())
                .partnerTopic(tags.partnerTopic())
                .partnerExternalReference(tags.partnerExternalReference())
                .contentType(objectHead.contentType())
                .scanStatus(newScanStatus)
                .build());

        return Optional.of(new LegacyScanResultData(tags, objectHead.contentType()));
    }

    private static LocalDateTime toLocalDateTime(Long epochMillis) {
        return epochMillis == null ? null :
                Instant.ofEpochMilli(epochMillis).atZone(ZoneId.systemDefault()).toLocalDateTime();
    }

    /**
     * Heals a still-pending scan status from the scanStatus tag: a scan result processed by an MES &lt; 11.0.0
     * instance during a rolling deployment updated the S3 object tags but not the database. A terminal verdict
     * in the scanStatus tag is adopted into the database (only while the row is still pending) and returned.
     * Read-only on S3.
     *
     * @return the healed terminal scan status, or the row's terminal status if the row concurrently left the
     * pending state (the database is authoritative, not the tag), or empty if the scan is genuinely still pending
     */
    public Optional<ScanStatus> healPendingScanStatus(UUID messageId) {
        ScanStatus tagScanStatus;
        try {
            Map<String, String> tagMap = objectStore.getObjectTags(BucketType.PARTNER, messageId.toString());
            tagScanStatus = tagsParser.getTagsfromMap(tagMap).scanStatus();
        } catch (Exception e) {
            // best effort: e.g. the S3 object may already have expired - the message then stays pending (fail-closed)
            log.warn("Could not read the S3 object tags to heal the pending scan status of message {}", messageId, e);
            return Optional.empty();
        }
        if (tagScanStatus == null || !tagScanStatus.isTerminal()) {
            return Optional.empty();
        }
        boolean rowHealed = inboundMessageRepository.updateScanStatusIfPending(messageId, tagScanStatus);
        if (rowHealed) {
            log.info("Healed the pending scan status of message {} to {} from the scanStatus tag (scan result was " +
                    "processed by an MES < 11.0.0 instance)", messageId, tagScanStatus);
            return Optional.of(tagScanStatus);
        }
        // the row left the pending state concurrently (e.g. a scan result was processed in the meantime): the
        // database is authoritative, so delivery is gated on the re-read row status, never on the tag verdict
        Optional<ScanStatus> rowScanStatus = inboundMessageRepository.findLatestByMessageId(messageId)
                .map(InboundMessage::getScanStatus)
                .filter(ScanStatus::isTerminal);
        log.info("Did not heal the scan status of message {} from the scanStatus tag ({}) - the row was no longer " +
                "pending and carries the authoritative status {}", messageId, tagScanStatus, rowScanStatus.orElse(null));
        return rowScanStatus;
    }

    public record LegacyScanResultData(S3ObjectTags previousTags, String contentType) {
    }

}
