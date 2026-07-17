package ch.admin.bit.jeap.messageexchange.domain;

import ch.admin.bit.jeap.messageexchange.domain.database.InboundMessageRepository;
import ch.admin.bit.jeap.messageexchange.domain.database.MessageRepository;
import ch.admin.bit.jeap.messageexchange.domain.dto.MessageSearchResultDto;
import ch.admin.bit.jeap.messageexchange.domain.dto.MessageSearchResultWithContentDto;
import ch.admin.bit.jeap.messageexchange.domain.exception.MalwareScanFailedOrBlockedException;
import ch.admin.bit.jeap.messageexchange.domain.exception.MismatchedContentException;
import ch.admin.bit.jeap.messageexchange.domain.exception.MismatchedContentTypeException;
import ch.admin.bit.jeap.messageexchange.domain.legacy.LegacyS3ObjectTagsFactory;
import ch.admin.bit.jeap.messageexchange.domain.legacy.LegacyS3ObjectTagsParser;
import ch.admin.bit.jeap.messageexchange.domain.legacy.LegacyS3TagFallbackService;
import ch.admin.bit.jeap.messageexchange.domain.legacy.LegacyS3TagFallbackService.LegacyScanResultData;
import ch.admin.bit.jeap.messageexchange.domain.legacy.S3ObjectTags;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.MalwareScanProperties;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.PublishedScanStatus;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.S3ObjectMalwareScanResultInfo;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.ScanStatus;
import ch.admin.bit.jeap.messageexchange.domain.messaging.EventPublisher;
import ch.admin.bit.jeap.messageexchange.domain.metrics.MetricsService;
import ch.admin.bit.jeap.messageexchange.domain.objectstore.BucketType;
import ch.admin.bit.jeap.messageexchange.domain.objectstore.ObjectStore;
import ch.admin.bit.jeap.messageexchange.domain.sent.MessageSentProperties;
import ch.admin.bit.jeap.messageexchange.domain.xml.XmlValidatingOutputStream;
import ch.admin.bit.jeap.messageexchange.malware.api.MalwareScanResult;
import ch.admin.bit.jeap.messageexchange.malware.api.MalwareScanTrigger;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class MessageExchangeService {

    private final ObjectStore objectStore;
    private final EventPublisher eventPublisher;
    private final MessageRepository messageRepository;
    private final InboundMessageRepository inboundMessageRepository;
    private final MalwareScanProperties malwareScanProperties;
    private final MessageSentProperties messageSentProperties;
    private final MetricsService metricsService;
    private final LegacyS3ObjectTagsParser legacyTagsParser;
    private final LegacyS3TagFallbackService legacyTagFallbackService;
    private final LegacyS3ObjectTagsFactory legacyTagsFactory;
    private final Optional<MalwareScanTrigger> malwareScanTriggerOptional;

    public Optional<MessageContent> getMessageContentFromInternalApplication(String bpId, UUID messageId) {
        try {
            return getMessageContentFromInternalApplication(bpId, messageId, null);
        } catch (MismatchedContentTypeException e) {
            // This should never happen as we do not check for content type here
            throw new IllegalStateException(e);
        }
    }

    public Optional<MessageContent> getMessageContentFromInternalApplication(String bpId, UUID messageId, String requestedContentType) throws MismatchedContentTypeException {
        String objectKey = createInternalMessageObjectKey(bpId, messageId);
        log.trace("Retrieve payload for internal message with messageId {} from key {}", messageId, objectKey);

        if (requestedContentType != null) {
            Optional<String> contentTypeOptional = objectStore.getContentType(BucketType.INTERNAL, objectKey);
            if (contentTypeOptional.isEmpty()) {
                return Optional.empty();
            }
            validateContentType(messageId, contentTypeOptional.get(), requestedContentType);
        }

        return objectStore.loadMessage(BucketType.INTERNAL, objectKey);
    }

    public Optional<MessageSearchResultWithContentDto> getMessageFromInternalApplication(String bpId, UUID messageId, String requestedContentType) throws MismatchedContentTypeException {
        Optional<Message> messageOptional = messageRepository.findByBpIdAndMessageId(bpId, messageId);
        if (messageOptional.isEmpty()) {
            return Optional.empty();
        }
        Message message = messageOptional.get();
        String objectKey = createInternalMessageObjectKey(bpId, messageId);
        log.trace("Retrieve payload for internal message with messageId {} from key {}", messageId, objectKey);

        if (requestedContentType != null) {
            Optional<String> contentTypeOptional = objectStore.getContentType(BucketType.INTERNAL, objectKey);
            if (contentTypeOptional.isEmpty()) {
                return Optional.empty();
            }
            validateContentType(messageId, contentTypeOptional.get(), requestedContentType);
        }

        Optional<MessageContent> messageContent = objectStore.loadMessage(BucketType.INTERNAL, objectKey);

        return messageContent.map(content -> new MessageSearchResultWithContentDto(
                message.getMessageId(),
                content,
                message.getPartnerTopic(),
                message.getPartnerExternalReference(),
                message.getMetadata()));
    }


    public Optional<MessageSearchResultWithContentDto> getNextMessageFromInternalApplication(UUID lastMessageId, String bpId, String partnerTopic, String topicName, String partnerExternalReference) {
        Optional<Message> nextMessageId = messageRepository.getNextMessage(lastMessageId, bpId, partnerTopic, topicName, partnerExternalReference);
        if (nextMessageId.isPresent()) {
            Message message = nextMessageId.get();
            Optional<MessageContent> messageFromInternalApplication = getMessageContentFromInternalApplication(bpId, message.getMessageId());
            if (messageFromInternalApplication.isPresent()) {
                log.trace("Found new message with messageId {} for bpId {} after lastMessageId {}", message.getMessageId(), bpId, lastMessageId);

                return messageFromInternalApplication.map(content -> new MessageSearchResultWithContentDto(
                        message.getMessageId(),
                        content,
                        message.getPartnerTopic(),
                        message.getPartnerExternalReference(),
                        message.getMetadata()));
            }
        }
        log.trace("No new message found for bpId {} after lastMessageId {}", bpId, lastMessageId);
        return Optional.empty();
    }


    public Optional<MessageContent> getMessageFromPartner(UUID messageId) {
        try {
            return getMessageFromPartner(messageId, null);
        } catch (MismatchedContentTypeException e) {
            // This should never happen as we do not check for content type here
            throw new IllegalStateException(e);
        }
    }

    public Optional<MessageContent> getMessageFromPartner(UUID messageId, String requestedContentType) throws MismatchedContentTypeException {
        log.trace("Retrieve payload for partner message with messageId {} and requestedContentType {}", messageId, requestedContentType);
        Optional<InboundMessage> inboundMessageOptional = inboundMessageRepository.findLatestByMessageId(messageId);
        if (inboundMessageOptional.isPresent() && inboundMessageOptional.get().getScanStatus() != null) {
            return getMessageFromPartnerWithDatabaseScanStatus(messageId, requestedContentType, inboundMessageOptional.get());
        }
        // LEGACY-TAG-FALLBACK: message stored by MES < 11.0.0, its scan status is only available as S3 object
        // tag - remove with the contract story (JEAP-7252)
        return getMessageFromPartnerWithLegacyTags(messageId, requestedContentType);
    }

    private Optional<MessageContent> getMessageFromPartnerWithDatabaseScanStatus(UUID messageId, String requestedContentType, InboundMessage inboundMessage) throws MismatchedContentTypeException {
        if (requestedContentType != null) {
            // rows updated by a scan result before the metadata backfill may lack the content type in the database
            String contentType = inboundMessage.getContentType() != null ? inboundMessage.getContentType() :
                    objectStore.getContentType(BucketType.PARTNER, messageId.toString()).orElse(null);
            if (contentType == null) {
                return Optional.empty();
            }
            validateContentType(messageId, contentType, requestedContentType);
        }
        ScanStatus scanStatus = inboundMessage.getScanStatus();
        if (scanStatus == ScanStatus.SCAN_PENDING) {
            // LEGACY-TAG-FALLBACK: a scan result processed by an MES < 11.0.0 instance during a rolling deployment
            // updated the S3 tags but not the database - heal the verdict from the tags; remove with the contract
            // story (JEAP-7252)
            scanStatus = legacyTagFallbackService.healPendingScanStatus(messageId).orElse(scanStatus);
        }
        checkScanStatus(messageId, inboundMessage.getBpId(), scanStatus);
        return objectStore.loadMessage(BucketType.PARTNER, messageId.toString());
    }

    /**
     * LEGACY-TAG-FALLBACK: remove with the contract story (JEAP-7252).
     */
    private Optional<MessageContent> getMessageFromPartnerWithLegacyTags(UUID messageId, String requestedContentType) throws MismatchedContentTypeException {
        if (requestedContentType != null) {
            Optional<String> contentTypeOptional = objectStore.getContentType(BucketType.PARTNER, messageId.toString());
            if (contentTypeOptional.isEmpty()) {
                return Optional.empty();
            }
            validateContentType(messageId, contentTypeOptional.get(), requestedContentType);
        }
        Optional<MessageContent> messageInfoWithTagsOptional = objectStore.loadMessageWithTags(BucketType.PARTNER, messageId.toString());
        if (messageInfoWithTagsOptional.isEmpty()) {
            return Optional.empty();
        }
        MessageContent messageContent = messageInfoWithTagsOptional.get();
        try {
            S3ObjectTags s3ObjectTags = legacyTagsParser.getTagsfromMap(messageContent.tags());
            if (malwareScanProperties.isEnabled() && s3ObjectTags.hasNoMetadataTags()) {
                // An object without any metadata tags was stored by MES >= 11.0.0, but its database record is
                // unexpectedly missing, e.g. because the upload crashed between storing the object and the
                // database record. Its scan is still pending - it must not be delivered based on the missing
                // tag scan status (fail-closed).
                log.warn("Message {} has no metadata tags and no database record - not delivering because its malware scan is still pending", messageId);
                throw MalwareScanFailedOrBlockedException.malwareScanFailedOrBlockedException(messageId, ScanStatus.SCAN_PENDING);
            }
            checkScanStatus(messageId, s3ObjectTags.bpId(), s3ObjectTags.scanStatus());
        } catch (RuntimeException e) {
            // the object stream is already open - close it before rethrowing so the pooled connection is released
            closeQuietly(messageContent.inputStream());
            throw e;
        }
        return messageInfoWithTagsOptional;
    }

    private static void closeQuietly(InputStream inputStream) {
        try {
            inputStream.close();
        } catch (IOException e) {
            log.debug("Failed to close message content input stream", e);
        }
    }

    private static void validateContentType(UUID messageId, String contentType, String requestedContentType) throws MismatchedContentTypeException {
        if (!requestedContentType.equalsIgnoreCase(contentType)) {
            log.warn("Message {} has content type {} but requestedContentType is {}", messageId, contentType, requestedContentType);
            throw MismatchedContentTypeException.requestedContentTypeIncorrect(messageId, contentType, requestedContentType);
        }
    }

    /**
     * Legacy save with XML validation for partner messages.
     * This method is kept for backward compatibility.
     */
    public void saveNewMessageFromPartner(UUID messageId, String bpId, String messageType, MessageContent rawMessageContent) throws IOException, MismatchedContentException {
        try (InputStream inputStream = XmlValidatingOutputStream.wrapInputStreamWithXmlValidation(messageId, bpId, rawMessageContent)) {
            saveNewMessageFromPartner(inputStream, messageId, bpId, messageType, null, null, rawMessageContent, MediaType.APPLICATION_XML_VALUE);
        }
    }

    public void saveNewMessageFromPartner(UUID messageId, String bpId, String messageType, String partnerTopic, String partnerExternalReference, MessageContent rawMessageContent, String contentType) throws IOException, MismatchedContentException {
        saveNewMessageFromPartner(rawMessageContent.inputStream(), messageId, bpId, messageType, partnerTopic, partnerExternalReference, rawMessageContent, contentType);
    }

    @SuppressWarnings("java:S107")
    private void saveNewMessageFromPartner(InputStream inputStream, UUID messageId, String bpId, String messageType, String partnerTopic, String partnerExternalReference, MessageContent rawMessageContent, String contentType) throws IOException, MismatchedContentException {
        Optional<InboundMessage> inboundMessageOptional = inboundMessageRepository.findByBpIdAndMessageId(bpId, messageId);
        if (inboundMessageOptional.isEmpty()) {
            storeMessageInS3AndDatabase(inputStream, messageId, bpId, messageType, partnerTopic, partnerExternalReference, rawMessageContent, contentType, false);
        } else {
            handleDuplicatePartnerMessage(inputStream, inboundMessageOptional.get(), messageId, bpId, rawMessageContent, messageType, partnerTopic, partnerExternalReference, contentType);
        }
    }

    @SuppressWarnings("java:S107")
    private void storeMessageInS3AndDatabase(InputStream inputStream, UUID messageId, String bpId, String messageType, String partnerTopic, String partnerExternalReference, MessageContent rawMessageContent, String contentType, boolean reStoredExistingMessage) throws IOException {
        ScanStatus initialScanStatus = malwareScanProperties.isEnabled() ? ScanStatus.SCAN_PENDING : ScanStatus.NOT_SCANNED;
        InboundMessage inboundMessage = newInboundMessage(messageId, bpId, messageType, partnerTopic, partnerExternalReference, rawMessageContent.contentLength(), contentType, initialScanStatus);

        if (reStoredExistingMessage) {
            // The replaced object content is scanned anew: the existing row (including a previous terminal
            // scan status) is reset BEFORE the object is stored, so a scan result for the new object can never
            // be reverted afterwards. Should the put below fail, the row is pending without an object -
            // fail-closed and safe to retry (the row's content length is unchanged, see the duplicate check).
            inboundMessageRepository.upsertScanStatusAndMetadata(inboundMessage);
        }

        // The metadata tags for MES < 11.0.0 instances are written only atomically at object creation, which
        // cannot race with the GuardDuty object tagging. LEGACY-TAG-FALLBACK: remove with the contract story (JEAP-7252)
        Map<String, String> legacyTags = legacyTagsFactory.createUploadTags(bpId, messageType, partnerTopic, partnerExternalReference, initialScanStatus);
        objectStore.storeMessage(BucketType.PARTNER, messageId.toString(), new MessageContent(inputStream, rawMessageContent.contentLength(), legacyTags), contentType);

        if (!reStoredExistingMessage) {
            // A new message writes its row only after the object exists: a row left behind by a failed upload
            // would break partner retries with corrected content (the duplicate check rejects a changed content
            // length). Upsert instead of insert as a safety net for rows the duplicate check could not see,
            // e.g. from a concurrent upload of the same message. The scan result of the just-stored object may
            // already have been processed (and backfilled by the legacy tag fallback) before this upsert - its
            // terminal verdict must not be reverted to pending.
            inboundMessageRepository.upsertScanStatusAndMetadataKeepingTerminalStatus(inboundMessage);
        }

        // the database record exists before the scan is triggered: the scan result handler reads it
        publishEventOrTriggerScan(messageId, bpId, messageType, partnerTopic, partnerExternalReference, contentType);
    }

    @SuppressWarnings("java:S107")
    private InboundMessage newInboundMessage(UUID messageId, String bpId, String messageType, String partnerTopic, String partnerExternalReference, int contentLength, String contentType, ScanStatus initialScanStatus) {
        return InboundMessage.builder()
                .messageId(messageId)
                .bpId(bpId)
                .contentLength(contentLength)
                .messageType(messageType)
                .partnerTopic(StringUtils.hasText(partnerTopic) ? partnerTopic : null)
                .partnerExternalReference(StringUtils.hasText(partnerExternalReference) ? partnerExternalReference : null)
                .contentType(contentType)
                .scanStatus(initialScanStatus)
                .build();
    }

    private void publishEventOrTriggerScan(UUID messageId, String bpId, String messageType, String partnerTopic, String partnerExternalReference, String contentType) {
        if (!malwareScanProperties.isEnabled()) {
            // Publish ReceivedEvent and events from configured listeners
            eventPublisher.publishMessageReceivedEvent(messageId, bpId, messageType, partnerTopic, partnerExternalReference, PublishedScanStatus.NOT_SCANNED, contentType);
        } else {
            malwareScanTriggerOptional.ifPresent(malwareScanTrigger -> malwareScanTrigger.triggerScan(messageId.toString(), objectStore.getBucketName(BucketType.PARTNER)));
        }
    }

    @SuppressWarnings("java:S107")
    private void handleDuplicatePartnerMessage(InputStream inputStream, InboundMessage inboundMessage, UUID messageId, String bpId, MessageContent rawMessageContent,
                                               String messageType, String partnerTopic, String partnerExternalReference, String contentType) throws MismatchedContentException, IOException {

        metricsService.duplicatedMessageIdReceived(bpId);

        if (rawMessageContent.contentLength() != inboundMessage.getContentLength()) {
            log.warn("Duplicated messageIds found: message {} for bpId {} has different content length", messageId, bpId);
            throw MismatchedContentException.contentLengthDoesNotMatch(messageId);
        }

        Optional<MessageContent> existingMessageOptional = objectStore.loadMessage(BucketType.PARTNER, messageId.toString());
        if (existingMessageOptional.isPresent()) {
            String checksumExistingMessage = computeChecksum(existingMessageOptional.get().inputStream());
            String checksumIncomingMessage = computeChecksum(inputStream);
            log.debug("Duplicated messageIds found: checksum existing: '{}' / checksum incoming: '{}'", checksumExistingMessage, checksumIncomingMessage);

            if (!checksumIncomingMessage.equals(checksumExistingMessage)) {
                log.warn("Duplicated messageIds found: message {} for bpId {} has different checksum", messageId, bpId);
                throw MismatchedContentException.checksumDoesNotMatch(messageId);
            }

            log.info("Duplicated messageIds found: ignoring message {} for bpId {} with identical content", messageId, bpId);

        } else {
            log.info("Duplicated messageIds found: message with id {} for bpId {} not found in S3. Saving it again...", messageId, bpId);
            storeMessageInS3AndDatabase(inputStream, messageId, bpId, messageType, partnerTopic, partnerExternalReference, rawMessageContent, contentType, true);
        }
    }

    private String computeChecksum(InputStream inputStream) throws IOException {
        MessageDigest messageDigest;

        try {
            messageDigest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }

        try (DigestInputStream dis = new DigestInputStream(inputStream, messageDigest)) {
            byte[] buffer = new byte[8192];
            while (dis.read(buffer) != -1) {
                // just consume
            }
        }
        return toHex(messageDigest.digest());
    }

    private String toHex(byte[] hash) {
        StringBuilder hex = new StringBuilder();
        for (byte b : hash) {
            hex.append(String.format("%02x", b));
        }
        return hex.toString();
    }

    /**
     * Legacy save with XML validation for internal messages.
     * This method is kept for backward compatibility.
     */
    public void saveNewMessageFromInternalApplicationLegacy(Message message, MessageContent rawMessageContent) throws IOException {
        UUID messageId = message.getMessageId();
        String bpId = message.getBpId();
        try (InputStream inputStream = XmlValidatingOutputStream.wrapInputStreamWithXmlValidation(messageId, bpId, rawMessageContent)) {
            saveNewMessageFromInternalApplication(inputStream, messageId, bpId, message, rawMessageContent);
        }
    }

    public void saveNewMessageFromInternalApplication(Message message, MessageContent rawMessageContent) throws IOException {
        saveNewMessageFromInternalApplication(rawMessageContent.inputStream(), message.getMessageId(), message.getBpId(), message, rawMessageContent);
    }

    public void saveNewMessageFromInternalApplication(InputStream inputStream, UUID messageId, String bpId, Message message, MessageContent rawMessageContent) throws IOException {
        MessageContent messageInfo = new MessageContent(inputStream, rawMessageContent.contentLength());

        // Save to S3
        // This will validate the XML at the same time by copying output to the XmlValidatingOutputStream
        objectStore.storeMessage(BucketType.INTERNAL, createInternalMessageObjectKey(bpId, messageId), messageInfo, message.getContentType());

        // Store message in database
        messageRepository.save(message);

        if (messageSentProperties.isEnabled()) {
            eventPublisher.publishMessageSentEvent(message);
        }
    }

    public List<MessageSearchResultDto> getMessages(String bpId, String topicName, String groupId, UUID lastMessageId, String partnerTopic, String partnerExternalReference, int size) {
        return messageRepository.getMessages(bpId, topicName, groupId, lastMessageId, partnerTopic, partnerExternalReference, size);
    }

    public void onMalwareScanResult(S3ObjectMalwareScanResultInfo scanResultInfo) {
        long messageArrivalTimeInMillis = System.currentTimeMillis();
        validatePartnerBucketName(scanResultInfo.bucketName());

        ScanStatus newScanStatus = ScanStatus.fromScanResult(scanResultInfo.scanResult());
        if (newScanStatus == ScanStatus.SCAN_FAILED) {
            log.warn("Malware scan failed: {}", scanResultInfo);
        }
        UUID messageId = UUID.fromString(scanResultInfo.objectKey());

        // The terminal status is only committed once the event data is available: a row without metadata takes
        // the legacy tag fallback, which resolves the event data from S3 before persisting the status. Scan
        // result processing is swallow-and-acknowledge, so a failure while resolving must leave the row pending
        // (fail-closed like any lost result) instead of deliverable with the received event never published.
        Optional<InboundMessage> previousStateOptional = inboundMessageRepository.findLatestByMessageId(messageId)
                .filter(row -> row.getMessageType() != null)
                .flatMap(row -> inboundMessageRepository.updateScanStatusReturningPreviousState(messageId, newScanStatus));

        ScanStatus previousScanStatus;
        Long saveTimeInMillis;
        ReceivedMessage receivedMessage;
        if (previousStateOptional.isPresent()) {
            InboundMessage previousState = previousStateOptional.get();
            previousScanStatus = previousState.getScanStatus();
            saveTimeInMillis = previousState.getCreatedAtEpochMillis();
            receivedMessage = new ReceivedMessage(messageId, previousState.getBpId(), previousState.getMessageType(),
                    previousState.getPartnerTopic(), previousState.getPartnerExternalReference(), previousState.getContentType());
        } else {
            // LEGACY-TAG-FALLBACK: message stored by MES < 11.0.0, its metadata is only available as S3 object
            // tags - remove with the contract story (JEAP-7252)
            Optional<LegacyScanResultData> legacyDataOptional = legacyTagFallbackService.handleScanResult(messageId, scanResultInfo.bucketName(), newScanStatus);
            if (legacyDataOptional.isEmpty()) {
                // orphaned object without metadata tags and database record: the scan result was dropped
                // (already logged) and the message stays undeliverable (fail-closed)
                return;
            }
            LegacyScanResultData legacyData = legacyDataOptional.get();
            S3ObjectTags previousTags = legacyData.previousTags();
            previousScanStatus = previousTags.scanStatus();
            saveTimeInMillis = previousTags.saveTimeInMillis();
            receivedMessage = new ReceivedMessage(messageId, previousTags.bpId(), previousTags.messageType(),
                    previousTags.partnerTopic(), previousTags.partnerExternalReference(), legacyData.contentType());
        }

        updateLegacyScanStatusTag(messageId, newScanStatus);

        metricsService.publishMetrics(scanResultInfo.scanResult(), messageArrivalTimeInMillis, saveTimeInMillis);
        notifyIfRequired(scanResultInfo.scanResult(), newScanStatus, previousScanStatus, receivedMessage);
    }

    /**
     * While the legacy tag compatibility is enabled, the scanStatus tag is updated after a malware scan result
     * exactly like MES &lt; 11.0.0 did, so that 10.x instances deliver correctly during a rolling deployment.
     * Best effort: the database is authoritative, and a failed tag update must not lose the received event.
     * LEGACY-TAG-FALLBACK: remove with the contract story (JEAP-7252).
     */
    private void updateLegacyScanStatusTag(UUID messageId, ScanStatus newScanStatus) {
        Map<String, String> scanStatusTag = legacyTagsFactory.createScanStatusUpdateTags(newScanStatus);
        if (scanStatusTag.isEmpty()) {
            return;
        }
        try {
            objectStore.updateTags(BucketType.PARTNER, messageId.toString(), scanStatusTag);
        } catch (RuntimeException e) {
            log.error("Failed to update the legacy scanStatus tag of message {} to {} - MES < 11.0.0 instances will " +
                    "not deliver this message; the authoritative scan status in the database is unaffected", messageId, newScanStatus, e);
        }
    }

    private void validatePartnerBucketName(String bucketName) {
        String expectedBucketName = objectStore.getBucketName(BucketType.PARTNER);
        if (!expectedBucketName.equals(bucketName)) {
            throw new IllegalStateException("Bucket name mismatch. Expected: " + expectedBucketName + ", actual: " + bucketName);
        }
    }

    private void notifyIfRequired(MalwareScanResult scanResult, ScanStatus newScanStatus, ScanStatus previousScanStatus, ReceivedMessage message) {
        if (shouldNotifyMessageReceivedEvent(scanResult, previousScanStatus)) {
            eventPublisher.publishMessageReceivedEvent(message.messageId(), message.bpId(), message.messageType(),
                    message.partnerTopic(), message.partnerExternalReference(), newScanStatus.toPublishedScanStatus(), message.contentType());
        } else {
            log.debug("Not publishing MessageReceivedEvent again for message with id '{}' from business partner with id '{}' " +
                            "and with external reference '{}' and with scan result '{}'.", message.messageId(),
                    message.bpId(), message.partnerExternalReference(), scanResult);
        }
    }

    private boolean shouldNotifyMessageReceivedEvent(MalwareScanResult scanResult, ScanStatus previousScanStatus) {
        return threatsHaveBeenDetected(scanResult) ||  // we always want to notify about threats
                !malwareScanWasDisabledWhenReceivingNotifiedMessage(previousScanStatus); // already notified when message was received
    }

    private boolean threatsHaveBeenDetected(MalwareScanResult scanResult) {
        return scanResult == MalwareScanResult.THREATS_FOUND;
    }

    private boolean malwareScanWasDisabledWhenReceivingNotifiedMessage(ScanStatus previousScanStatus) {
        return previousScanStatus == ScanStatus.NOT_SCANNED;
    }

    private static String createInternalMessageObjectKey(String bpId, UUID messageId) {
        return bpId + "/" + messageId;
    }

    private void checkScanStatus(UUID messageId, String bpId, ScanStatus scanStatus) {
        if (!doDeliverMessageWithStatus(scanStatus)) {
            log.error("Message {}-{} cannot be delivered because its malware scan status is {}", bpId, messageId, scanStatus);
            throw MalwareScanFailedOrBlockedException.malwareScanFailedOrBlockedException(messageId, scanStatus);
        }
    }

    private boolean doDeliverMessageWithStatus(ScanStatus scanStatus) {
        if (scanStatus == ScanStatus.THREATS_FOUND) {
            // explicitly never deliver threats
            return false;
        }
        return scanStatus == null || scanStatus == ScanStatus.NOT_SCANNED || scanStatus == ScanStatus.NO_THREATS_FOUND ||
                !malwareScanProperties.isEnabled() && (scanStatus == ScanStatus.SCAN_PENDING || scanStatus == ScanStatus.SCAN_FAILED);
    }

    private record ReceivedMessage(UUID messageId, String bpId, String messageType, String partnerTopic,
                                   String partnerExternalReference, String contentType) {
    }
}
