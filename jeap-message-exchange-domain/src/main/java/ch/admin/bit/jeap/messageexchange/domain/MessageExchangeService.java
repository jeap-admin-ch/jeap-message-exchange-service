package ch.admin.bit.jeap.messageexchange.domain;

import ch.admin.bit.jeap.messageexchange.domain.database.InboundMessageRepository;
import ch.admin.bit.jeap.messageexchange.domain.database.MessageRepository;
import ch.admin.bit.jeap.messageexchange.domain.dto.MessageSearchResultDto;
import ch.admin.bit.jeap.messageexchange.domain.dto.MessageSearchResultWithContentDto;
import ch.admin.bit.jeap.messageexchange.domain.exception.MalwareScanFailedOrBlockedException;
import ch.admin.bit.jeap.messageexchange.domain.exception.MismatchedContentException;
import ch.admin.bit.jeap.messageexchange.domain.exception.MismatchedContentTypeException;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.MalwareScanProperties;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.PublishedScanStatus;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.S3ObjectMalwareScanResultInfo;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.ScanStatus;
import ch.admin.bit.jeap.messageexchange.domain.messaging.EventPublisher;
import ch.admin.bit.jeap.messageexchange.domain.metrics.MetricsService;
import ch.admin.bit.jeap.messageexchange.domain.objectstore.*;
import ch.admin.bit.jeap.messageexchange.domain.sent.MessageSentProperties;
import ch.admin.bit.jeap.messageexchange.domain.xml.XmlValidatingOutputStream;
import ch.admin.bit.jeap.messageexchange.malware.api.MalwareScanResult;
import ch.admin.bit.jeap.messageexchange.malware.api.MalwareScanTrigger;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

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
    private final S3ObjectTagsService tagsService;
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
        checkScanStatus(messageId, messageInfoWithTagsOptional.get());
        return messageInfoWithTagsOptional;
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
            storeInputStreamInS3AndSendEvents(inputStream, messageId, bpId, messageType, partnerTopic, partnerExternalReference, rawMessageContent, contentType);
            inboundMessageRepository.save(InboundMessage.builder().messageId(messageId).bpId(bpId).contentLength(rawMessageContent.contentLength()).build());
        } else {
            handleDuplicatePartnerMessage(inputStream, inboundMessageOptional.get(), messageId, bpId, rawMessageContent, messageType, partnerTopic, partnerExternalReference, contentType);
        }
    }

    @SuppressWarnings("java:S107")
    private void storeInputStreamInS3AndSendEvents(InputStream inputStream, UUID messageId, String bpId, String messageType, String partnerTopic, String partnerExternalReference, MessageContent rawMessageContent, String contentType) throws IOException {
        ScanStatus scanStatus = malwareScanProperties.isEnabled() ? ScanStatus.SCAN_PENDING : ScanStatus.NOT_SCANNED;
        Map<String, String> tags = tagsService.toMap(bpId, messageType, partnerTopic, partnerExternalReference, scanStatus, System.currentTimeMillis());

        MessageContent messageContent = new MessageContent(inputStream, rawMessageContent.contentLength(), tags);

        // Save to S3
        objectStore.storeMessage(BucketType.PARTNER, messageId.toString(), messageContent, contentType);

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
            storeInputStreamInS3AndSendEvents(inputStream, messageId, bpId, messageType, partnerTopic, partnerExternalReference, rawMessageContent, contentType);
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
        S3ObjectMetadata s3ObjectMetadata = updateAndGetValidatedTags(scanResultInfo);

        metricsService.publishMetrics(scanResultInfo.scanResult(), messageArrivalTimeInMillis, s3ObjectMetadata.tags().saveTimeInMillis());

        if (shouldNotifyMessageReceivedEvent(scanResultInfo, s3ObjectMetadata)) {
            publishMessageReceivedEvent(scanResultInfo.objectKey(), s3ObjectMetadata);
        } else {
            log.debug("Not publishing MessageReceivedEvent again for message with id '{}' from business partner with id '{}' " +
                            "and with external reference '{}' and with scan result '{}'.", scanResultInfo.objectKey(),
                    s3ObjectMetadata.tags().bpId(), s3ObjectMetadata.tags().partnerExternalReference(), scanResultInfo.scanResult());
        }
    }

    private boolean shouldNotifyMessageReceivedEvent(S3ObjectMalwareScanResultInfo scanResultInfo, S3ObjectMetadata s3ObjectMetadata) {
        return threatsHaveBeenDetected(scanResultInfo.scanResult()) ||  // we always want to notify about threats
                !malwareScanWasDisabledWhenReceivingNotifiedMessage(s3ObjectMetadata); // already notified when message was received
    }

    private boolean threatsHaveBeenDetected(MalwareScanResult scanResult) {
        return scanResult == MalwareScanResult.THREATS_FOUND;
    }

    private boolean malwareScanWasDisabledWhenReceivingNotifiedMessage(S3ObjectMetadata metadata) {
        return (metadata.previousTags() != null) && (metadata.previousTags().scanStatus() == ScanStatus.NOT_SCANNED);
    }

    private void publishMessageReceivedEvent(String objectKey, S3ObjectMetadata s3ObjectMetadata) {
        UUID messageId = UUID.fromString(objectKey);
        S3ObjectTags actualTags = s3ObjectMetadata.tags();
        ScanStatus scanStatus = actualTags.scanStatus();
        PublishedScanStatus externalPublishedScanStatus = scanStatus.toPublishedScanStatus();

        eventPublisher.publishMessageReceivedEvent(messageId, actualTags.bpId(), actualTags.messageType(), actualTags.partnerTopic(), actualTags.partnerExternalReference(), externalPublishedScanStatus, s3ObjectMetadata.contentType());
    }

    private static String createInternalMessageObjectKey(String bpId, UUID messageId) {
        return bpId + "/" + messageId;
    }

    private S3ObjectMetadata updateAndGetValidatedTags(S3ObjectMalwareScanResultInfo internalScanResult) {
        ScanStatus scanStatus = ScanStatus.fromScanResult(internalScanResult.scanResult());
        if (scanStatus == ScanStatus.SCAN_FAILED) {
            log.warn("Malware scan failed: {}", internalScanResult);
        }
        String bucketName = internalScanResult.bucketName();
        String objectKey = internalScanResult.objectKey();
        return updateAndGetValidatedTags(bucketName, objectKey, scanStatus);
    }

    private S3ObjectMetadata updateAndGetValidatedTags(String bucketName, String objectKey, ScanStatus scanStatus) {
        Map<String, String> tagsToUpdate = tagsService.toMap(scanStatus);
        S3ObjectTagsUpdateResult updateResult = objectStore.updateTagsAndGetTags(BucketType.PARTNER, bucketName, objectKey, tagsToUpdate);
        return new S3ObjectMetadata(
                tagsService.getTagsfromMapAndValidate(bucketName, objectKey, updateResult.currentTags()),
                tagsService.getTagsfromMapAndValidate(bucketName, objectKey, updateResult.previousTags()),
                objectStore.getContentType(BucketType.PARTNER, bucketName, objectKey));
    }

    private void checkScanStatus(UUID messageId, MessageContent messageInfoWithTags) {
        S3ObjectTags s3ObjectTags = tagsService.getTagsfromMap(messageInfoWithTags.tags());
        ScanStatus scanStatus = s3ObjectTags.scanStatus();
        if (!doDeliverMessageWithStatus(scanStatus)) {
            log.error("Message {}-{} cannot be delivered because its malware scan status is {}", s3ObjectTags.bpId(), messageId, scanStatus);
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
}
