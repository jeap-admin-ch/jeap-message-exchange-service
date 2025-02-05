package ch.admin.bit.jeap.messageexchange.domain;

import ch.admin.bit.jeap.messageexchange.domain.database.MessageRepository;
import ch.admin.bit.jeap.messageexchange.domain.dto.MessageSearchResultDto;
import ch.admin.bit.jeap.messageexchange.domain.exception.MalwareScanFailedOrBlockedException;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.MalwareScanProperties;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.PublishedScanStatus;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.S3ObjectMalwareScanResultInfo;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.ScanStatus;
import ch.admin.bit.jeap.messageexchange.domain.messaging.EventPublisher;
import ch.admin.bit.jeap.messageexchange.domain.metrics.MetricsService;
import ch.admin.bit.jeap.messageexchange.domain.objectstore.BucketType;
import ch.admin.bit.jeap.messageexchange.domain.objectstore.ObjectStore;
import ch.admin.bit.jeap.messageexchange.domain.objectstore.S3ObjectTags;
import ch.admin.bit.jeap.messageexchange.domain.objectstore.S3ObjectTagsService;
import ch.admin.bit.jeap.messageexchange.domain.xml.XmlValidatingOutputStream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
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
    private final MalwareScanProperties malwareScanProperties;
    private final MetricsService metricsService;
    private final S3ObjectTagsService tagsService;

    public Optional<MessageContent> getMessageFromInternalApplication(String bpId, UUID messageId) {
        String objectKey = createInternalMessageObjectKey(bpId, messageId);
        log.trace("Retrieve payload for internal message with messageId {} from key {}", messageId, objectKey);
        return objectStore.loadMessage(BucketType.INTERNAL, objectKey);
    }

    public Optional<NextMessageResultDto> getNextMessageFromInternalApplication(UUID lastMessageId, String bpId, String partnerTopic, String topicName) {
        Optional<UUID> nextMessageId = messageRepository.getNextMessageId(lastMessageId, bpId, partnerTopic, topicName);
        if (nextMessageId.isPresent()) {
            Optional<MessageContent> messageInfo = getMessageFromInternalApplication(bpId, nextMessageId.get());
            if (messageInfo.isPresent()) {
                log.trace("Found new message with messageId {} for bpId {} after lastMessageId {}", nextMessageId.get(), bpId, lastMessageId);
                return Optional.of(new NextMessageResultDto(nextMessageId.get(), messageInfo.get()));
            }
        }
        log.trace("No new message found for bpId {} after lastMessageId {}", bpId, lastMessageId);
        return Optional.empty();
    }

    public Optional<MessageContent> getMessageFromPartner(UUID messageId) {
        log.trace("Retrieve payload for partner message with messageId {}", messageId);

        Optional<MessageContent> messageInfoWithTagsOptional = objectStore.loadMessageWithTags(BucketType.PARTNER, messageId.toString());
        if (messageInfoWithTagsOptional.isEmpty()) {
            return Optional.empty();
        }
        checkScanStatus(messageId, messageInfoWithTagsOptional.get());
        return messageInfoWithTagsOptional;
    }

    public void saveNewMessageFromPartner(UUID messageId, String bpId, String messageType, MessageContent rawMessageContent) throws IOException {
        try (InputStream inputStream = XmlValidatingOutputStream.wrapInputStreamWithXmlValidation(messageId, bpId, rawMessageContent)) {
            ScanStatus scanStatus = malwareScanProperties.isEnabled() ? ScanStatus.SCAN_PENDING : ScanStatus.NOT_SCANNED;
            Map<String, String> tags = tagsService.toMap(bpId, messageType, scanStatus, System.currentTimeMillis());

            MessageContent messageContent = new MessageContent(inputStream, rawMessageContent.contentLength(), tags);

            // Save to S3
            // This will validate the XML at the same time by copying output to the XmlValidatingOutputStream
            objectStore.storeMessage(BucketType.PARTNER, messageId.toString(), messageContent);

            if (!malwareScanProperties.isEnabled()) {
                // Publish ReceivedEvent and events from configured listeners
                eventPublisher.publishMessageReceivedEvent(messageId, bpId, messageType, PublishedScanStatus.NOT_SCANNED);
            }
        }
    }

    public void saveNewMessageFromInternalApplication(Message message, MessageContent rawMessageContent) throws IOException {
        try (InputStream inputStream = XmlValidatingOutputStream.wrapInputStreamWithXmlValidation(message.getMessageId(), message.getBpId(), rawMessageContent)) {
            MessageContent messageInfo = new MessageContent(inputStream, rawMessageContent.contentLength());
            // Save to S3
            // This will validate the XML at the same time by copying output to the XmlValidatingOutputStream
            objectStore.storeMessage(BucketType.INTERNAL, createInternalMessageObjectKey(message.getBpId(), message.getMessageId()), messageInfo);

            // Store message in database
            messageRepository.save(message);
        }
    }

    public List<MessageSearchResultDto> getMessages(String bpId, String topicName, String groupId, UUID lastMessageId, String partnerTopic, int size) {
        return messageRepository.getMessages(bpId, topicName, groupId, lastMessageId, partnerTopic, size);
    }

    public void onMalwareScanResult(S3ObjectMalwareScanResultInfo scanResultInfo) {
        long messageArrivalTimeInMillis = System.currentTimeMillis();
        S3ObjectTags s3ObjectTags = updateAndGetValidatedTags(scanResultInfo);

        metricsService.publishMetrics(scanResultInfo.scanResult(), messageArrivalTimeInMillis, s3ObjectTags.saveTimeInMillis());

        publishMessageReceivedEvent(scanResultInfo.objectKey(), s3ObjectTags);
    }

    private void publishMessageReceivedEvent(String objectKey, S3ObjectTags actualTags) {
        UUID messageId = UUID.fromString(objectKey);
        ScanStatus scanStatus = actualTags.scanStatus();
        PublishedScanStatus externalPublishedScanStatus = scanStatus.toPublishedScanStatus();

        eventPublisher.publishMessageReceivedEvent(messageId, actualTags.bpId(), actualTags.messageType(), externalPublishedScanStatus);
    }

    private static String createInternalMessageObjectKey(String bpId, UUID messageId) {
        return bpId + "/" + messageId;
    }

    private S3ObjectTags updateAndGetValidatedTags(S3ObjectMalwareScanResultInfo internalScanResult) {
        ScanStatus scanStatus = ScanStatus.fromScanResult(internalScanResult.scanResult());
        if (scanStatus == ScanStatus.SCAN_FAILED) {
            log.warn("Malware scan failed: {}", internalScanResult);
        }
        String bucketName = internalScanResult.bucketName();
        String objectKey = internalScanResult.objectKey();
        return updateAndGetValidatedTags(bucketName, objectKey, scanStatus);
    }

    private S3ObjectTags updateAndGetValidatedTags(String bucketName, String objectKey, ScanStatus scanStatus) {
        Map<String, String> tagsToUpdate = tagsService.toMap(scanStatus);
        Map<String, String> actualTags = objectStore.updateTagsAndGetTags(BucketType.PARTNER, bucketName, objectKey, tagsToUpdate);
        return tagsService.getTagsfromMapAndValidate(bucketName, objectKey, actualTags);
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
        return scanStatus == null || scanStatus == ScanStatus.NOT_SCANNED || scanStatus == ScanStatus.NO_THREATS_FOUND;
    }
}