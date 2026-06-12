package ch.admin.bit.jeap.messageexchange.kafka;

import ch.admin.bit.jeap.messageexchange.domain.Message;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.PublishedScanStatus;
import ch.admin.bit.jeap.messageexchange.domain.messaging.EventPublisher;
import ch.admin.bit.jeap.messageexchange.event.message.received.B2BMessageReceivedEvent;
import ch.admin.bit.jeap.messageexchange.event.message.received.S3ObjectMalwareScanStatus;
import ch.admin.bit.jeap.messageexchange.event.message.sent.B2BMessageSentEvent;
import ch.admin.bit.jeap.messaging.avro.AvroMessage;
import ch.admin.bit.jeap.messaging.avro.AvroMessageKey;
import ch.admin.bit.jeap.messaging.kafka.properties.KafkaProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
@Component
public class KafkaEventPublisher implements EventPublisher {

    private final KafkaTemplate<AvroMessageKey, AvroMessage> kafkaTemplate;
    private final TopicConfiguration topicConfiguration;
    private final KafkaProperties kafkaProperties;

    @Override
    public void publishMessageReceivedEvent(UUID messageId, String bpId, String type, String partnerTopic, String partnerExternalReference, PublishedScanStatus externalPublishedScanStatus, String contentType) {
        S3ObjectMalwareScanStatus scanStatus = mapStatus(externalPublishedScanStatus);
        B2BMessageReceivedEvent messageReceivedEvent = getB2BMessageReceivedEvent(messageId, bpId, type, partnerTopic, partnerExternalReference, scanStatus, contentType);
        String topicName = topicConfiguration.getMessageReceived();
        log.debug("Publishing MessageReceivedEvent {} to topic {}.", messageReceivedEvent, topicName);
        kafkaTemplate.send(topicName, messageReceivedEvent);
    }

    @Override
    public void publishMessageSentEvent(Message message) {
        B2BMessageSentEvent messageSentEvent = getB2BMessageSentEvent(message);
        String topicName = topicConfiguration.getMessageSent();
        log.debug("Publishing MessageSentEvent {} to topic {}.", messageSentEvent, topicName);
        kafkaTemplate.send(topicName, messageSentEvent);
    }

    private B2BMessageReceivedEvent getB2BMessageReceivedEvent(UUID messageId, String bpId, String type, String partnerTopic, String partnerExternalReference, S3ObjectMalwareScanStatus scanStatus, String contentType) {
        return B2BMessageReceivedEventBuilder.create()
                .bpId(bpId)
                .messageId(messageId.toString())
                .type(type)
                .variant(type)
                .scanStatus(scanStatus)
                .partnerTopic(partnerTopic)
                .partnerExternalReference(partnerExternalReference)
                .contentType(contentType)
                .systemName(kafkaProperties.getSystemName())
                .serviceName(kafkaProperties.getServiceName())
                .idempotenceId(messageId + "_" + scanStatus.name())
                .build();
    }

    private B2BMessageSentEvent getB2BMessageSentEvent(Message message) {
        return B2BMessageSentEventBuilder.create()
                .bpId(message.getBpId())
                .messageId(message.getMessageId().toString())
                .type(message.getMessageType())
                .variant(message.getMessageType())
                .contentType(message.getContentType())
                .topicName(message.getTopicName())
                .groupId(message.getGroupId())
                .partnerTopic(message.getPartnerTopic())
                .systemName(kafkaProperties.getSystemName())
                .serviceName(kafkaProperties.getServiceName())
                .idempotenceId(message.getMessageId().toString())
                .build();
    }

    private S3ObjectMalwareScanStatus mapStatus(PublishedScanStatus externalPublishedScanStatus) {
        return S3ObjectMalwareScanStatus.valueOf(externalPublishedScanStatus.name());
    }
}
