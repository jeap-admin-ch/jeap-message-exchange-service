package ch.admin.bit.jeap.messageexchange.kafka;

import ch.admin.bit.jeap.messageexchange.domain.malwarescan.PublishedScanStatus;
import ch.admin.bit.jeap.messageexchange.domain.messaging.EventPublisher;
import ch.admin.bit.jeap.messageexchange.event.message.received.B2BMessageReceivedEvent;
import ch.admin.bit.jeap.messageexchange.event.message.received.S3ObjectMalwareScanStatus;
import ch.admin.bit.jeap.messageexchange.plugin.api.listener.MessageReceivedListener;
import ch.admin.bit.jeap.messageexchange.plugin.api.listener.MessageResult;
import ch.admin.bit.jeap.messaging.avro.AvroMessage;
import ch.admin.bit.jeap.messaging.avro.AvroMessageKey;
import ch.admin.bit.jeap.messaging.kafka.properties.KafkaProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
@RequiredArgsConstructor
@Component
public class KafkaEventPublisher implements EventPublisher {

    private final KafkaTemplate<AvroMessageKey, AvroMessage> kafkaTemplate;
    private final TopicConfiguration topicConfiguration;
    private final KafkaProperties kafkaProperties;
    private final List<MessageReceivedListener> messageReceivedListeners;

    @Override
    public void publishMessageReceivedEvent(UUID messageId, String bpId, String type, PublishedScanStatus externalPublishedScanStatus) {

        List<CompletableFuture<SendResult<AvroMessageKey, AvroMessage>>> sendResults = new ArrayList<>();
        S3ObjectMalwareScanStatus scanStatus = mapStatus(externalPublishedScanStatus);
        MessageResult b2BMessageReceivedEvent = getB2BMessageReceivedEvent(messageId, bpId, type, scanStatus);
        log.debug("Publishing event {} to topic {}.", b2BMessageReceivedEvent, b2BMessageReceivedEvent.topicName());
        sendResults.add(kafkaTemplate.send(b2BMessageReceivedEvent.topicName(), b2BMessageReceivedEvent.message()));

        // Publish others events from configured listeners
        log.debug("Publishing events from configured plugin-api: found {} listeners", messageReceivedListeners.size());
        for (MessageReceivedListener listener : messageReceivedListeners) {
            MessageResult messageResult = listener.onMessageReceived(messageId, bpId, type);
            log.debug("Publishing event {} to topic {}.", messageResult, messageResult.topicName());
            sendResults.add(kafkaTemplate.send(messageResult.topicName(), messageResult.message()));
        }

        CompletableFuture.allOf(sendResults.toArray(CompletableFuture[]::new)).join();
    }

    private MessageResult getB2BMessageReceivedEvent(UUID messageId, String bpId, String type, S3ObjectMalwareScanStatus scanStatus) {
        B2BMessageReceivedEvent messageReceivedEvent = B2BMessageReceivedEventBuilder.create()
                .bpId(bpId)
                .messageId(messageId.toString())
                .type(type)
                .scanStatus(scanStatus)
                .systemName(kafkaProperties.getSystemName())
                .serviceName(kafkaProperties.getServiceName())
                .idempotenceId(messageId.toString())
                .build();

        return new MessageResult(topicConfiguration.getMessageReceived(), messageReceivedEvent);
    }

    private S3ObjectMalwareScanStatus mapStatus(PublishedScanStatus externalPublishedScanStatus) {
        return S3ObjectMalwareScanStatus.valueOf(externalPublishedScanStatus.name());
    }
}
