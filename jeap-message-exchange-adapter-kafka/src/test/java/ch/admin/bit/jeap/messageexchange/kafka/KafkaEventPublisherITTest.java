package ch.admin.bit.jeap.messageexchange.kafka;

import ch.admin.bit.jeap.messageexchange.domain.database.MessageRepository;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.PublishedScanStatus;
import ch.admin.bit.jeap.messageexchange.domain.metrics.MetricsService;
import ch.admin.bit.jeap.messageexchange.domain.objectstore.ObjectStore;
import ch.admin.bit.jeap.messageexchange.event.message.received.B2BMessageReceivedEvent;
import ch.admin.bit.jeap.messageexchange.event.message.received.S3ObjectMalwareScanStatus;
import ch.admin.bit.jeap.messaging.kafka.test.KafkaIntegrationTestBase;
import ch.admin.bit.jeap.messaging.kafka.test.TestKafkaListener;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        properties = {
                "jeap.messageexchange.kafka.topic.message-received=message-received",
                "jeap.messaging.kafka.error-topic-name=error",
                "jeap.messaging.kafka.system-name=my-system-name",
                "jeap.messaging.kafka.service-name=my-service-name",
                "spring.application.name=junit"
        },
        classes = TestApp.class)
@ExtendWith(MockitoExtension.class)
class KafkaEventPublisherITTest extends KafkaIntegrationTestBase {

    @Autowired
    private KafkaEventPublisher kafkaEventPublisher;

    @MockitoBean
    @SuppressWarnings("unused")
    private ObjectStore objectStore;

    @MockitoBean
    @SuppressWarnings("unused")
    private MessageRepository messageRepository;

    @MockitoBean
    @SuppressWarnings("unused")
    private MetricsService metricsService;

    private final List<B2BMessageReceivedEvent> messages = new CopyOnWriteArrayList<>();

    private final List<B2BMessageReceivedEvent> messagesOnJunitTest = new CopyOnWriteArrayList<>();

    @Test
    void publishMessageReceivedEvent() {
        String bpId = UUID.randomUUID().toString();
        UUID messageId = UUID.randomUUID();
        String messageType = "myMessageType";
        String partnerTopic = "partner-topic";
        String partnerExternalReference = "external-ref-123";
        PublishedScanStatus externalPublishedScanStatus = PublishedScanStatus.NO_THREATS_FOUND;
        String contentType = MediaType.APPLICATION_XML_VALUE;

        kafkaEventPublisher.publishMessageReceivedEvent(messageId, bpId, messageType, partnerTopic, partnerExternalReference, externalPublishedScanStatus, contentType);

        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .until(() -> (messages.size() == 1 && messagesOnJunitTest.size() == 1));

        // message sent from mes
        assertThat(messages.getFirst().getReferences().getMessageReference().getBpId()).isEqualTo(bpId);
        assertThat(messages.getFirst().getReferences().getMessageReference().getMessageId()).isEqualTo(messageId.toString());
        assertThat(messages.getFirst().getReferences().getMessageReference().getType()).isEqualTo(messageType);
        assertThat(messages.getFirst().getType().getVariant()).isEqualTo(messageType);
        assertThat(messages.getFirst().getPublisher().getSystem()).isEqualTo("my-system-name");
        assertThat(messages.getFirst().getPublisher().getService()).isEqualTo("my-service-name");
        assertThat(messages.getFirst().getIdentity().getIdempotenceId()).isEqualTo(messageId.toString());
        assertThat(messages.getFirst().getPayload().getScanStatus()).isEqualTo(S3ObjectMalwareScanStatus.NO_THREATS_FOUND);
        assertThat(messages.getFirst().getReferences().getMessageReference().getContentType()).isEqualTo(contentType);
        assertThat(messages.getFirst().getReferences().getMessageReference().getPartnerTopic()).isEqualTo(partnerTopic);
        assertThat(messages.getFirst().getReferences().getMessageReference().getPartnerExternalReference()).isEqualTo(partnerExternalReference);

        // message sent from plugin-api listener
        assertThat(messagesOnJunitTest.getFirst().getReferences().getMessageReference().getBpId()).isEqualTo(bpId + "_plugin");
        assertThat(messagesOnJunitTest.getFirst().getReferences().getMessageReference().getMessageId()).isEqualTo(messageId + "_plugin");
        assertThat(messagesOnJunitTest.getFirst().getReferences().getMessageReference().getType()).isEqualTo(messageType + "_plugin");
        assertThat(messagesOnJunitTest.getFirst().getType().getVariant()).isEqualTo("junit");
        assertThat(messagesOnJunitTest.getFirst().getReferences().getMessageReference().getContentType()).isEqualTo("junit");
        assertThat(messagesOnJunitTest.getFirst().getPublisher().getSystem()).isEqualTo("junit");
        assertThat(messagesOnJunitTest.getFirst().getPublisher().getService()).isEqualTo("junit");

    }

    @TestKafkaListener(topics = "message-received")
    public void onEvent(@Payload B2BMessageReceivedEvent message) {
        this.messages.add(message);
    }

    @TestKafkaListener(topics = "junit-test")
    public void onEventOnJunitTest(@Payload B2BMessageReceivedEvent message) {
        this.messagesOnJunitTest.add(message);
    }

}
