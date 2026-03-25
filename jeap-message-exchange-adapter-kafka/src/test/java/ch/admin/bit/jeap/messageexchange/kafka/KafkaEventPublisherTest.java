package ch.admin.bit.jeap.messageexchange.kafka;

import ch.admin.bit.jeap.messageexchange.domain.malwarescan.PublishedScanStatus;
import ch.admin.bit.jeap.messageexchange.event.message.received.B2BMessageReceivedEvent;
import ch.admin.bit.jeap.messaging.avro.AvroMessage;
import ch.admin.bit.jeap.messaging.avro.AvroMessageKey;
import ch.admin.bit.jeap.messaging.kafka.properties.KafkaProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaEventPublisherTest {

    @Mock
    private KafkaTemplate<AvroMessageKey, AvroMessage> kafkaTemplate;

    @Mock
    private TopicConfiguration topicConfiguration;

    @Mock
    private KafkaProperties kafkaProperties;

    private KafkaEventPublisher kafkaEventPublisher;

    @BeforeEach
    void setup() {
        when(topicConfiguration.getMessageReceived()).thenReturn("test-topic");
        when(kafkaProperties.getSystemName()).thenReturn("test-system");
        when(kafkaProperties.getServiceName()).thenReturn("test-service");
        when(kafkaTemplate.send(anyString(), any(AvroMessage.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        kafkaEventPublisher = new KafkaEventPublisher(kafkaTemplate, topicConfiguration, kafkaProperties, List.of());
    }

    @ParameterizedTest
    @EnumSource(PublishedScanStatus.class)
    void idempotenceId_includesScanStatus(PublishedScanStatus scanStatus) {
        UUID messageId = UUID.randomUUID();

        kafkaEventPublisher.publishMessageReceivedEvent(messageId, "bpId", "type", null, null,
                scanStatus, MediaType.APPLICATION_XML_VALUE);

        ArgumentCaptor<B2BMessageReceivedEvent> captor = ArgumentCaptor.forClass(B2BMessageReceivedEvent.class);
        verify(kafkaTemplate).send(eq("test-topic"), captor.capture());
        B2BMessageReceivedEvent event = captor.getValue();

        assertThat(event.getIdentity().getIdempotenceId())
                .isEqualTo(messageId + "_" + scanStatus.name());
    }
}
