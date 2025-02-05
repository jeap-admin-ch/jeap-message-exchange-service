package ch.admin.bit.jeap.messageexchange.domain;

import ch.admin.bit.jeap.messageexchange.domain.database.MessageRepository;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.MalwareScanProperties;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.PublishedScanStatus;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.S3ObjectMalwareScanResultInfo;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.ScanResult;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.ScanStatus;
import ch.admin.bit.jeap.messageexchange.domain.messaging.EventPublisher;
import ch.admin.bit.jeap.messageexchange.domain.metrics.MetricsService;
import ch.admin.bit.jeap.messageexchange.domain.objectstore.BucketType;
import ch.admin.bit.jeap.messageexchange.domain.objectstore.ObjectStore;
import ch.admin.bit.jeap.messageexchange.domain.objectstore.S3ObjectTagsService;
import ch.admin.bit.jeap.messageexchange.domain.xml.InvalidXMLInputException;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MessageExchangeServiceTest {

    private MessageExchangeService messageExchangeService;

    private String storedMessage;
    private Map<String, String> storedTags = new HashMap<>();

    @MockitoBean
    private final ObjectStore objectStore = new ObjectStore() {

        @Override
        public void storeMessage(BucketType bucketType, String objectKey, MessageContent messageContent) throws IOException {
            storeMessage(bucketType, objectKey, messageContent, messageContent.tags());
        }

        private void storeMessage(@SuppressWarnings("unused") BucketType bucketType, @SuppressWarnings("unused") String objectKey, MessageContent messageContent, Map<String, String> tags) throws IOException {
            try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
                messageContent.inputStream().transferTo(os);
                storedMessage = os.toString(UTF_8);
            }
            storedTags = tags;
        }

        @Override
        public Optional<MessageContent> loadMessage(BucketType bucketType, String objectKey) {
            return Optional.of(new MessageContent(new ByteArrayInputStream(storedMessage.getBytes(UTF_8)), storedMessage.getBytes(UTF_8).length));
        }

        @Override
        public Optional<MessageContent> loadMessageWithTags(BucketType bucketType, String string) {
            return Optional.of(new MessageContent(new ByteArrayInputStream(storedMessage.getBytes(UTF_8)), storedMessage.getBytes(UTF_8).length, storedTags));
        }

        @Override
        public Map<String, String> updateTagsAndGetTags(BucketType bucketType, String bucketName, String objectKey, Map<String, String> tagsToUpdate) {
            storedTags.putAll(tagsToUpdate);
            return storedTags;
        }
    };

    @Mock
    private EventPublisher eventPublisher;

    @Mock
    private MessageRepository messageRepository;

    @Mock
    private MalwareScanProperties malwareScanProperties;

    @Mock
    private MetricsService metricsService;

    @BeforeEach
    void setup() {
        messageExchangeService = new MessageExchangeService(objectStore, eventPublisher, messageRepository, malwareScanProperties, metricsService, new S3ObjectTagsService());
    }

    @Test
    @SneakyThrows
    void saveNewMessageFromPartner_xmlValid_messageSaved() {
        UUID messageId = UUID.randomUUID();
        String bpId = "bpId";
        String messageType = "messageType";
        String xmlContentString = "<valid/>";
        InputStream xmlContent = new ByteArrayInputStream(xmlContentString.getBytes(UTF_8));

        messageExchangeService.saveNewMessageFromPartner(messageId, bpId, messageType, new MessageContent(xmlContent, 42));

        verify(eventPublisher, times(1)).publishMessageReceivedEvent(messageId, bpId, messageType, PublishedScanStatus.NOT_SCANNED);
        assertThat(storedMessage).isEqualTo(xmlContentString);
        assertThat(storedTags).containsKeys("bpId", "messageType", "scanStatus", "saveTimeInMillis");
        assertThat(storedTags).containsEntry("scanStatus", ScanStatus.NOT_SCANNED.name());

        verify(messageRepository, never()).save(any(Message.class));
    }

    @Test
    @SneakyThrows
    void saveNewMessageFromPartnerMalwareScanEnabled_xmlValid_messageSaved() {
        UUID messageId = UUID.randomUUID();
        String bpId = "bpId";
        String messageType = "messageType";
        String xmlContentString = "<valid/>";
        InputStream xmlContent = new ByteArrayInputStream(xmlContentString.getBytes(UTF_8));

        when(malwareScanProperties.isEnabled()).thenReturn(true);

        messageExchangeService.saveNewMessageFromPartner(messageId, bpId, messageType, new MessageContent(xmlContent, 42));

        verify(eventPublisher, never()).publishMessageReceivedEvent(any(UUID.class), anyString(), anyString(), any(PublishedScanStatus.class));
        assertThat(storedMessage).isEqualTo(xmlContentString);
        assertThat(storedTags).containsKeys("bpId", "messageType", "scanStatus", "saveTimeInMillis");
        assertThat(storedTags).containsEntry("scanStatus", ScanStatus.SCAN_PENDING.name());

        verify(messageRepository, never()).save(any(Message.class));
    }

    @Test
    @SneakyThrows
    void saveNewMessageFromPartner_xmlInvalid_messageNotSaved() {
        UUID messageId = UUID.randomUUID();
        String bpId = "bpId";
        String messageType = "messageType";
        InputStream xmlContent = new ByteArrayInputStream("<invalid<xml".getBytes(UTF_8));

        MessageContent messageContent = new MessageContent(xmlContent, 42);

        assertThrows(InvalidXMLInputException.class, () -> messageExchangeService.saveNewMessageFromPartner(messageId, bpId, messageType, messageContent));

        verify(eventPublisher, never()).publishMessageReceivedEvent(any(UUID.class), anyString(), anyString(), any(PublishedScanStatus.class));
        assertThat(storedMessage).isNull();
        verify(messageRepository, never()).save(any(Message.class));
    }

    @Test
    @SneakyThrows
    void saveNewMessageFromInternal_xmlValid_messageSaved() {
        Message message = Message.builder()
                .messageId(UUID.randomUUID())
                .bpId("bpId")
                .messageType("messageType")
                .topicName("topicName")
                .build();
        String xmlContentString = "<valid/>";
        InputStream xmlContent = new ByteArrayInputStream(xmlContentString.getBytes(UTF_8));

        messageExchangeService.saveNewMessageFromInternalApplication(message, new MessageContent(xmlContent, 42));

        verify(eventPublisher, never()).publishMessageReceivedEvent(any(UUID.class), anyString(), anyString(), any(PublishedScanStatus.class));
        assertThat(storedMessage).isEqualTo(xmlContentString);

        verify(messageRepository, times(1)).save(message);
    }

    @Test
    @SneakyThrows
    void saveNewMessageFromInternal_xmlInvalid_messageNotSaved() {
        Message message = Message.builder()
                .messageId(UUID.randomUUID())
                .bpId("bpId")
                .messageType("messageType")
                .topicName("topicName")
                .build();
        InputStream xmlContent = new ByteArrayInputStream("<invalid<xml".getBytes(UTF_8));

        MessageContent messageContent = new MessageContent(xmlContent, 42);

        assertThrows(InvalidXMLInputException.class, () -> messageExchangeService.saveNewMessageFromInternalApplication(message, messageContent));

        verify(eventPublisher, never()).publishMessageReceivedEvent(any(UUID.class), anyString(), anyString(), any(PublishedScanStatus.class));
        assertThat(storedMessage).isNull();

        verify(messageRepository, never()).save(message);
    }


    @Test
    @SneakyThrows
    void getNextMessageFromInternalApplication_newMessageExists_thenReturnsContent() {
        UUID lastMessageId = UUID.randomUUID();
        String bpId = "bpId";
        UUID messageId = UUID.randomUUID();
        when(messageRepository.getNextMessageId(lastMessageId, bpId, null, null)).thenReturn(Optional.of(messageId));
        this.storedMessage = "<content>test</content>";

        Optional<NextMessageResultDto> nextMessageContent = messageExchangeService.getNextMessageFromInternalApplication(lastMessageId, bpId, null, null);

        assertThat(nextMessageContent).isPresent();
        assertThat(nextMessageContent.get().messageId()).isEqualTo(messageId);
        assertThat(nextMessageContent.get().messageContent().inputStream().readAllBytes()).isEqualTo(storedMessage.getBytes(UTF_8));
    }

    @Test
    @SneakyThrows
    void getNextMessageFromInternalApplication_noNewMessageExists_thenReturnsEmpty() {
        UUID lastMessageId = UUID.randomUUID();
        String bpId = "bpId";

        Optional<NextMessageResultDto> nextMessageContent = messageExchangeService.getNextMessageFromInternalApplication(lastMessageId, bpId, null, null);

        assertThat(nextMessageContent).isEmpty();
    }

    @Test
    void handleMalwareScanResult_NoThreatsFound() {
        UUID messageId = UUID.randomUUID();
        String bpId = "bpId";
        String messageType = "messageType";
        String saveTimeInMillis = String.valueOf(System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(2));
        ScanStatus scanStatus = ScanStatus.SCAN_PENDING;

        this.storedMessage = "<content>test</content>";
        this.storedTags = new HashMap<>(Map.of(
                "bpId", bpId,
                "messageType", messageType,
                "scanStatus", scanStatus.name(),
                "saveTimeInMillis", saveTimeInMillis
        ));

        S3ObjectMalwareScanResultInfo internalScanResult = new S3ObjectMalwareScanResultInfo(ScanResult.NO_THREATS_FOUND, "bucketName", messageId.toString());
        messageExchangeService.onMalwareScanResult(internalScanResult);

        verify(eventPublisher, times(1)).publishMessageReceivedEvent(messageId, bpId, messageType, PublishedScanStatus.NO_THREATS_FOUND);

        assertThat(storedTags).containsKeys("bpId", "messageType", "scanStatus", "saveTimeInMillis");
        assertThat(storedTags).containsEntry("scanStatus", ScanStatus.NO_THREATS_FOUND.name());

        verify(metricsService, times(1)).publishMetrics(eq(ScanResult.NO_THREATS_FOUND), anyLong(), eq(Long.parseLong(saveTimeInMillis)));
    }

    @Test
    void onMalwareScanResult_NoSaveTime() {
        UUID messageId = UUID.randomUUID();
        String bpId = "bpId";
        String messageType = "messageType";
        ScanStatus scanStatus = ScanStatus.SCAN_PENDING;

        this.storedMessage = "<content>test</content>";
        this.storedTags = new HashMap<>(Map.of(
                "bpId", bpId,
                "messageType", messageType,
                "scanStatus", scanStatus.name()
                // Missing: "saveTimeInMillis", saveTimeInMillis
        ));

        S3ObjectMalwareScanResultInfo internalScanResult = new S3ObjectMalwareScanResultInfo(ScanResult.FAILED, "bucketName", messageId.toString());
        messageExchangeService.onMalwareScanResult(internalScanResult);

        verify(eventPublisher, times(1)).publishMessageReceivedEvent(messageId, bpId, messageType, PublishedScanStatus.SCAN_FAILED);

        assertThat(storedTags).containsKeys("bpId", "messageType", "scanStatus");
        assertThat(storedTags).containsEntry("scanStatus", ScanStatus.SCAN_FAILED.name());

        verify(metricsService, never()).publishMetrics(any(ScanResult.class), anyLong(), anyLong());
    }

    @Test
    void handleMalwareScanResult_missingTags() {
        UUID messageId = UUID.randomUUID();

        this.storedMessage = "<content>test</content>";
        this.storedTags = new HashMap<>();

        S3ObjectMalwareScanResultInfo internalScanResult = new S3ObjectMalwareScanResultInfo(ScanResult.NO_THREATS_FOUND, "bucketName", messageId.toString());
        assertThrows(IllegalStateException.class, () -> messageExchangeService.onMalwareScanResult(internalScanResult));

        verify(eventPublisher, never()).publishMessageReceivedEvent(any(UUID.class), anyString(), anyString(), any(PublishedScanStatus.class));

        verify(metricsService, never()).publishMetrics(any(ScanResult.class), anyLong(), anyLong());
    }
}
