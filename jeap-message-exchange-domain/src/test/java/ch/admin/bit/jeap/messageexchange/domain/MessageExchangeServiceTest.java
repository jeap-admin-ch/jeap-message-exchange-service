package ch.admin.bit.jeap.messageexchange.domain;

import ch.admin.bit.jeap.messageexchange.domain.database.MessageRepository;
import ch.admin.bit.jeap.messageexchange.domain.dto.MessageSearchResultWithContentDto;
import ch.admin.bit.jeap.messageexchange.domain.exception.MalwareScanFailedOrBlockedException;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.MalwareScanProperties;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.PublishedScanStatus;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.S3ObjectMalwareScanResultInfo;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.ScanStatus;
import ch.admin.bit.jeap.messageexchange.domain.messaging.EventPublisher;
import ch.admin.bit.jeap.messageexchange.domain.metrics.MetricsService;
import ch.admin.bit.jeap.messageexchange.domain.objectstore.BucketType;
import ch.admin.bit.jeap.messageexchange.domain.objectstore.ObjectStore;
import ch.admin.bit.jeap.messageexchange.domain.objectstore.S3ObjectTagsUpdateResult;
import ch.admin.bit.jeap.messageexchange.domain.objectstore.S3ObjectTagsService;
import ch.admin.bit.jeap.messageexchange.domain.sent.MessageSentProperties;
import ch.admin.bit.jeap.messageexchange.domain.xml.InvalidXMLInputException;
import ch.admin.bit.jeap.messageexchange.malware.api.MalwareScanResult;
import ch.admin.bit.jeap.messageexchange.malware.api.MalwareScanTrigger;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
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
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;

@SuppressWarnings("unused")
@ExtendWith(MockitoExtension.class)
class MessageExchangeServiceTest {

    private static final String PARTNER_BUCKET_NAME = "the_bucket";

    private MessageExchangeService messageExchangeService;

    private String storedMessage;
    private Map<String, String> storedTags = new HashMap<>();

    @MockitoBean
    private final ObjectStore objectStore = new ObjectStore() {

        @Override
        public void storeMessage(BucketType bucketType, String objectKey, MessageContent messageContent, String contentType) throws IOException {
            storeMessage(bucketType, objectKey, messageContent, messageContent.tags(), contentType);
        }

        @SuppressWarnings("java:S1172")
        private void storeMessage(BucketType bucketType, String objectKey, MessageContent messageContent, Map<String, String> tags, String contentType) throws IOException {
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
        public S3ObjectTagsUpdateResult updateTagsAndGetTags(BucketType bucketType, String bucketName, String objectKey, Map<String, String> tagsToUpdate) {
            Map<String, String> previousTags = new HashMap<>(storedTags);
            storedTags.putAll(tagsToUpdate);
            return new S3ObjectTagsUpdateResult(storedTags, previousTags);
        }

        @Override
        public String getBucketName(BucketType bucketType) {
            return PARTNER_BUCKET_NAME;
        }

        @Override
        public Optional<String> getContentType(BucketType bucketType, String objectKey) {
            return Optional.of(MediaType.APPLICATION_XML_VALUE);
        }

        @Override
        public String getContentType(BucketType bucketType, String bucketName, String objectKey) {
            return MediaType.APPLICATION_XML_VALUE;
        }
    };

    @Mock
    private EventPublisher eventPublisher;

    @Mock
    private MessageRepository messageRepository;

    @Mock
    private MalwareScanProperties malwareScanProperties;

    @Mock
    private MessageSentProperties messageSentProperties;

    @Mock
    private MetricsService metricsService;

    @BeforeEach
    void setup() {
        messageExchangeService = new MessageExchangeService(objectStore, eventPublisher, messageRepository, malwareScanProperties, messageSentProperties, metricsService, new S3ObjectTagsService(), Optional.empty());
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

        verify(eventPublisher, times(1)).publishMessageReceivedEvent(messageId, bpId, messageType, null, null, PublishedScanStatus.NOT_SCANNED, MediaType.APPLICATION_XML_VALUE);
        assertThat(storedMessage).isEqualTo(xmlContentString);
        assertThat(storedTags)
                .containsKeys("bpId", "messageType", "scanStatus", "saveTimeInMillis")
                .containsEntry("scanStatus", ScanStatus.NOT_SCANNED.name());

        verify(messageRepository, never()).save(any(Message.class));
    }

    @Test
    @SneakyThrows
    void saveNewMessageFromPartnerMalwareScanEnabled_xmlValid_messageNotSaved() {
        UUID messageId = UUID.randomUUID();
        String bpId = "bpId";
        String messageType = "messageType";
        String xmlContentString = "<valid/>";
        InputStream xmlContent = new ByteArrayInputStream(xmlContentString.getBytes(UTF_8));

        when(malwareScanProperties.isEnabled()).thenReturn(true);

        messageExchangeService.saveNewMessageFromPartner(messageId, bpId, messageType, new MessageContent(xmlContent, 42));

        verify(eventPublisher, never()).publishMessageReceivedEvent(any(UUID.class), anyString(), anyString(), anyString(), anyString(), any(PublishedScanStatus.class), anyString());
        assertThat(storedMessage).isEqualTo(xmlContentString);
        assertThat(storedTags)
                .containsKeys("bpId", "messageType", "scanStatus", "saveTimeInMillis")
                .containsEntry("scanStatus", ScanStatus.SCAN_PENDING.name());

        verify(messageRepository, never()).save(any(Message.class));
    }

    @Test
    @SneakyThrows
    void saveNewMessageFromPartnerMalwareScanEnabledAndNotifierSet_xmlValid_messageNotSaved() {
        when(malwareScanProperties.isEnabled()).thenReturn(true);
        MalwareScanTrigger malwareScanTrigger = mock(MalwareScanTrigger.class);

        messageExchangeService = new MessageExchangeService(objectStore, eventPublisher, messageRepository, malwareScanProperties, messageSentProperties, metricsService, new S3ObjectTagsService(), Optional.of(malwareScanTrigger));

        UUID messageId = UUID.randomUUID();
        String bpId = "bpId";
        String messageType = "messageType";
        String xmlContentString = "<valid/>";
        InputStream xmlContent = new ByteArrayInputStream(xmlContentString.getBytes(UTF_8));

        messageExchangeService.saveNewMessageFromPartner(messageId, bpId, messageType, new MessageContent(xmlContent, 42));

        verify(malwareScanTrigger).triggerScan(eq(messageId.toString()), eq(PARTNER_BUCKET_NAME), any(InputStream.class), anyInt());

        verify(eventPublisher, never()).publishMessageReceivedEvent(any(UUID.class), anyString(), anyString(), anyString(), anyString(), any(PublishedScanStatus.class), anyString());
        assertThat(storedMessage).isEqualTo(xmlContentString);
        assertThat(storedTags)
                .containsKeys("bpId", "messageType", "scanStatus", "saveTimeInMillis")
                .containsEntry("scanStatus", ScanStatus.SCAN_PENDING.name());

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

        verify(eventPublisher, never()).publishMessageReceivedEvent(any(UUID.class), anyString(), anyString(), anyString(), anyString(), any(PublishedScanStatus.class), anyString());
        assertThat(storedMessage).isNull();
        verify(messageRepository, never()).save(any(Message.class));
    }

    @Test
    @SneakyThrows
    void saveNewMessageFromPartnerWithContentType_xmlInvalid_messageSaved() {
        UUID messageId = UUID.randomUUID();
        String bpId = "bpId";
        String messageType = "messageType";
        String xmlContentString = "<invalid<xml";
        InputStream xmlContent = new ByteArrayInputStream(xmlContentString.getBytes(UTF_8));

        MessageContent messageContent = new MessageContent(xmlContent, 42);

        messageExchangeService.saveNewMessageFromPartner(messageId, bpId, messageType, null, null, messageContent, MediaType.APPLICATION_XML_VALUE);

        verify(eventPublisher, times(1)).publishMessageReceivedEvent(messageId, bpId, messageType, null, null, PublishedScanStatus.NOT_SCANNED, MediaType.APPLICATION_XML_VALUE);
        assertThat(storedMessage).isEqualTo(xmlContentString);
        assertThat(storedTags)
                .containsKeys("bpId", "messageType", "scanStatus", "saveTimeInMillis")
                .containsEntry("scanStatus", ScanStatus.NOT_SCANNED.name());

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
                .contentType(MediaType.APPLICATION_XML_VALUE)
                .build();
        String xmlContentString = "<valid/>";
        InputStream xmlContent = new ByteArrayInputStream(xmlContentString.getBytes(UTF_8));

        messageExchangeService.saveNewMessageFromInternalApplication(message, new MessageContent(xmlContent, 42));

        assertThat(storedMessage).isEqualTo(xmlContentString);

        verify(messageRepository, times(1)).save(message);
        verifyNoInteractions(eventPublisher);
    }

    @Test
    @SneakyThrows
    void saveNewMessageFromInternalLegacy_xmlValid_messageSaved() {
        Message message = Message.builder()
                .messageId(UUID.randomUUID())
                .bpId("bpId")
                .messageType("messageType")
                .topicName("topicName")
                .contentType(MediaType.APPLICATION_XML_VALUE)
                .build();
        String xmlContentString = "<valid/>";
        InputStream xmlContent = new ByteArrayInputStream(xmlContentString.getBytes(UTF_8));

        messageExchangeService.saveNewMessageFromInternalApplicationLegacy(message, new MessageContent(xmlContent, 42));

        assertThat(storedMessage).isEqualTo(xmlContentString);

        verify(messageRepository, times(1)).save(message);
        verifyNoInteractions(eventPublisher);
    }

    @Test
    @SneakyThrows
    void saveNewMessageFromInternal_xmlValid_messageSaved_publish_message_sent_enabled() {
        when(messageSentProperties.isEnabled()).thenReturn(true);
        Message message = Message.builder()
                .messageId(UUID.randomUUID())
                .bpId("bpId")
                .messageType("messageType")
                .topicName("topicName")
                .contentType(MediaType.APPLICATION_XML_VALUE)
                .build();
        String xmlContentString = "<valid/>";
        InputStream xmlContent = new ByteArrayInputStream(xmlContentString.getBytes(UTF_8));

        messageExchangeService.saveNewMessageFromInternalApplication(message, new MessageContent(xmlContent, 42));

        assertThat(storedMessage).isEqualTo(xmlContentString);

        verify(messageRepository, times(1)).save(message);
        verify(eventPublisher, times(1)).publishMessageSentEvent(message);
        verifyNoMoreInteractions(eventPublisher);
    }

    @Test
    @SneakyThrows
    void saveNewMessageFromInternalLegacy_xmlValid_messageSaved_publish_message_sent_enabled() {
        when(messageSentProperties.isEnabled()).thenReturn(true);
        Message message = Message.builder()
                .messageId(UUID.randomUUID())
                .bpId("bpId")
                .messageType("messageType")
                .topicName("topicName")
                .contentType(MediaType.APPLICATION_XML_VALUE)
                .build();
        String xmlContentString = "<valid/>";
        InputStream xmlContent = new ByteArrayInputStream(xmlContentString.getBytes(UTF_8));

        messageExchangeService.saveNewMessageFromInternalApplicationLegacy(message, new MessageContent(xmlContent, 42));

        assertThat(storedMessage).isEqualTo(xmlContentString);

        verify(messageRepository, times(1)).save(message);
        verify(eventPublisher, times(1)).publishMessageSentEvent(message);
        verifyNoMoreInteractions(eventPublisher);
    }

    @Test
    @SneakyThrows
    void saveNewMessageFromInternal_xmlInvalid_messageSaved() {
        Message message = Message.builder()
                .messageId(UUID.randomUUID())
                .bpId("bpId")
                .messageType("messageType")
                .topicName("topicName")
                .contentType(MediaType.APPLICATION_XML_VALUE)
                .build();
        String xmlContentString = "<invalid<xml";
        InputStream xmlContent = new ByteArrayInputStream(xmlContentString.getBytes(UTF_8));

        messageExchangeService.saveNewMessageFromInternalApplication(message, new MessageContent(xmlContent, 42));

        assertThat(storedMessage).isEqualTo(xmlContentString);

        verify(messageRepository, times(1)).save(message);
        verifyNoInteractions(eventPublisher);
    }

    @Test
    @SneakyThrows
    void saveNewMessageFromInternalLegacy_xmlInvalid_messageNotSaved() {
        Message message = Message.builder()
                .messageId(UUID.randomUUID())
                .bpId("bpId")
                .messageType("messageType")
                .topicName("topicName")
                .contentType(MediaType.APPLICATION_XML_VALUE)
                .build();
        InputStream xmlContent = new ByteArrayInputStream("<invalid<xml".getBytes(UTF_8));

        MessageContent messageContent = new MessageContent(xmlContent, 42);

        assertThrows(InvalidXMLInputException.class, () -> messageExchangeService.saveNewMessageFromInternalApplicationLegacy(message, messageContent));

        verify(eventPublisher, never()).publishMessageReceivedEvent(any(UUID.class), anyString(), anyString(), anyString(), anyString(), any(PublishedScanStatus.class), anyString());
        assertThat(storedMessage).isNull();

        verify(messageRepository, never()).save(message);
    }


    @Test
    @SneakyThrows
    void getNextMessageFromInternalApplication_newMessageExists_thenReturnsContent() {
        UUID lastMessageId = UUID.randomUUID();
        String bpId = "bpId";
        UUID messageId = UUID.randomUUID();
        Message message = Message.builder()
                .messageId(messageId)
                .bpId(bpId)
                .messageType("messageType")
                .topicName("topicName")
                .contentType(MediaType.APPLICATION_XML_VALUE)
                .build();

        when(messageRepository.getNextMessage(lastMessageId, bpId, null, null, null)).thenReturn(Optional.of(message));
        this.storedMessage = "<content>test</content>";

        Optional<MessageSearchResultWithContentDto> nextMessageContent = messageExchangeService.getNextMessageFromInternalApplication(lastMessageId, bpId, null, null, null);

        assertThat(nextMessageContent).isPresent();
        assertThat(nextMessageContent.get().messageId()).isEqualTo(messageId);
        assertThat(nextMessageContent.get().messageContent().inputStream().readAllBytes()).isEqualTo(storedMessage.getBytes(UTF_8));
    }

    @Test
    @SneakyThrows
    void getNextMessageFromInternalApplication_noNewMessageExists_thenReturnsEmpty() {
        UUID lastMessageId = UUID.randomUUID();
        String bpId = "bpId";

        Optional<MessageSearchResultWithContentDto> nextMessageContent = messageExchangeService.getNextMessageFromInternalApplication(lastMessageId, bpId, null, null, null);

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

        S3ObjectMalwareScanResultInfo internalScanResult = new S3ObjectMalwareScanResultInfo(MalwareScanResult.NO_THREATS_FOUND, "bucketName", messageId.toString());
        messageExchangeService.onMalwareScanResult(internalScanResult);

        verify(eventPublisher, times(1)).publishMessageReceivedEvent(messageId, bpId, messageType, null, null, PublishedScanStatus.NO_THREATS_FOUND, MediaType.APPLICATION_XML_VALUE);

        assertThat(storedTags)
                .containsKeys("bpId", "messageType", "scanStatus", "saveTimeInMillis")
                .containsEntry("scanStatus", ScanStatus.NO_THREATS_FOUND.name());

        verify(metricsService, times(1)).publishMetrics(eq(MalwareScanResult.NO_THREATS_FOUND), anyLong(), eq(Long.parseLong(saveTimeInMillis)));
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

        S3ObjectMalwareScanResultInfo internalScanResult = new S3ObjectMalwareScanResultInfo(MalwareScanResult.FAILED, "bucketName", messageId.toString());
        messageExchangeService.onMalwareScanResult(internalScanResult);

        verify(eventPublisher, times(1)).publishMessageReceivedEvent(messageId, bpId, messageType, null, null, PublishedScanStatus.SCAN_FAILED, MediaType.APPLICATION_XML_VALUE);

        assertThat(storedTags)
                .containsKeys("bpId", "messageType", "scanStatus")
                .containsEntry("scanStatus", ScanStatus.SCAN_FAILED.name());

        verify(metricsService, never()).publishMetrics(any(MalwareScanResult.class), anyLong(), anyLong());
    }

    @Test
    void onMalwareScanResult_ScanPendingAndScanDisabled_shouldPublishEvent() {
        // Scenario: Message was received with scanning ON (SCAN_PENDING), scan result arrives after scanning turned OFF.
        // The message received event must be published.
        UUID messageId = storeTestMessageInStatus(ScanStatus.SCAN_PENDING);

        S3ObjectMalwareScanResultInfo scanResult = new S3ObjectMalwareScanResultInfo(MalwareScanResult.NO_THREATS_FOUND, "bucketName", messageId.toString());
        // the malwareScanProperties mock returns false by default for isEnabled() -> scanning is turned off
        messageExchangeService.onMalwareScanResult(scanResult);

        verify(eventPublisher, times(1)).publishMessageReceivedEvent(messageId, "bpId", "messageType", null, null, PublishedScanStatus.NO_THREATS_FOUND, MediaType.APPLICATION_XML_VALUE);
    }

    @Test
    void onMalwareScanResult_NotScanned_shouldNotPublishEventAgain() {
        // Scenario: Message was received with scanning OFF (NOT_SCANNED) but still got scanned for malware without any threats found.
        // The message received event was already published when the message was received, so it should NOT be published again.
        UUID messageId = storeTestMessageInStatus(ScanStatus.NOT_SCANNED);

        S3ObjectMalwareScanResultInfo scanResult = new S3ObjectMalwareScanResultInfo(MalwareScanResult.NO_THREATS_FOUND, "bucketName", messageId.toString());
        messageExchangeService.onMalwareScanResult(scanResult);

        verify(eventPublisher, never()).publishMessageReceivedEvent(any(UUID.class), anyString(), anyString(), anyString(), anyString(), any(PublishedScanStatus.class), anyString());
    }

    @Test
    void onMalwareScanResult_NotScannedButThreatsFound_shouldPublishEvent() {
        // Scenario: Message was received with scanning OFF (NOT_SCANNED), but still got scanned for malware with threats found.
        // Threats should always be notified, even if the message received event was already published before as NOT_SCANNED.
        UUID messageId = storeTestMessageInStatus(ScanStatus.NOT_SCANNED);

        S3ObjectMalwareScanResultInfo internalScanResult = new S3ObjectMalwareScanResultInfo(MalwareScanResult.THREATS_FOUND, "bucketName", messageId.toString());
        messageExchangeService.onMalwareScanResult(internalScanResult);

        verify(eventPublisher, times(1)).publishMessageReceivedEvent(messageId, "bpId", "messageType", null, null, PublishedScanStatus.THREATS_FOUND, MediaType.APPLICATION_XML_VALUE);
    }

    private UUID storeTestMessageInStatus(ScanStatus scanStatus) {
        UUID messageId = UUID.randomUUID();
        String saveTimeInMillis = String.valueOf(System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(2));
        this.storedMessage = "<content>test</content>";
        this.storedTags = new HashMap<>(Map.of(
                "bpId", "bpId",
                "messageType", "messageType",
                "scanStatus", scanStatus.name(),
                "saveTimeInMillis", saveTimeInMillis
        ));
        return messageId;
    }

    @Test
    void handleMalwareScanResult_missingTags() {
        UUID messageId = UUID.randomUUID();

        this.storedMessage = "<content>test</content>";
        this.storedTags = new HashMap<>();

        S3ObjectMalwareScanResultInfo internalScanResult = new S3ObjectMalwareScanResultInfo(MalwareScanResult.NO_THREATS_FOUND, "bucketName", messageId.toString());
        assertThrows(IllegalStateException.class, () -> messageExchangeService.onMalwareScanResult(internalScanResult));

        verify(eventPublisher, never()).publishMessageReceivedEvent(any(UUID.class), anyString(), anyString(), anyString(), anyString(), any(PublishedScanStatus.class), anyString());

        verify(metricsService, never()).publishMetrics(any(MalwareScanResult.class), anyLong(), anyLong());
    }

    static Stream<Arguments> deliverableWhenMalwareScanDisabled() {
        return Stream.of(
                Arguments.of(ScanStatus.NOT_SCANNED, true, "NOT_SCANNED should always be deliverable"),
                Arguments.of(ScanStatus.NO_THREATS_FOUND, true, "NO_THREATS_FOUND should always be deliverable"),
                Arguments.of(ScanStatus.SCAN_PENDING, true, "SCAN_PENDING should be deliverable when scanning is disabled"),
                Arguments.of(ScanStatus.SCAN_FAILED, true, "SCAN_FAILED should be deliverable when scanning is disabled"),
                Arguments.of(ScanStatus.THREATS_FOUND, false, "THREATS_FOUND should never be deliverable")
        );
    }

    @ParameterizedTest(name = "malware scan disabled, {0}: shouldDeliver={1}")
    @MethodSource("deliverableWhenMalwareScanDisabled")
    void getMessageFromPartner_malwareScanDisabled(ScanStatus scanStatus, boolean shouldDeliver, String description) {
        UUID messageId = UUID.randomUUID();
        prepareStoredMessageWithScanStatus(scanStatus);
        // Note: mockito mock malwareScanProperties returns false by default for isEnabled()

        if (shouldDeliver) {
            Optional<MessageContent> result = messageExchangeService.getMessageFromPartner(messageId);
            assertThat(result).as(description).isPresent();
        } else {
            assertThrows(MalwareScanFailedOrBlockedException.class,
                    () -> messageExchangeService.getMessageFromPartner(messageId), description);
        }
    }

    static Stream<Arguments> deliverableWhenMalwareScanEnabled() {
        return Stream.of(
                Arguments.of(ScanStatus.NOT_SCANNED, true, false, "NOT_SCANNED should always be deliverable"),
                Arguments.of(ScanStatus.NO_THREATS_FOUND, true, false, "NO_THREATS_FOUND should always be deliverable"),
                Arguments.of(ScanStatus.SCAN_PENDING, false, true, "SCAN_PENDING should not be deliverable when scanning is enabled"),
                Arguments.of(ScanStatus.SCAN_FAILED, false, true, "SCAN_FAILED should not be deliverable when scanning is enabled"),
                Arguments.of(ScanStatus.THREATS_FOUND, false, false, "THREATS_FOUND should never be deliverable")
        );
    }

    @ParameterizedTest(name = "malware scan enabled, {0}: shouldDeliver={1}")
    @MethodSource("deliverableWhenMalwareScanEnabled")
    void getMessageFromPartner_malwareScanEnabled(ScanStatus scanStatus, boolean shouldDeliver, boolean malwareScanPropsStubbingRequired, String description) {
        UUID messageId = UUID.randomUUID();
        prepareStoredMessageWithScanStatus(scanStatus);
        if (malwareScanPropsStubbingRequired) {
            when(malwareScanProperties.isEnabled()).thenReturn(true);
        }

        if (shouldDeliver) {
            Optional<MessageContent> result = messageExchangeService.getMessageFromPartner(messageId);
            assertThat(result).as(description).isPresent();
        } else {
            assertThrows(MalwareScanFailedOrBlockedException.class,
                    () -> messageExchangeService.getMessageFromPartner(messageId), description);
        }
    }

    private void prepareStoredMessageWithScanStatus(ScanStatus scanStatus) {
        this.storedMessage = "<content>test</content>";
        S3ObjectTagsService tagsService = new S3ObjectTagsService();
        this.storedTags = new HashMap<>(tagsService.toMap("some-bp-id", "some-message-type", null, null, scanStatus, System.currentTimeMillis()));
    }
}
