package ch.admin.bit.jeap.messageexchange.domain;

import ch.admin.bit.jeap.messageexchange.domain.database.InboundMessageRepository;
import ch.admin.bit.jeap.messageexchange.domain.database.MessageRepository;
import ch.admin.bit.jeap.messageexchange.domain.dto.MessageSearchResultWithContentDto;
import ch.admin.bit.jeap.messageexchange.domain.exception.MalwareScanFailedOrBlockedException;
import ch.admin.bit.jeap.messageexchange.domain.exception.MismatchedContentException;
import ch.admin.bit.jeap.messageexchange.domain.exception.MismatchedContentTypeException;
import ch.admin.bit.jeap.messageexchange.domain.legacy.LegacyS3ObjectTagsFactory;
import ch.admin.bit.jeap.messageexchange.domain.legacy.LegacyS3ObjectTagsParser;
import ch.admin.bit.jeap.messageexchange.domain.legacy.LegacyS3TagFallbackService;
import ch.admin.bit.jeap.messageexchange.domain.legacy.LegacyTagCompatibilityProperties;
import ch.admin.bit.jeap.messageexchange.domain.legacy.ObjectHead;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.MalwareScanProperties;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.PublishedScanStatus;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.S3ObjectMalwareScanResultInfo;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.ScanStatus;
import ch.admin.bit.jeap.messageexchange.domain.messaging.EventPublisher;
import ch.admin.bit.jeap.messageexchange.domain.metrics.MetricsService;
import ch.admin.bit.jeap.messageexchange.domain.objectstore.BucketType;
import ch.admin.bit.jeap.messageexchange.domain.objectstore.ObjectStore;
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
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
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

@SuppressWarnings("unused")
@ExtendWith(MockitoExtension.class)
class MessageExchangeServiceTest {

    private static final String PARTNER_BUCKET_NAME = "the_bucket";
    private static final String BP_ID = "bpId";
    private static final String MESSAGE_TYPE = "messageType";

    private MessageExchangeService messageExchangeService;

    private String storedMessage;
    // simulates the S3 object tags: written by tests to fabricate legacy (< 11.0.0) message state and by the
    // transitional scanStatus tag update after a malware scan result
    private Map<String, String> storedTags = new HashMap<>();
    private Map<String, String> tagsPassedToStoreMessage;
    private boolean failTagUpdate;

    private final ObjectStore objectStore = new ObjectStore() {

        @Override
        public void storeMessage(BucketType bucketType, String objectKey, MessageContent messageContent, String contentType) throws IOException {
            try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
                messageContent.inputStream().transferTo(os);
                storedMessage = os.toString(UTF_8);
            }
            tagsPassedToStoreMessage = messageContent.tags();
        }

        @Override
        public Optional<MessageContent> loadMessage(BucketType bucketType, String objectKey) {
            if (storedMessage == null) {
                return Optional.empty();
            }
            return Optional.of(new MessageContent(new ByteArrayInputStream(storedMessage.getBytes(UTF_8)), storedMessage.getBytes(UTF_8).length));
        }

        @Override
        public Optional<MessageContent> loadMessageWithTags(BucketType bucketType, String objectKey) {
            if (storedMessage == null) {
                return Optional.empty();
            }
            return Optional.of(new MessageContent(new ByteArrayInputStream(storedMessage.getBytes(UTF_8)), storedMessage.getBytes(UTF_8).length, storedTags));
        }

        @Override
        public Map<String, String> getObjectTags(BucketType bucketType, String objectKey) {
            return storedTags;
        }

        @Override
        public Optional<ObjectHead> getObjectHead(BucketType bucketType, String objectKey) {
            if (storedMessage == null) {
                return Optional.empty();
            }
            return Optional.of(new ObjectHead(MediaType.APPLICATION_XML_VALUE, storedMessage.getBytes(UTF_8).length));
        }

        @Override
        public String getBucketName(BucketType bucketType) {
            return PARTNER_BUCKET_NAME;
        }

        @Override
        public Optional<String> getContentType(BucketType bucketType, String objectKey) {
            if (storedMessage == null) {
                return Optional.empty();
            }
            return Optional.of(MediaType.APPLICATION_XML_VALUE);
        }

        @Override
        public void updateTags(BucketType bucketType, String objectKey, Map<String, String> tagsToUpdate) {
            if (failTagUpdate) {
                throw new IllegalStateException("simulated tag update failure");
            }
            storedTags.putAll(tagsToUpdate);
        }
    };

    @Mock
    private EventPublisher eventPublisher;

    @Mock
    private MessageRepository messageRepository;

    @Mock
    private InboundMessageRepository inboundMessageRepository;

    @Mock
    private MalwareScanProperties malwareScanProperties;

    @Mock
    private MessageSentProperties messageSentProperties;

    @Mock
    private MetricsService metricsService;

    @BeforeEach
    @SneakyThrows
    void setup() {
        messageExchangeService = createMessageExchangeService(Optional.empty());
    }

    private MessageExchangeService createMessageExchangeService(Optional<MalwareScanTrigger> malwareScanTrigger) {
        return createMessageExchangeService(malwareScanTrigger, true);
    }

    private MessageExchangeService createMessageExchangeService(Optional<MalwareScanTrigger> malwareScanTrigger, boolean legacyTagCompatibilityEnabled) {
        LegacyS3ObjectTagsParser legacyTagsParser = new LegacyS3ObjectTagsParser();
        LegacyS3TagFallbackService legacyTagFallbackService = new LegacyS3TagFallbackService(objectStore, inboundMessageRepository, legacyTagsParser);
        LegacyTagCompatibilityProperties legacyTagCompatibilityProperties = new LegacyTagCompatibilityProperties();
        legacyTagCompatibilityProperties.setEnabled(legacyTagCompatibilityEnabled);
        LegacyS3ObjectTagsFactory legacyTagsFactory = new LegacyS3ObjectTagsFactory(legacyTagCompatibilityProperties);
        return new MessageExchangeService(objectStore, eventPublisher, messageRepository, inboundMessageRepository,
                malwareScanProperties, messageSentProperties, metricsService, legacyTagsParser, legacyTagFallbackService, legacyTagsFactory, malwareScanTrigger);
    }

    @Test
    @SneakyThrows
    void saveNewMessageFromPartner_xmlValid_messageSaved() {
        UUID messageId = UUID.randomUUID();
        String xmlContentString = "<valid/>";
        InputStream xmlContent = new ByteArrayInputStream(xmlContentString.getBytes(UTF_8));

        messageExchangeService.saveNewMessageFromPartner(messageId, BP_ID, MESSAGE_TYPE, new MessageContent(xmlContent, 42));

        verify(eventPublisher, times(1)).publishMessageReceivedEvent(messageId, BP_ID, MESSAGE_TYPE, null, null, PublishedScanStatus.NOT_SCANNED, MediaType.APPLICATION_XML_VALUE);
        assertThat(storedMessage).isEqualTo(xmlContentString);
        // the authoritative metadata is persisted in the database; the v10-compatible metadata tags are still
        // written atomically at object creation (LEGACY-TAG-FALLBACK, JEAP-7252)
        assertLegacyUploadTags(ScanStatus.NOT_SCANNED);

        // a new message keeps a terminal scan verdict that raced ahead of this upsert
        ArgumentCaptor<InboundMessage> captor = ArgumentCaptor.forClass(InboundMessage.class);
        verify(inboundMessageRepository).upsertScanStatusAndMetadataKeepingTerminalStatus(captor.capture());
        verify(inboundMessageRepository, never()).upsertScanStatusAndMetadata(any(InboundMessage.class));
        InboundMessage capturedInboundMessage = captor.getValue();
        assertThat(capturedInboundMessage.getBpId()).isEqualTo(BP_ID);
        assertThat(capturedInboundMessage.getMessageId()).isEqualTo(messageId);
        assertThat(capturedInboundMessage.getContentLength()).isEqualTo(42);
        assertThat(capturedInboundMessage.getMessageType()).isEqualTo(MESSAGE_TYPE);
        assertThat(capturedInboundMessage.getContentType()).isEqualTo(MediaType.APPLICATION_XML_VALUE);
        assertThat(capturedInboundMessage.getScanStatus()).isEqualTo(ScanStatus.NOT_SCANNED);

        verify(messageRepository, never()).save(any(Message.class));
    }

    @Test
    @SneakyThrows
    void saveNewMessageFromPartner_messageIdAlreadySent_contentMatches_messageNotSaved() {
        UUID messageId = UUID.randomUUID();
        String xmlContentString = "<valid/>";
        InputStream xmlContent = new ByteArrayInputStream(xmlContentString.getBytes(UTF_8));
        InboundMessage inboundMessage = InboundMessage.builder().bpId(BP_ID).messageId(messageId).contentLength(42).build();
        when(inboundMessageRepository.findByBpIdAndMessageId(BP_ID, messageId)).thenReturn(Optional.of(inboundMessage));
        objectStore.storeMessage(BucketType.PARTNER, messageId.toString(),
                new MessageContent(new ByteArrayInputStream(xmlContentString.getBytes(UTF_8)), xmlContentString.getBytes(UTF_8).length),
                MediaType.APPLICATION_XML_VALUE);
        messageExchangeService.saveNewMessageFromPartner(messageId, BP_ID, MESSAGE_TYPE, new MessageContent(xmlContent, 42));

        verify(eventPublisher, never()).publishMessageReceivedEvent(messageId, BP_ID, MESSAGE_TYPE, null, null, PublishedScanStatus.NOT_SCANNED, MediaType.APPLICATION_XML_VALUE);

        verifyNoInboundMessageUpserted();
        verify(messageRepository, never()).save(any(Message.class));
    }

    @Test
    @SneakyThrows
    void saveNewMessageFromPartner_messageIdAlreadySent_contentLengthDoesNotMatch_throwsException() {
        UUID messageId = UUID.randomUUID();
        String xmlContentString = "<valid/>";
        InputStream xmlContent = new ByteArrayInputStream(xmlContentString.getBytes(UTF_8));
        InboundMessage inboundMessage = InboundMessage.builder().bpId(BP_ID).messageId(messageId).contentLength(404).build();
        when(inboundMessageRepository.findByBpIdAndMessageId(BP_ID, messageId)).thenReturn(Optional.of(inboundMessage));

        MismatchedContentException mismatchedContentException = assertThrows(MismatchedContentException.class, () -> messageExchangeService.saveNewMessageFromPartner(messageId, BP_ID, MESSAGE_TYPE, new MessageContent(xmlContent, 42)));
        assertThat(mismatchedContentException.getMessage()).contains("Message with messageId " + messageId + " already exists but has different content length");

        verifyNoInboundMessageUpserted();
    }

    @Test
    @SneakyThrows
    void saveNewMessageFromPartner_messageIdAlreadySent_contentDoesNotMatch_throwsException() {
        UUID messageId = UUID.randomUUID();
        String xmlContentString = "<valid/>";
        InputStream xmlContent = new ByteArrayInputStream(xmlContentString.getBytes(UTF_8));
        InboundMessage inboundMessage = InboundMessage.builder().bpId(BP_ID).messageId(messageId).contentLength(42).build();
        objectStore.storeMessage(BucketType.PARTNER, messageId.toString(),
                new MessageContent(new ByteArrayInputStream("<different>content</different>".getBytes(UTF_8)), "<different>content</different>".getBytes(UTF_8).length),
                MediaType.APPLICATION_XML_VALUE);
        when(inboundMessageRepository.findByBpIdAndMessageId(BP_ID, messageId)).thenReturn(Optional.of(inboundMessage));

        MismatchedContentException mismatchedContentException = assertThrows(MismatchedContentException.class, () -> messageExchangeService.saveNewMessageFromPartner(messageId, BP_ID, MESSAGE_TYPE, new MessageContent(xmlContent, 42)));
        assertThat(mismatchedContentException.getMessage()).contains("Message with messageId " + messageId + " already exists but has different content");

        verifyNoInboundMessageUpserted();
    }

    @Test
    @SneakyThrows
    void saveNewMessageFromPartner_messageIdAlreadySent_contentNotFoundInS3_messageSavedAgain() {
        UUID messageId = UUID.randomUUID();
        String xmlContentString = "<valid/>";
        InputStream xmlContent = new ByteArrayInputStream(xmlContentString.getBytes(UTF_8));
        InboundMessage inboundMessage = InboundMessage.builder().bpId(BP_ID).messageId(messageId).contentLength(42).build();
        when(inboundMessageRepository.findByBpIdAndMessageId(BP_ID, messageId)).thenReturn(Optional.of(inboundMessage));
        // the row must be reset before the replaced object is stored, so a scan result for the new object can
        // never be reverted afterwards
        doAnswer(invocation -> {
            assertThat(storedMessage).as("row reset must happen before the object is stored").isNull();
            return null;
        }).when(inboundMessageRepository).upsertScanStatusAndMetadata(any(InboundMessage.class));

        messageExchangeService.saveNewMessageFromPartner(messageId, BP_ID, MESSAGE_TYPE, new MessageContent(xmlContent, 42));
        verify(eventPublisher).publishMessageReceivedEvent(messageId, BP_ID, MESSAGE_TYPE, null, null, PublishedScanStatus.NOT_SCANNED, MediaType.APPLICATION_XML_VALUE);
        assertThat(storedMessage).isEqualTo(xmlContentString);
        assertLegacyUploadTags(ScanStatus.NOT_SCANNED);

        // the existing database row is refreshed with the metadata and initial scan status of the re-saved
        // message - unconditionally, as the replaced content is scanned anew
        ArgumentCaptor<InboundMessage> captor = ArgumentCaptor.forClass(InboundMessage.class);
        verify(inboundMessageRepository).upsertScanStatusAndMetadata(captor.capture());
        verify(inboundMessageRepository, never()).upsertScanStatusAndMetadataKeepingTerminalStatus(any(InboundMessage.class));
        assertThat(captor.getValue().getMessageType()).isEqualTo(MESSAGE_TYPE);
        assertThat(captor.getValue().getScanStatus()).isEqualTo(ScanStatus.NOT_SCANNED);
    }

    @Test
    @SneakyThrows
    void saveNewMessageFromPartnerMalwareScanEnabled_xmlValid_messageSaved() {
        UUID messageId = UUID.randomUUID();
        String xmlContentString = "<valid/>";
        InputStream xmlContent = new ByteArrayInputStream(xmlContentString.getBytes(UTF_8));

        when(malwareScanProperties.isEnabled()).thenReturn(true);

        messageExchangeService.saveNewMessageFromPartner(messageId, BP_ID, MESSAGE_TYPE, new MessageContent(xmlContent, 42));

        verify(eventPublisher, never()).publishMessageReceivedEvent(any(UUID.class), anyString(), anyString(), anyString(), anyString(), any(PublishedScanStatus.class), anyString());
        assertThat(storedMessage).isEqualTo(xmlContentString);
        assertLegacyUploadTags(ScanStatus.SCAN_PENDING);

        ArgumentCaptor<InboundMessage> captor = ArgumentCaptor.forClass(InboundMessage.class);
        verify(inboundMessageRepository).upsertScanStatusAndMetadataKeepingTerminalStatus(captor.capture());
        assertThat(captor.getValue().getScanStatus()).isEqualTo(ScanStatus.SCAN_PENDING);

        verify(messageRepository, never()).save(any(Message.class));
    }

    @Test
    @SneakyThrows
    void saveNewMessageFromPartnerMalwareScanEnabledAndNotifierSet_xmlValid_scanTriggeredAfterDatabaseSave() {
        when(malwareScanProperties.isEnabled()).thenReturn(true);
        MalwareScanTrigger malwareScanTrigger = mock(MalwareScanTrigger.class);
        // the database record must already exist when the scan is triggered: the scan result handler reads it
        doAnswer(invocation -> {
            verify(inboundMessageRepository).upsertScanStatusAndMetadataKeepingTerminalStatus(any(InboundMessage.class));
            return null;
        }).when(malwareScanTrigger).triggerScan(anyString(), anyString());

        messageExchangeService = createMessageExchangeService(Optional.of(malwareScanTrigger));

        UUID messageId = UUID.randomUUID();
        String xmlContentString = "<valid/>";
        InputStream xmlContent = new ByteArrayInputStream(xmlContentString.getBytes(UTF_8));

        messageExchangeService.saveNewMessageFromPartner(messageId, BP_ID, MESSAGE_TYPE, new MessageContent(xmlContent, 42));

        verify(malwareScanTrigger).triggerScan(messageId.toString(), PARTNER_BUCKET_NAME);

        verify(eventPublisher, never()).publishMessageReceivedEvent(any(UUID.class), anyString(), anyString(), anyString(), anyString(), any(PublishedScanStatus.class), anyString());
        assertThat(storedMessage).isEqualTo(xmlContentString);
        assertLegacyUploadTags(ScanStatus.SCAN_PENDING);

        verify(messageRepository, never()).save(any(Message.class));
    }

    @Test
    @SneakyThrows
    void saveNewMessageFromPartner_xmlInvalid_messageNotSaved() {
        UUID messageId = UUID.randomUUID();
        InputStream xmlContent = new ByteArrayInputStream("<invalid<xml".getBytes(UTF_8));

        MessageContent messageContent = new MessageContent(xmlContent, 42);

        assertThrows(InvalidXMLInputException.class, () -> messageExchangeService.saveNewMessageFromPartner(messageId, BP_ID, MESSAGE_TYPE, messageContent));

        verify(eventPublisher, never()).publishMessageReceivedEvent(any(UUID.class), anyString(), anyString(), anyString(), anyString(), any(PublishedScanStatus.class), anyString());
        assertThat(storedMessage).isNull();

        verifyNoInboundMessageUpserted();

        verify(messageRepository, never()).save(any(Message.class));
    }

    @Test
    @SneakyThrows
    void saveNewMessageFromPartnerWithContentType_xmlInvalid_messageSaved() {
        UUID messageId = UUID.randomUUID();
        String xmlContentString = "<invalid<xml";
        InputStream xmlContent = new ByteArrayInputStream(xmlContentString.getBytes(UTF_8));

        MessageContent messageContent = new MessageContent(xmlContent, 42);

        messageExchangeService.saveNewMessageFromPartner(messageId, BP_ID, MESSAGE_TYPE, null, null, messageContent, MediaType.APPLICATION_XML_VALUE);

        verify(eventPublisher, times(1)).publishMessageReceivedEvent(messageId, BP_ID, MESSAGE_TYPE, null, null, PublishedScanStatus.NOT_SCANNED, MediaType.APPLICATION_XML_VALUE);
        assertThat(storedMessage).isEqualTo(xmlContentString);
        assertLegacyUploadTags(ScanStatus.NOT_SCANNED);

        verify(inboundMessageRepository).upsertScanStatusAndMetadataKeepingTerminalStatus(any(InboundMessage.class));

        verify(messageRepository, never()).save(any(Message.class));
    }

    @Test
    @SneakyThrows
    void saveNewMessageFromInternal_xmlValid_messageSaved() {
        Message message = Message.builder()
                .messageId(UUID.randomUUID())
                .bpId(BP_ID)
                .messageType(MESSAGE_TYPE)
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
                .bpId(BP_ID)
                .messageType(MESSAGE_TYPE)
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
                .bpId(BP_ID)
                .messageType(MESSAGE_TYPE)
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
                .bpId(BP_ID)
                .messageType(MESSAGE_TYPE)
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
                .bpId(BP_ID)
                .messageType(MESSAGE_TYPE)
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
                .bpId(BP_ID)
                .messageType(MESSAGE_TYPE)
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
        UUID messageId = UUID.randomUUID();
        Message message = Message.builder()
                .messageId(messageId)
                .bpId(BP_ID)
                .messageType(MESSAGE_TYPE)
                .topicName("topicName")
                .contentType(MediaType.APPLICATION_XML_VALUE)
                .build();

        when(messageRepository.getNextMessage(lastMessageId, BP_ID, null, null, null)).thenReturn(Optional.of(message));
        this.storedMessage = "<content>test</content>";

        Optional<MessageSearchResultWithContentDto> nextMessageContent = messageExchangeService.getNextMessageFromInternalApplication(lastMessageId, BP_ID, null, null, null);

        assertThat(nextMessageContent).isPresent();
        assertThat(nextMessageContent.get().messageId()).isEqualTo(messageId);
        assertThat(nextMessageContent.get().messageContent().inputStream().readAllBytes()).isEqualTo(storedMessage.getBytes(UTF_8));
    }

    @Test
    @SneakyThrows
    void getNextMessageFromInternalApplication_noNewMessageExists_thenReturnsEmpty() {
        UUID lastMessageId = UUID.randomUUID();

        Optional<MessageSearchResultWithContentDto> nextMessageContent = messageExchangeService.getNextMessageFromInternalApplication(lastMessageId, BP_ID, null, null, null);

        assertThat(nextMessageContent).isEmpty();
    }

    // Malware scan result handling for messages stored by MES >= 11.0.0: scan status is managed in the database

    @Test
    void onMalwareScanResult_databaseRow_noThreatsFound_publishesEventFromDatabaseAndUpdatesLegacyScanStatusTag() {
        UUID messageId = UUID.randomUUID();
        this.storedMessage = "<content>test</content>";
        this.storedTags = new HashMap<>();
        InboundMessage previousState = inboundMessage(messageId, ScanStatus.SCAN_PENDING);
        when(inboundMessageRepository.findLatestByMessageId(messageId)).thenReturn(Optional.of(previousState));
        when(inboundMessageRepository.updateScanStatusReturningPreviousState(messageId, ScanStatus.NO_THREATS_FOUND))
                .thenReturn(Optional.of(previousState));

        S3ObjectMalwareScanResultInfo scanResult = new S3ObjectMalwareScanResultInfo(MalwareScanResult.NO_THREATS_FOUND, PARTNER_BUCKET_NAME, messageId.toString());
        messageExchangeService.onMalwareScanResult(scanResult);

        verify(eventPublisher, times(1)).publishMessageReceivedEvent(messageId, BP_ID, MESSAGE_TYPE, "partnerTopic", "partnerExternalReference", PublishedScanStatus.NO_THREATS_FOUND, MediaType.APPLICATION_XML_VALUE);
        verify(metricsService, times(1)).publishMetrics(eq(MalwareScanResult.NO_THREATS_FOUND), anyLong(), eq(previousState.getCreatedAtEpochMillis()));
        // the scanStatus tag is updated exactly like MES < 11.0.0 did, keeping 10.x instances working during
        // a rolling deployment (LEGACY-TAG-FALLBACK, JEAP-7252)
        assertThat(storedTags).containsExactlyEntriesOf(Map.of("scanStatus", ScanStatus.NO_THREATS_FOUND.name()));
        verifyNoInboundMessageUpserted();
    }

    @Test
    void onMalwareScanResult_databaseRow_legacyTagCompatibilityDisabled_leavesTagsUntouched() {
        messageExchangeService = createMessageExchangeService(Optional.empty(), false);
        UUID messageId = UUID.randomUUID();
        this.storedMessage = "<content>test</content>";
        this.storedTags = new HashMap<>();
        when(inboundMessageRepository.findLatestByMessageId(messageId)).thenReturn(Optional.of(inboundMessage(messageId, ScanStatus.SCAN_PENDING)));
        when(inboundMessageRepository.updateScanStatusReturningPreviousState(messageId, ScanStatus.NO_THREATS_FOUND))
                .thenReturn(Optional.of(inboundMessage(messageId, ScanStatus.SCAN_PENDING)));

        messageExchangeService.onMalwareScanResult(new S3ObjectMalwareScanResultInfo(MalwareScanResult.NO_THREATS_FOUND, PARTNER_BUCKET_NAME, messageId.toString()));

        verify(eventPublisher, times(1)).publishMessageReceivedEvent(messageId, BP_ID, MESSAGE_TYPE, "partnerTopic", "partnerExternalReference", PublishedScanStatus.NO_THREATS_FOUND, MediaType.APPLICATION_XML_VALUE);
        assertThat(storedTags).isEmpty();
    }

    @Test
    void onMalwareScanResult_legacyScanStatusTagUpdateFails_stillPublishesEvent() {
        // the tag update is best effort: the database is authoritative, and a failed tag update must not lose
        // the received event
        UUID messageId = UUID.randomUUID();
        this.storedMessage = "<content>test</content>";
        this.failTagUpdate = true;
        when(inboundMessageRepository.findLatestByMessageId(messageId)).thenReturn(Optional.of(inboundMessage(messageId, ScanStatus.SCAN_PENDING)));
        when(inboundMessageRepository.updateScanStatusReturningPreviousState(messageId, ScanStatus.NO_THREATS_FOUND))
                .thenReturn(Optional.of(inboundMessage(messageId, ScanStatus.SCAN_PENDING)));

        messageExchangeService.onMalwareScanResult(new S3ObjectMalwareScanResultInfo(MalwareScanResult.NO_THREATS_FOUND, PARTNER_BUCKET_NAME, messageId.toString()));

        verify(eventPublisher, times(1)).publishMessageReceivedEvent(messageId, BP_ID, MESSAGE_TYPE, "partnerTopic", "partnerExternalReference", PublishedScanStatus.NO_THREATS_FOUND, MediaType.APPLICATION_XML_VALUE);
    }

    @Test
    void onMalwareScanResult_databaseRow_previousStatusNotScanned_shouldNotPublishEventAgain() {
        UUID messageId = UUID.randomUUID();
        when(inboundMessageRepository.findLatestByMessageId(messageId)).thenReturn(Optional.of(inboundMessage(messageId, ScanStatus.NOT_SCANNED)));
        when(inboundMessageRepository.updateScanStatusReturningPreviousState(messageId, ScanStatus.NO_THREATS_FOUND))
                .thenReturn(Optional.of(inboundMessage(messageId, ScanStatus.NOT_SCANNED)));

        messageExchangeService.onMalwareScanResult(new S3ObjectMalwareScanResultInfo(MalwareScanResult.NO_THREATS_FOUND, PARTNER_BUCKET_NAME, messageId.toString()));

        verify(eventPublisher, never()).publishMessageReceivedEvent(any(UUID.class), anyString(), anyString(), anyString(), anyString(), any(PublishedScanStatus.class), anyString());
    }

    @Test
    void onMalwareScanResult_databaseRow_previousStatusNotScannedButThreatsFound_shouldPublishEvent() {
        UUID messageId = UUID.randomUUID();
        when(inboundMessageRepository.findLatestByMessageId(messageId)).thenReturn(Optional.of(inboundMessage(messageId, ScanStatus.NOT_SCANNED)));
        when(inboundMessageRepository.updateScanStatusReturningPreviousState(messageId, ScanStatus.THREATS_FOUND))
                .thenReturn(Optional.of(inboundMessage(messageId, ScanStatus.NOT_SCANNED)));

        messageExchangeService.onMalwareScanResult(new S3ObjectMalwareScanResultInfo(MalwareScanResult.THREATS_FOUND, PARTNER_BUCKET_NAME, messageId.toString()));

        verify(eventPublisher, times(1)).publishMessageReceivedEvent(messageId, BP_ID, MESSAGE_TYPE, "partnerTopic", "partnerExternalReference", PublishedScanStatus.THREATS_FOUND, MediaType.APPLICATION_XML_VALUE);
    }

    @Test
    void onMalwareScanResult_databaseRow_scanFailed_publishesScanFailed() {
        UUID messageId = UUID.randomUUID();
        when(inboundMessageRepository.findLatestByMessageId(messageId)).thenReturn(Optional.of(inboundMessage(messageId, ScanStatus.SCAN_PENDING)));
        when(inboundMessageRepository.updateScanStatusReturningPreviousState(messageId, ScanStatus.SCAN_FAILED))
                .thenReturn(Optional.of(inboundMessage(messageId, ScanStatus.SCAN_PENDING)));

        messageExchangeService.onMalwareScanResult(new S3ObjectMalwareScanResultInfo(MalwareScanResult.FAILED, PARTNER_BUCKET_NAME, messageId.toString()));

        verify(eventPublisher, times(1)).publishMessageReceivedEvent(messageId, BP_ID, MESSAGE_TYPE, "partnerTopic", "partnerExternalReference", PublishedScanStatus.SCAN_FAILED, MediaType.APPLICATION_XML_VALUE);
    }

    @Test
    void onMalwareScanResult_wrongBucketName_throwsIllegalStateException() {
        UUID messageId = UUID.randomUUID();

        S3ObjectMalwareScanResultInfo scanResult = new S3ObjectMalwareScanResultInfo(MalwareScanResult.NO_THREATS_FOUND, "some-other-bucket", messageId.toString());
        assertThrows(IllegalStateException.class, () -> messageExchangeService.onMalwareScanResult(scanResult));

        verifyNoInteractions(inboundMessageRepository, eventPublisher, metricsService);
    }

    // LEGACY-TAG-FALLBACK tests: scan results for messages stored by MES < 11.0.0 (metadata in S3 object tags)

    @Test
    void onMalwareScanResult_legacyMessage_noThreatsFound_publishesEventFromTagsAndBackfillsDatabase() {
        UUID messageId = UUID.randomUUID();
        String saveTimeInMillis = String.valueOf(System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(2));
        storeLegacyTestMessageInStatus(ScanStatus.SCAN_PENDING, saveTimeInMillis);
        Map<String, String> tagsBeforeScanResult = Map.copyOf(storedTags);

        S3ObjectMalwareScanResultInfo scanResult = new S3ObjectMalwareScanResultInfo(MalwareScanResult.NO_THREATS_FOUND, PARTNER_BUCKET_NAME, messageId.toString());
        messageExchangeService.onMalwareScanResult(scanResult);

        verify(eventPublisher, times(1)).publishMessageReceivedEvent(messageId, BP_ID, MESSAGE_TYPE, null, null, PublishedScanStatus.NO_THREATS_FOUND, MediaType.APPLICATION_XML_VALUE);
        verify(metricsService, times(1)).publishMetrics(eq(MalwareScanResult.NO_THREATS_FOUND), anyLong(), eq(Long.parseLong(saveTimeInMillis)));

        // the scanStatus tag is updated exactly like MES < 11.0.0 did; the previous scan status used for the
        // event suppression was read before the update
        Map<String, String> expectedTags = new HashMap<>(tagsBeforeScanResult);
        expectedTags.put("scanStatus", ScanStatus.NO_THREATS_FOUND.name());
        assertThat(storedTags).isEqualTo(expectedTags);
        ArgumentCaptor<InboundMessage> captor = ArgumentCaptor.forClass(InboundMessage.class);
        verify(inboundMessageRepository).upsertScanStatusAndMetadata(captor.capture());
        InboundMessage backfilled = captor.getValue();
        assertThat(backfilled.getMessageId()).isEqualTo(messageId);
        assertThat(backfilled.getBpId()).isEqualTo(BP_ID);
        assertThat(backfilled.getMessageType()).isEqualTo(MESSAGE_TYPE);
        assertThat(backfilled.getContentType()).isEqualTo(MediaType.APPLICATION_XML_VALUE);
        assertThat(backfilled.getScanStatus()).isEqualTo(ScanStatus.NO_THREATS_FOUND);
        assertThat(backfilled.getContentLength()).isEqualTo(storedMessage.getBytes(UTF_8).length);
        assertThat(backfilled.getCreatedAtEpochMillis()).isEqualTo(Long.parseLong(saveTimeInMillis));
    }

    @Test
    void onMalwareScanResult_legacyMessage_noSaveTime_publishesMetricsWithoutSaveTime() {
        UUID messageId = UUID.randomUUID();

        this.storedMessage = "<content>test</content>";
        this.storedTags = new HashMap<>(Map.of(
                "bpId", BP_ID,
                "messageType", MESSAGE_TYPE,
                "scanStatus", ScanStatus.SCAN_PENDING.name()
                // Missing: "saveTimeInMillis"
        ));

        S3ObjectMalwareScanResultInfo scanResult = new S3ObjectMalwareScanResultInfo(MalwareScanResult.FAILED, PARTNER_BUCKET_NAME, messageId.toString());
        messageExchangeService.onMalwareScanResult(scanResult);

        verify(eventPublisher, times(1)).publishMessageReceivedEvent(messageId, BP_ID, MESSAGE_TYPE, null, null, PublishedScanStatus.SCAN_FAILED, MediaType.APPLICATION_XML_VALUE);
        // without a save time the scan result counter is still incremented, only the duration timer is skipped
        verify(metricsService).publishMetrics(eq(MalwareScanResult.FAILED), anyLong(), isNull());
        verify(inboundMessageRepository).upsertScanStatusAndMetadata(any(InboundMessage.class));
    }

    @Test
    void onMalwareScanResult_legacyMessage_scanPendingAndScanDisabled_shouldPublishEvent() {
        // Scenario: Message was received with scanning ON (SCAN_PENDING), scan result arrives after scanning turned OFF.
        // The message received event must be published.
        UUID messageId = UUID.randomUUID();
        storeLegacyTestMessageInStatus(ScanStatus.SCAN_PENDING);

        S3ObjectMalwareScanResultInfo scanResult = new S3ObjectMalwareScanResultInfo(MalwareScanResult.NO_THREATS_FOUND, PARTNER_BUCKET_NAME, messageId.toString());
        // the malwareScanProperties mock returns false by default for isEnabled() -> scanning is turned off
        messageExchangeService.onMalwareScanResult(scanResult);

        verify(eventPublisher, times(1)).publishMessageReceivedEvent(messageId, BP_ID, MESSAGE_TYPE, null, null, PublishedScanStatus.NO_THREATS_FOUND, MediaType.APPLICATION_XML_VALUE);
    }

    @Test
    void onMalwareScanResult_legacyMessage_notScanned_shouldNotPublishEventAgain() {
        // Scenario: Message was received with scanning OFF (NOT_SCANNED) but still got scanned for malware without any threats found.
        // The message received event was already published when the message was received, so it should NOT be published again.
        UUID messageId = UUID.randomUUID();
        storeLegacyTestMessageInStatus(ScanStatus.NOT_SCANNED);

        S3ObjectMalwareScanResultInfo scanResult = new S3ObjectMalwareScanResultInfo(MalwareScanResult.NO_THREATS_FOUND, PARTNER_BUCKET_NAME, messageId.toString());
        messageExchangeService.onMalwareScanResult(scanResult);

        verify(eventPublisher, never()).publishMessageReceivedEvent(any(UUID.class), anyString(), anyString(), anyString(), anyString(), any(PublishedScanStatus.class), anyString());
        // the scan status is still persisted in the database
        verify(inboundMessageRepository).upsertScanStatusAndMetadata(any(InboundMessage.class));
    }

    @Test
    void onMalwareScanResult_legacyMessage_notScannedButThreatsFound_shouldPublishEvent() {
        // Scenario: Message was received with scanning OFF (NOT_SCANNED), but still got scanned for malware with threats found.
        // Threats should always be notified, even if the message received event was already published before as NOT_SCANNED.
        UUID messageId = UUID.randomUUID();
        storeLegacyTestMessageInStatus(ScanStatus.NOT_SCANNED);

        S3ObjectMalwareScanResultInfo scanResult = new S3ObjectMalwareScanResultInfo(MalwareScanResult.THREATS_FOUND, PARTNER_BUCKET_NAME, messageId.toString());
        messageExchangeService.onMalwareScanResult(scanResult);

        verify(eventPublisher, times(1)).publishMessageReceivedEvent(messageId, BP_ID, MESSAGE_TYPE, null, null, PublishedScanStatus.THREATS_FOUND, MediaType.APPLICATION_XML_VALUE);
    }

    @Test
    void onMalwareScanResult_orphanedObjectWithoutTagsAndDatabaseRecord_dropsResultWithoutPublishing() {
        // the upload crashed between storing the object and the database record (legacy tag compatibility
        // disabled): the result is dropped with a warning, the message stays fail-closed
        UUID messageId = UUID.randomUUID();

        this.storedMessage = "<content>test</content>";
        this.storedTags = new HashMap<>();

        S3ObjectMalwareScanResultInfo scanResult = new S3ObjectMalwareScanResultInfo(MalwareScanResult.NO_THREATS_FOUND, PARTNER_BUCKET_NAME, messageId.toString());
        messageExchangeService.onMalwareScanResult(scanResult);

        verify(eventPublisher, never()).publishMessageReceivedEvent(any(UUID.class), anyString(), anyString(), anyString(), anyString(), any(PublishedScanStatus.class), anyString());
        verifyNoInboundMessageUpserted();
        verify(metricsService, never()).publishMetrics(any(MalwareScanResult.class), anyLong(), any());
    }

    @Test
    void onMalwareScanResult_legacyMessage_incompleteTags_throwsAndDoesNotPublish() {
        UUID messageId = UUID.randomUUID();

        this.storedMessage = "<content>test</content>";
        this.storedTags = new HashMap<>(Map.of("bpId", BP_ID));

        S3ObjectMalwareScanResultInfo scanResult = new S3ObjectMalwareScanResultInfo(MalwareScanResult.NO_THREATS_FOUND, PARTNER_BUCKET_NAME, messageId.toString());
        assertThrows(IllegalStateException.class, () -> messageExchangeService.onMalwareScanResult(scanResult));

        verify(eventPublisher, never()).publishMessageReceivedEvent(any(UUID.class), anyString(), anyString(), anyString(), anyString(), any(PublishedScanStatus.class), anyString());
        verify(inboundMessageRepository, never()).updateScanStatusReturningPreviousState(any(UUID.class), any(ScanStatus.class));
        verifyNoInboundMessageUpserted();
        verify(metricsService, never()).publishMetrics(any(MalwareScanResult.class), anyLong(), any());
    }

    @Test
    void onMalwareScanResult_rowWithoutMetadata_incompleteTags_doesNotCommitScanStatus() {
        // fail-closed: the terminal status must not be committed before the event data could be resolved -
        // the swallowed failure would otherwise leave a deliverable row whose received event is never published
        UUID messageId = UUID.randomUUID();
        this.storedMessage = "<content>test</content>";
        this.storedTags = new HashMap<>(Map.of("bpId", BP_ID));
        InboundMessage rowWithoutMetadata = InboundMessage.builder()
                .messageId(messageId)
                .bpId(BP_ID)
                .contentLength(42)
                .scanStatus(ScanStatus.SCAN_PENDING)
                .build();
        when(inboundMessageRepository.findLatestByMessageId(messageId)).thenReturn(Optional.of(rowWithoutMetadata));

        S3ObjectMalwareScanResultInfo scanResult = new S3ObjectMalwareScanResultInfo(MalwareScanResult.NO_THREATS_FOUND, PARTNER_BUCKET_NAME, messageId.toString());
        assertThrows(IllegalStateException.class, () -> messageExchangeService.onMalwareScanResult(scanResult));

        verify(eventPublisher, never()).publishMessageReceivedEvent(any(UUID.class), anyString(), anyString(), anyString(), anyString(), any(PublishedScanStatus.class), anyString());
        verify(inboundMessageRepository, never()).updateScanStatusReturningPreviousState(any(UUID.class), any(ScanStatus.class));
        verifyNoInboundMessageUpserted();
    }

    private void storeLegacyTestMessageInStatus(ScanStatus scanStatus) {
        storeLegacyTestMessageInStatus(scanStatus, String.valueOf(System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(2)));
    }

    private void storeLegacyTestMessageInStatus(ScanStatus scanStatus, String saveTimeInMillis) {
        this.storedMessage = "<content>test</content>";
        this.storedTags = new HashMap<>(Map.of(
                "bpId", BP_ID,
                "messageType", MESSAGE_TYPE,
                "scanStatus", scanStatus.name(),
                "saveTimeInMillis", saveTimeInMillis
        ));
    }

    private InboundMessage inboundMessage(UUID messageId, ScanStatus previousScanStatus) {
        return InboundMessage.builder()
                .messageId(messageId)
                .bpId(BP_ID)
                .contentLength(42)
                .overrideCreatedAt(LocalDateTime.now().minusSeconds(2))
                .messageType(MESSAGE_TYPE)
                .partnerTopic("partnerTopic")
                .partnerExternalReference("partnerExternalReference")
                .contentType(MediaType.APPLICATION_XML_VALUE)
                .scanStatus(previousScanStatus)
                .build();
    }

    // Message delivery gated by the scan status from the database (messages stored by MES >= 11.0.0)

    static Stream<Arguments> deliverableWhenMalwareScanDisabled() {
        return Stream.of(
                Arguments.of(ScanStatus.NOT_SCANNED, true, "NOT_SCANNED should always be deliverable"),
                Arguments.of(ScanStatus.NO_THREATS_FOUND, true, "NO_THREATS_FOUND should always be deliverable"),
                Arguments.of(ScanStatus.SCAN_PENDING, true, "SCAN_PENDING should be deliverable when scanning is disabled"),
                Arguments.of(ScanStatus.SCAN_FAILED, true, "SCAN_FAILED should be deliverable when scanning is disabled"),
                Arguments.of(ScanStatus.THREATS_FOUND, false, "THREATS_FOUND should never be deliverable")
        );
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

    @ParameterizedTest(name = "malware scan disabled, database scan status {0}: shouldDeliver={1}")
    @MethodSource("deliverableWhenMalwareScanDisabled")
    void getMessageFromPartner_databaseScanStatus_malwareScanDisabled(ScanStatus scanStatus, boolean shouldDeliver, String description) {
        UUID messageId = UUID.randomUUID();
        this.storedMessage = "<content>test</content>";
        when(inboundMessageRepository.findLatestByMessageId(messageId)).thenReturn(Optional.of(inboundMessage(messageId, scanStatus)));
        // Note: mockito mock malwareScanProperties returns false by default for isEnabled()

        if (shouldDeliver) {
            Optional<MessageContent> result = messageExchangeService.getMessageFromPartner(messageId);
            assertThat(result).as(description).isPresent();
        } else {
            assertThrows(MalwareScanFailedOrBlockedException.class,
                    () -> messageExchangeService.getMessageFromPartner(messageId), description);
        }
    }

    @ParameterizedTest(name = "malware scan enabled, database scan status {0}: shouldDeliver={1}")
    @MethodSource("deliverableWhenMalwareScanEnabled")
    void getMessageFromPartner_databaseScanStatus_malwareScanEnabled(ScanStatus scanStatus, boolean shouldDeliver, boolean malwareScanPropsStubbingRequired, String description) {
        UUID messageId = UUID.randomUUID();
        this.storedMessage = "<content>test</content>";
        when(inboundMessageRepository.findLatestByMessageId(messageId)).thenReturn(Optional.of(inboundMessage(messageId, scanStatus)));
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

    // LEGACY-TAG-FALLBACK tests: delivery gated by the scan status from the S3 object tags (messages stored by MES < 11.0.0)

    @ParameterizedTest(name = "malware scan disabled, legacy tag scan status {0}: shouldDeliver={1}")
    @MethodSource("deliverableWhenMalwareScanDisabled")
    void getMessageFromPartner_legacyTags_malwareScanDisabled(ScanStatus scanStatus, boolean shouldDeliver, String description) {
        UUID messageId = UUID.randomUUID();
        prepareLegacyStoredMessageWithScanStatus(scanStatus);
        // Note: the malwareScanProperties mock returns false by default for isEnabled(), and the
        // findLatestByMessageId mock returns an empty Optional by default, which selects the legacy tag fallback

        if (shouldDeliver) {
            Optional<MessageContent> result = messageExchangeService.getMessageFromPartner(messageId);
            assertThat(result).as(description).isPresent();
        } else {
            assertThrows(MalwareScanFailedOrBlockedException.class,
                    () -> messageExchangeService.getMessageFromPartner(messageId), description);
        }
    }

    @ParameterizedTest(name = "malware scan enabled, legacy tag scan status {0}: shouldDeliver={1}")
    @MethodSource("deliverableWhenMalwareScanEnabled")
    void getMessageFromPartner_legacyTags_malwareScanEnabled(ScanStatus scanStatus, boolean shouldDeliver, boolean malwareScanPropsStubbingRequired, String description) {
        UUID messageId = UUID.randomUUID();
        prepareLegacyStoredMessageWithScanStatus(scanStatus);
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

    @Test
    void getMessageFromPartner_legacyMessageWithoutScanStatusTag_shouldDeliver() {
        // Objects stored before malware scanning was introduced have no scanStatus tag at all
        UUID messageId = UUID.randomUUID();
        this.storedMessage = "<content>test</content>";
        this.storedTags = new HashMap<>();

        Optional<MessageContent> result = messageExchangeService.getMessageFromPartner(messageId);

        assertThat(result).isPresent();
    }

    @Test
    void getMessageFromPartner_legacyMessageWithoutScanStatusTagAndScanningEnabled_shouldDeliver() {
        // A legacy message stored before malware scanning was introduced (metadata tags without a scanStatus
        // tag) stays deliverable after scanning is enabled
        UUID messageId = UUID.randomUUID();
        this.storedMessage = "<content>test</content>";
        this.storedTags = new HashMap<>(Map.of(
                "bpId", BP_ID,
                "messageType", MESSAGE_TYPE,
                "saveTimeInMillis", String.valueOf(System.currentTimeMillis())
        ));
        when(malwareScanProperties.isEnabled()).thenReturn(true);

        Optional<MessageContent> result = messageExchangeService.getMessageFromPartner(messageId);

        assertThat(result).isPresent();
    }

    @Test
    @SneakyThrows
    void getMessageFromPartner_databaseScanStatus_mismatchingContentType_throwsException() {
        UUID messageId = UUID.randomUUID();
        this.storedMessage = "<content>test</content>";
        // the database row carries application/xml as content type
        when(inboundMessageRepository.findLatestByMessageId(messageId)).thenReturn(Optional.of(inboundMessage(messageId, ScanStatus.NO_THREATS_FOUND)));

        assertThrows(MismatchedContentTypeException.class,
                () -> messageExchangeService.getMessageFromPartner(messageId, MediaType.APPLICATION_JSON_VALUE));
    }

    @Test
    @SneakyThrows
    void getMessageFromPartner_databaseRowWithoutContentType_matchingContentTypeFromS3Head_shouldDeliver() {
        // a row updated by a scan result before the metadata backfill may lack the content type in the database
        UUID messageId = UUID.randomUUID();
        this.storedMessage = "<content>test</content>";
        InboundMessage rowWithoutContentType = InboundMessage.builder()
                .messageId(messageId)
                .bpId(BP_ID)
                .contentLength(42)
                .scanStatus(ScanStatus.NO_THREATS_FOUND)
                .build();
        when(inboundMessageRepository.findLatestByMessageId(messageId)).thenReturn(Optional.of(rowWithoutContentType));

        Optional<MessageContent> result = messageExchangeService.getMessageFromPartner(messageId, MediaType.APPLICATION_XML_VALUE);

        assertThat(result).isPresent();
    }

    @Test
    @SneakyThrows
    void getMessageFromPartner_databaseRowWithoutContentType_objectAbsent_returnsEmpty() {
        UUID messageId = UUID.randomUUID();
        this.storedMessage = null;
        InboundMessage rowWithoutContentType = InboundMessage.builder()
                .messageId(messageId)
                .bpId(BP_ID)
                .contentLength(42)
                .scanStatus(ScanStatus.NO_THREATS_FOUND)
                .build();
        when(inboundMessageRepository.findLatestByMessageId(messageId)).thenReturn(Optional.of(rowWithoutContentType));

        Optional<MessageContent> result = messageExchangeService.getMessageFromPartner(messageId, MediaType.APPLICATION_XML_VALUE);

        assertThat(result).isEmpty();
    }

    @Test
    @SneakyThrows
    void getMessageFromPartner_deliverableRowButObjectExpired_returnsEmpty() {
        // the database record outlives the S3 object (retention +2 days): a GET after the object expired must
        // still result in a 404 (empty result)
        UUID messageId = UUID.randomUUID();
        this.storedMessage = null;
        when(inboundMessageRepository.findLatestByMessageId(messageId)).thenReturn(Optional.of(inboundMessage(messageId, ScanStatus.NO_THREATS_FOUND)));

        Optional<MessageContent> result = messageExchangeService.getMessageFromPartner(messageId, MediaType.APPLICATION_XML_VALUE);

        assertThat(result).isEmpty();
    }

    @Test
    @SneakyThrows
    void getMessageFromPartner_blockedRowButObjectExpired_staysBlocked() {
        // a malware-blocked message is rejected with 403 based on the database status even after the S3
        // object expired (previously 404)
        UUID messageId = UUID.randomUUID();
        this.storedMessage = null;
        when(inboundMessageRepository.findLatestByMessageId(messageId)).thenReturn(Optional.of(inboundMessage(messageId, ScanStatus.THREATS_FOUND)));

        assertThrows(MalwareScanFailedOrBlockedException.class,
                () -> messageExchangeService.getMessageFromPartner(messageId, MediaType.APPLICATION_XML_VALUE));
    }

    @Test
    void getMessageFromPartner_taglessObjectWithoutDatabaseRecordAndScanningEnabled_shouldNotDeliver() {
        // An object without metadata tags was stored by MES >= 11.0.0; if its database record is unexpectedly
        // missing (e.g. the upload crashed between storing the object and the record), its scan is still pending
        UUID messageId = UUID.randomUUID();
        this.storedMessage = "<content>test</content>";
        this.storedTags = new HashMap<>();
        when(malwareScanProperties.isEnabled()).thenReturn(true);

        assertThrows(MalwareScanFailedOrBlockedException.class, () -> messageExchangeService.getMessageFromPartner(messageId));
    }

    private void prepareLegacyStoredMessageWithScanStatus(ScanStatus scanStatus) {
        this.storedMessage = "<content>test</content>";
        this.storedTags = new HashMap<>(Map.of(
                "bpId", "some-bp-id",
                "messageType", "some-message-type",
                "scanStatus", scanStatus.name(),
                "saveTimeInMillis", String.valueOf(System.currentTimeMillis())
        ));
    }

    private void assertLegacyUploadTags(ScanStatus expectedScanStatus) {
        assertThat(tagsPassedToStoreMessage)
                .containsKeys("bpId", "messageType", "saveTimeInMillis")
                .containsEntry("scanStatus", expectedScanStatus.name());
    }

    private void verifyNoInboundMessageUpserted() {
        verify(inboundMessageRepository, never()).upsertScanStatusAndMetadata(any(InboundMessage.class));
        verify(inboundMessageRepository, never()).upsertScanStatusAndMetadataKeepingTerminalStatus(any(InboundMessage.class));
    }

    // LEGACY-TAG-FALLBACK tests: transitional v10-compatible upload tagging (JEAP-7252)

    @Test
    @SneakyThrows
    void saveNewMessageFromPartner_legacyTagCompatibilityDisabled_writesNoMetadataTags() {
        messageExchangeService = createMessageExchangeService(Optional.empty(), false);
        UUID messageId = UUID.randomUUID();
        InputStream xmlContent = new ByteArrayInputStream("<valid/>".getBytes(UTF_8));

        messageExchangeService.saveNewMessageFromPartner(messageId, BP_ID, MESSAGE_TYPE, new MessageContent(xmlContent, 42));

        assertThat(tagsPassedToStoreMessage).isEmpty();
        verify(inboundMessageRepository).upsertScanStatusAndMetadataKeepingTerminalStatus(any(InboundMessage.class));
    }

    // LEGACY-TAG-FALLBACK tests: healing of pending scan statuses from the S3 object tags (JEAP-7252)

    @Test
    void getMessageFromPartner_pendingInDatabaseButTerminalScanStatusTag_healsAndDelivers() {
        // a scan result processed by an MES < 11.0.0 instance during a rolling deployment updated the tags only
        UUID messageId = UUID.randomUUID();
        this.storedMessage = "<content>test</content>";
        this.storedTags = new HashMap<>(Map.of("scanStatus", ScanStatus.NO_THREATS_FOUND.name()));
        when(inboundMessageRepository.findLatestByMessageId(messageId)).thenReturn(Optional.of(inboundMessage(messageId, ScanStatus.SCAN_PENDING)));
        when(inboundMessageRepository.updateScanStatusIfPending(messageId, ScanStatus.NO_THREATS_FOUND)).thenReturn(true);

        Optional<MessageContent> result = messageExchangeService.getMessageFromPartner(messageId);

        assertThat(result).isPresent();
        verify(inboundMessageRepository).updateScanStatusIfPending(messageId, ScanStatus.NO_THREATS_FOUND);
    }

    @Test
    void getMessageFromPartner_healRacesWithScanResult_databaseStatusWinsOverStaleTag() {
        // the row left the pending state while healing: the concurrently written database status gates the
        // delivery, never the (possibly stale) tag verdict
        UUID messageId = UUID.randomUUID();
        this.storedMessage = "<content>test</content>";
        this.storedTags = new HashMap<>(Map.of("scanStatus", ScanStatus.NO_THREATS_FOUND.name()));
        when(inboundMessageRepository.findLatestByMessageId(messageId)).thenReturn(
                Optional.of(inboundMessage(messageId, ScanStatus.SCAN_PENDING)),
                Optional.of(inboundMessage(messageId, ScanStatus.THREATS_FOUND)));
        when(inboundMessageRepository.updateScanStatusIfPending(messageId, ScanStatus.NO_THREATS_FOUND)).thenReturn(false);

        assertThrows(MalwareScanFailedOrBlockedException.class, () -> messageExchangeService.getMessageFromPartner(messageId));
    }

    @Test
    void getMessageFromPartner_pendingInDatabaseAndTags_staysBlockedWithoutHealing() {
        UUID messageId = UUID.randomUUID();
        this.storedMessage = "<content>test</content>";
        this.storedTags = new HashMap<>(Map.of("scanStatus", ScanStatus.SCAN_PENDING.name()));
        when(malwareScanProperties.isEnabled()).thenReturn(true);
        when(inboundMessageRepository.findLatestByMessageId(messageId)).thenReturn(Optional.of(inboundMessage(messageId, ScanStatus.SCAN_PENDING)));

        assertThrows(MalwareScanFailedOrBlockedException.class, () -> messageExchangeService.getMessageFromPartner(messageId));
        verify(inboundMessageRepository, never()).updateScanStatusIfPending(any(UUID.class), any(ScanStatus.class));
    }
}
