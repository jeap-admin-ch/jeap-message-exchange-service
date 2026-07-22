package ch.admin.bit.jeap.messageexchange.persistence;

import ch.admin.bit.jeap.messageexchange.domain.InboundMessage;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.ScanStatus;
import ch.admin.bit.jeap.messageexchange.domain.messaging.EventPublisher;
import ch.admin.bit.jeap.messageexchange.domain.metrics.MetricsService;
import ch.admin.bit.jeap.messageexchange.domain.objectstore.ObjectStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest(classes = TestApp.class, properties = {
        "spring.application.name=junit"
})
@ContextConfiguration(classes = PersistenceConfiguration.class)
class JdbcInboundMessageRepositoryITTest {

    @Autowired
    private JdbcInboundMessageRepository inboundMessageRepository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @MockitoBean
    @SuppressWarnings("unused")
    private ObjectStore objectStore;

    @MockitoBean
    @SuppressWarnings("unused")
    private MetricsService metricsService;

    @MockitoBean
    @SuppressWarnings("unused")
    private EventPublisher eventPublisher;
    static PostgreSQLContainer postgres = new PostgreSQLContainer(DockerImageName.parse("postgres:17-alpine").asCompatibleSubstituteFor("postgres"));

    @BeforeAll
    static void startContainers() {
        postgres.start();
    }

    @AfterAll
    static void stopContainers() {
        postgres.stop();
    }

    @AfterEach
    void cleanDb() {
        jdbcTemplate.update("DELETE FROM inbound_message", Map.of());
    }

    @DynamicPropertySource
    static void dynamicProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", () -> postgres.getJdbcUrl());
        registry.add("spring.datasource.username", () -> postgres.getUsername());
        registry.add("spring.datasource.password", () -> postgres.getPassword());
    }

    @Test
    void upsertAndGetMessage() {
        UUID messageId = UUID.randomUUID();

        InboundMessage message = InboundMessage.builder()
                .messageId(messageId)
                .bpId("myBpId")
                .contentLength(100)
                .build();
        assertThat(message).isNotNull();

        inboundMessageRepository.upsertScanStatusAndMetadata(message);

        Optional<InboundMessage> result = inboundMessageRepository.findByBpIdAndMessageId("myBpId", messageId);
        assertThat(result).isPresent();
        InboundMessage savedMessage = result.get();
        BigInteger messageSequenceId = savedMessage.getSequenceId();
        assertThat(messageSequenceId).isNotNull();
        assertEquals(messageId, savedMessage.getMessageId());
        assertEquals("myBpId", savedMessage.getBpId());
        assertEquals(100, savedMessage.getContentLength());
        assertNotNull(savedMessage.getCreatedAt());

        LocalDateTime timeIn2000 = LocalDateTime.of(2000, Month.JANUARY, 1, 13, 45);
        InboundMessage message2 = InboundMessage.builder()
                .messageId(messageId)
                .bpId("myBpId2")
                .contentLength(200)
                .overrideCreatedAt(timeIn2000)
                .build();
        assertThat(message2).isNotNull();

        inboundMessageRepository.upsertScanStatusAndMetadata(message2);

        result = inboundMessageRepository.findByBpIdAndMessageId("myBpId2", messageId);
        assertThat(result).isPresent();
        savedMessage = result.get();
        assertThat(savedMessage.getSequenceId()).isEqualTo(messageSequenceId.add(BigInteger.ONE));
        assertEquals(messageId, savedMessage.getMessageId());
        assertEquals("myBpId2", savedMessage.getBpId());
        assertEquals(200, savedMessage.getContentLength());
        assertEquals(timeIn2000, savedMessage.getCreatedAt());
    }

    @Test
    void upsertMessage_sameMessageIdDifferentBpId_bothRowsSaved() {
        UUID messageId = UUID.randomUUID();

        InboundMessage message = InboundMessage.builder()
                .messageId(messageId)
                .bpId("bpId")
                .contentLength(123)
                .build();
        assertThat(message).isNotNull();

        inboundMessageRepository.upsertScanStatusAndMetadata(message);

        InboundMessage message2 = InboundMessage.builder()
                .messageId(messageId)
                .bpId("bpId2")
                .contentLength(123)
                .build();
        assertThat(message2).isNotNull();

        inboundMessageRepository.upsertScanStatusAndMetadata(message2);
    }


    @Test
    void findByBpIdAndMessageId_messageFound_returnsMessage() {
        InboundMessage message = storeMessageInDb(UUID.randomUUID().toString(), 123);

        Optional<InboundMessage> result = inboundMessageRepository.findByBpIdAndMessageId(message.getBpId(), message.getMessageId());
        assertThat(result).isPresent();
        assertThat(result.get().getBpId()).isEqualTo(message.getBpId());
        assertThat(result.get().getMessageId()).isEqualTo(message.getMessageId());
        assertThat(result.get().getContentLength()).isEqualTo(message.getContentLength());
    }

    @Test
    void findByBpIdAndMessageId_messageWithIdNotFound_returnsEmpty() {
        UUID messageId = UUID.randomUUID();
        String bpId = UUID.randomUUID().toString();

        Optional<InboundMessage> result = inboundMessageRepository.findByBpIdAndMessageId(bpId, messageId);
        assertThat(result).isEmpty();
    }

    @Test
    void deleteExpiredMessages() {
        boolean resultEmptyRepoNoMessagesDeleted = inboundMessageRepository.deleteExpiredMessages(14, 10);
        assertThat(resultEmptyRepoNoMessagesDeleted)
                .isFalse();

        LocalDateTime now = LocalDateTime.now();
        LocalDateTime nowMinus14Days = now.minusDays(14);
        LocalDateTime nowMinus48Days = now.minusDays(48);
        LocalDateTime nowMinus96Days = now.minusDays(96);
        InboundMessage notExpiredMessage = storeMessageInDb(now);
        InboundMessage expiredMessage1 = storeMessageInDb(nowMinus14Days);
        InboundMessage expiredMessage2 = storeMessageInDb(nowMinus48Days);
        InboundMessage expiredMessage3 = storeMessageInDb(nowMinus96Days);

        int olderThanDays = 12;
        int limit = 2;
        boolean resultLimit2TwoMessagesDeleted = inboundMessageRepository.deleteExpiredMessages(olderThanDays, limit);
        assertThat(resultLimit2TwoMessagesDeleted)
                .isTrue();

        boolean resultOneRemainingMessageDeleted = inboundMessageRepository.deleteExpiredMessages(olderThanDays, limit);
        assertThat(resultOneRemainingMessageDeleted)
                .isTrue();

        boolean resultNoMoreExpiredMessagesToDelete = inboundMessageRepository.deleteExpiredMessages(olderThanDays, limit);
        assertThat(resultNoMoreExpiredMessagesToDelete)
                .isFalse();

        assertThat(inboundMessageRepository.findByBpIdAndMessageId(expiredMessage1.getBpId(), expiredMessage1.getMessageId())).isEmpty();
        assertThat(inboundMessageRepository.findByBpIdAndMessageId(expiredMessage2.getBpId(), expiredMessage2.getMessageId())).isEmpty();
        assertThat(inboundMessageRepository.findByBpIdAndMessageId(expiredMessage3.getBpId(), expiredMessage3.getMessageId())).isEmpty();

        Optional<InboundMessage> message = inboundMessageRepository.findByBpIdAndMessageId(notExpiredMessage.getBpId(), notExpiredMessage.getMessageId());
        assertThat(message).isPresent();
    }

    @Test
    void upsertAndGetMessage_withScanMetadata_allFieldsRoundTrip() {
        UUID messageId = UUID.randomUUID();

        InboundMessage message = InboundMessage.builder()
                .messageId(messageId)
                .bpId("myBpId")
                .contentLength(100)
                .messageType("NI015")
                .partnerTopic("aPartnerTopic")
                .partnerExternalReference("anExternalReference")
                .contentType("application/xml")
                .scanStatus(ScanStatus.SCAN_PENDING)
                .build();

        inboundMessageRepository.upsertScanStatusAndMetadata(message);

        Optional<InboundMessage> result = inboundMessageRepository.findByBpIdAndMessageId("myBpId", messageId);
        assertThat(result).isPresent();
        InboundMessage savedMessage = result.get();
        assertEquals("NI015", savedMessage.getMessageType());
        assertEquals("aPartnerTopic", savedMessage.getPartnerTopic());
        assertEquals("anExternalReference", savedMessage.getPartnerExternalReference());
        assertEquals("application/xml", savedMessage.getContentType());
        assertEquals(ScanStatus.SCAN_PENDING, savedMessage.getScanStatus());
    }

    @Test
    void findLatestByMessageId_messageNotFound_returnsEmpty() {
        assertThat(inboundMessageRepository.findLatestByMessageId(UUID.randomUUID())).isEmpty();
    }

    @Test
    void updateScanStatusReturningPreviousState_returnsPreviousStatusAndCurrentMetadata() {
        UUID messageId = UUID.randomUUID();
        inboundMessageRepository.upsertScanStatusAndMetadata(inboundMessage(messageId, "myBpId", ScanStatus.SCAN_PENDING));

        Optional<InboundMessage> previousState = inboundMessageRepository.updateScanStatusReturningPreviousState(messageId, ScanStatus.NO_THREATS_FOUND);

        assertThat(previousState).isPresent();
        // the returned state carries the PREVIOUS scan status but the current metadata
        assertThat(previousState.get().getScanStatus()).isEqualTo(ScanStatus.SCAN_PENDING);
        assertThat(previousState.get().getBpId()).isEqualTo("myBpId");
        assertThat(previousState.get().getMessageType()).isEqualTo("NI015");
        assertThat(previousState.get().getContentType()).isEqualTo("application/xml");

        Optional<InboundMessage> updated = inboundMessageRepository.findLatestByMessageId(messageId);
        assertThat(updated).isPresent();
        assertThat(updated.get().getScanStatus()).isEqualTo(ScanStatus.NO_THREATS_FOUND);
    }

    @Test
    void updateScanStatusReturningPreviousState_legacyRowWithoutScanStatus_returnsNullPreviousStatus() {
        // simulates a row created by MES < 11.0.0 (no metadata / scan status columns)
        UUID messageId = UUID.randomUUID();
        InboundMessage legacyRow = InboundMessage.builder()
                .messageId(messageId)
                .bpId("myBpId")
                .contentLength(100)
                .build();
        inboundMessageRepository.upsertScanStatusAndMetadata(legacyRow);

        Optional<InboundMessage> previousState = inboundMessageRepository.updateScanStatusReturningPreviousState(messageId, ScanStatus.NO_THREATS_FOUND);

        assertThat(previousState).isPresent();
        assertThat(previousState.get().getScanStatus()).isNull();
        assertThat(previousState.get().getMessageType()).isNull();

        assertThat(inboundMessageRepository.findLatestByMessageId(messageId).orElseThrow().getScanStatus())
                .isEqualTo(ScanStatus.NO_THREATS_FOUND);
    }

    @Test
    void updateScanStatusReturningPreviousState_noRow_returnsEmpty() {
        Optional<InboundMessage> previousState = inboundMessageRepository.updateScanStatusReturningPreviousState(UUID.randomUUID(), ScanStatus.NO_THREATS_FOUND);

        assertThat(previousState).isEmpty();
    }

    @Test
    void updateScanStatusIfPending_pendingRow_updatesStatus() {
        UUID messageId = UUID.randomUUID();
        inboundMessageRepository.upsertScanStatusAndMetadata(inboundMessage(messageId, "myBpId", ScanStatus.SCAN_PENDING));

        assertThat(inboundMessageRepository.updateScanStatusIfPending(messageId, ScanStatus.NO_THREATS_FOUND)).isTrue();

        assertThat(inboundMessageRepository.findLatestByMessageId(messageId).orElseThrow().getScanStatus())
                .isEqualTo(ScanStatus.NO_THREATS_FOUND);
    }

    @Test
    void updateScanStatusIfPending_legacyRowWithoutStatus_updatesStatus() {
        UUID messageId = UUID.randomUUID();
        inboundMessageRepository.upsertScanStatusAndMetadata(InboundMessage.builder()
                .messageId(messageId)
                .bpId("myBpId")
                .contentLength(100)
                .build());

        assertThat(inboundMessageRepository.updateScanStatusIfPending(messageId, ScanStatus.THREATS_FOUND)).isTrue();

        assertThat(inboundMessageRepository.findLatestByMessageId(messageId).orElseThrow().getScanStatus())
                .isEqualTo(ScanStatus.THREATS_FOUND);
    }

    @Test
    void updateScanStatusIfPending_terminalRow_doesNotChangeStatus() {
        UUID messageId = UUID.randomUUID();
        inboundMessageRepository.upsertScanStatusAndMetadata(inboundMessage(messageId, "myBpId", ScanStatus.THREATS_FOUND));

        assertThat(inboundMessageRepository.updateScanStatusIfPending(messageId, ScanStatus.NO_THREATS_FOUND)).isFalse();

        assertThat(inboundMessageRepository.findLatestByMessageId(messageId).orElseThrow().getScanStatus())
                .isEqualTo(ScanStatus.THREATS_FOUND);
    }

    @Test
    void updateScanStatusIfPending_noRow_returnsFalse() {
        assertThat(inboundMessageRepository.updateScanStatusIfPending(UUID.randomUUID(), ScanStatus.NO_THREATS_FOUND)).isFalse();
    }

    @Test
    void upsertScanStatusAndMetadata_insertsWhenNoRowExists() {
        UUID messageId = UUID.randomUUID();

        inboundMessageRepository.upsertScanStatusAndMetadata(inboundMessage(messageId, "myBpId", ScanStatus.NO_THREATS_FOUND));

        Optional<InboundMessage> result = inboundMessageRepository.findLatestByMessageId(messageId);
        assertThat(result).isPresent();
        assertThat(result.get().getScanStatus()).isEqualTo(ScanStatus.NO_THREATS_FOUND);
        assertThat(result.get().getMessageType()).isEqualTo("NI015");
    }

    @Test
    void upsertScanStatusAndMetadata_onConflictUpdatesAllFieldsIncludingCreatedAt() {
        UUID messageId = UUID.randomUUID();
        LocalDateTime originalCreatedAt = LocalDateTime.of(2020, Month.JANUARY, 1, 13, 45);
        InboundMessage legacyRow = InboundMessage.builder()
                .messageId(messageId)
                .bpId("myBpId")
                .contentLength(100)
                .overrideCreatedAt(originalCreatedAt)
                .build();
        inboundMessageRepository.upsertScanStatusAndMetadata(legacyRow);

        LocalDateTime newCreatedAt = LocalDateTime.of(2026, Month.JULY, 8, 13, 45);
        InboundMessage update = InboundMessage.builder()
                .messageId(messageId)
                .bpId("myBpId")
                .contentLength(999)
                .overrideCreatedAt(newCreatedAt)
                .messageType("NI015")
                .contentType("application/xml")
                .scanStatus(ScanStatus.NO_THREATS_FOUND)
                .build();
        inboundMessageRepository.upsertScanStatusAndMetadata(update);

        Optional<InboundMessage> result = inboundMessageRepository.findByBpIdAndMessageId("myBpId", messageId);
        assertThat(result).isPresent();
        assertThat(result.get().getScanStatus()).isEqualTo(ScanStatus.NO_THREATS_FOUND);
        assertThat(result.get().getMessageType()).isEqualTo("NI015");
        assertThat(result.get().getContentType()).isEqualTo("application/xml");
        // createdAt and contentLength are updated too, keeping housekeeping retention aligned with the
        // S3 object lifetime when a message is stored again
        assertThat(result.get().getCreatedAt()).isEqualTo(newCreatedAt);
        assertThat(result.get().getContentLength()).isEqualTo(999);
    }

    @Test
    void upsertScanStatusAndMetadataKeepingTerminalStatus_insertsWhenNoRowExists() {
        UUID messageId = UUID.randomUUID();

        inboundMessageRepository.upsertScanStatusAndMetadataKeepingTerminalStatus(inboundMessage(messageId, "myBpId", ScanStatus.SCAN_PENDING));

        Optional<InboundMessage> result = inboundMessageRepository.findLatestByMessageId(messageId);
        assertThat(result).isPresent();
        assertThat(result.get().getScanStatus()).isEqualTo(ScanStatus.SCAN_PENDING);
        assertThat(result.get().getMessageType()).isEqualTo("NI015");
    }

    @Test
    void upsertScanStatusAndMetadataKeepingTerminalStatus_terminalRow_keepsVerdictAndUpdatesMetadata() {
        // the scan result of the just-stored object was processed (and backfilled by the legacy tag fallback)
        // before the upload's upsert - the verdict must not be reverted to SCAN_PENDING
        for (ScanStatus terminalStatus : ScanStatus.values()) {
            if (!terminalStatus.isTerminal()) {
                continue;
            }
            UUID messageId = UUID.randomUUID();
            inboundMessageRepository.upsertScanStatusAndMetadata(InboundMessage.builder()
                    .messageId(messageId)
                    .bpId("myBpId")
                    .contentLength(100)
                    .scanStatus(terminalStatus)
                    .build());

            LocalDateTime newCreatedAt = LocalDateTime.of(2026, Month.JULY, 8, 13, 45);
            inboundMessageRepository.upsertScanStatusAndMetadataKeepingTerminalStatus(inboundMessage(messageId, "myBpId", ScanStatus.SCAN_PENDING, newCreatedAt));

            InboundMessage result = inboundMessageRepository.findByBpIdAndMessageId("myBpId", messageId).orElseThrow();
            assertThat(result.getScanStatus()).as("terminal status %s must be kept", terminalStatus).isEqualTo(terminalStatus);
            assertThat(result.getMessageType()).isEqualTo("NI015");
            assertThat(result.getCreatedAt()).isEqualTo(newCreatedAt);
        }
    }

    @Test
    void upsertScanStatusAndMetadataKeepingTerminalStatus_pendingRow_overwritesScanStatus() {
        UUID messageId = UUID.randomUUID();
        inboundMessageRepository.upsertScanStatusAndMetadata(inboundMessage(messageId, "myBpId", ScanStatus.SCAN_PENDING));

        inboundMessageRepository.upsertScanStatusAndMetadataKeepingTerminalStatus(inboundMessage(messageId, "myBpId", ScanStatus.NOT_SCANNED));

        assertThat(inboundMessageRepository.findByBpIdAndMessageId("myBpId", messageId).orElseThrow().getScanStatus())
                .isEqualTo(ScanStatus.NOT_SCANNED);
    }

    @Test
    void upsertScanStatusAndMetadataKeepingTerminalStatus_rowWithoutScanStatus_overwritesScanStatus() {
        UUID messageId = UUID.randomUUID();
        inboundMessageRepository.upsertScanStatusAndMetadata(InboundMessage.builder()
                .messageId(messageId)
                .bpId("myBpId")
                .contentLength(100)
                .build());

        inboundMessageRepository.upsertScanStatusAndMetadataKeepingTerminalStatus(inboundMessage(messageId, "myBpId", ScanStatus.SCAN_PENDING));

        assertThat(inboundMessageRepository.findByBpIdAndMessageId("myBpId", messageId).orElseThrow().getScanStatus())
                .isEqualTo(ScanStatus.SCAN_PENDING);
    }

    private InboundMessage inboundMessage(UUID messageId, String bpId, ScanStatus scanStatus) {
        return inboundMessage(messageId, bpId, scanStatus, null);
    }

    private InboundMessage inboundMessage(UUID messageId, String bpId, ScanStatus scanStatus, LocalDateTime createdAt) {
        return InboundMessage.builder()
                .messageId(messageId)
                .bpId(bpId)
                .contentLength(100)
                .overrideCreatedAt(createdAt)
                .messageType("NI015")
                .partnerTopic("aPartnerTopic")
                .partnerExternalReference("anExternalReference")
                .contentType("application/xml")
                .scanStatus(scanStatus)
                .build();
    }

    private InboundMessage storeMessageInDb(String bpId, int contentLength) {
        InboundMessage message = InboundMessage.builder()
                .messageId(UUID.randomUUID())
                .bpId(bpId)
                .contentLength(contentLength)
                .build();
        inboundMessageRepository.upsertScanStatusAndMetadata(message);
        return message;
    }

    private InboundMessage storeMessageInDb(LocalDateTime createdAt) {
        InboundMessage message = InboundMessage.builder()
                .messageId(UUID.randomUUID())
                .bpId("123")
                .contentLength(123)
                .overrideCreatedAt(createdAt)
                .build();
        inboundMessageRepository.upsertScanStatusAndMetadata(message);
        return message;
    }


}
