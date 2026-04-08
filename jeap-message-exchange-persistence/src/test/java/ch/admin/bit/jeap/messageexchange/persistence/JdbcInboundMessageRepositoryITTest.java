package ch.admin.bit.jeap.messageexchange.persistence;

import ch.admin.bit.jeap.messageexchange.domain.InboundMessage;
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
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigInteger;
import java.time.LocalDateTime;
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
@Testcontainers
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
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17-alpine");

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
    void saveAndGetMessage() {
        UUID messageId = UUID.randomUUID();

        InboundMessage message = InboundMessage.builder()
                .messageId(messageId)
                .bpId("myBpId")
                .contentLength(100)
                .build();
        assertThat(message).isNotNull();

        inboundMessageRepository.save(message);

        Optional<InboundMessage> result = inboundMessageRepository.findByBpIdAndMessageId("myBpId", messageId);
        assertThat(result).isPresent();
        InboundMessage savedMessage = result.get();
        BigInteger messageSequenceId = savedMessage.getSequenceId();
        assertThat(messageSequenceId).isNotNull();
        assertEquals(messageId, savedMessage.getMessageId());
        assertEquals("myBpId", savedMessage.getBpId());
        assertEquals(100, savedMessage.getContentLength());
        assertNotNull(savedMessage.getCreatedAt());

        LocalDateTime timeIn2000 = LocalDateTime.of(2000, 1, 1, 13, 45);
        InboundMessage message2 = InboundMessage.builder()
                .messageId(messageId)
                .bpId("myBpId2")
                .contentLength(200)
                .overrideCreatedAt(timeIn2000)
                .build();
        assertThat(message2).isNotNull();

        inboundMessageRepository.save(message2);

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
    void saveMessage_sameBpIdAndMessageId_ignoreDuplicateException() {
        UUID messageId = UUID.randomUUID();

        InboundMessage message = InboundMessage.builder()
                .messageId(messageId)
                .bpId("bpId")
                .contentLength(123)
                .build();
        assertThat(message).isNotNull();

        // message is saved
        inboundMessageRepository.save(message);

        // duplicate exception is ignored
        inboundMessageRepository.save(message);
    }

    @Test
    void saveMessage_sameMessageId_messagesSaved() {
        UUID messageId = UUID.randomUUID();

        InboundMessage message = InboundMessage.builder()
                .messageId(messageId)
                .bpId("bpId")
                .contentLength(123)
                .build();
        assertThat(message).isNotNull();

        inboundMessageRepository.save(message);

        InboundMessage message2 = InboundMessage.builder()
                .messageId(messageId)
                .bpId("bpId2")
                .contentLength(123)
                .build();
        assertThat(message2).isNotNull();

        inboundMessageRepository.save(message2);
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

    private InboundMessage storeMessageInDb(String bpId, int contentLength) {
        InboundMessage message = InboundMessage.builder()
                .messageId(UUID.randomUUID())
                .bpId(bpId)
                .contentLength(contentLength)
                .build();
        inboundMessageRepository.save(message);
        return message;
    }

    private InboundMessage storeMessageInDb(LocalDateTime createdAt) {
        InboundMessage message = InboundMessage.builder()
                .messageId(UUID.randomUUID())
                .bpId("123")
                .contentLength(123)
                .overrideCreatedAt(createdAt)
                .build();
        inboundMessageRepository.save(message);
        return message;
    }


}
