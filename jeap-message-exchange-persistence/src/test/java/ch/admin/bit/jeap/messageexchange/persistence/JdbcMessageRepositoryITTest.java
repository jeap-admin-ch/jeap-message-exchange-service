package ch.admin.bit.jeap.messageexchange.persistence;

import ch.admin.bit.jeap.messageexchange.domain.Message;
import ch.admin.bit.jeap.messageexchange.domain.dto.MessageSearchResultDto;
import ch.admin.bit.jeap.messageexchange.domain.messaging.EventPublisher;
import ch.admin.bit.jeap.messageexchange.domain.metrics.MetricsService;
import ch.admin.bit.jeap.messageexchange.domain.objectstore.ObjectStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.List;
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
class JdbcMessageRepositoryITTest {

    @Autowired
    private JdbcMessageRepository messageRepository;

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
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine");

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
        jdbcTemplate.update("DELETE FROM b2bhub_db_table", Map.of());
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

        Message message = Message.builder()
                .messageId(messageId)
                .bpId("myBpId")
                .messageType("myMessageType")
                .groupId("myGroupId")
                .topicName("myTopicName")
                .partnerTopic("myPartnerTopic")
                .contentType(MediaType.APPLICATION_XML_VALUE)
                .build();
        assertThat(message).isNotNull();

        messageRepository.save(message);

        Optional<Message> result = messageRepository.findByMessageId(messageId);
        assertThat(result).isPresent();
        Message savedMessage = result.get();
        BigInteger messageSequenceId = savedMessage.getSequenceId();
        assertThat(messageSequenceId).isNotNull();
        assertEquals(messageId, savedMessage.getMessageId());
        assertEquals("myBpId", savedMessage.getBpId());
        assertEquals("myTopicName", savedMessage.getTopicName());
        assertEquals("myMessageType", savedMessage.getMessageType());
        assertEquals("myGroupId", savedMessage.getGroupId());
        assertEquals("myPartnerTopic", savedMessage.getPartnerTopic());
        assertNotNull(savedMessage.getDatePublished());

        UUID message2Id = UUID.randomUUID();
        LocalDateTime timeIn2000 = LocalDateTime.of(2000, 1, 1, 13, 45);
        Message message2 = Message.builder()
                .messageId(message2Id)
                .bpId("myBpId2")
                .messageType("myMessageType2")
                .groupId("myGroupId2")
                .topicName("myTopicName2")
                .partnerTopic("myPartnerTopic2")
                .overrideCreatedAt(timeIn2000)
                .contentType(MediaType.APPLICATION_XML_VALUE)
                .build();
        assertThat(message2).isNotNull();

        messageRepository.save(message2);

        result = messageRepository.findByMessageId(message2Id);
        assertThat(result).isPresent();
        savedMessage = result.get();
        assertThat(savedMessage.getSequenceId()).isEqualTo(messageSequenceId.add(BigInteger.ONE));
        assertEquals(message2Id, savedMessage.getMessageId());
        assertEquals("myBpId2", savedMessage.getBpId());
        assertEquals("myTopicName2", savedMessage.getTopicName());
        assertEquals("myMessageType2", savedMessage.getMessageType());
        assertEquals("myGroupId2", savedMessage.getGroupId());
        assertEquals("myPartnerTopic2", savedMessage.getPartnerTopic());
        assertEquals(timeIn2000, savedMessage.getDatePublished());
    }

    @Test
    void saveWithOnlyRequiredAttributesAndGetMessage() {
        UUID messageId = UUID.randomUUID();

        Message message = Message.builder()
                .messageId(messageId)
                .bpId("bpId")
                .messageType("messageType")
                .topicName("topicName")
                .contentType(MediaType.APPLICATION_XML_VALUE)
                .build();
        assertThat(message).isNotNull();

        messageRepository.save(message);

        Optional<Message> result = messageRepository.findByMessageId(messageId);
        assertThat(result).isPresent();
        Message savedMessage = result.get();
        BigInteger messageSequenceId = savedMessage.getSequenceId();
        assertThat(messageSequenceId).isNotNull();
        assertEquals(messageId, savedMessage.getMessageId());

    }

    @Test
    void saveMessage_sameMessageId_idempotentIsIgnored() {
        UUID messageId = UUID.randomUUID();

        Message message = Message.builder()
                .messageId(messageId)
                .bpId("bpId")
                .messageType("messageType")
                .groupId("groupId")
                .topicName("topicName")
                .partnerTopic("partnerTopic")
                .contentType(MediaType.APPLICATION_XML_VALUE)
                .build();
        assertThat(message).isNotNull();

        // message is saved
        messageRepository.save(message);

        // message is not saved but no error is thrown
        messageRepository.save(message);
    }

    @Test
    void getMessages() {
        String bpId = UUID.randomUUID().toString();

        Message message1 = storeMessageInDb(bpId, "partnerTopic1", "topicName1", "groupId1");
        Message message2 = storeMessageInDb(bpId, "partnerTopic2", "topicName2", "groupId2");

        List<MessageSearchResultDto> messages = messageRepository.getMessages(bpId, null, null, null, null, 10);
        assertThat(messages).hasSize(2);

        messages = messageRepository.getMessages(bpId, null, null, message1.getMessageId(), null, 10);
        assertThat(messages).hasSize(1);

        messages = messageRepository.getMessages(bpId, null, null, message2.getMessageId(), null, 10);
        assertThat(messages).isEmpty();

        messages = messageRepository.getMessages(bpId, null, null, null, null, 1);
        assertThat(messages).hasSize(1);

        messages = messageRepository.getMessages(bpId, message1.getTopicName(), null, null, null, 10);
        assertThat(messages).hasSize(1);

        messages = messageRepository.getMessages(bpId, message2.getTopicName(), null, null, null, 10);
        assertThat(messages).hasSize(1);

        messages = messageRepository.getMessages("other", null, null, null, null, 10);
        assertThat(messages).isEmpty();

        messages = messageRepository.getMessages(bpId, message2.getTopicName(), message2.getGroupId(), null, message2.getPartnerTopic(), 10);
        assertThat(messages).hasSize(1);

        messages = messageRepository.getMessages(bpId, message2.getTopicName(), message2.getGroupId(), message1.getMessageId(), message2.getPartnerTopic(), 10);
        assertThat(messages).hasSize(1);

        messages = messageRepository.getMessages(bpId, message2.getTopicName(), message2.getGroupId(), message2.getMessageId(), message2.getPartnerTopic(), 10);
        assertThat(messages).isEmpty();

    }

    @Test
    void getNextMessageId_newMessage_returnsUuid() {
        String bpId = UUID.randomUUID().toString();

        Message message1 = storeMessageInDb(bpId, "partnerTopic1", "topicName1", "groupId1");
        Message message2 = storeMessageInDb(bpId, "partnerTopic2", "topicName2", "groupId2");

        Optional<UUID> nextMessageId = messageRepository.getNextMessageId(message1.getMessageId(), bpId, null, null);
        assertThat(nextMessageId)
                .isPresent()
                .contains(message2.getMessageId());
    }

    @Test
    void getNextMessageId_newMessageWithAllArgs_returnsUuid() {
        String bpId = UUID.randomUUID().toString();

        Message message1 = storeMessageInDb(bpId, "partnerTopic1", "topicName1", "groupId1");
        Message message2 = storeMessageInDb(bpId, "partnerTopic2", "topicName2", "groupId2");

        Optional<UUID> nextMessageId = messageRepository.getNextMessageId(message1.getMessageId(), bpId, "partnerTopic2", "topicName2");
        assertThat(nextMessageId)
                .isPresent()
                .contains(message2.getMessageId());

        nextMessageId = messageRepository.getNextMessageId(message1.getMessageId(), bpId, null, "topicName2");
        assertThat(nextMessageId)
                .isPresent()
                .contains(message2.getMessageId());

        nextMessageId = messageRepository.getNextMessageId(message1.getMessageId(), bpId, "partnerTopic2", null);
        assertThat(nextMessageId)
                .isPresent()
                .contains(message2.getMessageId());

        nextMessageId = messageRepository.getNextMessageId(message1.getMessageId(), bpId, "foo", "topicName2");
        assertThat(nextMessageId).isEmpty();

        nextMessageId = messageRepository.getNextMessageId(message1.getMessageId(), bpId, "partnerTopic2", "bar");
        assertThat(nextMessageId).isEmpty();

    }

    @Test
    void getNextMessageId_noNewMessage_returnsEmpty() {
        String bpId = UUID.randomUUID().toString();

        Message message1 = storeMessageInDb(bpId, "partnerTopic1", "topicName1", "groupId1");

        Optional<UUID> nextMessageId = messageRepository.getNextMessageId(message1.getMessageId(), bpId, null, null);
        assertThat(nextMessageId).isEmpty();
    }

    @Test
    void getNextMessageId_noMessageForBpId_returnsEmpty() {
        String bpId = UUID.randomUUID().toString();

        Message message1 = storeMessageInDb(bpId, "partnerTopic1", "topicName1", "groupId1");
        storeMessageInDb(bpId, "partnerTopic2", "topicName2", "groupId2");

        Optional<UUID> nextMessageId = messageRepository.getNextMessageId(message1.getMessageId(), "dummy", null, null);
        assertThat(nextMessageId).isEmpty();
    }

    @Test
    void findByMessageId_messageWithIdNotFound_returnsEmpty() {
        UUID messageId = UUID.randomUUID();

        Optional<Message> result = messageRepository.findByMessageId(messageId);
        assertThat(result).isEmpty();
    }

    @Test
    void deleteExpiredMessages() {
        boolean resultEmptyRepoNoMessagesDeleted = messageRepository.deleteExpiredMessages(14, 10);
        assertThat(resultEmptyRepoNoMessagesDeleted)
                .isFalse();

        LocalDateTime now = LocalDateTime.now();
        LocalDateTime nowMinus14Days = now.minusDays(14);
        LocalDateTime nowMinus48Days = now.minusDays(48);
        LocalDateTime nowMinus96Days = now.minusDays(96);
        UUID notExpiredMessageId = storeMessageInDb(now).getMessageId();
        storeMessageInDb(nowMinus14Days);
        storeMessageInDb(nowMinus48Days);
        storeMessageInDb(nowMinus96Days);

        int olderThanDays = 12;
        int limit = 2;
        boolean resultLimit2TwoMessagesDeleted = messageRepository.deleteExpiredMessages(olderThanDays, limit);
        assertThat(resultLimit2TwoMessagesDeleted)
                .isTrue();

        boolean resultOneRemainingMessageDeleted = messageRepository.deleteExpiredMessages(olderThanDays, limit);
        assertThat(resultOneRemainingMessageDeleted)
                .isTrue();

        boolean resultNoMoreExpiredMessagesToDelete = messageRepository.deleteExpiredMessages(olderThanDays, limit);
        assertThat(resultNoMoreExpiredMessagesToDelete)
                .isFalse();

        List<MessageSearchResultDto> messages = messageRepository.getMessages("123", "topic", null, null, null, 10);
        assertThat(messages)
                .hasSize(1)
                .first().hasFieldOrPropertyWithValue("messageId", notExpiredMessageId);
    }

    private Message storeMessageInDb(String bpId, String partnerTopic, String topicName, String groupId) {
        Message message = Message.builder()
                .messageId(UUID.randomUUID())
                .bpId(bpId)
                .messageType("messageType")
                .groupId(groupId)
                .topicName(topicName)
                .partnerTopic(partnerTopic)
                .contentType(MediaType.APPLICATION_XML_VALUE)
                .build();
        messageRepository.save(message);
        return message;
    }

    private Message storeMessageInDb(LocalDateTime createdAt) {
        Message message = Message.builder()
                .messageId(UUID.randomUUID())
                .bpId("123")
                .messageType("messageType")
                .topicName("topic")
                .overrideCreatedAt(createdAt)
                .contentType(MediaType.APPLICATION_XML_VALUE)
                .build();
        messageRepository.save(message);
        return message;
    }


}
