package ch.admin.bit.jeap.messageexchange.domain;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MessageTest {

    @Test
    void buildMessage_requiredFieldsProvided_messageCreated() {
        Message message = Message.builder()
                .messageId(UUID.randomUUID())
                .bpId("bpID")
                .messageType("messageType")
                .topicName("topicName")
                .build();

        assertThat(message).isNotNull();
        assertThat(message.getGroupId()).isNull();
        assertThat(message.getPartnerTopic()).isNull();
    }

    @Test
    void buildMessage_messageIdMissing_messageNotCreated() {
        Message.MessageBuilder messageBuilder = Message.builder()
                .bpId("bpID")
                .messageType("messageType")
                .topicName("topicName");
        assertThrows(NullPointerException.class, messageBuilder::build);
    }

    @Test
    void buildMessage_bpIdMissing_messageNotCreated() {
        Message.MessageBuilder messageBuilder = Message.builder()
                .messageId(UUID.randomUUID())
                .messageType("messageType")
                .topicName("topicName");
        assertThrows(NullPointerException.class, messageBuilder::build);
    }

    @Test
    void buildMessage_messageTypeMissing_messageNotCreated() {
        Message.MessageBuilder messageBuilder = Message.builder()
                .messageId(UUID.randomUUID())
                .bpId("bpID")
                .topicName("topicName");
        assertThrows(NullPointerException.class, messageBuilder::build);
    }

    @Test
    void buildMessage_topicNameMissing_messageNotCreated() {
        Message.MessageBuilder messageBuilder = Message.builder()
                .messageId(UUID.randomUUID())
                .bpId("bpID")
                .messageType("messageType");
        assertThrows(NullPointerException.class, messageBuilder::build);
    }
}
