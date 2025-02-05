package ch.admin.bit.jeap.messageexchange.web;

import ch.admin.bit.jeap.messageexchange.event.message.received.B2BMessageReceivedEvent;
import ch.admin.bit.jeap.messaging.kafka.test.TestKafkaListener;
import lombok.Getter;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
@Getter
class TestEventConsumer {

    private final List<B2BMessageReceivedEvent> messages = new CopyOnWriteArrayList<>();

    @TestKafkaListener(topics = "message-received")
    public void onEvent(@Payload B2BMessageReceivedEvent message) {
        messages.add(message);
    }

    public boolean hasMessageWithIdempotenceId(UUID messageId) {
        return getMessages().stream()
                .anyMatch(m -> m.getIdentity().getIdempotenceId().equals(messageId.toString()));
    }

    public B2BMessageReceivedEvent getMessageByIdempotenceId(UUID messageId) {
        return getMessages().stream()
                .filter(m -> m.getIdentity().getIdempotenceId().equals(messageId.toString()))
                .findFirst()
                .orElseThrow();
    }
}
