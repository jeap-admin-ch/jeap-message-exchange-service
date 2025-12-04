package ch.admin.bit.jeap.messageexchange.kafka;

import ch.admin.bit.jeap.messageexchange.event.message.received.B2BMessageReceivedEvent;
import ch.admin.bit.jeap.messageexchange.plugin.api.listener.MessageReceivedListener;
import ch.admin.bit.jeap.messageexchange.plugin.api.listener.MessageResult;

import java.util.UUID;

public class TestMessageReceivedListener implements MessageReceivedListener {

    @Override
    public MessageResult onMessageReceived(UUID messageId, String bpId, String type) {
        B2BMessageReceivedEvent messageReceivedEvent = B2BMessageReceivedEventBuilder.create()
                .bpId(bpId + "_plugin")
                .messageId(messageId + "_plugin")
                .type(type + "_plugin")
                .systemName("junit")
                .serviceName("junit")
                .idempotenceId(messageId.toString())
                .contentType("junit")
                .build();

        return new MessageResult("junit-test", messageReceivedEvent);
    }
}
