package ch.admin.bit.jeap.messageexchange.web;

import ch.admin.bit.jeap.messageexchange.event.message.received.B2BMessageReceivedEvent;
import ch.admin.bit.jeap.messaging.annotations.JeapMessageProducerContract;

@JeapMessageProducerContract(appName = "junit", value = B2BMessageReceivedEvent.TypeRef.class, topic = "message-received")
public final class MessagingContracts {
}
