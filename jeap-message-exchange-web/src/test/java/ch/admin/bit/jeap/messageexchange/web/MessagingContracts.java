package ch.admin.bit.jeap.messageexchange.web;

import ch.admin.bit.jeap.messageexchange.event.message.received.B2BMessageReceivedEvent;
import ch.admin.bit.jeap.messaging.annotations.JeapMessageConsumerContract;
import ch.admin.bit.jeap.messaging.annotations.JeapMessageProducerContract;
import ch.admin.bit.jeap.s3.malware.scanned.S3ObjectMalwareScannedEvent;

@JeapMessageProducerContract(appName = "junit", value = B2BMessageReceivedEvent.TypeRef.class, topic = "message-received")
@JeapMessageConsumerContract(appName = "junit", value = S3ObjectMalwareScannedEvent.TypeRef.class, topic = "malware-scan-result")
public final class MessagingContracts {
}
