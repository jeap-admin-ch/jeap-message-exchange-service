package ch.admin.bit.jeap.messageexchange.domain.messaging;

import ch.admin.bit.jeap.messageexchange.domain.malwarescan.PublishedScanStatus;

import java.util.UUID;

public interface EventPublisher {
    void publishMessageReceivedEvent(UUID messageId, String bpId, String type, PublishedScanStatus externalPublishedScanStatus);
}
