package ch.admin.bit.jeap.messageexchange.domain.dto;

import java.util.Map;
import java.util.UUID;

public record MessageSearchResultDto(
        UUID messageId,
        String messageType,
        String contentType,
        String groupId,
        String partnerTopic,
        String partnerExternalReference,
        Map<String, String> metadata) {
}
