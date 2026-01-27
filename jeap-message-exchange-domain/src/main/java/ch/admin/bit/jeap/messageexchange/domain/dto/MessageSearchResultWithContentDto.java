package ch.admin.bit.jeap.messageexchange.domain.dto;

import ch.admin.bit.jeap.messageexchange.domain.MessageContent;

import java.util.Map;
import java.util.UUID;

public record MessageSearchResultWithContentDto(UUID messageId, MessageContent messageContent, String partnerTopic, String partnerExternalReference, Map<String, String> metadata) {
}
