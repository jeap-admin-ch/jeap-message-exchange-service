package ch.admin.bit.jeap.messageexchange.domain;

import java.util.UUID;

public record NextMessageResultDto(UUID messageId, MessageContent messageContent) {
}
