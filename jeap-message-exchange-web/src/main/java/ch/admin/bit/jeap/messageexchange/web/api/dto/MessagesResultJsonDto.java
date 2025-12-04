package ch.admin.bit.jeap.messageexchange.web.api.dto;

import ch.admin.bit.jeap.messageexchange.domain.dto.MessageSearchResultDto;

import java.util.List;

public record MessagesResultJsonDto(List<MessageResultDto> messages) {

    public static MessagesResultJsonDto createDto(List<MessageSearchResultDto> searchResults) {
        return new MessagesResultJsonDto(
                searchResults.stream()
                        .map(MessageResultDto::createV4Dto)
                        .toList());
    }
}

