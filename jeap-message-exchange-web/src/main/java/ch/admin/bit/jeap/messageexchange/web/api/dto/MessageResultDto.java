package ch.admin.bit.jeap.messageexchange.web.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.UUID;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY;

public record MessageResultDto(
        UUID messageId,
        String messageType,
        @JsonInclude(NON_EMPTY) String groupId,
        @JsonInclude(NON_EMPTY) String partnerTopic) {

    /**
     * Creates a message search result that does not include the groupId and partnerTopic in the response,
     * conforming to the schema of the response in the partner API v2.
     */
    public static MessageResultDto createV2Dto(ch.admin.bit.jeap.messageexchange.domain.dto.MessageSearchResultDto domainDto) {
        return new MessageResultDto(
                domainDto.messageId(),
                domainDto.messageType(), null, null);
    }

    /**
     * Creates a message search result that includes the groupId and partnerTopic in the response.
     */
    public static MessageResultDto createV3Dto(ch.admin.bit.jeap.messageexchange.domain.dto.MessageSearchResultDto domainDto) {
        return new MessageResultDto(
                domainDto.messageId(),
                domainDto.messageType(),
                domainDto.groupId(),
                domainDto.partnerTopic());
    }
}
