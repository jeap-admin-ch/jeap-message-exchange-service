package ch.admin.bit.jeap.messageexchange.web.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Map;
import java.util.UUID;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY;

public record MessageResultDto(
        UUID messageId,
        String messageType,
        @JsonInclude(NON_EMPTY) String groupId,
        @JsonInclude(NON_EMPTY) String partnerTopic,
        @JsonInclude(NON_EMPTY) String contentType,
        @JsonInclude(NON_EMPTY) String partnerExternalReference,
        @JsonInclude(NON_EMPTY) Map<String, String> metadata) {

    /**
     * Creates a message search result that does not include the groupId and partnerTopic in the response,
     * conforming to the schema of the response in the partner API v2.
     */
    public static MessageResultDto createV2Dto(ch.admin.bit.jeap.messageexchange.domain.dto.MessageSearchResultDto domainDto) {
        return new MessageResultDto(
                domainDto.messageId(),
                domainDto.messageType(),
                null,
                null,
                null,
                null,
                null);
    }

    /**
     * Creates a message search result that includes the groupId and partnerTopic in the response.
     */
    public static MessageResultDto createV3Dto(ch.admin.bit.jeap.messageexchange.domain.dto.MessageSearchResultDto domainDto) {
        return new MessageResultDto(
                domainDto.messageId(),
                domainDto.messageType(),
                domainDto.groupId(),
                domainDto.partnerTopic(),
                null,
                null,
                null);
    }

    /**
     * Creates a message search result that includes the contentType in the response.
     */
    public static MessageResultDto createV4Dto(ch.admin.bit.jeap.messageexchange.domain.dto.MessageSearchResultDto domainDto) {
        return new MessageResultDto(
                domainDto.messageId(),
                domainDto.messageType(),
                domainDto.groupId(),
                domainDto.partnerTopic(),
                domainDto.contentType(),
                domainDto.partnerExternalReference(),
                domainDto.metadata() == null || domainDto.metadata().isEmpty() ? null : new java.util.TreeMap<>(domainDto.metadata()));
    }
}
