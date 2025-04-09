package ch.admin.bit.jeap.messageexchange.web.api.dto;

import ch.admin.bit.jeap.messageexchange.domain.dto.MessageSearchResultDto;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;

@Schema(name = "messages")
@JacksonXmlRootElement(localName = "messages")
public record MessagesResultDto(
        @JacksonXmlElementWrapper(useWrapping = false)
        @JacksonXmlProperty(localName = "message")
        @Schema(name = "message")
        List<MessageResultDto> messages) {

    /**
     * Creates a list of message search results that do not include the groupId and partnerTopic in the response,
     * conforming to the schema of the response in the partner API v2.
     */
    public static MessagesResultDto createV2Dto(List<MessageSearchResultDto> searchResults) {
        return new MessagesResultDto(
                searchResults.stream()
                        .map(MessageResultDto::createV2Dto)
                        .toList());
    }

    /**
     * Creates a list of message search results that include the groupId and partnerTopic in the response.
     */
    public static MessagesResultDto createV3Dto(List<MessageSearchResultDto> searchResults) {
        return new MessagesResultDto(
                searchResults.stream()
                        .map(MessageResultDto::createV3Dto)
                        .toList());
    }
}

