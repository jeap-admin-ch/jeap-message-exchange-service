package ch.admin.bit.jeap.messageexchange.web.api;

import ch.admin.bit.jeap.messageexchange.domain.MessageExchangeService;
import ch.admin.bit.jeap.messageexchange.domain.dto.MessageSearchResultDto;
import ch.admin.bit.jeap.messageexchange.domain.exception.MismatchedContentTypeException;
import ch.admin.bit.jeap.messageexchange.web.api.dto.MessagesResultJsonDto;
import ch.admin.bit.jeap.messageexchange.web.api.exception.InvalidBpIdException;
import ch.admin.bit.jeap.messageexchange.web.api.exception.UnsupportedMediaTypeException;
import ch.admin.bit.jeap.messageexchange.web.api.mdc.MessageIdBpIdMdcCloseable;
import ch.admin.bit.jeap.messageexchange.web.api.stream.ControllerStreams;
import ch.admin.bit.jeap.security.resource.semanticAuthentication.ServletSemanticAuthorization;
import io.micrometer.core.annotation.Timed;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import static ch.admin.bit.jeap.messageexchange.web.api.HeaderNames.*;
import static ch.admin.bit.jeap.messageexchange.web.api.QueryParameterNames.*;
import static org.springframework.util.StringUtils.hasText;

@RestController
@RequiredArgsConstructor
@Slf4j
@Validated
@RequestMapping("/api/partner/v4/messages")
public class MessagePartnerV4Controller {

    private static final String MESSAGE_ID = "messageId";

    private final MessageExchangeService messageExchangeService;
    private final ControllerStreams controllerStreams;
    private final ServletSemanticAuthorization jeapSemanticAuthorization;
    private final MetadataConverter metadataConverter;

    @PutMapping(value = "/{messageId}")
    @Operation(summary = "Submits a new message",
            requestBody = @RequestBody(description = "Message",
                    required = true,
                    content = @Content(mediaType = "*/*", schema = @Schema(type = "string"))))
    @PreAuthorize(Roles.HAS_ROLE_WRITE_MESSAGE_IN)
    @Timed(value = "jeap_mes_partner_controller_send_message", description = "Time taken to save a message", percentiles = {0.5, 0.8, 0.95, 0.99})
    public ResponseEntity<Void> sendMessage(
            @PathVariable(MESSAGE_ID) @Parameter(description = "Unique message identification as UUID: cc7d5097-4d3f-4fff-af91-fd3680199642") UUID messageId,
            @RequestHeader(HEADER_BP_ID) @Parameter(description = "Partner identification") String bpId,
            @RequestHeader(HEADER_MESSAGE_TYPE) @Parameter(description = "Business type definition of the message body") String messageType,
            @RequestHeader(HttpHeaders.CONTENT_TYPE) @Parameter(description = "Content-Type of the message body") String contentTypeHeader,
            @RequestHeader(value = HEADER_PARTNER_TOPIC, required = false) @Parameter(description = "Partner Topic") String partnerTopic,
            @RequestHeader(value = HEADER_PARTNER_EXTERNAL_REFERENCE, required = false) @Parameter(description = "Partner External Reference") String partnerExternalReference,
            HttpServletRequest request) throws InvalidBpIdException, IOException, UnsupportedMediaTypeException {

        String contentType = controllerStreams.validateContentType(contentTypeHeader);

        try (var ignored = MessageIdBpIdMdcCloseable.mdcMessageIdAndBpId(messageId, bpId)) {
            validateAuthorizedForBpId(bpId, Roles.MESSAGE_IN, Roles.WRITE);
            log.info("Send new message with messageId {}, bpId {}, messageType {}, size {}, contentType {}", messageId, bpId, messageType, request.getContentLength(), contentType);
            messageExchangeService.saveNewMessageFromPartner(messageId, bpId, messageType, partnerTopic, partnerExternalReference, controllerStreams.getRequestContent(request), contentType);
            log.debug("Message with messageId {} successfully saved", messageId);
            return new ResponseEntity<>(HttpStatus.CREATED);
        }
    }

    @GetMapping(value = "", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "Returns a list of messages for the given BP (Business Partner), topicName and / or groupId")
    @PreAuthorize(Roles.HAS_ROLE_READ_MESSAGE_OUT)
    @Timed(value = "jeap_mes_partner_controller_get_messages", description = "Time taken to retrieve a list of messages", percentiles = {0.5, 0.8, 0.95, 0.99})
    public MessagesResultJsonDto getMessages(
            @RequestHeader(HEADER_BP_ID) @Parameter(description = "Partner identification") String bpId,
            @RequestParam(value = QUERY_PARAM_TOPIC_NAME, required = false) @Parameter(description = "Get only messages from given topicName") String topicName,
            @RequestParam(value = QUERY_PARAM_GROUP_ID, required = false) @Parameter(description = "Get only messages with given groupId") String groupId,
            @RequestParam(value = QUERY_PARAM_LAST_MESSAGE_ID, required = false) @Parameter(description = "Get only messages which were published after the given messageId") UUID lastMessageId,
            @RequestParam(value = QUERY_PARAM_PARTNER_TOPIC, required = false) @Parameter(description = "Partner Topic") String partnerTopic,
            @RequestParam(value = QUERY_PARAM_PARTNER_EXTERNAL_REFERENCE, required = false) @Parameter(description = "Partner External Reference") String partnerExternalReference,
            @RequestParam(value = QUERY_PARAM_SIZE, defaultValue = "1000") @Parameter(description = "Number of messages returned") int size)
            throws InvalidBpIdException {
        return getMessages(bpId, topicName, groupId, lastMessageId, partnerTopic, partnerExternalReference, size, MessagesResultJsonDto::createDto);
    }

    private MessagesResultJsonDto getMessages(String bpId, String topicName, String groupId, UUID lastMessageId, String partnerTopic, String partnerExternalReference, int size, Function<List<MessageSearchResultDto>, MessagesResultJsonDto> dtoFactory) throws InvalidBpIdException {

        try (var ignored = MessageIdBpIdMdcCloseable.mdcBpId(bpId)) {
            validateAuthorizedForBpId(bpId, Roles.MESSAGE_OUT, Roles.READ);
            log.debug("Get messages with bpId {}, topicName {}, groupId {}, lastMessageId {}, partnerTopic {}, partnerExternalReference {}, size {}", bpId, topicName, groupId, lastMessageId, partnerTopic, partnerExternalReference, size);
            List<MessageSearchResultDto> searchResults = messageExchangeService.getMessages(bpId, topicName, groupId, lastMessageId, partnerTopic, partnerExternalReference, size);
            return dtoFactory.apply(searchResults);
        }
    }

    @GetMapping(value = "/{messageId}")
    @Operation(summary = "Get message")
    @PreAuthorize(Roles.HAS_ROLE_READ_MESSAGE_OUT)
    @Timed(value = "jeap_mes_partner_controller_get_message", description = "Time taken to retrieve a message", percentiles = {0.5, 0.8, 0.95, 0.99})
    public ResponseEntity<InputStreamResource> getMessage(
            @PathVariable(MESSAGE_ID) @Parameter(description = "Unique message identification as UUID: cc7d5097-4d3f-4fff-af91-fd3680199642") UUID messageId,
            @RequestHeader(HEADER_BP_ID) @Parameter(description = "Partner identification") String bpId,
            @RequestHeader(HttpHeaders.ACCEPT) @Parameter(description = "Content-Type of the message body") String accept
    ) throws InvalidBpIdException, MismatchedContentTypeException {

        try (var ignored = MessageIdBpIdMdcCloseable.mdcMessageIdAndBpId(messageId, bpId)) {
            validateAuthorizedForBpId(bpId, Roles.MESSAGE_OUT, Roles.READ);
            log.debug("Received get message request for messageId {} and bpId {}", messageId, bpId);
            return messageExchangeService.getMessageFromInternalApplication(bpId, messageId, accept)
                    .map(message -> ControllerStreams.toResponseWithHeaders(
                            message.messageContent(),
                            message.messageId().toString(),
                            message.partnerTopic(),
                            message.partnerExternalReference(),
                            metadataConverter.convertToBase64(message.metadata())
                    ))
                    .orElseGet(() -> new ResponseEntity<>(HttpStatus.NOT_FOUND));
        }
    }

    @GetMapping(value = "/{messageId}/next")
    @Operation(summary = "Get next message")
    @PreAuthorize(Roles.HAS_ROLE_READ_MESSAGE_OUT)
    @Timed(value = "jeap_mes_partner_controller_get_next_message", description = "Time taken to retrieve the next message", percentiles = {0.5, 0.8, 0.95, 0.99})
    public ResponseEntity<InputStreamResource> getNextMessage(
            @PathVariable(MESSAGE_ID) @Parameter(description = "Unique message identification as UUID: cc7d5097-4d3f-4fff-af91-fd3680199642") UUID lastMessageId,
            @RequestHeader(HEADER_BP_ID) @Parameter(description = "Partner identification") String bpId,
            @RequestParam(value = QUERY_PARAM_PARTNER_TOPIC, required = false) @Parameter(description = "Partner Topic") String partnerTopic,
            @RequestParam(value = QUERY_PARAM_TOPIC_NAME, required = false) @Parameter(description = "Get only messages from given topicName") String topicName,
            @RequestParam(value = QUERY_PARAM_PARTNER_EXTERNAL_REFERENCE, required = false) @Parameter(description = "Partner External Reference") String partnerExternalReference) throws InvalidBpIdException {

        try (var ignored = MessageIdBpIdMdcCloseable.mdcMessageIdAndBpId(lastMessageId, bpId)) {
            validateAuthorizedForBpId(bpId, Roles.MESSAGE_OUT, Roles.READ);
            log.debug("Received get next message request with lastMessageId {}, bpId {}, partnerTopic {}, topicName {}, partnerExternalReference {}", lastMessageId, bpId, partnerTopic, topicName, partnerExternalReference);
            return messageExchangeService.getNextMessageFromInternalApplication(lastMessageId, bpId, partnerTopic, topicName, partnerExternalReference)
                    .map(nextMessageResultDto -> ControllerStreams.toResponseWithHeaders(
                            nextMessageResultDto.messageContent(),
                            nextMessageResultDto.messageId().toString(),
                            nextMessageResultDto.partnerTopic(),
                            nextMessageResultDto.partnerExternalReference(),
                            metadataConverter.convertToBase64(nextMessageResultDto.metadata())
                    ))
                    .orElseGet(() -> new ResponseEntity<>(HttpStatus.NOT_FOUND));
        }
    }

    void validateAuthorizedForBpId(String bpId, String resource, String operation) throws InvalidBpIdException {
        if (!hasText(bpId)) {
            throw InvalidBpIdException.missingBpId();
        }
        if (!jeapSemanticAuthorization.hasRoleForPartner(resource, operation, bpId)) {
            throw InvalidBpIdException.unauthorizedBpId(bpId);
        }
    }
}
