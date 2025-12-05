package ch.admin.bit.jeap.messageexchange.web.api;

import ch.admin.bit.jeap.messageexchange.domain.Message;
import ch.admin.bit.jeap.messageexchange.domain.MessageExchangeService;
import ch.admin.bit.jeap.messageexchange.web.api.exception.MissingRequiredHeaderException;
import ch.admin.bit.jeap.messageexchange.web.api.mdc.MessageIdBpIdMdcCloseable;
import ch.admin.bit.jeap.messageexchange.web.api.stream.ControllerStreams;
import io.micrometer.core.annotation.Timed;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.UUID;

import static ch.admin.bit.jeap.messageexchange.web.api.DeprecatedHeaderNames.*;
import static ch.admin.bit.jeap.messageexchange.web.api.HeaderNames.*;
import static ch.admin.bit.jeap.messageexchange.web.api.LegacyHeaderHelper.checkVariables;

/**
 * @deprecated
 */
@RestController
@RequestMapping("/api/internal/v2/messages")
@RequiredArgsConstructor
@Slf4j
@Validated
@Deprecated(since="4.0.0", forRemoval=true) // replaced by MessageInternalV3Controller
public class MessageInternalController {

    private final MessageExchangeService messageExchangeService;
    private final ControllerStreams controllerStreams;

    @PutMapping(value = "/{messageId}", consumes = MediaType.APPLICATION_XML_VALUE)
    @Operation(summary = "Sends a new message to a business partner",
            requestBody = @RequestBody(description = "XML message",
                    required = true, content = @Content(mediaType = MediaType.APPLICATION_XML_VALUE,
                    schema = @Schema(name = "anyxml"))))
    @PreAuthorize(Roles.HAS_ROLE_WRITE_MESSAGE_OUT)
    @Timed(value = "jeap_mes_internal_controller_send_message", description = "Time taken to send a message", percentiles = {0.5, 0.8, 0.95, 0.99})
    public ResponseEntity<Void> sendMessage(
            @PathVariable("messageId") @Parameter(description = "Message identification as UUID 12345678-1234-1234-1234-123456789012") UUID messageId,
            @RequestHeader(value = HEADER_BP_ID, required = false) @Parameter(description = "Receiving partner identification") String bpId,
            @RequestHeader(value = HEADER_BP_ID_OLD, required = false) @Parameter(description = "Receiving partner identification") String bpIdOld,
            @RequestParam("topicName") @Parameter(description = "Publish the message into a certain topic") String topicName,
            @RequestParam(value = "groupId", required = false) @Parameter(description = "Grouping identifier to group multiple messages") String groupId,
            @RequestHeader(value = HEADER_MESSAGE_TYPE, required = false) @Parameter(description = "Business type definition of the message body") String messageType,
            @RequestHeader(value = HEADER_MESSAGE_TYPE_OLD, required = false) @Parameter(description = "Business type definition of the message body") String messageTypeOld,
            @RequestHeader(value = HEADER_PARTNER_TOPIC, required = false) @Parameter(description = "Partner Topic") String partnerTopic,
            @RequestHeader(value = HEADER_PARTNER_TOPIC_OLD, required = false) @Parameter(description = "Partner Topic") String partnerTopicOld,
            HttpServletRequest request) throws IOException, MissingRequiredHeaderException {

        bpId = checkVariables(bpId, bpIdOld, HEADER_BP_ID_OLD, HEADER_BP_ID, true);
        messageType = checkVariables(messageType, messageTypeOld, HEADER_MESSAGE_TYPE_OLD, HEADER_MESSAGE_TYPE, true);
        partnerTopic = checkVariables(partnerTopic, partnerTopicOld, HEADER_PARTNER_TOPIC_OLD, HEADER_PARTNER_TOPIC, false);

        try (var ignored = MessageIdBpIdMdcCloseable.mdcMessageIdAndBpId(messageId, bpId)) {
            Message message = Message.builder()
                    .messageId(messageId)
                    .bpId(bpId)
                    .topicName(topicName)
                    .groupId(groupId)
                    .messageType(messageType)
                    .partnerTopic(partnerTopic)
                    .contentType(MediaType.APPLICATION_XML_VALUE)
                    .build();

            log.info("Send new message {} with size {} to partner", message, request.getContentLength());

            messageExchangeService.saveNewMessageFromInternalApplicationLegacy(message, controllerStreams.getRequestContent(request));
            log.debug("Message with messageId {} successfully saved", messageId);
            return new ResponseEntity<>(HttpStatus.CREATED);
        }
    }

    @GetMapping(value = "/{messageId}", produces = MediaType.APPLICATION_XML_VALUE)
    @Operation(summary = "Receive a message from a partner with the messageId",
            responses = @ApiResponse(responseCode = "403", description = "Not delivering because of the malware scan status")
    )
    @PreAuthorize(Roles.HAS_ROLE_READ_MESSAGE_IN)
    @Timed(value = "jeap_mes_internal_controller_get_message", description = "Time taken to retrieve a message", percentiles = {0.5, 0.8, 0.95, 0.99})
    public ResponseEntity<InputStreamResource> getMessage(@PathVariable("messageId") @Parameter(description = "Message identification as UUID 12345678-1234-1234-1234-123456789012") UUID messageId) {
        try (var ignored = MessageIdBpIdMdcCloseable.mdcMessageId(messageId)) {
            log.debug("Received get message request for messageId {}", messageId);
            return messageExchangeService.getMessageFromPartner(messageId)
                    .map(ControllerStreams::toResponseEntityWithoutResponseHeaders)
                    .orElseGet(() -> new ResponseEntity<>(HttpStatus.NOT_FOUND));
        }
    }
}
