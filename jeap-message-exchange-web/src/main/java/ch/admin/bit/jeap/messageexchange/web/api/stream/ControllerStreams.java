package ch.admin.bit.jeap.messageexchange.web.api.stream;

import ch.admin.bit.jeap.messageexchange.domain.MessageContent;
import ch.admin.bit.jeap.messageexchange.web.api.MessageExchangeApiProperties;
import ch.admin.bit.jeap.messageexchange.web.api.exception.UnsupportedMediaTypeException;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;

import static ch.admin.bit.jeap.messageexchange.web.api.DeprecatedHeaderNames.HEADER_MESSAGE_ID_OLD;
import static ch.admin.bit.jeap.messageexchange.web.api.HeaderNames.HEADER_MESSAGE_ID;

@Slf4j
@Component
@RequiredArgsConstructor
public class ControllerStreams {

    private static final int BUFFER_SIZE = 16384;

    private final MessageExchangeApiProperties properties;

    public static ResponseEntity<InputStreamResource> toResponseEntityWithoutResponseHeaders(MessageContent messageContent) {
        return ResponseEntity.ok()
                .contentLength(messageContent.contentLength())
                .body(new InputStreamResource(messageContent.inputStream()));
    }

    //TODO: JEAP-5099 remove
    public static ResponseEntity<InputStreamResource> toResponseIncludingLegacyMessageIdHeader(MessageContent messageContent, String messageId) {
        return ResponseEntity.ok()
                //TODO: JEAP-5099 remove old header
                .header(HEADER_MESSAGE_ID_OLD, messageId)
                .header(HEADER_MESSAGE_ID, messageId)
                .contentLength(messageContent.contentLength())
                .body(new InputStreamResource(messageContent.inputStream()));
    }

    public static ResponseEntity<InputStreamResource> toResponseWithMessageIdHeader(MessageContent messageContent, String messageId) {
        return ResponseEntity.ok()
                .header(HEADER_MESSAGE_ID, messageId)
                .contentLength(messageContent.contentLength())
                .body(new InputStreamResource(messageContent.inputStream()));
    }

    public MessageContent getRequestContent(HttpServletRequest request) throws IOException {
        // The S3 api requires a valid content length, otherwise the object will have a content-length of zero
        int contentLength = request.getContentLength();
        if (contentLength != -1) {
            return new MessageContent(request.getInputStream(), contentLength);
        } else {
            return readRequestOfUnknownLengthToByteArray(request);
        }
    }

    public String validateContentType(String contentType) throws UnsupportedMediaTypeException {
        List<MediaType> mediaTypes = properties.getMediaTypes();
        String contentTypeToValidate = contentType.split(";")[0];
        log.debug("Validating content type '{}' with allowed mediaTypes '{}'", contentType, mediaTypes);
        if (!mediaTypes.stream().map(MediaType::toString).toList().contains(contentTypeToValidate)){
            throw UnsupportedMediaTypeException.mediaTypeNotSupported(contentType);
        }
        return contentTypeToValidate;
    }

    private MessageContent readRequestOfUnknownLengthToByteArray(HttpServletRequest request) throws IOException {
        log.info("No content length given - fully reading request");
        try (var inputStream = request.getInputStream()) {
            ZeroCopySizeLimitedByteArrayOutputStream buffer = new ZeroCopySizeLimitedByteArrayOutputStream(BUFFER_SIZE, properties.getMaxRequestBodySizeInBytes());
            inputStream.transferTo(buffer);
            return new MessageContent(buffer.getInputStream(), buffer.size());
        }
    }
}
