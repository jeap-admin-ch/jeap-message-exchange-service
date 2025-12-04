package ch.admin.bit.jeap.messageexchange.domain.exception;

import java.util.UUID;

public class MismatchedContentTypeException extends Exception {

    private static final String CONTENT_TYPE_ERROR_MESSAGE = "Message %s has content type %s but requested content type is %s";

    private MismatchedContentTypeException(String message) {
        super(message);
    }

    public static MismatchedContentTypeException requestedContentTypeIncorrect(UUID messageId, String contentType, String requestedContentType) {
        return new MismatchedContentTypeException(CONTENT_TYPE_ERROR_MESSAGE.formatted(messageId, contentType, requestedContentType));
    }
}
