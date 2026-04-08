package ch.admin.bit.jeap.messageexchange.domain.exception;

import java.util.UUID;

public class MismatchedContentException extends Exception {

    private static final String CONTENT_LENGTH_ERROR_MESSAGE = "Message with messageId %s already exists but has different content length";

    private static final String CHECKSUM_ERROR_MESSAGE = "Message with messageId %s already exists but has different content";

    private MismatchedContentException(String message) {
        super(message);
    }

    public static MismatchedContentException contentLengthDoesNotMatch(UUID messageId) {
        return new MismatchedContentException(CONTENT_LENGTH_ERROR_MESSAGE.formatted(messageId));
    }

    public static MismatchedContentException checksumDoesNotMatch(UUID messageId) {
        return new MismatchedContentException(CHECKSUM_ERROR_MESSAGE.formatted(messageId));
    }
}
