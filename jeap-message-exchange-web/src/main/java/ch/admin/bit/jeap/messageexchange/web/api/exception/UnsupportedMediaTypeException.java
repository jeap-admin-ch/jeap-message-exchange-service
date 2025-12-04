package ch.admin.bit.jeap.messageexchange.web.api.exception;

public class UnsupportedMediaTypeException extends Exception {

    private static final String MEDIA_TYPE_NOT_SUPPORTED = "The media type '%s' is not supported.";

    private UnsupportedMediaTypeException(String msg) {
        super(msg);
    }

    public static UnsupportedMediaTypeException mediaTypeNotSupported(String mediaType) {
        return new UnsupportedMediaTypeException(MEDIA_TYPE_NOT_SUPPORTED.formatted(mediaType));
    }
}
