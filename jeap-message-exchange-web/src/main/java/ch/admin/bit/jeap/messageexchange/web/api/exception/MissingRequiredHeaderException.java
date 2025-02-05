package ch.admin.bit.jeap.messageexchange.web.api.exception;

//TODO: JEAP-5099 delete class
public class MissingRequiredHeaderException extends Exception {

    private static final String REQUIRED_HEADER_S_MISSING_IN_REQUEST = "Required header '%s' missing in request.";
    private static final String DIFFERENT_HEADERS = "The supplied values for the 2 headers '%s' and '%s' are not equal";

    private MissingRequiredHeaderException(String msg) {
        super(msg);
    }

    public static MissingRequiredHeaderException missingHeader(String headerName) {
        return new MissingRequiredHeaderException(REQUIRED_HEADER_S_MISSING_IN_REQUEST.formatted(headerName));
    }

    public static MissingRequiredHeaderException differentHeaders(String label, String labelOld) {
        return new MissingRequiredHeaderException(DIFFERENT_HEADERS.formatted(label, labelOld));
    }
}
