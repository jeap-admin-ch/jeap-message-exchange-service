package ch.admin.bit.jeap.messageexchange.web.api.exception;

public class InvalidBpIdException extends Exception {

    private static final String UNAUTHORIZED_FOR_BP_ID_ERROR_MESSAGE = "Not authorized for BP ID '%s' given in request.";

    private InvalidBpIdException(String msg) {
        super(msg);
    }

    public static InvalidBpIdException unauthorizedBpId(String bpId) {
        return new InvalidBpIdException(UNAUTHORIZED_FOR_BP_ID_ERROR_MESSAGE.formatted(bpId));
    }

    public static InvalidBpIdException missingBpId() {
        return new InvalidBpIdException("BP ID missing in request.");
    }
}
