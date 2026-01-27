package ch.admin.bit.jeap.messageexchange.web.api.exception;

public class InvalidMetadataException extends Exception {

    private InvalidMetadataException(String msg) {
        super(msg);
    }

    public static InvalidMetadataException invalidBase64() {
        return new InvalidMetadataException("The content of mes-metadata cannot be decoded from Base64");
    }

    public static InvalidMetadataException invalidJson() {
        return new InvalidMetadataException("The content of mes-metadata is not a valid JSON content");
    }

    public static InvalidMetadataException invalidJsonSchema() {
        return new InvalidMetadataException("The content of mes-metadata is not valid against the mes-metadata JSON schema");
    }
}
