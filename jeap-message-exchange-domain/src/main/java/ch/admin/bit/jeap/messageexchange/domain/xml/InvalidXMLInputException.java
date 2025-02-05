package ch.admin.bit.jeap.messageexchange.domain.xml;

import lombok.Getter;

import javax.xml.stream.XMLStreamException;
import java.util.UUID;

@Getter
public class InvalidXMLInputException extends RuntimeException {

    private final UUID messageId;

    private final String bpId;

    private InvalidXMLInputException(UUID messageId, String bpId, XMLStreamException e) {
        super("XML content invalid - not a well-formed XML message: " + e.getMessage(), e);
        this.messageId = messageId;
        this.bpId = bpId;
    }

    private InvalidXMLInputException(UUID messageId, String bpId) {
        super("XML content invalid - incomplete XML");
        this.messageId = messageId;
        this.bpId = bpId;
    }

    public static InvalidXMLInputException invalid(UUID messageId, String bpId, XMLStreamException ex) {
        return new InvalidXMLInputException(messageId, bpId, ex);
    }

    public static InvalidXMLInputException incomplete(UUID messageId, String bpId) {
        return new InvalidXMLInputException(messageId, bpId);
    }
}
