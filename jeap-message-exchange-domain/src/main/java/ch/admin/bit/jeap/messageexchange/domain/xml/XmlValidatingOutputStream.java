package ch.admin.bit.jeap.messageexchange.domain.xml;

import ch.admin.bit.jeap.messageexchange.domain.MessageContent;
import com.fasterxml.aalto.AsyncByteArrayFeeder;
import com.fasterxml.aalto.AsyncXMLInputFactory;
import com.fasterxml.aalto.AsyncXMLStreamReader;
import com.fasterxml.aalto.stax.InputFactoryImpl;
import org.apache.commons.io.input.TeeInputStream;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;

public class XmlValidatingOutputStream extends OutputStream {

    private static final AsyncXMLInputFactory XML_INPUT_FACTORY = new InputFactoryImpl();

    private final AsyncXMLStreamReader<AsyncByteArrayFeeder> parser;
    private final int contentLength;
    private int bytesProcessed;
    private final String bpId;
    private final UUID messageId;
    private final byte[] singleByteBuffer;

    public static InputStream wrapInputStreamWithXmlValidation(UUID messageId, String bpId, MessageContent messageContent) {
        OutputStream branch = new XmlValidatingOutputStream(messageContent.contentLength(), bpId, messageId);
        return new TeeInputStream(messageContent.inputStream(), branch, true);
    }

    XmlValidatingOutputStream(int contentLength, String bpId, UUID messageId) {
        this.contentLength = contentLength == -1 ? Integer.MAX_VALUE : contentLength;
        this.bytesProcessed = 0;
        this.bpId = bpId;
        this.messageId = messageId;
        this.parser = XML_INPUT_FACTORY.createAsyncForByteArray();
        this.singleByteBuffer = new byte[1];
    }

    @Override
    public void close() throws IOException {
        parser.getInputFeeder().endOfInput();
        try {
            parser.close();
        } catch (XMLStreamException e) {
            throw new IOException("Failed to close parser", e);
        }
    }

    @Override
    public void write(int data) throws IOException {
        singleByteBuffer[0] = (byte) data;
        write(singleByteBuffer);
    }

    @Override
    public void write(byte[] data) throws IOException {
        write(data, 0, data.length);
    }

    @Override
    public void write(byte[] data, int off, int len) throws IOException {
        try {
            parser.getInputFeeder().feedInput(data, off, len);
            bytesProcessed += len;

            int tokenType = 0;
            // The event "EVENT_INCOMPLETE" signals that the XML parser needs more input to be able to parse the next
            // token. At this point, wait for the next call to write() to provide more data.
            while (parser.hasNext() && tokenType != AsyncXMLStreamReader.EVENT_INCOMPLETE) {
                tokenType = parser.next();
            }

            if (bytesProcessed >= contentLength) {
                parser.getInputFeeder().endOfInput();

                // Consume remaining token
                if (tokenType == AsyncXMLStreamReader.EVENT_INCOMPLETE && parser.hasNext()) {
                    tokenType = parser.next();
                }
                if (XMLStreamConstants.END_DOCUMENT != tokenType) {
                    throw InvalidXMLInputException.incomplete(messageId, bpId);
                }
            }
        } catch (XMLStreamException e) {
            throw InvalidXMLInputException.invalid(messageId, bpId, e);
        }
    }
}
