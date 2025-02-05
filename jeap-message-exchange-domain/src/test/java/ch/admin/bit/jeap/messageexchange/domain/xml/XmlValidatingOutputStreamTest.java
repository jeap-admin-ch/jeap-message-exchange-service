package ch.admin.bit.jeap.messageexchange.domain.xml;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@SuppressWarnings("resource")
class XmlValidatingOutputStreamTest {

    @Test
    void write_simpleXml() {
        byte[] data = "<note/>".getBytes(UTF_8);
        XmlValidatingOutputStream xmlValidatingOutputStream = createXmlValidatingOutputStream(data.length);

        assertDoesNotThrow(() ->
                xmlValidatingOutputStream.write(data));
    }

    @Test
    void write_simpleXml_withOffset() {
        int offset = 5;
        byte[] data = "12345<note/>".getBytes(UTF_8);
        XmlValidatingOutputStream xmlValidatingOutputStream = createXmlValidatingOutputStream(data.length);

        assertDoesNotThrow(() ->
                xmlValidatingOutputStream.write(data, offset, data.length - offset));
    }

    @Test
    void write_anotherValidXml() {
        byte[] data = """
                <content>
                    <test>test</test>
                </content>""".getBytes(UTF_8);
        XmlValidatingOutputStream xmlValidatingOutputStream = createXmlValidatingOutputStream(data.length);

        assertDoesNotThrow(() ->
                xmlValidatingOutputStream.write(data));
    }

    @Test
    void write_invalidIncompleteXml() throws IOException {
        byte[] data = "<note/".getBytes(UTF_8);
        XmlValidatingOutputStream xmlValidatingOutputStream = createXmlValidatingOutputStream(data.length);

        assertThatThrownBy(() ->
                xmlValidatingOutputStream.write(data))
                .isInstanceOf(InvalidXMLInputException.class)
                .hasMessageContaining("incomplete");
    }

    @Test
    void write_invalidIncompleteElementXml() {
        byte[] data = "<note><bad</note>".getBytes(UTF_8);
        XmlValidatingOutputStream xmlValidatingOutputStream = createXmlValidatingOutputStream(data.length);

        assertThatThrownBy(() ->
                xmlValidatingOutputStream.write(data))
                .isInstanceOf(InvalidXMLInputException.class)
                .hasMessageContaining("well-formed");
    }

    @Test
    void write_invalidBadXml() {
        byte[] data = "thisisnotxml".getBytes(UTF_8);
        XmlValidatingOutputStream xmlValidatingOutputStream = createXmlValidatingOutputStream(data.length);

        assertThatThrownBy(() ->
                xmlValidatingOutputStream.write(data))
                .isInstanceOf(InvalidXMLInputException.class)
                .hasMessageContaining("well-formed ");
    }

    @Test
    void write_withDoctype() {
        byte[] data = """
                <?xml version="1.0" encoding="UTF-8"?>
                <!DOCTYPE note SYSTEM "Note.dtd">
                <note>
                <to>Tove</to>
                <from>Jani</from>
                <heading>Reminder</heading>
                <body>Don't forget me this weekend!</body>
                </note>
                """.getBytes(UTF_8);
        XmlValidatingOutputStream xmlValidatingOutputStream = createXmlValidatingOutputStream(data.length);

        assertDoesNotThrow(() ->
                xmlValidatingOutputStream.write(data));
    }

    @Test
    void write_withNamespaces() {
        byte[] data = """
                <?xml version="1.0" encoding="UTF-8"?>
                       <root xmlns:ns1="http://example.com/ns1" xmlns:ns2="http://example.com/ns2">
                           <ns1:item>Item 1</ns1:item>
                           <ns2:item>Item 2</ns2:item>
                           <ns1:item>Item 3</ns1:item>
                           <ns2:item>Item 4</ns2:item>
                       </root>
                """.getBytes(UTF_8);
        XmlValidatingOutputStream xmlValidatingOutputStream = createXmlValidatingOutputStream(data.length);

        assertDoesNotThrow(() ->
                xmlValidatingOutputStream.write(data));
    }

    private static XmlValidatingOutputStream createXmlValidatingOutputStream(int contentLength) {
        return new XmlValidatingOutputStream(contentLength, "1", UUID.randomUUID());
    }
}
