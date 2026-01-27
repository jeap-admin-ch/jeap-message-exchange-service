package ch.admin.bit.jeap.messageexchange.web.api;

import ch.admin.bit.jeap.messageexchange.web.api.exception.InvalidMetadataException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MetadataConverterTest {

    private final MetadataConverter metadataConverter = new MetadataConverter(new ObjectMapper());
    private final Base64.Encoder encoder = java.util.Base64.getEncoder();

    @Test
    @SneakyThrows
    void convertToJson_notBase64Valid_throwsException() {
        InvalidMetadataException invalidMetadataException = assertThrows(InvalidMetadataException.class,
                () -> metadataConverter.convertToJson("foo_invalid_base64_.,?"));
        assertThat(invalidMetadataException.getMessage())
                .isEqualTo("The content of mes-metadata cannot be decoded from Base64");
    }

    @Test
    @SneakyThrows
    void convertToJson_notJsonValid_throwsException() {
        InvalidMetadataException invalidMetadataException = assertThrows(InvalidMetadataException.class,
                () -> metadataConverter.convertToJson("foobar"));
        assertThat(invalidMetadataException.getMessage())
                .isEqualTo("The content of mes-metadata is not a valid JSON content");
    }

    @Test
    @SneakyThrows
    void convertToJson_notJsonSchemaValid_throwsException() {
        String invalidContent = """
                {
                  "id": "12345",
                  "senderBpId": {
                    "foo": "bar"
                  }
                }
                """;
        InvalidMetadataException invalidMetadataException = assertThrows(InvalidMetadataException.class,
                () -> metadataConverter.convertToJson(encoder.encodeToString(invalidContent.getBytes())));
        assertThat(invalidMetadataException.getMessage())
                .isEqualTo("The content of mes-metadata is not valid against the mes-metadata JSON schema");
    }

    @Test
    @SneakyThrows
    void convertToJson_jsonSchemaValid_returnsJsonContent() {
        String content = """
                {
                  "id": "12345",
                  "foo": "bar"
                }
                """;

        Map<String,String> headerValue = metadataConverter.convertToJson(encoder.encodeToString(content.getBytes()));
        assertThat(headerValue).isEqualTo(Map.of("id", "12345", "foo", "bar"));
    }
}

