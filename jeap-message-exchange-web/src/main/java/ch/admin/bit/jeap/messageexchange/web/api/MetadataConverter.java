package ch.admin.bit.jeap.messageexchange.web.api;

import ch.admin.bit.jeap.messageexchange.web.api.exception.InvalidMetadataException;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import com.networknt.schema.*;
import com.networknt.schema.Error;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.util.Base64;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class MetadataConverter {

    private final Schema schemaFromSchemaLocation;
    private final JsonMapper jsonMapper;

    public MetadataConverter(JsonMapper jsonMapper) {
        schemaFromSchemaLocation = SchemaRegistry.builder().build().getSchema(SchemaLocation.of("classpath:schema/mes-metadata-schema.json"));
        schemaFromSchemaLocation.initializeValidators();
        this.jsonMapper = jsonMapper;
    }

    public Map<String, String> convertToJson(String base64Content) throws InvalidMetadataException {
        String content = decodeBase64(base64Content);
        List<Error> errors = validateJson(content);
        if (!errors.isEmpty()) {
            log.warn("Metadata validation failed with {} errors.", errors.size());
            throw InvalidMetadataException.invalidJsonSchema();
        }
        try {
            return jsonMapper.readValue(content, new TypeReference<>() {});
        } catch (JacksonException _) {
            throw InvalidMetadataException.invalidJson();
        }
    }

    public String convertToBase64(Map<String, String> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return null;
        }
        return Base64.getEncoder().encodeToString(jsonMapper.writeValueAsString(metadata).getBytes());
    }

    private List<Error> validateJson(String headerContent) throws InvalidMetadataException {
        try {
            return schemaFromSchemaLocation.validate(headerContent, InputFormat.JSON);
        } catch (Exception e) {
            log.warn("Cannot read json metadata", e);
            throw InvalidMetadataException.invalidJson();
        }
    }

    private String decodeBase64(String base64Content) throws InvalidMetadataException {
        try {
            return new String(Base64.getDecoder().decode(base64Content));
        } catch (Exception e) {
            log.warn("Cannot decode base64 metadata", e);
            throw InvalidMetadataException.invalidBase64();
        }
    }

}
