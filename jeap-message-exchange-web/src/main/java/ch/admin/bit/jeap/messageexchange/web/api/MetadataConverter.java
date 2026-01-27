package ch.admin.bit.jeap.messageexchange.web.api;

import ch.admin.bit.jeap.messageexchange.web.api.exception.InvalidMetadataException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.*;
import com.networknt.schema.Error;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Base64;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class MetadataConverter {

    private final Schema schemaFromSchemaLocation;
    private final ObjectMapper objectMapper;

    public MetadataConverter(ObjectMapper objectMapper) {
        schemaFromSchemaLocation = SchemaRegistry.builder().build().getSchema(SchemaLocation.of("classpath:schema/mes-metadata-schema.json"));
        schemaFromSchemaLocation.initializeValidators();
        this.objectMapper = objectMapper;
    }

    public Map<String, String> convertToJson(String base64Content) throws InvalidMetadataException {
        String content = decodeBase64(base64Content);
        List<Error> errors = validateJson(content);
        if (!errors.isEmpty()) {
            log.warn("Metadata validation failed with {} errors.", errors.size());
            throw InvalidMetadataException.invalidJsonSchema();
        }
        try {
            return objectMapper.readValue(content, new TypeReference<>() {});
        } catch (JsonProcessingException e) {
            throw InvalidMetadataException.invalidJson();
        }
    }

    public String convertToBase64(Map<String, String> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return null;
        }
        try {
            return Base64.getEncoder().encodeToString(objectMapper.writeValueAsString(metadata).getBytes());
        } catch (JsonProcessingException e) {
            log.warn("Cannot convert metadata to base64", e);
            throw new IllegalStateException("Cannot convert metadata to base64", e);
        }
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
