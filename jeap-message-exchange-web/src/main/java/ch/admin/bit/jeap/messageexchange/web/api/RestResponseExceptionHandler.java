package ch.admin.bit.jeap.messageexchange.web.api;

import ch.admin.bit.jeap.messageexchange.domain.exception.MismatchedContentTypeException;
import ch.admin.bit.jeap.messageexchange.domain.exception.MalwareScanFailedOrBlockedException;
import ch.admin.bit.jeap.messageexchange.domain.xml.InvalidXMLInputException;
import ch.admin.bit.jeap.messageexchange.web.api.exception.InvalidBpIdException;
import ch.admin.bit.jeap.messageexchange.web.api.exception.InvalidMetadataException;
import ch.admin.bit.jeap.messageexchange.web.api.exception.MissingRequiredHeaderException;
import ch.admin.bit.jeap.messageexchange.web.api.exception.UnsupportedMediaTypeException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@Slf4j
@RestControllerAdvice
public class RestResponseExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler(InvalidXMLInputException.class)
    public ResponseEntity<String> handleInvalidXMLInputException(InvalidXMLInputException ex) {
        log.warn("Invalid XML input provided for messageId {} and bpId {}", ex.getMessageId(), ex.getBpId(), ex);
        return ResponseEntity
                .badRequest()
                .body("Invalid XML input provided: " + ex.getMessage());
    }

    @ExceptionHandler(InvalidBpIdException.class)
    public ResponseEntity<String> handleInvalidBpIdException(InvalidBpIdException ex) {
        log.warn("Invalid BP ID: {}", ex.getMessage());
        return ResponseEntity.status(403)
                .body("bpId in token does not match bpId in header");
    }

    @ExceptionHandler(MalwareScanFailedOrBlockedException.class)
    public ResponseEntity<String> handleMalwareScanNotDeliveringException(MalwareScanFailedOrBlockedException ex) {
        return ResponseEntity.status(403)
                .body("Not delivering because of malware scan status");
    }

    @ExceptionHandler(UnsupportedMediaTypeException.class)
    public ResponseEntity<String> handleUnsupportedMediaTypeException(UnsupportedMediaTypeException ex) {
        return ResponseEntity.status(406)
                .body(ex.getMessage());
    }

    @ExceptionHandler(MismatchedContentTypeException.class)
    public ResponseEntity<String> handleIncorrectContentTypeException(MismatchedContentTypeException ex) {
        return ResponseEntity.status(406)
                .body(ex.getMessage());
    }

    @ExceptionHandler(InvalidMetadataException.class)
    public ResponseEntity<String> handleInvalidMetadataException(InvalidMetadataException ex) {
        return ResponseEntity.status(400)
                .body(ex.getMessage());
    }

    //TODO: JEAP-5099 remove method
    @ExceptionHandler(MissingRequiredHeaderException.class)
    public ResponseEntity<String> handleMissingRequiredHeaderException(MissingRequiredHeaderException ex) {
        log.warn("Missing required header: {}", ex.getMessage());
        return ResponseEntity.status(400)
                .body(ex.getMessage());
    }

}
