package ch.admin.bit.jeap.messageexchange.web.api.stream;

import ch.admin.bit.jeap.messageexchange.domain.MessageContent;
import ch.admin.bit.jeap.messageexchange.web.api.MessageExchangeApiProperties;
import ch.admin.bit.jeap.messageexchange.web.api.exception.UnsupportedMediaTypeException;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.DelegatingServletInputStream;
import org.springframework.security.web.firewall.RequestRejectedException;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@SuppressWarnings("resource")
class ControllerStreamsTest {

    private static final int MAX_REQUEST_BODY_SIZE_IN_BYTES = 10;
    private ControllerStreams controllerStreams;

    @BeforeEach
    void setUp() {
        MessageExchangeApiProperties props = new MessageExchangeApiProperties();
        props.setMaxRequestBodySizeInBytes(MAX_REQUEST_BODY_SIZE_IN_BYTES);
        controllerStreams = new ControllerStreams(props);
    }

    @Test
    void getRequestContent_withKnownContentLength() throws Exception {
        byte[] body = "short".getBytes(UTF_8);
        HttpServletRequest request = mockRequest(body, body.length);

        MessageContent requestContent = controllerStreams.getRequestContent(request);

        assertThat(requestContent.contentLength())
                .isEqualTo(body.length);
        byte[] bytesRead = requestContent.inputStream().readAllBytes();
        assertThat(new String(bytesRead, UTF_8))
                .isEqualTo("short");
    }

    @Test
    void getRequestContent_withoutKnownContentLength() throws Exception {
        byte[] body = "short".getBytes(UTF_8);
        HttpServletRequest request = mockRequest(body, -1);

        MessageContent requestContent = controllerStreams.getRequestContent(request);

        assertThat(requestContent.contentLength())
                .isEqualTo(body.length);
        byte[] bytesRead = requestContent.inputStream().readAllBytes();
        assertThat(new String(bytesRead, UTF_8))
                .isEqualTo("short");
    }

    @Test
    void getRequestContent_withoutKnownContentLength_exceedsMaxSize() throws Exception {
        byte[] body = "very long body that excees max size".getBytes(UTF_8);
        HttpServletRequest request = mockRequest(body, -1);

        assertThatThrownBy(() ->
                controllerStreams.getRequestContent(request))
                .isInstanceOf(RequestRejectedException.class)
                .hasMessageContaining("Request content exceeded limit of 10 bytes");
    }

    @Test
    void validateContentType_unsupportedMediaType_ExceptionThrown() {
        assertThatThrownBy(() ->
                controllerStreams.validateContentType("application/foo"))
                .isInstanceOf(UnsupportedMediaTypeException.class)
                .hasMessageContaining("The media type 'application/foo' is not supported.");
    }

    @Test
    void validateContentType_supportedMediaType_ExceptionNotThrown() {
        assertDoesNotThrow(() -> controllerStreams.validateContentType("application/xml"));
    }

    @Test
    void validateContentType_xmlWithCharset_ExceptionNotThrown() {
        assertDoesNotThrow(() -> controllerStreams.validateContentType("application/xml;charset=UTF-8"));
    }

    private static HttpServletRequest mockRequest(byte[] body, int contentLength) throws IOException {
        HttpServletRequest request = mock(HttpServletRequest.class);
        doReturn(contentLength).when(request).getContentLength();
        ServletInputStream stream = new DelegatingServletInputStream(new ByteArrayInputStream(body));
        doReturn(stream).when(request).getInputStream();
        return request;
    }
}
