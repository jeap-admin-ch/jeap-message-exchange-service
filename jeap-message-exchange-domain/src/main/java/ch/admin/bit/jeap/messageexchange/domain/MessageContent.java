package ch.admin.bit.jeap.messageexchange.domain;

import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

public record MessageContent(InputStream inputStream, int contentLength, Map<String, String> tags) {

    public MessageContent(InputStream inputStream, int contentLength) {
        this(inputStream, contentLength, Collections.emptyMap());
    }
}
