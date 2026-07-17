package ch.admin.bit.jeap.messageexchange.domain;

import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

/**
 * @param tags S3 object tags, only populated when loading a message via the legacy tag fallback for messages
 *             stored by MES &lt; 11.0.0, or on upload with the transitional metadata tags for mixed-version
 *             deployments - written only atomically at object creation, never updated afterwards.
 *             LEGACY-TAG-FALLBACK: remove with the contract story (JEAP-7252).
 */
public record MessageContent(InputStream inputStream, int contentLength, Map<String, String> tags) {

    public MessageContent(InputStream inputStream, int contentLength) {
        this(inputStream, contentLength, Collections.emptyMap());
    }
}
