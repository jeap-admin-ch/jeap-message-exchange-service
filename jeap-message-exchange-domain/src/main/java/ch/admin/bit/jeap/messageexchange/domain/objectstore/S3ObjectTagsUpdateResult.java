package ch.admin.bit.jeap.messageexchange.domain.objectstore;

import java.util.Map;

public record S3ObjectTagsUpdateResult(Map<String, String> currentTags, Map<String, String> previousTags) {
}
