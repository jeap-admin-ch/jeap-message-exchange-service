package ch.admin.bit.jeap.messageexchange.domain.legacy;

import ch.admin.bit.jeap.messageexchange.domain.malwarescan.ScanStatus;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Builds the message metadata tags expected by MES &lt; 11.0.0: the upload tags written atomically at object
 * creation and the scanStatus tag update written after a malware scan result. They keep mixed 10.x/11.x
 * instances working during zero-downtime deployments (see {@link LegacyTagCompatibilityProperties}); the
 * database stays authoritative.
 * LEGACY-TAG-FALLBACK: remove with the contract story (JEAP-7252).
 */
@Component
@RequiredArgsConstructor
public class LegacyS3ObjectTagsFactory {

    private final LegacyTagCompatibilityProperties legacyTagCompatibilityProperties;

    public Map<String, String> createUploadTags(String bpId, String messageType, String partnerTopic, String partnerExternalReference, ScanStatus scanStatus) {
        if (!legacyTagCompatibilityProperties.isEnabled()) {
            return Map.of();
        }

        Map<String, String> tags = new HashMap<>();
        tags.put(LegacyS3ObjectTagsParser.TAG_KEY_BP_ID, bpId);
        tags.put(LegacyS3ObjectTagsParser.TAG_KEY_MESSAGE_TYPE, messageType);
        tags.put(LegacyS3ObjectTagsParser.TAG_KEY_SCAN_STATUS, scanStatus.name());
        tags.put(LegacyS3ObjectTagsParser.TAG_KEY_SAVE_TIME_IN_MILLIS, String.valueOf(System.currentTimeMillis()));

        if (StringUtils.hasText(partnerTopic)) {
            tags.put(LegacyS3ObjectTagsParser.TAG_KEY_PARTNER_TOPIC, partnerTopic);
        }
        if (StringUtils.hasText(partnerExternalReference)) {
            tags.put(LegacyS3ObjectTagsParser.TAG_KEY_PARTNER_EXTERNAL_REFERENCE, partnerExternalReference);
        }

        return tags;
    }

    /**
     * The scanStatus tag update written after a malware scan result, exactly like MES &lt; 11.0.0 did.
     * Empty when the legacy tag compatibility is disabled - no tag update is performed then.
     */
    public Map<String, String> createScanStatusUpdateTags(ScanStatus scanStatus) {
        if (!legacyTagCompatibilityProperties.isEnabled()) {
            return Map.of();
        }
        return Map.of(LegacyS3ObjectTagsParser.TAG_KEY_SCAN_STATUS, scanStatus.name());
    }
}
