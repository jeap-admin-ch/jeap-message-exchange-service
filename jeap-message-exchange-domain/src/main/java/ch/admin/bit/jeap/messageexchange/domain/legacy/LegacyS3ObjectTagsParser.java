package ch.admin.bit.jeap.messageexchange.domain.legacy;

import ch.admin.bit.jeap.messageexchange.domain.malwarescan.ScanStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Parses the message metadata tags written to S3 objects by MES &lt; 11.0.0.
 * LEGACY-TAG-FALLBACK: remove with the contract story (JEAP-7252).
 */
@Service
@Slf4j
public class LegacyS3ObjectTagsParser {

    static final String TAG_KEY_BP_ID = "bpId";
    static final String TAG_KEY_MESSAGE_TYPE = "messageType";
    static final String TAG_KEY_PARTNER_TOPIC = "partnerTopic";
    static final String TAG_KEY_PARTNER_EXTERNAL_REFERENCE = "partnerExternalReference";
    static final String TAG_KEY_SCAN_STATUS = "scanStatus";
    static final String TAG_KEY_SAVE_TIME_IN_MILLIS = "saveTimeInMillis";

    public S3ObjectTags getTagsfromMapAndValidate(String bucketName, String objectKey, Map<String, String> tags) {
        return getTagsfromMap(tags, (bpId, messageType, scanStatusString) -> {
            if (bpId == null || messageType == null || scanStatusString == null) {
                log.error("Won't publish message! Not all needed tags are present for S3 Object {}-{}, tags: {}", bucketName, objectKey, tags);
                throw new IllegalStateException("Not all needed tags are present for S3 Object " + bucketName + "-" + objectKey + ", tags: " + tags);
            }
        });
    }

    public S3ObjectTags getTagsfromMap(Map<String, String> tags) {
        return getTagsfromMap(tags, ValidationHook.noOp());
    }

    private S3ObjectTags getTagsfromMap(Map<String, String> tags, ValidationHook validationHook) {
        String bpId = tags.getOrDefault(TAG_KEY_BP_ID, null);
        String messageType = tags.getOrDefault(TAG_KEY_MESSAGE_TYPE, null);
        String partnerTopic = tags.getOrDefault(TAG_KEY_PARTNER_TOPIC, null);
        String partnerExternalReference = tags.getOrDefault(TAG_KEY_PARTNER_EXTERNAL_REFERENCE, null);
        String scanStatusString = tags.getOrDefault(TAG_KEY_SCAN_STATUS, null);
        String saveTimeInMillisString = tags.getOrDefault(TAG_KEY_SAVE_TIME_IN_MILLIS, null);

        validationHook.validate(bpId, messageType, scanStatusString);

        return new S3ObjectTags(bpId,
                messageType,
                partnerTopic,
                partnerExternalReference,
                scanStatusString == null ? null : ScanStatus.valueOf(scanStatusString),
                saveTimeInMillisString == null ? null : Long.parseLong(saveTimeInMillisString)
        );
    }

    private interface ValidationHook {
        void validate(String bpId, String messageType, String scanStatusString);

        static ValidationHook noOp() {
            return (bpId, messageType, scanStatusString) -> {
            };
        }
    }

}
