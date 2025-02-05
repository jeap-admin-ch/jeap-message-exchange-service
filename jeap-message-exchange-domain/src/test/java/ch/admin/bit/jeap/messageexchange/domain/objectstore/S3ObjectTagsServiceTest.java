package ch.admin.bit.jeap.messageexchange.domain.objectstore;

import ch.admin.bit.jeap.messageexchange.domain.malwarescan.ScanStatus;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;


class S3ObjectTagsServiceTest {

    @Test
    void testToMap() {
        S3ObjectTagsService helper = new S3ObjectTagsService();
        Map<String, String> tags = helper.toMap("bpId123", "messageTypeABC", ScanStatus.NO_THREATS_FOUND, 123456789L);

        assertEquals("bpId123", tags.get("bpId"));
        assertEquals("messageTypeABC", tags.get("messageType"));
        assertEquals("NO_THREATS_FOUND", tags.get("scanStatus"));
        assertEquals("123456789", tags.get("saveTimeInMillis"));
    }

    @Test
    void testToMapWithScanStatus() {
        S3ObjectTagsService helper = new S3ObjectTagsService();
        Map<String, String> tags = helper.toMap(ScanStatus.THREATS_FOUND);

        assertEquals("THREATS_FOUND", tags.get("scanStatus"));
    }

    @Test
    void testGetTagsFromMap() {
        S3ObjectTagsService helper = new S3ObjectTagsService();
        Map<String, String> tags = Map.of(
                "bpId", "bpId123",
                "messageType", "messageTypeABC",
                "scanStatus", "NO_THREATS_FOUND",
                "saveTimeInMillis", "123456789"
        );

        S3ObjectTags s3ObjectTags = helper.getTagsfromMap(tags);

        assertEquals("bpId123", s3ObjectTags.bpId());
        assertEquals("messageTypeABC", s3ObjectTags.messageType());
        assertEquals(ScanStatus.NO_THREATS_FOUND, s3ObjectTags.scanStatus());
        assertEquals(123456789L, s3ObjectTags.saveTimeInMillis());
    }

    @Test
    void testGetTagsfromMap_withEmtpyMap() {
        S3ObjectTagsService helper = new S3ObjectTagsService();
        Map<String, String> tags = Collections.emptyMap();

        S3ObjectTags s3ObjectTags = helper.getTagsfromMap(tags);

        assertNull(s3ObjectTags.bpId());
        assertNull(s3ObjectTags.messageType());
        assertNull(s3ObjectTags.scanStatus());
        assertNull(s3ObjectTags.saveTimeInMillis());
    }

    @Test
    void testGetTagsFromMapAndValidate() {
        S3ObjectTagsService helper = new S3ObjectTagsService();
        Map<String, String> tags = Map.of(
                "bpId", "bpId123",
                "messageType", "messageTypeABC",
                "scanStatus", "NO_THREATS_FOUND",
                "saveTimeInMillis", "123456789"
        );

        S3ObjectTags s3ObjectTags = helper.getTagsfromMapAndValidate("my-bucket", "1234", tags);

        assertEquals("bpId123", s3ObjectTags.bpId());
        assertEquals("messageTypeABC", s3ObjectTags.messageType());
        assertEquals(ScanStatus.NO_THREATS_FOUND, s3ObjectTags.scanStatus());
        assertEquals(123456789L, s3ObjectTags.saveTimeInMillis());
    }

    @Test
    void testGetTagsfromMapAndValidate_withEmtpyMap() {
        S3ObjectTagsService helper = new S3ObjectTagsService();
        Map<String, String> tags = Collections.emptyMap();

        assertThrows(IllegalStateException.class, () -> helper.getTagsfromMapAndValidate("my-bucket", "1234", tags));
    }

    @Test
    void testGetTagsfromMapAndValidate_withBpIdNotSet() {
        S3ObjectTagsService helper = new S3ObjectTagsService();
        Map<String, String> tags = Map.of(
                "messageType", "messageTypeABC",
                "scanStatus", "NO_THREATS_FOUND",
                "saveTimeInMillis", "123456789"
        );

        assertThrows(IllegalStateException.class, () -> helper.getTagsfromMapAndValidate("my-bucket", "1234", tags));
    }

    @Test
    void testGetTagsfromMapAndValidate_withMessageTypeNotSet() {
        S3ObjectTagsService helper = new S3ObjectTagsService();
        Map<String, String> tags = Map.of(
                "bpId", "bpId123",
                "scanStatus", "NO_THREATS_FOUND",
                "saveTimeInMillis", "123456789"
        );

        assertThrows(IllegalStateException.class, () -> helper.getTagsfromMapAndValidate("my-bucket", "1234", tags));
    }

    @Test
    void testGetTagsfromMapAndValidate_withScanStatusNotSet() {
        S3ObjectTagsService helper = new S3ObjectTagsService();
        Map<String, String> tags = Map.of(
                "bpId", "bpId123",
                "messageType", "messageTypeABC",
                "saveTimeInMillis", "123456789"
        );

        assertThrows(IllegalStateException.class, () -> helper.getTagsfromMapAndValidate("my-bucket", "1234", tags));
    }
}