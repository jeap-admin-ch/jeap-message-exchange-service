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
    void testToMap_required() {
        S3ObjectTagsService helper = new S3ObjectTagsService();
        Map<String, String> tags = helper.toMap("bpId123", "messageTypeABC", null, null, ScanStatus.NO_THREATS_FOUND, 123456789L);

        assertEquals("bpId123", tags.get("bpId"));
        assertEquals("messageTypeABC", tags.get("messageType"));
        assertEquals("NO_THREATS_FOUND", tags.get("scanStatus"));
        assertEquals("123456789", tags.get("saveTimeInMillis"));
        assertNull(tags.get("partnerTopic"));
        assertNull(tags.get("partnerExternalReference"));
    }

    @Test
    void testToMap_full() {
        S3ObjectTagsService helper = new S3ObjectTagsService();
        Map<String, String> tags = helper.toMap("bpId123", "messageTypeABC", "partnerTopic1", "partnerExternalReferenceTest", ScanStatus.NO_THREATS_FOUND, 123456789L);

        assertEquals("bpId123", tags.get("bpId"));
        assertEquals("messageTypeABC", tags.get("messageType"));
        assertEquals("NO_THREATS_FOUND", tags.get("scanStatus"));
        assertEquals("123456789", tags.get("saveTimeInMillis"));
        assertEquals("partnerTopic1", tags.get("partnerTopic"));
        assertEquals("partnerExternalReferenceTest", tags.get("partnerExternalReference"));
    }

    @Test
    void testToMapWithScanStatus() {
        S3ObjectTagsService helper = new S3ObjectTagsService();
        Map<String, String> tags = helper.toMap(ScanStatus.THREATS_FOUND);

        assertEquals("THREATS_FOUND", tags.get("scanStatus"));
    }

    @Test
    void testGetTagsFromMap_required() {
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
    void testGetTagsFromMap_full() {
        S3ObjectTagsService helper = new S3ObjectTagsService();
        Map<String, String> tags = Map.of(
                "bpId", "bpId123",
                "messageType", "messageTypeABC",
                "partnerTopic", "pT123",
                "partnerExternalReference", "ext1",
                "scanStatus", "NO_THREATS_FOUND",
                "saveTimeInMillis", "123456789"
        );

        S3ObjectTags s3ObjectTags = helper.getTagsfromMap(tags);

        assertEquals("bpId123", s3ObjectTags.bpId());
        assertEquals("messageTypeABC", s3ObjectTags.messageType());
        assertEquals("pT123", s3ObjectTags.partnerTopic());
        assertEquals("ext1", s3ObjectTags.partnerExternalReference());
        assertEquals(ScanStatus.NO_THREATS_FOUND, s3ObjectTags.scanStatus());
        assertEquals(123456789L, s3ObjectTags.saveTimeInMillis());
    }

    @Test
    void testGetTagsFromMap_withEmptyMap() {
        S3ObjectTagsService helper = new S3ObjectTagsService();
        Map<String, String> tags = Collections.emptyMap();

        S3ObjectTags s3ObjectTags = helper.getTagsfromMap(tags);

        assertNull(s3ObjectTags.bpId());
        assertNull(s3ObjectTags.messageType());
        assertNull(s3ObjectTags.scanStatus());
        assertNull(s3ObjectTags.saveTimeInMillis());
        assertNull(s3ObjectTags.partnerTopic());
        assertNull(s3ObjectTags.partnerExternalReference());
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
    void testGetTagsFromMapAndValidate_withEmptyMap() {
        S3ObjectTagsService helper = new S3ObjectTagsService();
        Map<String, String> tags = Collections.emptyMap();

        assertThrows(IllegalStateException.class, () -> helper.getTagsfromMapAndValidate("my-bucket", "1234", tags));
    }

    @Test
    void testGetTagsFromMapAndValidate_withBpIdNotSet() {
        S3ObjectTagsService helper = new S3ObjectTagsService();
        Map<String, String> tags = Map.of(
                "messageType", "messageTypeABC",
                "scanStatus", "NO_THREATS_FOUND",
                "saveTimeInMillis", "123456789"
        );

        assertThrows(IllegalStateException.class, () -> helper.getTagsfromMapAndValidate("my-bucket", "1234", tags));
    }

    @Test
    void testGetTagsFromMapAndValidate_withMessageTypeNotSet() {
        S3ObjectTagsService helper = new S3ObjectTagsService();
        Map<String, String> tags = Map.of(
                "bpId", "bpId123",
                "scanStatus", "NO_THREATS_FOUND",
                "saveTimeInMillis", "123456789"
        );

        assertThrows(IllegalStateException.class, () -> helper.getTagsfromMapAndValidate("my-bucket", "1234", tags));
    }

    @Test
    void testGetTagsFromMapAndValidate_withScanStatusNotSet() {
        S3ObjectTagsService helper = new S3ObjectTagsService();
        Map<String, String> tags = Map.of(
                "bpId", "bpId123",
                "messageType", "messageTypeABC",
                "saveTimeInMillis", "123456789"
        );

        assertThrows(IllegalStateException.class, () -> helper.getTagsfromMapAndValidate("my-bucket", "1234", tags));
    }
}
