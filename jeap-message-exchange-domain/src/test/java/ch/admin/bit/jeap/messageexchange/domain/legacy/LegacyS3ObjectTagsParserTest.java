package ch.admin.bit.jeap.messageexchange.domain.legacy;

import ch.admin.bit.jeap.messageexchange.domain.malwarescan.ScanStatus;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;


class LegacyS3ObjectTagsParserTest {

    @Test
    void testGetTagsFromMap_required() {
        LegacyS3ObjectTagsParser parser = new LegacyS3ObjectTagsParser();
        Map<String, String> tags = Map.of(
                "bpId", "bpId123",
                "messageType", "messageTypeABC",
                "scanStatus", "NO_THREATS_FOUND",
                "saveTimeInMillis", "123456789"
        );

        S3ObjectTags s3ObjectTags = parser.getTagsfromMap(tags);

        assertEquals("bpId123", s3ObjectTags.bpId());
        assertEquals("messageTypeABC", s3ObjectTags.messageType());
        assertEquals(ScanStatus.NO_THREATS_FOUND, s3ObjectTags.scanStatus());
        assertEquals(123456789L, s3ObjectTags.saveTimeInMillis());
    }

    @Test
    void testGetTagsFromMap_full() {
        LegacyS3ObjectTagsParser parser = new LegacyS3ObjectTagsParser();
        Map<String, String> tags = Map.of(
                "bpId", "bpId123",
                "messageType", "messageTypeABC",
                "partnerTopic", "pT123",
                "partnerExternalReference", "ext1",
                "scanStatus", "NO_THREATS_FOUND",
                "saveTimeInMillis", "123456789"
        );

        S3ObjectTags s3ObjectTags = parser.getTagsfromMap(tags);

        assertEquals("bpId123", s3ObjectTags.bpId());
        assertEquals("messageTypeABC", s3ObjectTags.messageType());
        assertEquals("pT123", s3ObjectTags.partnerTopic());
        assertEquals("ext1", s3ObjectTags.partnerExternalReference());
        assertEquals(ScanStatus.NO_THREATS_FOUND, s3ObjectTags.scanStatus());
        assertEquals(123456789L, s3ObjectTags.saveTimeInMillis());
    }

    @Test
    void testGetTagsFromMap_withEmptyMap() {
        LegacyS3ObjectTagsParser parser = new LegacyS3ObjectTagsParser();
        Map<String, String> tags = Collections.emptyMap();

        S3ObjectTags s3ObjectTags = parser.getTagsfromMap(tags);

        assertNull(s3ObjectTags.bpId());
        assertNull(s3ObjectTags.messageType());
        assertNull(s3ObjectTags.scanStatus());
        assertNull(s3ObjectTags.saveTimeInMillis());
        assertNull(s3ObjectTags.partnerTopic());
        assertNull(s3ObjectTags.partnerExternalReference());
    }

    @Test
    void testGetTagsFromMapAndValidate() {
        LegacyS3ObjectTagsParser parser = new LegacyS3ObjectTagsParser();
        Map<String, String> tags = Map.of(
                "bpId", "bpId123",
                "messageType", "messageTypeABC",
                "scanStatus", "NO_THREATS_FOUND",
                "saveTimeInMillis", "123456789"
        );

        S3ObjectTags s3ObjectTags = parser.getTagsfromMapAndValidate("my-bucket", "1234", tags);

        assertEquals("bpId123", s3ObjectTags.bpId());
        assertEquals("messageTypeABC", s3ObjectTags.messageType());
        assertEquals(ScanStatus.NO_THREATS_FOUND, s3ObjectTags.scanStatus());
        assertEquals(123456789L, s3ObjectTags.saveTimeInMillis());
    }

    @Test
    void testGetTagsFromMapAndValidate_withEmptyMap() {
        LegacyS3ObjectTagsParser parser = new LegacyS3ObjectTagsParser();
        Map<String, String> tags = Collections.emptyMap();

        assertThrows(IllegalStateException.class, () -> parser.getTagsfromMapAndValidate("my-bucket", "1234", tags));
    }

    @Test
    void testGetTagsFromMapAndValidate_withBpIdNotSet() {
        LegacyS3ObjectTagsParser parser = new LegacyS3ObjectTagsParser();
        Map<String, String> tags = Map.of(
                "messageType", "messageTypeABC",
                "scanStatus", "NO_THREATS_FOUND",
                "saveTimeInMillis", "123456789"
        );

        assertThrows(IllegalStateException.class, () -> parser.getTagsfromMapAndValidate("my-bucket", "1234", tags));
    }

    @Test
    void testGetTagsFromMapAndValidate_withMessageTypeNotSet() {
        LegacyS3ObjectTagsParser parser = new LegacyS3ObjectTagsParser();
        Map<String, String> tags = Map.of(
                "bpId", "bpId123",
                "scanStatus", "NO_THREATS_FOUND",
                "saveTimeInMillis", "123456789"
        );

        assertThrows(IllegalStateException.class, () -> parser.getTagsfromMapAndValidate("my-bucket", "1234", tags));
    }

    @Test
    void testGetTagsFromMapAndValidate_withScanStatusNotSet() {
        LegacyS3ObjectTagsParser parser = new LegacyS3ObjectTagsParser();
        Map<String, String> tags = Map.of(
                "bpId", "bpId123",
                "messageType", "messageTypeABC",
                "saveTimeInMillis", "123456789"
        );

        assertThrows(IllegalStateException.class, () -> parser.getTagsfromMapAndValidate("my-bucket", "1234", tags));
    }
}
