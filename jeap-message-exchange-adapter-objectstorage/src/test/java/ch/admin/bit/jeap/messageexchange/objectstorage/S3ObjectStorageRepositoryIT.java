package ch.admin.bit.jeap.messageexchange.objectstorage;

import ch.admin.bit.jeap.messageexchange.domain.MessageContent;
import ch.admin.bit.jeap.messageexchange.domain.legacy.ObjectHead;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.model.*;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;


class S3ObjectStorageRepositoryIT extends AbstractS3ObjectRepositoryTestBase {

    @Autowired
    private S3ObjectStorageRepository s3ObjectStorageRepository;

    @Test
    void testPutObject() {
        String objectKey = "test-object-key";
        byte[] objectContent = "test-object-content".getBytes(UTF_8);
        MessageContent messageContent = new MessageContent(new ByteArrayInputStream(objectContent), objectContent.length);
        s3ObjectStorageRepository.putObject(TEST_BUCKET_NAME, objectKey, messageContent, MediaType.APPLICATION_XML_VALUE);

        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(TEST_BUCKET_NAME).key(objectKey).build();
        ResponseBytes<GetObjectResponse> objectAsBytes = s3Client.getObject(getObjectRequest, ResponseTransformer.toBytes());
        HeadObjectResponse headObjectResponse = s3Client.headObject(HeadObjectRequest.builder().bucket(TEST_BUCKET_NAME).key(objectKey).build());
        assertThat(objectAsBytes.asByteArray()).isEqualTo(objectContent);
        assertThat(headObjectResponse.expiration()).contains("MessageExchangeLifecyclePolicy-15");
    }

    @Test
    void testPutObject_withoutTags_writesExactlyOneTag_theLifecycleTag() {
        // objects are tagged atomically at creation; without transitional legacy tags only the lifecycle tag is written
        String objectKey = "invariant-object-key";
        byte[] objectContent = "test-object-content".getBytes(UTF_8);
        MessageContent messageContent = new MessageContent(new ByteArrayInputStream(objectContent), objectContent.length);
        s3ObjectStorageRepository.putObject(TEST_BUCKET_NAME, objectKey, messageContent, MediaType.APPLICATION_XML_VALUE);

        List<Tag> tagSet = s3Client.getObjectTagging(GetObjectTaggingRequest.builder().bucket(TEST_BUCKET_NAME).key(objectKey).build()).tagSet();
        assertThat(tagSet).hasSize(1);
        assertThat(tagSet.getFirst().key()).isEqualTo("MessageExchangeLifecyclePolicy");
        assertThat(tagSet.getFirst().value()).isEqualTo("15");
    }

    @Test
    void testPutObject_withTags_writesLifecycleTagAndProvidedTagsAtomically() {
        // transitional v10-compatible metadata tags are written together with the lifecycle tag in the
        // atomic PutObject request (LEGACY-TAG-FALLBACK, JEAP-7252)
        String objectKey = "tagged-object-key";
        byte[] objectContent = "test-object-content".getBytes(UTF_8);
        MessageContent messageContentWithTags = new MessageContent(new ByteArrayInputStream(objectContent), objectContent.length,
                Map.of("bpId", "aBpId", "scanStatus", "SCAN_PENDING"));
        s3ObjectStorageRepository.putObject(TEST_BUCKET_NAME, objectKey, messageContentWithTags, MediaType.APPLICATION_XML_VALUE);

        Map<String, String> tags = s3ObjectStorageRepository.getTagsOnObject(TEST_BUCKET_NAME, objectKey);
        assertThat(tags)
                .hasSize(3)
                .containsEntry("MessageExchangeLifecyclePolicy", "15")
                .containsEntry("bpId", "aBpId")
                .containsEntry("scanStatus", "SCAN_PENDING");
    }

    @Test
    void testGetObjectWithTags_legacyObject() {
        // LEGACY-TAG-FALLBACK: simulates an object stored by MES < 11.0.0 with metadata tags written by that version
        String objectKey = "legacy-object-key";
        byte[] objectContent = "test-object-content".getBytes(UTF_8);

        Map<String, String> tags = Map.of(
                "bpId", "aBpId",
                "messageType", "aMessageType",
                "scanStatus", "NO_THREATS_FOUND",
                "saveTimeInMillis", String.valueOf(System.currentTimeMillis())
        );
        putLegacyObject(objectKey, objectContent, tags);

        Optional<MessageContent> objectWithTags = s3ObjectStorageRepository.getObjectWithTags(TEST_BUCKET_NAME, objectKey);
        assertThat(objectWithTags).isPresent();
        assertThat(objectWithTags.get().tags()).containsAllEntriesOf(tags);
    }

    @Test
    void testGetTagsOnObject_legacyObject() {
        String objectKey = "legacy-object-key-tags";
        Map<String, String> tags = Map.of("bpId", "aBpId", "scanStatus", "SCAN_PENDING");
        putLegacyObject(objectKey, "content".getBytes(UTF_8), tags);

        Map<String, String> tagsOnObject = s3ObjectStorageRepository.getTagsOnObject(TEST_BUCKET_NAME, objectKey);

        assertThat(tagsOnObject).containsAllEntriesOf(tags);
    }

    @Test
    void testUpdateTags_mergesUpdatedTagsIntoExistingTags() {
        // transitional v10-style scanStatus tag update after a malware scan result (LEGACY-TAG-FALLBACK, JEAP-7252)
        String objectKey = "update-tags-object-key";
        Map<String, String> tags = Map.of("bpId", "aBpId", "scanStatus", "SCAN_PENDING");
        putLegacyObject(objectKey, "content".getBytes(UTF_8), tags);

        s3ObjectStorageRepository.updateTags(TEST_BUCKET_NAME, objectKey, Map.of("scanStatus", "NO_THREATS_FOUND"));

        Map<String, String> tagsOnObject = s3ObjectStorageRepository.getTagsOnObject(TEST_BUCKET_NAME, objectKey);
        assertThat(tagsOnObject)
                .hasSize(2)
                .containsEntry("bpId", "aBpId")
                .containsEntry("scanStatus", "NO_THREATS_FOUND");
    }

    @Test
    void testGetObjectHead() {
        String objectKey = "head-object-key";
        byte[] objectContent = "test-object-content".getBytes(UTF_8);
        MessageContent messageContent = new MessageContent(new ByteArrayInputStream(objectContent), objectContent.length);
        s3ObjectStorageRepository.putObject(TEST_BUCKET_NAME, objectKey, messageContent, MediaType.APPLICATION_XML_VALUE);

        Optional<ObjectHead> objectHead = s3ObjectStorageRepository.getObjectHead(TEST_BUCKET_NAME, objectKey);

        assertThat(objectHead).isPresent();
        assertThat(objectHead.get().contentType()).isEqualTo(MediaType.APPLICATION_XML_VALUE);
        assertThat(objectHead.get().contentLength()).isEqualTo(objectContent.length);
    }

    @Test
    void testGetObjectHead_notFound() {
        Optional<ObjectHead> objectHead = s3ObjectStorageRepository.getObjectHead(TEST_BUCKET_NAME, "does-not-exist");

        assertThat(objectHead).isEmpty();
    }

    @Test
    void testPutObject_alreadyDefined_messageSaved() {
        final String objectKey = "test-object-key";
        final byte[] objectContent1 = "test-object-content1".getBytes(UTF_8);
        final byte[] objectContent2 = "test-object-content2".getBytes(UTF_8);

        MessageContent messageContent1 = new MessageContent(new ByteArrayInputStream(objectContent1), objectContent1.length);
        s3ObjectStorageRepository.putObject(TEST_BUCKET_NAME, objectKey, messageContent1, MediaType.APPLICATION_XML_VALUE);
        MessageContent messageContent2 = new MessageContent(new ByteArrayInputStream(objectContent2), objectContent2.length);
        s3ObjectStorageRepository.putObject(TEST_BUCKET_NAME, objectKey, messageContent2, MediaType.APPLICATION_XML_VALUE);

        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(TEST_BUCKET_NAME).key(objectKey).build();
        ResponseBytes<GetObjectResponse> objectAsBytes = s3Client.getObject(getObjectRequest, ResponseTransformer.toBytes());
        HeadObjectResponse headObjectResponse = s3Client.headObject(HeadObjectRequest.builder().bucket(TEST_BUCKET_NAME).key(objectKey).build());
        assertThat(objectAsBytes.asByteArray()).isEqualTo(objectContent2);
        assertThat(headObjectResponse.expiration()).contains("MessageExchangeLifecyclePolicy-15");
    }

    @Test
    void testGetObject() {
        String objectKey = "test-object-key";
        byte[] objectContent = "test-object-content".getBytes(UTF_8);
        MessageContent putMessageContent = new MessageContent(new ByteArrayInputStream(objectContent), objectContent.length);
        s3ObjectStorageRepository.putObject(TEST_BUCKET_NAME, objectKey, putMessageContent, MediaType.APPLICATION_XML_VALUE);

        Optional<MessageContent> optionalMessageContent = s3ObjectStorageRepository.getObject(TEST_BUCKET_NAME, objectKey);

        assertThat(optionalMessageContent)
                .isPresent();
        MessageContent messageContent = optionalMessageContent.get();
        assertThat(messageContent.inputStream())
                .hasBinaryContent(objectContent);
        assertThat(messageContent.contentLength())
                .isEqualTo(objectContent.length);
    }

    @Test
    void testGetObject_notFound() {
        Optional<MessageContent> optionalMessageContent =
                s3ObjectStorageRepository.getObject(TEST_BUCKET_NAME, "does-not-exist");

        assertThat(optionalMessageContent)
                .isEmpty();
    }

    @Test
    void testGetObjectWithTags_notFound() {
        Optional<MessageContent> optionalMessageContent =
                s3ObjectStorageRepository.getObjectWithTags(TEST_BUCKET_NAME, "does-not-exist");

        assertThat(optionalMessageContent)
                .isEmpty();
    }

    private void putLegacyObject(String objectKey, byte[] objectContent, Map<String, String> tags) {
        List<Tag> tagSet = tags.entrySet().stream()
                .map(entry -> Tag.builder().key(entry.getKey()).value(entry.getValue()).build())
                .toList();
        s3Client.putObject(PutObjectRequest.builder()
                        .bucket(TEST_BUCKET_NAME)
                        .key(objectKey)
                        .contentType(MediaType.APPLICATION_XML_VALUE)
                        .tagging(Tagging.builder().tagSet(tagSet).build())
                        .build(),
                RequestBody.fromBytes(objectContent));
    }

}
