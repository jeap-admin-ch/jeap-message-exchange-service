package ch.admin.bit.jeap.messageexchange.objectstorage;

import ch.admin.bit.jeap.messageexchange.domain.MessageContent;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.ByteArrayInputStream;
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
        s3ObjectStorageRepository.putObject(TEST_BUCKET_NAME, objectKey, messageContent);

        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(TEST_BUCKET_NAME).key(objectKey).build();
        ResponseBytes<GetObjectResponse> objectAsBytes = s3Client.getObject(getObjectRequest, ResponseTransformer.toBytes());
        HeadObjectResponse headObjectResponse = s3Client.headObject(HeadObjectRequest.builder().bucket(TEST_BUCKET_NAME).key(objectKey).build());
        assertThat(objectAsBytes.asByteArray()).isEqualTo(objectContent);
        assertThat(headObjectResponse.expiration()).contains("MessageExchangeLifecyclePolicy-15");
    }

    @Test
    void testPutAndGetObjectWithTags() {
        String objectKey = "test-object-key";
        byte[] objectContent = "test-object-content".getBytes(UTF_8);

        String bpId = "aBpId";
        String messageType = "aMessageType";
        String scanStatus = "NO_THREATS_FOUND";
        long saveTimeInMillis = System.currentTimeMillis();

        Map<String, String> tags = Map.of(
                "bpId", bpId,
                "messageType", messageType,
                "scanStatus", scanStatus,
                "saveTimeInMillis", String.valueOf(saveTimeInMillis)
        );
        MessageContent messageContent = new MessageContent(new ByteArrayInputStream(objectContent), objectContent.length, tags);
        s3ObjectStorageRepository.putObject(TEST_BUCKET_NAME, objectKey, messageContent);

        Optional<MessageContent> objectWithTags = s3ObjectStorageRepository.getObjectWithTags(TEST_BUCKET_NAME, objectKey);
        assertThat(objectWithTags).isPresent();
        assertThat(objectWithTags.get().tags()).containsAllEntriesOf(tags);
    }

    @Test
    void testPutObject_alreadyDefined_messageSaved() {
        final String objectKey = "test-object-key";
        final byte[] objectContent1 = "test-object-content1".getBytes(UTF_8);
        final byte[] objectContent2 = "test-object-content2".getBytes(UTF_8);

        MessageContent messageContent1 = new MessageContent(new ByteArrayInputStream(objectContent1), objectContent1.length);
        s3ObjectStorageRepository.putObject(TEST_BUCKET_NAME, objectKey, messageContent1);
        MessageContent messageContent2 = new MessageContent(new ByteArrayInputStream(objectContent2), objectContent2.length);
        s3ObjectStorageRepository.putObject(TEST_BUCKET_NAME, objectKey, messageContent2);

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
        s3ObjectStorageRepository.putObject(TEST_BUCKET_NAME, objectKey, putMessageContent);

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

}
