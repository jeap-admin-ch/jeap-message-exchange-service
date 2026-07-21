package ch.admin.bit.jeap.messageexchange.objectstorage;

import ch.admin.bit.jeap.messageexchange.domain.MessageContent;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.InputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end check of both retry-safe upload paths against a real S3-compatible storage: bodies up to the
 * threshold are buffered in memory, larger bodies are streamed through the no-retry client. Both must store
 * the content correctly.
 */
@TestPropertySource(properties = "jeap.messageexchange.objectstorage.connection.upload-retry-memory-buffer-threshold=8B")
class S3ObjectStorageRepositoryBufferingIT extends AbstractS3ObjectRepositoryTestBase {

    @Autowired
    private S3ObjectStorageRepository s3ObjectStorageRepository;

    @Test
    void putObject_smallNonMarkStream_bufferedUploadStoresContent() {
        String objectKey = "buffered-object-key";
        byte[] content = "12345".getBytes(UTF_8); // below the 8B threshold

        s3ObjectStorageRepository.putObject(TEST_BUCKET_NAME, objectKey, new MessageContent(nonMarkStream(content), content.length), MediaType.APPLICATION_XML_VALUE);

        assertThat(readObject(objectKey)).isEqualTo(content);
    }

    @Test
    void putObject_largeNonMarkStream_streamedUploadThroughNoRetryClientStoresContent() {
        String objectKey = "streamed-object-key";
        byte[] content = "x".repeat(1024).getBytes(UTF_8); // above the 8B threshold

        s3ObjectStorageRepository.putObject(TEST_BUCKET_NAME, objectKey, new MessageContent(nonMarkStream(content), content.length), MediaType.APPLICATION_XML_VALUE);

        assertThat(readObject(objectKey)).isEqualTo(content);
    }

    private byte[] readObject(String objectKey) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(TEST_BUCKET_NAME).key(objectKey).build();
        ResponseBytes<GetObjectResponse> objectAsBytes = s3Client.getObject(getObjectRequest, ResponseTransformer.toBytes());
        return objectAsBytes.asByteArray();
    }

    private static InputStream nonMarkStream(byte[] content) {
        return new FilterInputStream(new ByteArrayInputStream(content)) {
            @Override
            public boolean markSupported() {
                return false;
            }
        };
    }
}
