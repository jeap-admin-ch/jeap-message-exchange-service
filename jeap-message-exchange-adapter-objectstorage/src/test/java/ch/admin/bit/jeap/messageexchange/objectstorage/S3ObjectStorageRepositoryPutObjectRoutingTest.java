package ch.admin.bit.jeap.messageexchange.objectstorage;

import ch.admin.bit.jeap.messageexchange.domain.MessageContent;
import ch.admin.bit.jeap.messageexchange.domain.housekeeping.HousekeepingProperties;
import ch.admin.bit.jeap.messageexchange.objectstorage.lifecycle.S3LifecycleConfigurationInitializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.http.MediaType;
import org.springframework.util.unit.DataSize;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.Tag;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Verifies the client/body routing of {@link S3ObjectStorageRepository#putObject}: uploads that cannot be
 * re-read on an SDK retry are either buffered in memory (up to the configured threshold) or sent through the
 * no-retry client so they fail fast with the actual S3 error.
 */
class S3ObjectStorageRepositoryPutObjectRoutingTest {

    private static final String BUCKET = "test-bucket";
    private static final String KEY = "test-key";
    private static final long THRESHOLD_BYTES = 10;

    private S3Client retryingClient;
    private S3Client noRetryClient;
    private S3ObjectStorageConnectionProperties properties;
    private S3ObjectStorageRepository repository;

    @BeforeEach
    void setUp() {
        retryingClient = mock(S3Client.class);
        noRetryClient = mock(S3Client.class);
        lenient().when(retryingClient.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());
        lenient().when(noRetryClient.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());

        properties = new S3ObjectStorageConnectionProperties();
        properties.setUploadRetryMemoryBufferThreshold(DataSize.ofBytes(THRESHOLD_BYTES));

        S3LifecycleConfigurationInitializer lifecycleConfigurationInitializer = mock(S3LifecycleConfigurationInitializer.class);
        when(lifecycleConfigurationInitializer.lifecyclePolicyTag())
                .thenReturn(Tag.builder().key("MessageExchangeLifecyclePolicy").value("15").build());

        repository = new S3ObjectStorageRepository(retryingClient, noRetryClient, properties,
                new HousekeepingProperties(), lifecycleConfigurationInitializer);
    }

    @Test
    void putObject_markSupportedStream_usesRetryingClientRegardlessOfSize() {
        byte[] content = "x".repeat(100).getBytes(UTF_8); // above the threshold
        MessageContent messageContent = new MessageContent(new ByteArrayInputStream(content), content.length);

        repository.putObject(BUCKET, KEY, messageContent, MediaType.APPLICATION_XML_VALUE);

        verify(retryingClient).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        verifyNoInteractions(noRetryClient);
    }

    @Test
    void putObject_smallNonMarkStream_isBufferedAndUsesRetryingClient() throws IOException {
        byte[] content = "small".getBytes(UTF_8);
        MessageContent messageContent = new MessageContent(nonMarkStream(content), content.length);

        repository.putObject(BUCKET, KEY, messageContent, MediaType.APPLICATION_XML_VALUE);

        ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
        verify(retryingClient).putObject(any(PutObjectRequest.class), bodyCaptor.capture());
        verifyNoInteractions(noRetryClient);
        RequestBody requestBody = bodyCaptor.getValue();
        assertThat(requestBody.optionalContentLength()).contains((long) content.length);
        // the buffered body must be re-readable and contain the exact content
        assertThat(requestBody.contentStreamProvider().newStream().readAllBytes()).isEqualTo(content);
        assertThat(requestBody.contentStreamProvider().newStream().readAllBytes()).isEqualTo(content);
    }

    @Test
    void putObject_largeNonMarkStream_usesNoRetryClient() {
        byte[] content = "x".repeat((int) THRESHOLD_BYTES + 1).getBytes(UTF_8);
        MessageContent messageContent = new MessageContent(nonMarkStream(content), content.length);

        repository.putObject(BUCKET, KEY, messageContent, MediaType.APPLICATION_XML_VALUE);

        verify(noRetryClient).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        verifyNoInteractions(retryingClient);
    }

    @Test
    void putObject_fixDisabled_streamsThroughRetryingClient() {
        properties.setUploadRetryFixEnabled(false);
        byte[] content = "x".repeat((int) THRESHOLD_BYTES + 1).getBytes(UTF_8);
        MessageContent messageContent = new MessageContent(nonMarkStream(content), content.length);

        repository.putObject(BUCKET, KEY, messageContent, MediaType.APPLICATION_XML_VALUE);

        verify(retryingClient).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        verifyNoInteractions(noRetryClient);
    }

    @Test
    void putObject_truncatedBody_throwsWithoutUploading() {
        byte[] content = "abc".getBytes(UTF_8);
        MessageContent messageContent = new MessageContent(nonMarkStream(content), content.length + 2);

        assertThatThrownBy(() -> repository.putObject(BUCKET, KEY, messageContent, MediaType.APPLICATION_XML_VALUE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ended prematurely");

        verifyNoInteractions(retryingClient, noRetryClient);
    }

    @Test
    void putObject_zeroLengthBody_isBufferedAndUsesRetryingClient() {
        MessageContent messageContent = new MessageContent(nonMarkStream(new byte[0]), 0);

        repository.putObject(BUCKET, KEY, messageContent, MediaType.APPLICATION_XML_VALUE);

        verify(retryingClient).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        verifyNoInteractions(noRetryClient);
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
