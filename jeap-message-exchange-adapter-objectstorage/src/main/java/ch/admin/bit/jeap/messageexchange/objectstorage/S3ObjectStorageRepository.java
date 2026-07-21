package ch.admin.bit.jeap.messageexchange.objectstorage;

import ch.admin.bit.jeap.messageexchange.domain.MessageContent;
import ch.admin.bit.jeap.messageexchange.domain.housekeeping.HousekeepingProperties;
import ch.admin.bit.jeap.messageexchange.objectstorage.lifecycle.S3LifecycleConfigurationInitializer;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class S3ObjectStorageRepository extends S3ObjectStorageReadOnlyRepository {

    private final S3Client noRetryS3Client;
    private final S3ObjectStorageConnectionProperties connectionProperties;
    private final HousekeepingProperties housekeepingProperties;
    private final S3LifecycleConfigurationInitializer lifecycleConfigurationInitializer;

    public S3ObjectStorageRepository(S3Client s3Client, S3Client noRetryS3Client, S3ObjectStorageConnectionProperties connectionProperties, HousekeepingProperties housekeepingProperties, S3LifecycleConfigurationInitializer lifecycleConfigurationInitializer) {
        super(s3Client);
        this.noRetryS3Client = noRetryS3Client;
        this.connectionProperties = connectionProperties;
        this.housekeepingProperties = housekeepingProperties;
        this.lifecycleConfigurationInitializer = lifecycleConfigurationInitializer;
    }

    @PostConstruct
    public void checkBucketAccess() {
        log.info("Verifying access to bucket {}", connectionProperties.getBucketNameInternal());
        s3Client.headBucket(HeadBucketRequest.builder()
                .bucket(connectionProperties.getBucketNameInternal())
                .build());
        log.info("Verifying access to bucket {}", connectionProperties.getBucketNamePartner());
        s3Client.headBucket(HeadBucketRequest.builder()
                .bucket(connectionProperties.getBucketNamePartner())
                .build());
        log.info("Bucket access granted");

        if (housekeepingProperties.isEnabled()) {
            lifecycleConfigurationInitializer.ensureLifecyclePolicyPresent(connectionProperties.getBucketNameInternal());
            lifecycleConfigurationInitializer.ensureLifecyclePolicyPresent(connectionProperties.getBucketNamePartner());
        }
    }

    public void putObject(String bucketName, String objectKey, MessageContent messageContent, String contentType) {
        long contentLength = messageContent.contentLength();

        // The authoritative message metadata and the malware scan status live in PostgreSQL (inbound_message).
        // S3 objects are tagged atomically at creation; the tags passed with the message content are only
        // written transitionally for MES < 11.0.0 compatibility, as is the scanStatus tag update in
        // updateTags below (LEGACY-TAG-FALLBACK, removed with JEAP-7252).
        List<Tag> tagSet = new ArrayList<>();
        tagSet.add(lifecycleConfigurationInitializer.lifecyclePolicyTag());
        messageContent.tags().forEach((key, value) -> tagSet.add(Tag.builder().key(key).value(value).build()));
        Tagging tagging = Tagging.builder()
                .tagSet(tagSet)
                .build();

        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .contentType(contentType)
                .tagging(tagging)
                .contentLength(contentLength)
                .build();
        PutObjectResponse response = upload(putObjectRequest, messageContent);
        String versionId = response.versionId();
        log.info("Object {}:{} with size {} uploaded to bucket {}.", objectKey, versionId == null ? "" : versionId, contentLength, bucketName);
    }

    private PutObjectResponse upload(PutObjectRequest putObjectRequest, MessageContent messageContent) {
        InputStream inputStream = messageContent.inputStream();
        int contentLength = messageContent.contentLength();
        if (!connectionProperties.isUploadBufferingEnabled()) {
            // escape hatch: pre-12.1.0 behavior, an S3 retry cannot re-read the stream and fails with
            // "Content input stream does not support mark/reset"
            return s3Client.putObject(putObjectRequest, RequestBody.fromInputStream(inputStream, contentLength));
        }
        if (inputStream.markSupported()) {
            // already re-readable (e.g. a request body buffered by the web layer), the SDK resets it on retry
            return s3Client.putObject(putObjectRequest, RequestBody.fromInputStream(inputStream, contentLength));
        }
        if (contentLength <= connectionProperties.getUploadRetryMemoryBufferThreshold().toBytes()) {
            return s3Client.putObject(putObjectRequest, RequestBody.fromBytes(readBodyFully(inputStream, contentLength)));
        }
        // a body above the buffer threshold is streamed without retries: the one-shot stream cannot be re-read,
        // so the request fails fast with the actual S3 error and the client must retry the idempotent PUT
        return noRetryS3Client.putObject(putObjectRequest, RequestBody.fromInputStream(inputStream, contentLength));
    }

    private static byte[] readBodyFully(InputStream inputStream, int contentLength) {
        // Reads exactly contentLength bytes without a trailing zero-length read: the stream may be a tee into
        // the XML validator, which rejects further input once it has seen the declared content length.
        try {
            byte[] body = new byte[contentLength];
            int bytesRead = 0;
            while (bytesRead < contentLength) {
                int count = inputStream.read(body, bytesRead, contentLength - bytesRead);
                if (count < 0) {
                    throw new IllegalStateException(
                            "Request body ended prematurely: expected %d bytes but read %d".formatted(contentLength, bytesRead));
                }
                bytesRead += count;
            }
            return body;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read request body", e);
        }
    }

    /**
     * Updates tags on an existing object with a read-merge-put of the complete tag set, exactly like
     * MES &lt; 11.0.0 did for the scanStatus tag. PutObjectTagging is a full-replace API, so this write can
     * race with the GuardDuty object tagging (JEAP-7230) - acceptable only because the tags are no longer
     * authoritative: the malware scan status lives in PostgreSQL, and this update is written solely for
     * MES &lt; 11.0.0 instances during a rolling deployment.
     * LEGACY-TAG-FALLBACK: remove with the contract story (JEAP-7252).
     */
    public void updateTags(String bucketName, String objectKey, Map<String, String> tagsToUpdate) {
        // an update of the tags replaces all existing tags, so the existing tags have to be read first
        Map<String, String> allTags = new HashMap<>(getTagsOnObject(bucketName, objectKey));
        allTags.putAll(tagsToUpdate);

        List<Tag> tagSet = allTags.entrySet().stream()
                .map(entry -> Tag.builder().key(entry.getKey()).value(entry.getValue()).build())
                .toList();
        PutObjectTaggingRequest request = PutObjectTaggingRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .tagging(t -> t.tagSet(tagSet).build())
                .build();
        s3Client.putObjectTagging(request);
        log.debug("Tags updated for object {} in bucket {}", objectKey, bucketName);
    }

}
