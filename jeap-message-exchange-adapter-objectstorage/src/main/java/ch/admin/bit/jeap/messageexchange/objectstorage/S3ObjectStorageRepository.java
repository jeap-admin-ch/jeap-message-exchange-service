package ch.admin.bit.jeap.messageexchange.objectstorage;

import ch.admin.bit.jeap.messageexchange.domain.MessageContent;
import ch.admin.bit.jeap.messageexchange.domain.housekeeping.HousekeepingProperties;
import ch.admin.bit.jeap.messageexchange.objectstorage.lifecycle.S3LifecycleConfigurationInitializer;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.internal.util.Mimetype;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class S3ObjectStorageRepository extends S3ObjectStorageReadOnlyRepository {

    private final S3ObjectStorageConnectionProperties connectionProperties;
    private final HousekeepingProperties housekeepingProperties;
    private final S3LifecycleConfigurationInitializer lifecycleConfigurationInitializer;

    public S3ObjectStorageRepository(S3Client s3Client, S3ObjectStorageConnectionProperties connectionProperties, HousekeepingProperties housekeepingProperties, S3LifecycleConfigurationInitializer lifecycleConfigurationInitializer) {
        super(s3Client);
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

        List<Tag> tagSet = createTags(messageContent.tags());

        PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .contentType(contentType)
                .tagging(Tagging.builder()
                        .tagSet(tagSet)
                        .build())
                .contentLength(contentLength);
        PutObjectRequest putObjectRequest = requestBuilder.build();
        RequestBody requestBody = RequestBody.fromInputStream(messageContent.inputStream(), contentLength);
        PutObjectResponse response = s3Client.putObject(putObjectRequest, requestBody);
        String versionId = response.versionId();
        log.info("Object {}:{} with size {} uploaded to bucket {}.", objectKey, versionId == null ? "" : versionId, contentLength, bucketName);
    }

    public Map<String, String> updateTagsAndGetTags(String bucketName, String objectKey, Map<String, String> tagsToUpdate) {
        // an update of the tags replaces all existing tags, so we have to get the existing tags first
        Map<String, String> existingTags = getTagsOnObject(bucketName, objectKey);
        Map<String, String> allTags = new HashMap<>(existingTags);
        allTags.putAll(tagsToUpdate);

        updateTags(bucketName, objectKey, allTags);

        return allTags;
    }

    private void updateTags(String bucketName, String objectKey, Map<String, String> tags) {
        List<Tag> tagSet = toTags(tags);

        PutObjectTaggingRequest request = PutObjectTaggingRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .tagging(Tagging.builder()
                        .tagSet(tagSet)
                        .build())
                .build();

        s3Client.putObjectTagging(request);
        log.debug("Tags updated for object {} in bucket {}", objectKey, bucketName);
    }

    private List<Tag> createTags(Map<String, String> additionalTags) {
        List<Tag> tagSet = new ArrayList<>();
        tagSet.add(lifecycleConfigurationInitializer.lifecyclePolicyTag());
        tagSet.addAll(toTags(additionalTags));
        return tagSet;
    }

    private List<Tag> toTags(Map<String, String> tags) {
        return tags.entrySet().stream()
                .map(entry -> Tag.builder().key(entry.getKey()).value(entry.getValue()).build())
                .collect(Collectors.toList());
    }

}
