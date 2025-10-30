package ch.admin.bit.jeap.messageexchange.objectstorage;

import ch.admin.bit.jeap.messageexchange.domain.MessageContent;
import ch.admin.bit.jeap.messageexchange.domain.objectstore.BucketType;
import ch.admin.bit.jeap.messageexchange.domain.objectstore.ObjectStore;
import io.micrometer.core.annotation.Timed;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Optional;

import static ch.admin.bit.jeap.messageexchange.domain.objectstore.BucketType.INTERNAL;

@Slf4j
public class ObjectStoreAdapter implements ObjectStore {

    private final S3ObjectStorageRepository objectStorageRepository;
    private final String bucketNameInternal;
    private final String bucketNamePartner;

    private final S3ObjectStorageFallbackRepository objectStorageFallbackRepository;
    private final String fallbackBucketNameInternal;
    private final String fallbackBucketNamePartner;

    public ObjectStoreAdapter(S3ObjectStorageRepository objectStorageRepository,
                              String bucketNameInternal,
                              String bucketNamePartner,
                              S3ObjectStorageFallbackRepository s3ObjectStorageFallbackRepository,
                              String fallbackBucketNameInternal,
                              String fallbackBucketNamePartner) {
        this.objectStorageRepository = objectStorageRepository;
        this.bucketNameInternal = bucketNameInternal;
        this.bucketNamePartner = bucketNamePartner;

        this.objectStorageFallbackRepository = s3ObjectStorageFallbackRepository;
        this.fallbackBucketNameInternal = fallbackBucketNameInternal;
        this.fallbackBucketNamePartner = fallbackBucketNamePartner;
    }

    @Override
    @Timed(value = "jeap_mes_objectstore_put", description = "Time taken to store a message in s3", percentiles = {0.5, 0.8, 0.95, 0.99})
    public void storeMessage(BucketType bucketType, String objectKey, MessageContent messageContent) {
        objectStorageRepository.putObject(getBucketName(bucketType), objectKey, messageContent);
    }

    @Override
    @Timed(value = "jeap_mes_objectstore_get", extraTags = {"with_tags", "false"}, description = "Time taken to retrieve the payload of a message in s3", percentiles = {0.5, 0.8, 0.95, 0.99})
    public Optional<MessageContent> loadMessage(BucketType bucketType, String objectKey) {
        Optional<MessageContent> content = objectStorageRepository.getObject(getBucketName(bucketType), objectKey);
        if (content.isPresent() || this.objectStorageFallbackRepository == null) {
            return content;
        }
        return this.objectStorageFallbackRepository.getObject(getFallbackBucketName(bucketType), objectKey);
    }

    @Override
    @Timed(value = "jeap_mes_objectstore_get", extraTags = {"with_tags", "true"}, description = "Time taken to retrieve the payload of a message in s3", percentiles = {0.5, 0.8, 0.95, 0.99})
    public Optional<MessageContent> loadMessageWithTags(BucketType bucketType, String objectKey) {
        Optional<MessageContent> content = objectStorageRepository.getObjectWithTags(getBucketName(bucketType), objectKey);
        if (content.isPresent() || this.objectStorageFallbackRepository == null) {
            return content;
        }
        return this.objectStorageFallbackRepository.getObjectWithTags(getFallbackBucketName(bucketType), objectKey);
    }

    @Override
    @Timed(value = "jeap_mes_objectstore_update_tags", description = "Time taken to update the tags of a message in s3", percentiles = {0.5, 0.8, 0.95, 0.99})
    public Map<String, String> updateTagsAndGetTags(BucketType bucketType, String bucketName, String objectKey, Map<String, String> tagsToUpdate) {
        String expectedBucketName = getBucketName(bucketType);
        if (!expectedBucketName.equals(bucketName)) {
            throw new IllegalStateException("Bucket name mismatch. Expected: " + expectedBucketName + ", actual: " + bucketName);
        }
        return objectStorageRepository.updateTagsAndGetTags(bucketName, objectKey, tagsToUpdate);
    }

    public String getBucketName(BucketType bucketType) {
        if (INTERNAL == bucketType) {
            return this.bucketNameInternal;
        }
        return this.bucketNamePartner;
    }

    private String getFallbackBucketName(BucketType bucketType) {
        if (INTERNAL == bucketType) {
            return this.fallbackBucketNameInternal;
        }
        return this.fallbackBucketNamePartner;
    }
}
