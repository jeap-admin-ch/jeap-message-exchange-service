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

    public ObjectStoreAdapter(S3ObjectStorageRepository objectStorageRepository,
                              String bucketNameInternal,
                              String bucketNamePartner) {
        this.objectStorageRepository = objectStorageRepository;
        this.bucketNameInternal = bucketNameInternal;
        this.bucketNamePartner = bucketNamePartner;
    }

    @Override
    @Timed(value = "jeap_mes_objectstore_put", description = "Time taken to store a message in s3", percentiles = {0.5, 0.8, 0.95, 0.99})
    public void storeMessage(BucketType bucketType, String objectKey, MessageContent messageContent, String contentType) {
        objectStorageRepository.putObject(getBucketName(bucketType), objectKey, messageContent, contentType);
    }

    @Override
    @Timed(value = "jeap_mes_objectstore_get", extraTags = {"with_tags", "false"}, description = "Time taken to retrieve the payload of a message in s3", percentiles = {0.5, 0.8, 0.95, 0.99})
    public Optional<MessageContent> loadMessage(BucketType bucketType, String objectKey) {
        return objectStorageRepository.getObject(getBucketName(bucketType), objectKey);
    }

    @Override
    @Timed(value = "jeap_mes_objectstore_get", extraTags = {"with_tags", "true"}, description = "Time taken to retrieve the payload of a message in s3", percentiles = {0.5, 0.8, 0.95, 0.99})
    public Optional<MessageContent> loadMessageWithTags(BucketType bucketType, String objectKey) {
        return objectStorageRepository.getObjectWithTags(getBucketName(bucketType), objectKey);
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

    @Override
    @Timed(value = "jeap_mes_objectstore_get_content_type", extraTags = {"with_tags", "true"}, description = "Time taken to retrieve the content type of a message in s3", percentiles = {0.5, 0.8, 0.95, 0.99})
    public Optional<String> getContentType(BucketType bucketType, String objectKey) {
        return objectStorageRepository.getContentType(getBucketName(bucketType), objectKey);
    }

    @Override
    public String getContentType(BucketType bucketType, String bucketName, String objectKey) {
        String expectedBucketName = getBucketName(bucketType);
        if (!expectedBucketName.equals(bucketName)) {
            throw new IllegalStateException("Bucket name mismatch. Expected: " + expectedBucketName + ", actual: " + bucketName);
        }
        return objectStorageRepository.getContentType(getBucketName(bucketType), objectKey)
                .orElseThrow(() -> new IllegalStateException("Object with key not found: " + objectKey));
    }

    public String getBucketName(BucketType bucketType) {
        if (INTERNAL == bucketType) {
            return this.bucketNameInternal;
        }
        return this.bucketNamePartner;
    }
}
