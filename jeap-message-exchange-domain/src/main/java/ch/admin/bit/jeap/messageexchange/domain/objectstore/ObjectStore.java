package ch.admin.bit.jeap.messageexchange.domain.objectstore;

import ch.admin.bit.jeap.messageexchange.domain.MessageContent;
import ch.admin.bit.jeap.messageexchange.domain.legacy.ObjectHead;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public interface ObjectStore {

    void storeMessage(BucketType bucketType, String objectKey, MessageContent messageInfo, String contentType) throws IOException;

    Optional<MessageContent> loadMessage(BucketType bucketType, String objectKey);

    String getBucketName(BucketType bucketType);

    Optional<String> getContentType(BucketType bucketType, String objectKey);

    // Legacy S3 tag compatibility for MES < 11.0.0 instances and messages.
    // LEGACY-TAG-FALLBACK: remove with the contract story (JEAP-7252).

    Optional<MessageContent> loadMessageWithTags(BucketType bucketType, String objectKey);

    Map<String, String> getObjectTags(BucketType bucketType, String objectKey);

    Optional<ObjectHead> getObjectHead(BucketType bucketType, String objectKey);

    /**
     * Updates the given tags on an existing object with a read-merge-put of the complete tag set, exactly
     * like MES &lt; 11.0.0 did. Only used to keep the scanStatus tag in sync for 10.x instances during a
     * rolling deployment while the legacy tag compatibility is enabled; the database stays authoritative.
     */
    void updateTags(BucketType bucketType, String objectKey, Map<String, String> tagsToUpdate);

}
