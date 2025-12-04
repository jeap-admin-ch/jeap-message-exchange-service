package ch.admin.bit.jeap.messageexchange.domain.objectstore;

import ch.admin.bit.jeap.messageexchange.domain.MessageContent;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public interface ObjectStore {

    void storeMessage(BucketType bucketType, String objectKey, MessageContent messageInfo, String contentType) throws IOException;

    Optional<MessageContent> loadMessage(BucketType bucketType, String objectKey);

    Optional<MessageContent> loadMessageWithTags(BucketType bucketType, String string);

    Map<String,String> updateTagsAndGetTags(BucketType bucketType, String bucketName, String objectKey, Map<String, String> tagsToUpdate);

    String getBucketName(BucketType bucketType);

    Optional<String> getContentType(BucketType bucketType, String objectKey);

    String getContentType(BucketType bucketType, String bucketName, String objectKey);

}
