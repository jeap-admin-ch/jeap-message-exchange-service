package ch.admin.bit.jeap.messageexchange.domain.objectstore;

import ch.admin.bit.jeap.messageexchange.domain.MessageContent;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public interface ObjectStore {

    void storeMessage(BucketType bucketType, String objectKey, MessageContent messageInfo) throws IOException;

    Optional<MessageContent> loadMessage(BucketType bucketType, String objectKey);

    Optional<MessageContent> loadMessageWithTags(BucketType bucketType, String string);

    Map<String,String> updateTagsAndGetTags(BucketType bucketType, String bucketName, String objectKey, Map<String, String> tagsToUpdate);

}