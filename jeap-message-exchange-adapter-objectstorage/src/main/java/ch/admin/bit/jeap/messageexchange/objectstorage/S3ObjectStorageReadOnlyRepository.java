package ch.admin.bit.jeap.messageexchange.objectstorage;

import ch.admin.bit.jeap.messageexchange.domain.MessageContent;
import ch.admin.bit.jeap.messageexchange.domain.legacy.ObjectHead;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class S3ObjectStorageReadOnlyRepository {

    private static final String NO_SUCH_KEY = "NoSuchKey";

    protected final S3Client s3Client;

    public Optional<MessageContent> getObject(String bucketName, String objectKey) {
        try {
            MessageContent messageContent = getMessageContent(bucketName, objectKey);
            return Optional.of(messageContent);
        } catch (S3Exception ex) {
            if (ex.awsErrorDetails() != null && NO_SUCH_KEY.equalsIgnoreCase(ex.awsErrorDetails().errorCode())) {
                return Optional.empty();
            }
            throw ex;
        }
    }

    public Optional<String> getContentType(String bucketName, String objectKey) {
        return headObject(bucketName, objectKey)
                .map(HeadObjectResponse::contentType);
    }

    /**
     * Reads the content type and content length of an object via headObject.
     * LEGACY-TAG-FALLBACK: only used to backfill messages stored by MES &lt; 11.0.0 into the database -
     * remove with the contract story (JEAP-7252).
     */
    public Optional<ObjectHead> getObjectHead(String bucketName, String objectKey) {
        return headObject(bucketName, objectKey)
                .map(response -> new ObjectHead(response.contentType(), Math.toIntExact(response.contentLength())));
    }

    /**
     * LEGACY-TAG-FALLBACK: messages stored by MES &lt; 11.0.0 carry their metadata in object tags -
     * remove with the contract story (JEAP-7252).
     */
    public Optional<MessageContent> getObjectWithTags(String bucketName, String objectKey) {
        Optional<MessageContent> messageContentOptional = getObject(bucketName, objectKey);
        if (!messageContentOptional.isPresent()) {
            return Optional.empty();
        }
        Map<String, String> tags = getTagsOnObject(bucketName, objectKey);
        MessageContent messageContent = messageContentOptional.get();
        return Optional.of(new MessageContent(messageContent.inputStream(), messageContent.contentLength(), tags));
    }

    /**
     * LEGACY-TAG-FALLBACK: messages stored by MES &lt; 11.0.0 carry their metadata in object tags -
     * remove with the contract story (JEAP-7252).
     */
    public Map<String, String> getTagsOnObject(String bucketName, String objectKey) {
        GetObjectTaggingRequest request = GetObjectTaggingRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .build();

        GetObjectTaggingResponse tagging = s3Client.getObjectTagging(request);
        return tagging.tagSet().stream().collect(Collectors.toMap(Tag::key, Tag::value));
    }

    private Optional<HeadObjectResponse> headObject(String bucketName, String objectKey) {
        try {
            HeadObjectResponse headObjectResponse = s3Client.headObject(HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .build());
            log.debug("Head for object {} in bucket {}: contentType={}", objectKey, bucketName, headObjectResponse.contentType());
            return Optional.of(headObjectResponse);
        } catch (S3Exception ex) {
            if (ex.awsErrorDetails() != null && NO_SUCH_KEY.equalsIgnoreCase(ex.awsErrorDetails().errorCode())) {
                return Optional.empty();
            }
            throw ex;
        }
    }

    private MessageContent getMessageContent(String bucketName, String objectKey) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .key(objectKey)
                .bucket(bucketName)
                .build();
        ResponseInputStream<GetObjectResponse> response = s3Client.getObject(getObjectRequest, ResponseTransformer.toInputStream());
        return new MessageContent(response, Math.toIntExact(response.response().contentLength()));
    }
}
