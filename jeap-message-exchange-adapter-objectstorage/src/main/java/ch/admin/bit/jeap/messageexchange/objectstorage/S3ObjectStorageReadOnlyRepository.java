package ch.admin.bit.jeap.messageexchange.objectstorage;

import ch.admin.bit.jeap.messageexchange.domain.MessageContent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.Tag;

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

    public Optional<MessageContent> getObjectWithTags(String bucketName, String objectKey) {
        Optional<MessageContent> messageContentOptional = getObject(bucketName, objectKey);
        if (!messageContentOptional.isPresent()) {
            return Optional.empty();
        }
        Map<String, String> tags = getTagsOnObject(bucketName, objectKey);
        MessageContent messageContent = messageContentOptional.get();
        return Optional.of(new MessageContent(messageContent.inputStream(), messageContent.contentLength(), tags));
    }

    protected Map<String, String> getTagsOnObject(String bucketName, String objectKey) {
        GetObjectTaggingRequest request = GetObjectTaggingRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .build();

        GetObjectTaggingResponse tagging = s3Client.getObjectTagging(request);
        return tagging.tagSet().stream().collect(Collectors.toMap(Tag::key, Tag::value));
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
