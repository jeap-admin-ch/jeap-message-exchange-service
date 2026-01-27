package ch.admin.bit.jeap.messageexchange.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

@AllArgsConstructor
@Getter
public class Message {

    private BigInteger sequenceId;

    private UUID messageId;

    private String topicName;

    private String bpId;

    private String groupId;

    private String messageType;

    private LocalDateTime datePublished;

    private String partnerTopic;

    private String contentType;

    private String partnerExternalReference;

    private Map<String, String> metadata;

    @Builder
    private Message(@NonNull String bpId,
                    String groupId,
                    @NonNull UUID messageId,
                    @NonNull String messageType,
                    LocalDateTime overrideCreatedAt,
                    String partnerTopic,
                    @NonNull String topicName,
                    @NonNull String contentType,
                    String partnerExternalReference,
                    Map<String, String> metadata) {
        this.bpId = bpId;
        this.datePublished = Objects.requireNonNullElseGet(overrideCreatedAt, LocalDateTime::now);
        this.groupId = groupId;
        this.messageId = messageId;
        this.messageType = messageType;
        this.partnerTopic = partnerTopic;
        this.topicName = topicName;
        this.contentType = contentType;
        this.partnerExternalReference = partnerExternalReference;
        this.metadata = metadata;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Message{");
        sb.append("messageId=").append(messageId);
        if (sequenceId != null) {
            sb.append(",sequenceId=").append(sequenceId);
        }
        sb.append(", topicName='").append(topicName).append('\'');
        sb.append(", bpId='").append(bpId).append('\'');
        sb.append(", groupId='").append(groupId).append('\'');
        sb.append(", messageType='").append(messageType).append('\'');
        sb.append(", datePublished=").append(datePublished);
        sb.append(", partnerTopic='").append(partnerTopic).append('\'');
        sb.append(", contentType='").append(contentType).append('\'');
        sb.append(", partnerExternalReference='").append(partnerExternalReference).append('\'');
        sb.append(", metadata='").append(metadata).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
