package ch.admin.bit.jeap.messageexchange.domain;

import ch.admin.bit.jeap.messageexchange.domain.malwarescan.ScanStatus;
import lombok.*;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Objects;
import java.util.UUID;

@AllArgsConstructor
@ToString
@Getter
public class InboundMessage {

    private BigInteger sequenceId;

    private UUID messageId;

    private String bpId;

    private int contentLength;

    private LocalDateTime createdAt;

    private String messageType;

    private String partnerTopic;

    private String partnerExternalReference;

    private String contentType;

    private ScanStatus scanStatus;

    @Builder
    private InboundMessage(@NonNull String bpId, @NonNull UUID messageId, LocalDateTime overrideCreatedAt, @NonNull Integer contentLength,
                           String messageType, String partnerTopic, String partnerExternalReference, String contentType, ScanStatus scanStatus) {
        this.bpId = bpId;
        this.createdAt = Objects.requireNonNullElseGet(overrideCreatedAt, LocalDateTime::now);
        this.messageId = messageId;
        this.contentLength = contentLength;
        this.messageType = messageType;
        this.partnerTopic = partnerTopic;
        this.partnerExternalReference = partnerExternalReference;
        this.contentType = contentType;
        this.scanStatus = scanStatus;
    }

    public Long getCreatedAtEpochMillis() {
        return createdAt == null ? null : createdAt.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

}
