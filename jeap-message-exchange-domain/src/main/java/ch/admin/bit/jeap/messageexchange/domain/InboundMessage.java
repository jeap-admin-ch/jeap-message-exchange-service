package ch.admin.bit.jeap.messageexchange.domain;

import lombok.*;

import java.math.BigInteger;
import java.time.LocalDateTime;
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

    @Builder
    private InboundMessage(@NonNull String bpId, @NonNull UUID messageId, LocalDateTime overrideCreatedAt, @NonNull Integer contentLength) {
        this.bpId = bpId;
        this.createdAt = Objects.requireNonNullElseGet(overrideCreatedAt, LocalDateTime::now);
        this.messageId = messageId;
        this.contentLength = contentLength;
    }

}
