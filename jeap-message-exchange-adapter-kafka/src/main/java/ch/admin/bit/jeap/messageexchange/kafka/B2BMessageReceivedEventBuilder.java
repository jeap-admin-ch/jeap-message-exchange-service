package ch.admin.bit.jeap.messageexchange.kafka;

import ch.admin.bit.jeap.domainevent.avro.AvroDomainEventBuilder;
import ch.admin.bit.jeap.messageexchange.event.message.received.B2BMessageReceivedEvent;
import ch.admin.bit.jeap.messageexchange.event.message.received.B2BMessageReceivedEventPayload;
import ch.admin.bit.jeap.messageexchange.event.message.received.B2BMessageReceivedEventReferences;
import ch.admin.bit.jeap.messageexchange.event.message.received.MessageReference;
import ch.admin.bit.jeap.messageexchange.event.message.received.S3ObjectMalwareScanStatus;
import ch.admin.bit.jeap.messaging.avro.AvroMessageBuilderException;
import lombok.Getter;

public class B2BMessageReceivedEventBuilder extends AvroDomainEventBuilder<B2BMessageReceivedEventBuilder, B2BMessageReceivedEvent> {
    @Getter
    private String serviceName;
    @Getter
    private String systemName;

    private String bpId;
    private String messageId;
    private String type;
    private S3ObjectMalwareScanStatus scanStatus;
    private String contentType;

    private B2BMessageReceivedEventBuilder() {
        super(B2BMessageReceivedEvent::new);
    }

    public static B2BMessageReceivedEventBuilder create() {
        return new B2BMessageReceivedEventBuilder();
    }

    public B2BMessageReceivedEventBuilder systemName(String systemName) {
        this.systemName = systemName;
        return this;
    }

    public B2BMessageReceivedEventBuilder serviceName(String serviceName) {
        this.serviceName = serviceName;
        return this;
    }

    public B2BMessageReceivedEventBuilder bpId(String bpId) {
        this.bpId = bpId;
        return this;
    }

    public B2BMessageReceivedEventBuilder messageId(String messageId) {
        this.messageId = messageId;
        return this;
    }

    public B2BMessageReceivedEventBuilder type(String type) {
        this.type = type;
        return this;
    }

    public B2BMessageReceivedEventBuilder scanStatus(S3ObjectMalwareScanStatus scanStatus) {
        this.scanStatus = scanStatus;
        return this;
    }

    public B2BMessageReceivedEventBuilder contentType(String contentType) {
        this.contentType = contentType;
        return this;
    }

    @Override
    protected B2BMessageReceivedEventBuilder self() {
        return this;
    }


    @Override
    public B2BMessageReceivedEvent build() {
        if (this.bpId == null) {
            throw AvroMessageBuilderException.propertyNull("B2BMessageReceivedEventReferences.bpId");
        }
        if (this.messageId == null) {
            throw AvroMessageBuilderException.propertyNull("B2BMessageReceivedEventReferences.messageId");
        }
        if (this.type == null) {
            throw AvroMessageBuilderException.propertyNull("B2BMessageReceivedEventReferences.type");
        }
        MessageReference reference = MessageReference.newBuilder()
                .setBpId(bpId)
                .setMessageId(messageId)
                .setType(type)
                .setContentType(contentType)
                .build();
        B2BMessageReceivedEventReferences references = B2BMessageReceivedEventReferences.newBuilder()
                .setMessageReference(reference)
                .build();
        setReferences(references);
        if (this.scanStatus != null) {
            B2BMessageReceivedEventPayload payload = B2BMessageReceivedEventPayload.newBuilder()
                    .setScanStatus(scanStatus)
                    .build();
            setPayload(payload);
        }

        return super.build();
    }
}
