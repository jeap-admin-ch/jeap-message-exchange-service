package ch.admin.bit.jeap.messageexchange.kafka;

import ch.admin.bit.jeap.domainevent.avro.AvroDomainEventBuilder;
import ch.admin.bit.jeap.messageexchange.event.message.sent.B2BMessageSentEvent;
import ch.admin.bit.jeap.messageexchange.event.message.sent.B2BMessageSentEventPayload;
import ch.admin.bit.jeap.messageexchange.event.message.sent.B2BMessageSentEventReferences;
import ch.admin.bit.jeap.messageexchange.event.message.sent.MessageReference;
import ch.admin.bit.jeap.messaging.avro.AvroMessageBuilderException;
import lombok.Getter;

public class B2BMessageSentEventBuilder extends AvroDomainEventBuilder<B2BMessageSentEventBuilder, B2BMessageSentEvent> {
    @Getter
    private String serviceName;
    @Getter
    private String systemName;

    private String bpId;
    private String messageId;
    private String type;

    private String topicName;
    private String groupId;
    private String partnerTopic;

    private B2BMessageSentEventBuilder() {
        super(B2BMessageSentEvent::new);
    }

    public static B2BMessageSentEventBuilder create() {
        return new B2BMessageSentEventBuilder();
    }

    public B2BMessageSentEventBuilder systemName(String systemName) {
        this.systemName = systemName;
        return this;
    }

    public B2BMessageSentEventBuilder serviceName(String serviceName) {
        this.serviceName = serviceName;
        return this;
    }

    public B2BMessageSentEventBuilder bpId(String bpId) {
        this.bpId = bpId;
        return this;
    }

    public B2BMessageSentEventBuilder messageId(String messageId) {
        this.messageId = messageId;
        return this;
    }

    public B2BMessageSentEventBuilder type(String type) {
        this.type = type;
        return this;
    }

    public B2BMessageSentEventBuilder topicName(String topicName) {
        this.topicName = topicName;
        return this;
    }

    public B2BMessageSentEventBuilder groupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    public B2BMessageSentEventBuilder partnerTopic(String partnerTopic) {
        this.partnerTopic = partnerTopic;
        return this;
    }

    @Override
    protected B2BMessageSentEventBuilder self() {
        return this;
    }

    @Override
    public B2BMessageSentEvent build() {
        if (this.bpId == null) {
            throw AvroMessageBuilderException.propertyNull("B2BMessageSentEventReferences.bpId");
        }
        if (this.messageId == null) {
            throw AvroMessageBuilderException.propertyNull("B2BMessageSentEventReferences.messageId");
        }
        if (this.type == null) {
            throw AvroMessageBuilderException.propertyNull("B2BMessageSentEventReferences.type");
        }
        if (this.topicName == null) {
            throw AvroMessageBuilderException.propertyNull("B2BMessageSentEventPayload.topicName");
        }
        MessageReference reference = MessageReference.newBuilder()
                .setBpId(bpId)
                .setMessageId(messageId)
                .setType(type)
                .build();
        B2BMessageSentEventReferences references = B2BMessageSentEventReferences.newBuilder()
                .setMessageReference(reference)
                .build();
        setReferences(references);

        B2BMessageSentEventPayload payload = B2BMessageSentEventPayload.newBuilder()
                .setTopicName(topicName)
                .setGroupId(groupId)
                .setPartnerTopic(partnerTopic)
                .build();
        setPayload(payload);

        return super.build();
    }
}
