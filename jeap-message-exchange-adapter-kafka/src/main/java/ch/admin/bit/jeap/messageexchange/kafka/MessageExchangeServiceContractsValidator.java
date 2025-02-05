package ch.admin.bit.jeap.messageexchange.kafka;

import ch.admin.bit.jeap.messageexchange.event.message.received.B2BMessageReceivedEvent;
import ch.admin.bit.jeap.messaging.avro.AvroMessageType;
import ch.admin.bit.jeap.messaging.kafka.contract.ContractsProvider;
import ch.admin.bit.jeap.messaging.kafka.contract.DefaultContractsValidator;
import ch.admin.bit.jeap.s3.malware.scanned.S3ObjectMalwareScannedEvent;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class MessageExchangeServiceContractsValidator {

    private static final AvroMessageType B2B_MESSAGE_RECEIVED_EVENT = new AvroMessageType(B2BMessageReceivedEvent.getClassSchema().getName(), B2BMessageReceivedEvent.MESSAGE_TYPE_VERSION$);

    private final DefaultContractsValidator defaultContractsValidator;
    private final TopicConfiguration topicConfiguration;

    public MessageExchangeServiceContractsValidator(@Value("${spring.application.name}") String appName, ContractsProvider contractsProvider, TopicConfiguration topicConfiguration) {
        defaultContractsValidator = new DefaultContractsValidator(appName, contractsProvider);
        this.topicConfiguration = topicConfiguration;
    }

    @PostConstruct
    public void checkContracts() {
        defaultContractsValidator.ensurePublisherContract(B2B_MESSAGE_RECEIVED_EVENT, topicConfiguration.getMessageReceived());
        if (topicConfiguration.getMalwareScanResult() != null) {
            defaultContractsValidator.ensureConsumerContract(S3ObjectMalwareScannedEvent.getClassSchema().getName(), topicConfiguration.getMalwareScanResult());
        }
    }
}