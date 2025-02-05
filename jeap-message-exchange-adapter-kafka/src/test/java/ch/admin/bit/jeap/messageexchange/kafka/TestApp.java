package ch.admin.bit.jeap.messageexchange.kafka;

import ch.admin.bit.jeap.messageexchange.event.message.received.B2BMessageReceivedEvent;
import ch.admin.bit.jeap.messaging.annotations.JeapMessageConsumerContract;
import ch.admin.bit.jeap.messaging.annotations.JeapMessageProducerContract;
import ch.admin.bit.jeap.s3.malware.scanned.S3ObjectMalwareScannedEvent;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@EnableConfigurationProperties(TopicConfiguration.class)
@JeapMessageProducerContract(value = B2BMessageReceivedEvent.TypeRef.class, topic = {"message-received", "junit-test"})
@JeapMessageConsumerContract(value = S3ObjectMalwareScannedEvent.TypeRef.class, topic = {"malware-scan", "junit-test"})
public class TestApp {

    @Bean
    public TestMessageReceivedListener testMessageReceivedListener() {
        return new TestMessageReceivedListener();
    }


}
