package ch.admin.bit.jeap.messageexchange.kafka;

import ch.admin.bit.jeap.messageexchange.event.message.received.B2BMessageReceivedEvent;
import ch.admin.bit.jeap.messageexchange.event.message.sent.B2BMessageSentEvent;
import ch.admin.bit.jeap.messaging.annotations.JeapMessageProducerContract;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@EnableConfigurationProperties(TopicConfiguration.class)
@JeapMessageProducerContract(value = B2BMessageReceivedEvent.TypeRef.class, topic = {"message-received", "junit-test"})
@JeapMessageProducerContract(value = B2BMessageSentEvent.TypeRef.class, topic = {"message-sent"})
public class TestApp {

    @Bean
    public TestMessageReceivedListener testMessageReceivedListener() {
        return new TestMessageReceivedListener();
    }

}
