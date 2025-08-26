package ch.admin.bit.jeap.messageexchange.kafka;

import ch.admin.bit.jeap.messageexchange.domain.malwarescan.MalwareScanProperties;
import ch.admin.bit.jeap.messageexchange.domain.sent.MessageSentProperties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TopicConfigurationTest {

    private KafkaAdmin kafkaAdmin;
    private MalwareScanProperties malwareScanProperties;
    private MessageSentProperties messageSentProperties;
    private TopicConfiguration topicConfiguration;

    private AdminClient adminClient;

    private TopicConfiguration.TopicConfigurationCloud topicConfigurationCloud;

    @BeforeEach
    void setUp() {
        kafkaAdmin = mock(KafkaAdmin.class);
        malwareScanProperties = mock(MalwareScanProperties.class);
        messageSentProperties = mock(MessageSentProperties.class);
        topicConfiguration = mock(TopicConfiguration.class);
        adminClient = mock(AdminClient.class);
        topicConfigurationCloud = new TopicConfiguration.TopicConfigurationCloud(kafkaAdmin, malwareScanProperties, messageSentProperties, topicConfiguration);
    }

    @Test
    void checkIfTopicsExist_allSet_noException() throws Exception {
        when(malwareScanProperties.isEnabled()).thenReturn(true);
        when(messageSentProperties.isEnabled()).thenReturn(true);

        when(topicConfiguration.getMessageReceived()).thenReturn("message-received");
        when(topicConfiguration.getMalwareScanResult()).thenReturn("malware-scan-result");
        when(topicConfiguration.getMessageSent()).thenReturn("message-sent");


        DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
        KafkaFuture<Map<String, TopicDescription>> kafkaFuture = mock(KafkaFuture.class);
        when(kafkaFuture.get()).thenReturn(new HashMap<>());
        when(describeTopicsResult.allTopicNames()).thenReturn(kafkaFuture);
        when(adminClient.describeTopics(List.of("message-received", "malware-scan-result", "message-sent"))).thenReturn(describeTopicsResult);

        assertDoesNotThrow(() -> topicConfigurationCloud.doCheckIfTopicExists(adminClient));
        verify(adminClient).describeTopics(List.of("message-received", "malware-scan-result", "message-sent"));
    }

    @Test
    void checkIfTopicsExist_allExceptMessageSentSet_noException() throws Exception {
        when(malwareScanProperties.isEnabled()).thenReturn(true);

        when(topicConfiguration.getMessageReceived()).thenReturn("message-received");
        when(topicConfiguration.getMalwareScanResult()).thenReturn("malware-scan-result");

        DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
        KafkaFuture<Map<String, TopicDescription>> kafkaFuture = mock(KafkaFuture.class);
        when(kafkaFuture.get()).thenReturn(new HashMap<>());
        when(describeTopicsResult.allTopicNames()).thenReturn(kafkaFuture);
        when(adminClient.describeTopics(List.of("message-received", "malware-scan-result"))).thenReturn(describeTopicsResult);

        assertDoesNotThrow(() -> topicConfigurationCloud.doCheckIfTopicExists(adminClient));
        verify(adminClient).describeTopics(List.of("message-received", "malware-scan-result"));
    }

    @Test
    void checkIfTopicsExist_allExceptMalwareScanResult_noException() throws Exception {
        when(messageSentProperties.isEnabled()).thenReturn(true);

        when(topicConfiguration.getMessageReceived()).thenReturn("message-received");
        when(topicConfiguration.getMessageSent()).thenReturn("message-sent");


        DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
        KafkaFuture<Map<String, TopicDescription>> kafkaFuture = mock(KafkaFuture.class);
        when(kafkaFuture.get()).thenReturn(new HashMap<>());
        when(describeTopicsResult.allTopicNames()).thenReturn(kafkaFuture);
        when(adminClient.describeTopics(List.of("message-received", "message-sent"))).thenReturn(describeTopicsResult);

        assertDoesNotThrow(() -> topicConfigurationCloud.doCheckIfTopicExists(adminClient));
        verify(adminClient).describeTopics(List.of("message-received", "message-sent"));
    }

    @Test
    void checkIfTopicsExist_malwareScanResultEnabledTopicNotSet_exception() throws Exception {
        when(malwareScanProperties.isEnabled()).thenReturn(true);

        when(topicConfiguration.getMessageReceived()).thenReturn("message-received");

        assertThrows(IllegalStateException.class, () -> topicConfigurationCloud.doCheckIfTopicExists(adminClient));
    }

    @Test
    void checkIfTopicsExist_messageSentEnabledTopicNotSet_exception() throws Exception {
        when(messageSentProperties.isEnabled()).thenReturn(true);

        when(topicConfiguration.getMessageReceived()).thenReturn("message-received");

        assertThrows(IllegalStateException.class, () -> topicConfigurationCloud.doCheckIfTopicExists(adminClient));
    }

}
