package ch.admin.bit.jeap.messageexchange.kafka;


import ch.admin.bit.jeap.messageexchange.domain.malwarescan.MalwareScanProperties;
import jakarta.annotation.PostConstruct;
import jakarta.validation.constraints.NotEmpty;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.util.StringUtils;
import org.springframework.validation.annotation.Validated;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Data
@Configuration
@ConfigurationProperties(prefix = "jeap.messageexchange.kafka.topic")
@Slf4j
@Validated
public class TopicConfiguration {

    @NotEmpty
    private String messageReceived;
    private String malwareScanResult;

    @Configuration
    @Profile("cloud|aws")
    @RequiredArgsConstructor
    @SuppressWarnings({"unused", "findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE"})
    private static class TopicConfigurationCloud {
        private final KafkaAdmin kafkaAdmin;
        private final MalwareScanProperties malwareScanProperties;
        private final TopicConfiguration topicConfiguration;

        @PostConstruct
        public void checkIfTopicsExist() throws ExecutionException, InterruptedException {
            try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
                List<String> topics = new ArrayList<>();
                topics.add(topicConfiguration.getMessageReceived());
                if (malwareScanProperties.isEnabled()) {
                    if (!StringUtils.hasText(topicConfiguration.getMalwareScanResult())) {
                        throw new IllegalStateException("Malware scan is enabled but no topic is configured");
                    }
                    topics.add(topicConfiguration.getMalwareScanResult());
                }
                log.info("Checking if topics exist: {}", topics);
                Map<String, TopicDescription> stringTopicDescriptionMap = adminClient.describeTopics(topics).allTopicNames().get();
                stringTopicDescriptionMap.forEach((name, desc) -> log.info("{}: {}", name, desc));
                log.info("All topics exist, good to go");
            }
        }
    }
}
