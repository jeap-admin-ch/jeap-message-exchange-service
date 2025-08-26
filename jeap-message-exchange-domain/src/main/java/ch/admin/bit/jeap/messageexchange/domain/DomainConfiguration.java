package ch.admin.bit.jeap.messageexchange.domain;

import ch.admin.bit.jeap.messageexchange.domain.housekeeping.HousekeepingProperties;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.MalwareScanProperties;
import ch.admin.bit.jeap.messageexchange.domain.sent.MessageSentProperties;
import net.javacrumbs.shedlock.spring.annotation.EnableSchedulerLock;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@AutoConfiguration
@ComponentScan
@EnableScheduling
@EnableSchedulerLock(defaultLockAtMostFor = "PT10H")
@EnableConfigurationProperties({HousekeepingProperties.class, MalwareScanProperties.class, MessageSentProperties.class})
public class DomainConfiguration {
}
