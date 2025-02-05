package ch.admin.bit.jeap.messageexchange.domain.housekeeping;

import ch.admin.bit.jeap.messageexchange.domain.database.MessageRepository;
import io.micrometer.core.annotation.Timed;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class HousekeepingService {

    private final MessageRepository messageRepository;
    private final HousekeepingProperties props;

    @Scheduled(cron = "${jeap.messageexchange.housekeeping.cron:-}")
    @SchedulerLock(name = "messageexchange-housekeeping", lockAtLeastFor = "5s", lockAtMostFor = "2h")
    @Timed(value = "jeap_mes_housekeeping", description = "Time taken to cleanup expired persistent messages")
    public void cleanupExpiredPersistentMessages() {
        if (!props.isEnabled()) {
            log.debug("Housekeeping is disabled");
            return;
        }

        log.info("Deleting expired messages with configuration {}", props);
        for (int i = 0; i < props.getMaxBatches(); i++) {
            boolean deletedExpiredMessages = messageRepository.deleteExpiredMessages(props.getExpirationDays(), props.getBatchSize());
            if (!deletedExpiredMessages) {
                log.info("Deleted expired messages in {} batches", i);
                break;
            }
            if (i == props.getMaxBatches() - 1) {
                log.warn("Deleted expired messages in {} batches, but maxBatches was reached - not cleaning up any more batches", i);
            }
        }
    }
}
