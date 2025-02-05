package ch.admin.bit.jeap.messageexchange.domain.housekeeping;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "jeap.messageexchange.housekeeping")
public class HousekeepingProperties {

    private String cron;
    private int batchSize = 100;
    private int maxBatches = 100000; // delete max. 10mil messages at a time by default
    private int expirationDays = 14;
    /**
     * Enable or disable the DB housekeeping job as well as the creation of S3 lifecycle rules to delete data. Use
     * for example if deploying to an existing or test environment where you don't want to delete data.
     */
    private boolean enabled = true;
}
