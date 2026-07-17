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

    /**
     * S3 objects expire one day after the DB expiration to make sure no inconsistencies occur when messages are
     * deleted from the storage but not yet from the DB.
     */
    public int getS3ExpirationDays() {
        return expirationDays + 1;
    }

    /**
     * Inbound message rows must outlive the corresponding S3 objects: the row is the sole source of the malware
     * scan status - without it, a still-existing S3 object without a scanStatus tag could be delivered unchecked.
     */
    public int getInboundMessageRetentionDays() {
        return getS3ExpirationDays() + 1;
    }
}
