package ch.admin.bit.jeap.messageexchange.domain.metrics;

import ch.admin.bit.jeap.messageexchange.malware.api.MalwareScanResult;

public interface MetricsService {

    /**
     * Publishes metrics for a malware scan.
     *
     * @param scanResult                 the scan result
     * @param messageArrivalTimeInMillis the time the malware scan event arrived
     * @param saveTimeInMillis           the time the message was saved on S3, if available
     */
    void publishMetrics(MalwareScanResult scanResult, long messageArrivalTimeInMillis, Long saveTimeInMillis);

}
