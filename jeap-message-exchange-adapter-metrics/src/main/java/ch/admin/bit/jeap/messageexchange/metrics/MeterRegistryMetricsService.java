package ch.admin.bit.jeap.messageexchange.metrics;

import ch.admin.bit.jeap.messageexchange.domain.metrics.MetricsService;
import ch.admin.bit.jeap.messageexchange.malware.api.MalwareScanResult;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;

@Service
public class MeterRegistryMetricsService implements MetricsService {

    private static final String DUPLICATED_MESSAGE_ID_RECEIVED = "jeap_mes_duplicated_message_id_received";
    private static final String BP_ID_TAG = "bp_id";

    private final Timer malwareScanDurationTimer;
    private final Map<MalwareScanResult, Counter> scanResultsCounter;
    private final MeterRegistry meterRegistry;

    public MeterRegistryMetricsService(MeterRegistry meterRegistry, @Value("${spring.application.name}") String applicationName) {
        this.meterRegistry = meterRegistry;
        malwareScanDurationTimer = Timer.builder("jeap_mes_malware_scan_duration_timer")
                .description("A timer to track durations of malware scans")
                .tags("applicationName", applicationName)
                .register(meterRegistry);
        scanResultsCounter = new EnumMap<>(MalwareScanResult.class);
        Arrays.stream(MalwareScanResult.values()).forEach(scanResult ->
                scanResultsCounter.put(scanResult,
                        Counter.builder("jeap_mes_malware_scan_result_counter")
                                .description("A counter to track the number of malware scan results")
                                .tags("applicationName", applicationName, "scanResult", scanResult.name())
                                .register(meterRegistry)
                )
        );
    }

    @Override
    public void publishMetrics(MalwareScanResult scanResult, long messageArrivalTimeInMillis, Long saveTimeInMillis) {
        if (saveTimeInMillis != null) {
            long durationInMillis = messageArrivalTimeInMillis - saveTimeInMillis;
            Duration duration = Duration.ofMillis(durationInMillis);
            malwareScanDurationTimer.record(duration);
        }
        scanResultsCounter.get(scanResult).increment();
    }

    @Override
    public void duplicatedMessageIdReceived(String bpId) {
        Counter.builder(DUPLICATED_MESSAGE_ID_RECEIVED)
                .description("Received duplicated message ids")
                .tag(BP_ID_TAG, bpId)
                .register(meterRegistry)
                .increment();
    }

}
