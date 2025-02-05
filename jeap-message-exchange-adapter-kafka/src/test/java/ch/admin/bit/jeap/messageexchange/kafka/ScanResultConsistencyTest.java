package ch.admin.bit.jeap.messageexchange.kafka;

import ch.admin.bit.jeap.messageexchange.domain.malwarescan.ScanResult;
import ch.admin.bit.jeap.s3.malware.scanned.S3ObjectMalwareScanResult;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ScanResultConsistencyTest {

    @Test
    public void testScanResultConsistency() {
        for (S3ObjectMalwareScanResult scanResult : S3ObjectMalwareScanResult.values()) {
            ScanResult otherScanResult = ScanResult.valueOf(scanResult.name());
            assertEquals(scanResult.name(), otherScanResult.name());
        }
    }

}
