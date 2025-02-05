package ch.admin.bit.jeap.messageexchange.domain.objectstore;

import ch.admin.bit.jeap.messageexchange.domain.malwarescan.ScanStatus;

public record S3ObjectTags(String bpId, String messageType, ScanStatus scanStatus, Long saveTimeInMillis) {
}
