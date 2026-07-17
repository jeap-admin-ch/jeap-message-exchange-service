package ch.admin.bit.jeap.messageexchange.domain.legacy;

import ch.admin.bit.jeap.messageexchange.domain.malwarescan.ScanStatus;

/**
 * Message metadata as stored in S3 object tags by MES &lt; 11.0.0.
 * LEGACY-TAG-FALLBACK: remove with the contract story (JEAP-7252).
 */
public record S3ObjectTags(String bpId, String messageType, String partnerTopic, String partnerExternalReference, ScanStatus scanStatus, Long saveTimeInMillis) {

    /**
     * True if none of the MES &lt; 11.0.0 metadata tags are present, i.e. the object was stored by
     * MES &gt;= 11.0.0 with the legacy tag compatibility disabled.
     */
    public boolean hasNoMetadataTags() {
        return bpId == null && messageType == null && scanStatus == null;
    }
}
