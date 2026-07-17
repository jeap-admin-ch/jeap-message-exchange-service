package ch.admin.bit.jeap.messageexchange.domain.legacy;

/**
 * Object metadata read via S3 headObject, used to backfill legacy messages into the database.
 * LEGACY-TAG-FALLBACK: remove with the contract story (JEAP-7252).
 */
public record ObjectHead(String contentType, int contentLength) {
}
