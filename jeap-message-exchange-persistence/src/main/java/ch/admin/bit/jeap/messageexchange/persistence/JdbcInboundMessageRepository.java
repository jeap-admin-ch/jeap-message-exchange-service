package ch.admin.bit.jeap.messageexchange.persistence;

import ch.admin.bit.jeap.messageexchange.domain.InboundMessage;
import ch.admin.bit.jeap.messageexchange.domain.database.InboundMessageRepository;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.ScanStatus;
import io.micrometer.core.annotation.Timed;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

@Repository
@RequiredArgsConstructor
public class JdbcInboundMessageRepository implements InboundMessageRepository {

    private static final String SELECT_BY_BP_ID_MESSAGE_ID = "SELECT * FROM inbound_message WHERE \"bpId\" = :bpId AND \"messageId\" = :messageId";
    private static final String SELECT_LATEST_BY_MESSAGE_ID = """
            SELECT * FROM inbound_message WHERE "messageId" = :messageId ORDER BY "createdAt" DESC, "sequenceId" DESC LIMIT 1
            """;

    private static final String UPDATE_SCAN_STATUS_RETURNING_PREVIOUS_STATE = """
            UPDATE inbound_message im
            SET "scanStatus" = :scanStatus
            FROM (SELECT "sequenceId", "scanStatus" AS "previousScanStatus"
                    FROM inbound_message
                   WHERE "messageId" = :messageId
                   ORDER BY "createdAt" DESC, "sequenceId" DESC
                   LIMIT 1
                   FOR UPDATE) prev
            WHERE im."sequenceId" = prev."sequenceId"
            RETURNING im."sequenceId", im."messageId", im."bpId", im."contentLength", im."createdAt",
                      im."messageType", im."partnerTopic", im."partnerExternalReference", im."contentType",
                      prev."previousScanStatus" AS "scanStatus"
            """;

    private static final String UPDATE_SCAN_STATUS_IF_PENDING = """
            UPDATE inbound_message im
            SET "scanStatus" = :scanStatus
            FROM (SELECT "sequenceId", "scanStatus"
                    FROM inbound_message
                   WHERE "messageId" = :messageId
                   ORDER BY "createdAt" DESC, "sequenceId" DESC
                   LIMIT 1
                   FOR UPDATE) latest
            WHERE im."sequenceId" = latest."sequenceId"
              AND (latest."scanStatus" IS NULL OR latest."scanStatus" = 'SCAN_PENDING')
            """;

    private static final String UPSERT_SCAN_STATUS_AND_METADATA = """
            INSERT INTO inbound_message("messageId","bpId","contentLength","createdAt","messageType","partnerTopic","partnerExternalReference","contentType","scanStatus")
            VALUES (:messageId,:bpId,:contentLength,:createdAt,:messageType,:partnerTopic,:partnerExternalReference,:contentType,:scanStatus)
            ON CONFLICT ("bpId","messageId") DO UPDATE
            SET "messageType" = EXCLUDED."messageType",
                "partnerTopic" = EXCLUDED."partnerTopic",
                "partnerExternalReference" = EXCLUDED."partnerExternalReference",
                "contentType" = EXCLUDED."contentType",
                "scanStatus" = EXCLUDED."scanStatus",
                "contentLength" = EXCLUDED."contentLength",
                "createdAt" = EXCLUDED."createdAt"
            """;

    // SQL literal list of the terminal scan statuses, e.g. 'NO_THREATS_FOUND','THREATS_FOUND','SCAN_FAILED'
    private static final String TERMINAL_SCAN_STATUSES = Arrays.stream(ScanStatus.values())
            .filter(ScanStatus::isTerminal)
            .map(status -> "'" + status.name() + "'")
            .collect(Collectors.joining(","));

    // A terminal scan status is never downgraded: the scan result of the just-stored object may have been
    // processed before this upsert - see InboundMessageRepository#upsertScanStatusAndMetadataKeepingTerminalStatus
    private static final String UPSERT_SCAN_STATUS_AND_METADATA_KEEPING_TERMINAL_STATUS = """
            INSERT INTO inbound_message("messageId","bpId","contentLength","createdAt","messageType","partnerTopic","partnerExternalReference","contentType","scanStatus")
            VALUES (:messageId,:bpId,:contentLength,:createdAt,:messageType,:partnerTopic,:partnerExternalReference,:contentType,:scanStatus)
            ON CONFLICT ("bpId","messageId") DO UPDATE
            SET "messageType" = EXCLUDED."messageType",
                "partnerTopic" = EXCLUDED."partnerTopic",
                "partnerExternalReference" = EXCLUDED."partnerExternalReference",
                "contentType" = EXCLUDED."contentType",
                "scanStatus" = CASE WHEN inbound_message."scanStatus" IN (%s)
                                    THEN inbound_message."scanStatus"
                                    ELSE EXCLUDED."scanStatus" END,
                "contentLength" = EXCLUDED."contentLength",
                "createdAt" = EXCLUDED."createdAt"
            """.formatted(TERMINAL_SCAN_STATUSES);

    private static final String DELETE_EXPIRED_MESSAGES = """
            WITH rows AS
                (SELECT "sequenceId" FROM inbound_message
                    WHERE "createdAt" < NOW() - INTERVAL '%d DAYS'
                    LIMIT %d)
            DELETE FROM inbound_message
                WHERE "sequenceId" IN (SELECT "sequenceId" FROM rows)
            """;

    private static final String SEQUENCE_ID = "sequenceId";
    private static final String MESSAGE_ID = "messageId";
    private static final String BP_ID = "bpId";
    private static final String CONTENT_LENGTH = "contentLength";
    private static final String CREATED_AT = "createdAt";
    private static final String MESSAGE_TYPE = "messageType";
    private static final String PARTNER_TOPIC = "partnerTopic";
    private static final String PARTNER_EXTERNAL_REFERENCE = "partnerExternalReference";
    private static final String CONTENT_TYPE = "contentType";
    private static final String SCAN_STATUS = "scanStatus";

    private static final RowMapper<InboundMessage> ROW_MAPPER = (rs, rowNum) -> {
        String scanStatus = rs.getString(SCAN_STATUS);
        return new InboundMessage(
                rs.getObject(SEQUENCE_ID, BigInteger.class),
                UUID.fromString(rs.getString(MESSAGE_ID)),
                rs.getString(BP_ID),
                rs.getInt(CONTENT_LENGTH),
                rs.getObject(CREATED_AT, LocalDateTime.class),
                rs.getString(MESSAGE_TYPE),
                rs.getString(PARTNER_TOPIC),
                rs.getString(PARTNER_EXTERNAL_REFERENCE),
                rs.getString(CONTENT_TYPE),
                scanStatus == null ? null : ScanStatus.valueOf(scanStatus)
        );
    };

    private final NamedParameterJdbcTemplate jdbcTemplate;

    // The duplicate check on upload is a read-after-write pattern (a retried upload must see the row written
    // by the previous attempt) - it must always read from the primary database, never from a read replica.
    @Override
    @Transactional(readOnly = true)
    @Timed(value = "jeap_mes_repository_find_by_bp_id_message_id", description = "Time taken to find a message in the DB", percentiles = {0.5, 0.8, 0.95, 0.99})
    public Optional<InboundMessage> findByBpIdAndMessageId(String bpId, UUID messageId) {
        Map<String, Object> params = Map.of(BP_ID, bpId, MESSAGE_ID, messageId.toString());
        return jdbcTemplate.query(SELECT_BY_BP_ID_MESSAGE_ID, params, ROW_MAPPER).stream().findFirst();
    }

    // The malware scan gate is a read-after-write pattern: the scan status is committed immediately before the
    // B2BMessageReceivedEvent is published, and consumers retrieve the message right after receiving the event.
    // It must always read from the primary database, never from a read replica (replica lag would cause 403s).
    @Override
    @Transactional(readOnly = true)
    @Timed(value = "jeap_mes_repository_find_latest_by_message_id", description = "Time taken to find the latest inbound message by messageId in the DB", percentiles = {0.5, 0.8, 0.95, 0.99})
    public Optional<InboundMessage> findLatestByMessageId(UUID messageId) {
        Map<String, Object> params = Map.of(MESSAGE_ID, messageId.toString());
        return jdbcTemplate.query(SELECT_LATEST_BY_MESSAGE_ID, params, ROW_MAPPER).stream().findFirst();
    }

    @Override
    @Transactional
    @Timed(value = "jeap_mes_repository_update_scan_status", description = "Time taken to update the scan status of an inbound message in the DB", percentiles = {0.5, 0.8, 0.95, 0.99})
    public Optional<InboundMessage> updateScanStatusReturningPreviousState(UUID messageId, ScanStatus newScanStatus) {
        Map<String, Object> params = Map.of(MESSAGE_ID, messageId.toString(), SCAN_STATUS, newScanStatus.name());
        return jdbcTemplate.query(UPDATE_SCAN_STATUS_RETURNING_PREVIOUS_STATE, params, ROW_MAPPER).stream().findFirst();
    }

    @Override
    @Transactional
    @Timed(value = "jeap_mes_repository_update_scan_status_if_pending", description = "Time taken to heal the scan status of a pending inbound message in the DB", percentiles = {0.5, 0.8, 0.95, 0.99})
    public boolean updateScanStatusIfPending(UUID messageId, ScanStatus newScanStatus) {
        Map<String, Object> params = Map.of(MESSAGE_ID, messageId.toString(), SCAN_STATUS, newScanStatus.name());
        return jdbcTemplate.update(UPDATE_SCAN_STATUS_IF_PENDING, params) > 0;
    }

    @Override
    @Transactional
    @Timed(value = "jeap_mes_repository_upsert_scan_status_and_metadata", description = "Time taken to upsert the scan status and metadata of an inbound message in the DB", percentiles = {0.5, 0.8, 0.95, 0.99})
    public void upsertScanStatusAndMetadata(InboundMessage inboundMessage) {
        jdbcTemplate.update(UPSERT_SCAN_STATUS_AND_METADATA, toParams(inboundMessage));
    }

    @Override
    @Transactional
    @Timed(value = "jeap_mes_repository_upsert_scan_status_and_metadata", description = "Time taken to upsert the scan status and metadata of an inbound message in the DB", percentiles = {0.5, 0.8, 0.95, 0.99})
    public void upsertScanStatusAndMetadataKeepingTerminalStatus(InboundMessage inboundMessage) {
        jdbcTemplate.update(UPSERT_SCAN_STATUS_AND_METADATA_KEEPING_TERMINAL_STATUS, toParams(inboundMessage));
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    @Timed(value = "jeap_mes_repository_delete_expired", description = "Time taken to delete expired messages", percentiles = {0.5, 0.8, 0.95, 0.99})
    public boolean deleteExpiredMessages(int olderThanDays, int limit) {
        return jdbcTemplate.update(DELETE_EXPIRED_MESSAGES.formatted(olderThanDays, limit), Map.of()) > 0;
    }

    private static MapSqlParameterSource toParams(InboundMessage inboundMessage) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue(MESSAGE_ID, inboundMessage.getMessageId().toString());
        params.addValue(BP_ID, inboundMessage.getBpId());
        params.addValue(CONTENT_LENGTH, inboundMessage.getContentLength());
        params.addValue(CREATED_AT, inboundMessage.getCreatedAt());
        params.addValue(MESSAGE_TYPE, inboundMessage.getMessageType());
        params.addValue(PARTNER_TOPIC, inboundMessage.getPartnerTopic());
        params.addValue(PARTNER_EXTERNAL_REFERENCE, inboundMessage.getPartnerExternalReference());
        params.addValue(CONTENT_TYPE, inboundMessage.getContentType());
        params.addValue(SCAN_STATUS, inboundMessage.getScanStatus() == null ? null : inboundMessage.getScanStatus().name());
        return params;
    }

}
