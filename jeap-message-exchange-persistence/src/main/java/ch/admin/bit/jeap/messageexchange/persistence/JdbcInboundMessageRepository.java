package ch.admin.bit.jeap.messageexchange.persistence;

import ch.admin.bit.jeap.db.tx.TransactionalReadReplica;
import ch.admin.bit.jeap.messageexchange.domain.InboundMessage;
import ch.admin.bit.jeap.messageexchange.domain.database.InboundMessageRepository;
import io.micrometer.core.annotation.Timed;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Repository
@RequiredArgsConstructor
@Slf4j
public class JdbcInboundMessageRepository implements InboundMessageRepository {

    private static final String INSERT_SQL = "INSERT INTO inbound_message(\"messageId\",\"bpId\",\"contentLength\",\"createdAt\") VALUES (:messageId,:bpId,:contentLength,:createdAt)";
    private static final String SELECT_BY_BP_ID_MESSAGE_ID = "SELECT * FROM inbound_message WHERE \"bpId\" = :bpId AND \"messageId\" = :messageId";

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

    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Override
    @Transactional
    @Timed(value = "jeap_mes_repository_save", description = "Time taken to save a inboundMessage in the DB", percentiles = {0.5, 0.8, 0.95, 0.99})
    public void save(InboundMessage inboundMessage) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue(MESSAGE_ID, inboundMessage.getMessageId().toString());
        params.addValue(BP_ID, inboundMessage.getBpId());
        params.addValue(CONTENT_LENGTH, inboundMessage.getContentLength());
        params.addValue(CREATED_AT, inboundMessage.getCreatedAt());

        try {
            jdbcTemplate.update(INSERT_SQL, params);
        } catch (DuplicateKeyException _) {
            log.warn("Message with bpId {} messageId {} already present in the database. Skipping...", inboundMessage.getBpId(), inboundMessage.getMessageId());
        }
    }

    @Override
    @TransactionalReadReplica
    @Timed(value = "jeap_mes_repository_find_by_bp_id_message_id", description = "Time taken to find a message in the DB", percentiles = {0.5, 0.8, 0.95, 0.99})
    public Optional<InboundMessage> findByBpIdAndMessageId(String bpId, UUID messageId) {
        Map<String, Object> params = Map.of(BP_ID, bpId, MESSAGE_ID, messageId.toString());
        return jdbcTemplate.query(SELECT_BY_BP_ID_MESSAGE_ID, params,
                (rs, rowNum) ->
                        new InboundMessage(
                                rs.getObject(SEQUENCE_ID, BigInteger.class),
                                UUID.fromString(rs.getString(MESSAGE_ID)),
                                rs.getString(BP_ID),
                                rs.getInt(CONTENT_LENGTH),
                                rs.getObject(CREATED_AT, LocalDateTime.class)
                        )
        ).stream().findFirst();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    @Timed(value = "jeap_mes_repository_delete_expired", description = "Time taken to delete expired messages", percentiles = {0.5, 0.8, 0.95, 0.99})
    public boolean deleteExpiredMessages(int olderThanDays, int limit) {
        return jdbcTemplate.update(DELETE_EXPIRED_MESSAGES.formatted(olderThanDays, limit), Map.of()) > 0;
    }

}
