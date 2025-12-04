package ch.admin.bit.jeap.messageexchange.persistence;

import ch.admin.bit.jeap.db.tx.TransactionalReadReplica;
import ch.admin.bit.jeap.messageexchange.domain.Message;
import ch.admin.bit.jeap.messageexchange.domain.database.MessageRepository;
import ch.admin.bit.jeap.messageexchange.domain.dto.MessageSearchResultDto;
import io.micrometer.core.annotation.Timed;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.*;

@Repository
@RequiredArgsConstructor
@Slf4j
public class JdbcMessageRepository implements MessageRepository {
    private static final String INSERT_SQL = "INSERT INTO b2bhub_db_table(\"messageId\",\"bpId\",\"topicName\",\"groupId\",\"messageType\",\"datePublished\",\"partnerTopic\",\"contentType\") VALUES (:messageId,:bpId,:topicName,:groupId,:messageType,:datePublished,:partnerTopic,:contentType)";
    private static final String SELECT_BY_MESSAGE_ID = "SELECT * FROM b2bhub_db_table WHERE \"messageId\" = :messageId";
    private static final String SELECT_SEQUENCE_ID_OF_MESSAGE = "SELECT \"sequenceId\" FROM b2bhub_db_table WHERE \"bpId\" = :bpId AND \"messageId\" = :messageId";
    private static final String SELECT_NEXT_MESSAGE_ID_AFTER_SEQUENCE_ID = """
                SELECT t."messageId" FROM b2bhub_db_table t
                WHERE t."bpId" = :bpId
                  AND t."sequenceId" > :sequenceId
                  %s
                  %s
                ORDER BY t."sequenceId" ASC
                LIMIT 1
            """;

    private static final String SELECT_NEXT_MESSAGES = """
                SELECT t."messageId", t."messageType", t."groupId", t."partnerTopic", t."contentType"  FROM b2bhub_db_table t
                WHERE t."bpId" = :bpId
                    %s
                    %s
                    %s
                    %s
                ORDER BY t."sequenceId"
                LIMIT :size
            """;
    private static final String DELETE_EXPIRED_MESSAGES = """
            WITH rows AS 
                (SELECT "sequenceId" FROM b2bhub_db_table 
                    WHERE "datePublished" < NOW() - INTERVAL '%d DAYS'
                    LIMIT %d)
            DELETE FROM b2bhub_db_table 
                WHERE "sequenceId" IN (SELECT "sequenceId" FROM rows)
            """;

    private static final String SEQUENCE_ID = "sequenceId";
    private static final String MESSAGE_TYPE = "messageType";
    private static final String PARTNER_TOPIC = "partnerTopic";
    private static final String CONTENT_TYPE = "contentType";
    private static final String DATE_PUBLISHED = "datePublished";
    private static final String GROUP_ID = "groupId";
    private static final String TOPIC_NAME = "topicName";
    private static final String BP_ID = "bpId";
    private static final String MESSAGE_ID = "messageId";

    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Override
    @Transactional
    @Timed(value = "jeap_mes_repository_save", description = "Time taken to save a message in the DB", percentiles = {0.5, 0.8, 0.95, 0.99})
    public void save(Message message) {

        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue(MESSAGE_ID, message.getMessageId().toString());
        params.addValue(BP_ID, message.getBpId());
        params.addValue(MESSAGE_TYPE, message.getMessageType());
        params.addValue(DATE_PUBLISHED, message.getDatePublished());
        params.addValue(PARTNER_TOPIC, message.getPartnerTopic());
        params.addValue(TOPIC_NAME, message.getTopicName());
        params.addValue(GROUP_ID, message.getGroupId());
        params.addValue(CONTENT_TYPE, message.getContentType());

        try {
            jdbcTemplate.update(INSERT_SQL, params);
        } catch (DuplicateKeyException e) {
            log.warn("Message with messageId {} already present in the database. Skipping...", message.getMessageId());
        }
    }

    @Override
    @TransactionalReadReplica
    @Timed(value = "jeap_mes_repository_find_by_message_id", description = "Time taken to find a message in the DB", percentiles = {0.5, 0.8, 0.95, 0.99})
    public Optional<Message> findByMessageId(UUID messageId) {
        Map<String, Object> params = Map.of(MESSAGE_ID, messageId.toString());
        return jdbcTemplate.query(SELECT_BY_MESSAGE_ID, params,
                (rs, rowNum) ->
                        new Message(
                                rs.getObject(SEQUENCE_ID, BigInteger.class),
                                UUID.fromString(rs.getString(MESSAGE_ID)),
                                rs.getString(TOPIC_NAME),
                                rs.getString(BP_ID),
                                rs.getString(GROUP_ID),
                                rs.getString(MESSAGE_TYPE),
                                rs.getObject(DATE_PUBLISHED, LocalDateTime.class),
                                rs.getString(PARTNER_TOPIC),
                                rs.getString(CONTENT_TYPE)
                        )
        ).stream().findFirst();
    }

    @Override
    @TransactionalReadReplica
    @Timed(value = "jeap_mes_repository_get_messages", description = "Time taken to get messages from the DB", percentiles = {0.5, 0.8, 0.95, 0.99})
    public List<MessageSearchResultDto> getMessages(String bpId, String topicName, String groupId, UUID lastMessageId, String partnerTopic, int size) {

        Optional<BigInteger> sequenceId = Optional.empty();

        if (lastMessageId != null) {
            sequenceId = getSequenceId(bpId, lastMessageId.toString());
        }

        Map<String, Object> params = new HashMap<>(Map.of(BP_ID, bpId, "size", size));

        String sequenceIdClause = "";
        String partnerTopicClause = "";
        String topicNameClause = "";
        String groupIdClause = "";

        if (sequenceId.isPresent()) {
            params.put(SEQUENCE_ID, sequenceId.get());
            sequenceIdClause = "AND t.\"sequenceId\" > :sequenceId";
        }
        if (StringUtils.hasText(partnerTopic)) {
            params.put(PARTNER_TOPIC, partnerTopic);
            partnerTopicClause = "AND t.\"partnerTopic\" = :partnerTopic";
        }
        if (StringUtils.hasText(topicName)) {
            params.put(TOPIC_NAME, topicName);
            topicNameClause = "AND t.\"topicName\" = :topicName";
        }
        if (StringUtils.hasText(groupId)) {
            params.put(GROUP_ID, groupId);
            groupIdClause = "AND t.\"groupId\" = :groupId";
        }

        return jdbcTemplate.query(SELECT_NEXT_MESSAGES.formatted(sequenceIdClause, partnerTopicClause, topicNameClause, groupIdClause),
                params,
                (rs, rowNum) ->
                        new MessageSearchResultDto(
                                UUID.fromString(rs.getString(MESSAGE_ID)),
                                rs.getString(MESSAGE_TYPE),
                                rs.getString(CONTENT_TYPE),
                                rs.getString(GROUP_ID),
                                rs.getString(PARTNER_TOPIC)
                        )
        );

    }

    @Override
    @TransactionalReadReplica
    @Timed(value = "jeap_mes_repository_get_next_message_id", description = "Time taken to get next message ID from the DB", percentiles = {0.5, 0.8, 0.95, 0.99})
    public Optional<UUID> getNextMessageId(UUID lastMessageId, String bpId, String partnerTopic, String topicName) {
        return getNextMessageIdAsString(lastMessageId, bpId, partnerTopic, topicName).map(UUID::fromString);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    @Timed(value = "jeap_mes_repository_delete_expired", description = "Time taken to delete expired messages", percentiles = {0.5, 0.8, 0.95, 0.99})
    public boolean deleteExpiredMessages(int olderThanDays, int limit) {
        return jdbcTemplate.update(DELETE_EXPIRED_MESSAGES.formatted(olderThanDays, limit), Map.of()) > 0;
    }

    private Optional<String> getNextMessageIdAsString(UUID lastMessageId, String bpId, String partnerTopic, String topicName) {
        Optional<BigInteger> sequenceId = getSequenceId(bpId, lastMessageId.toString());
        if (sequenceId.isPresent()) {
            Map<String, Object> params = new HashMap<>(Map.of(BP_ID, bpId, SEQUENCE_ID, sequenceId.get()));
            String partnerTopicClause = "";
            String topicNameClause = "";

            if (StringUtils.hasText(partnerTopic)) {
                params.put(PARTNER_TOPIC, partnerTopic);
                partnerTopicClause = "AND t.\"partnerTopic\" = :partnerTopic";
            }
            if (StringUtils.hasText(topicName)) {
                params.put(TOPIC_NAME, topicName);
                topicNameClause = "AND t.\"topicName\" = :topicName";
            }
            return jdbcTemplate.queryForList(SELECT_NEXT_MESSAGE_ID_AFTER_SEQUENCE_ID.formatted(partnerTopicClause, topicNameClause), params, String.class).stream().findAny();

        }
        return Optional.empty();
    }

    private Optional<BigInteger> getSequenceId(String bpId, String lastMessageId) {
        Map<String, Object> params = Map.of(MESSAGE_ID, lastMessageId, BP_ID, bpId);
        return jdbcTemplate.queryForList(SELECT_SEQUENCE_ID_OF_MESSAGE, params, BigInteger.class).stream().findAny();
    }

}
