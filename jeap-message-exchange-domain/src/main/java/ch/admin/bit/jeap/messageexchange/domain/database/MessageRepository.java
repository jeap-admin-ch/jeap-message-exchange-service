package ch.admin.bit.jeap.messageexchange.domain.database;

import ch.admin.bit.jeap.messageexchange.domain.Message;
import ch.admin.bit.jeap.messageexchange.domain.dto.MessageSearchResultDto;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface MessageRepository {

    void save(Message message);

    Optional<Message> findByMessageId(UUID messageId);

    List<MessageSearchResultDto> getMessages(String bpId, String topicName, String groupId, UUID lastMessageId, String partnerTopic, int size);

    Optional<UUID> getNextMessageId(UUID lastMessageId, String bpId, String partnerTopic, String topicName);

    /**
     * Deletes expired messages older than the given amount of days. Deletes at most the given limit amount of messages.
     * Creates a new transaction for the call to avoid large delete
     *
     * @return true if messages were deleted, false otherwise
     */
    boolean deleteExpiredMessages(int olderThanDays, int limit);
}
