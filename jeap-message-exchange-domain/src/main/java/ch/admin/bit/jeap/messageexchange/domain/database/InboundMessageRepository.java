package ch.admin.bit.jeap.messageexchange.domain.database;

import ch.admin.bit.jeap.messageexchange.domain.InboundMessage;

import java.util.Optional;
import java.util.UUID;

public interface InboundMessageRepository {

    void save(InboundMessage inboundMessage);

    Optional<InboundMessage> findByBpIdAndMessageId(String bpId, UUID messageId);

    /**
     * Deletes expired messages older than the given amount of days. Deletes at most the given limit amount of messages.
     * Creates a new transaction for the call to avoid large delete
     *
     * @return true if messages were deleted, false otherwise
     */
    boolean deleteExpiredMessages(int olderThanDays, int limit);
}
