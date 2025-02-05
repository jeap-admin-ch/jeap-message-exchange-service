package ch.admin.bit.jeap.messageexchange.domain.housekeeping;

import ch.admin.bit.jeap.messageexchange.domain.database.MessageRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class HousekeepingServiceTest {

    @Mock
    private MessageRepository repo;

    @Test
    void cleanupExpiredPersistentMessages() {
        HousekeepingProperties props = new HousekeepingProperties();
        HousekeepingService housekeepingService = new HousekeepingService(repo, props);

        when(repo.deleteExpiredMessages(props.getExpirationDays(), props.getBatchSize()))
                // first invocation returns true, second invocation returns false, meaning no more messages to delete
                .thenReturn(true, false);

        housekeepingService.cleanupExpiredPersistentMessages();

        verify(repo, times(2)).deleteExpiredMessages(props.getExpirationDays(), props.getBatchSize());
        verifyNoMoreInteractions(repo);
    }

    @Test
    void cleanupExpiredPersistentMessages_maxBatches() {
        HousekeepingProperties props = new HousekeepingProperties();
        props.setMaxBatches(2);
        HousekeepingService housekeepingService = new HousekeepingService(repo, props);

        when(repo.deleteExpiredMessages(props.getExpirationDays(), props.getBatchSize())).thenReturn(true);

        housekeepingService.cleanupExpiredPersistentMessages();

        verify(repo, times(2)).deleteExpiredMessages(props.getExpirationDays(), props.getBatchSize());
        verifyNoMoreInteractions(repo);
    }

    @Test
    void cleanupExpiredPersistentMessages_disabled() {
        HousekeepingProperties props = new HousekeepingProperties();
        props.setEnabled(false);
        HousekeepingService housekeepingService = new HousekeepingService(repo, props);

        housekeepingService.cleanupExpiredPersistentMessages();

        verifyNoInteractions(repo);
    }
}
