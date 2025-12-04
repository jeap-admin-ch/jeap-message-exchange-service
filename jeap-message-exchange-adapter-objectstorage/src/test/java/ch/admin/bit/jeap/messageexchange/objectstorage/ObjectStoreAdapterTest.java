package ch.admin.bit.jeap.messageexchange.objectstorage;

import ch.admin.bit.jeap.messageexchange.domain.MessageContent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;

import java.util.Optional;

import static ch.admin.bit.jeap.messageexchange.domain.objectstore.BucketType.INTERNAL;
import static ch.admin.bit.jeap.messageexchange.domain.objectstore.BucketType.PARTNER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class ObjectStoreAdapterTest {

    private S3ObjectStorageRepository objectStorageRepository;
    private ObjectStoreAdapter objectStoreAdapter;

    @BeforeEach
    void setUp() {
        objectStorageRepository = mock(S3ObjectStorageRepository.class);
        objectStoreAdapter = new ObjectStoreAdapter(objectStorageRepository, "internalBucket", "partnerBucket");
    }

    @Test
    void storeMessage_storesMessageInInternalBucket() {
        MessageContent messageContent = mock(MessageContent.class);
        objectStoreAdapter.storeMessage(INTERNAL, "objectKey", messageContent, MediaType.APPLICATION_XML_VALUE);
        verify(objectStorageRepository).putObject("internalBucket", "objectKey", messageContent, MediaType.APPLICATION_XML_VALUE);
    }

    @Test
    void loadMessage_loadsMessageFromPrimaryRepository() {
        MessageContent messageContent = mock(MessageContent.class);
        when(objectStorageRepository.getObject("internalBucket", "objectKey")).thenReturn(Optional.of(messageContent));
        Optional<MessageContent> result = objectStoreAdapter.loadMessage(INTERNAL, "objectKey");
        assertTrue(result.isPresent());
        assertEquals(messageContent, result.get());
    }

    @Test
    void loadMessage_loadsMessageFromPrimaryRepositoryPartnerBucket() {
        MessageContent messageContent = mock(MessageContent.class);
        when(objectStorageRepository.getObject("partnerBucket", "objectKey")).thenReturn(Optional.of(messageContent));
        Optional<MessageContent> result = objectStoreAdapter.loadMessage(PARTNER, "objectKey");
        assertTrue(result.isPresent());
        assertEquals(messageContent, result.get());
    }

    @Test
    void loadMessage_returnsEmptyWhenRepositoryIsEmpty() {
        when(objectStorageRepository.getObject("internalBucket", "objectKey")).thenReturn(Optional.empty());
        Optional<MessageContent> result = objectStoreAdapter.loadMessage(INTERNAL, "objectKey");
        assertTrue(result.isEmpty());
    }
}
