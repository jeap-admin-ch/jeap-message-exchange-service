package ch.admin.bit.jeap.messageexchange.objectstorage;

import static ch.admin.bit.jeap.messageexchange.domain.objectstore.BucketType.INTERNAL;
import static ch.admin.bit.jeap.messageexchange.domain.objectstore.BucketType.PARTNER;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

import ch.admin.bit.jeap.messageexchange.domain.MessageContent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

class ObjectStoreAdapterTest {

    private S3ObjectStorageRepository objectStorageRepository;
    private S3ObjectStorageFallbackRepository objectStorageFallbackRepository;
    private ObjectStoreAdapter objectStoreAdapter;

    @BeforeEach
    void setUp() {
        objectStorageRepository = mock(S3ObjectStorageRepository.class);
        objectStorageFallbackRepository = mock(S3ObjectStorageFallbackRepository.class);
        objectStoreAdapter = new ObjectStoreAdapter(objectStorageRepository, "internalBucket", "partnerBucket", objectStorageFallbackRepository, "fallbackInternalBucket", "fallbackPartnerBucket");
    }

    @Test
    void storeMessage_storesMessageInInternalBucket() {
        MessageContent messageContent = mock(MessageContent.class);
        objectStoreAdapter.storeMessage(INTERNAL, "objectKey", messageContent);
        verify(objectStorageRepository).putObject("internalBucket", "objectKey", messageContent);
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
    void loadMessage_loadsMessageFromFallbackRepositoryWhenPrimaryIsEmpty() {
        MessageContent messageContent = mock(MessageContent.class);
        when(objectStorageRepository.getObject("internalBucket", "objectKey")).thenReturn(Optional.empty());
        when(objectStorageFallbackRepository.getObject("fallbackInternalBucket", "objectKey")).thenReturn(Optional.of(messageContent));
        Optional<MessageContent> result = objectStoreAdapter.loadMessage(INTERNAL, "objectKey");
        assertTrue(result.isPresent());
        assertEquals(messageContent, result.get());
    }

    @Test
    void loadMessage_loadsMessageFromFallbackRepositoryPartnerBucketWhenPrimaryIsEmpty() {
        MessageContent messageContent = mock(MessageContent.class);
        when(objectStorageRepository.getObject("partnerBucket", "objectKey")).thenReturn(Optional.empty());
        when(objectStorageFallbackRepository.getObject("fallbackPartnerBucket", "objectKey")).thenReturn(Optional.of(messageContent));
        Optional<MessageContent> result = objectStoreAdapter.loadMessage(PARTNER, "objectKey");
        assertTrue(result.isPresent());
        assertEquals(messageContent, result.get());
    }

    @Test
    void loadMessage_returnsEmptyWhenBothRepositoriesAreEmpty() {
        when(objectStorageRepository.getObject("internalBucket", "objectKey")).thenReturn(Optional.empty());
        when(objectStorageFallbackRepository.getObject("fallbackInternalBucket", "objectKey")).thenReturn(Optional.empty());
        Optional<MessageContent> result = objectStoreAdapter.loadMessage(INTERNAL, "objectKey");
        assertTrue(result.isEmpty());
    }

    @Test
    void loadMessage_returnsEmptyWhenFallbackRepositoryIsNull() {
        objectStoreAdapter = new ObjectStoreAdapter(objectStorageRepository, "internalBucket", "partnerBucket", null, "fallbackInternalBucket", "fallbackPartnerBucket");
        when(objectStorageRepository.getObject("internalBucket", "objectKey")).thenReturn(Optional.empty());
        Optional<MessageContent> result = objectStoreAdapter.loadMessage(INTERNAL, "objectKey");
        assertTrue(result.isEmpty());
    }
}