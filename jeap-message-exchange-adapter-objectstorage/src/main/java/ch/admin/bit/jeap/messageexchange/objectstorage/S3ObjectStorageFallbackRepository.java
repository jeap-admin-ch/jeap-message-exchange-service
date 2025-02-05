package ch.admin.bit.jeap.messageexchange.objectstorage;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;

@Slf4j
public class S3ObjectStorageFallbackRepository extends S3ObjectStorageReadOnlyRepository {

    private final S3ObjectStorageConnectionProperties connectionProperties;

    public S3ObjectStorageFallbackRepository(S3Client s3Client, S3ObjectStorageConnectionProperties connectionProperties) {
        super(s3Client);
        this.connectionProperties = connectionProperties;
    }

    @PostConstruct
    public void checkBucketAccess() {
        log.info("Verifying access to fallback bucket {}", connectionProperties.getBucketNameInternal());
        s3Client.headBucket(HeadBucketRequest.builder()
                .bucket(connectionProperties.getBucketNameInternal())
                .build());
        log.info("Verifying access to fallback bucket {}", connectionProperties.getBucketNamePartner());
        s3Client.headBucket(HeadBucketRequest.builder()
                .bucket(connectionProperties.getBucketNamePartner())
                .build());
        log.info("Fallback Bucket access granted");

    }

}
