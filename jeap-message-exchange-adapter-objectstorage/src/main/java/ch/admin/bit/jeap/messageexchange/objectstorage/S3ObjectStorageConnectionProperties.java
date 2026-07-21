package ch.admin.bit.jeap.messageexchange.objectstorage;

import jakarta.validation.constraints.NotEmpty;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.util.unit.DataSize;
import org.springframework.validation.annotation.Validated;
import software.amazon.awssdk.regions.Region;

import java.time.Duration;


@Getter
@ToString
@Validated
public class S3ObjectStorageConnectionProperties {

    @Setter
    @NotEmpty
    private String bucketNameInternal;

    @Setter
    @NotEmpty
    private String bucketNamePartner;

    /**
     * Set for on-prem / custom S3 services. Leave empty to use the default AWS regional endpoint.
     */
    @Setter
    private String accessUrl;

    private Region region = Region.AWS_GLOBAL;

    /**
     * Leave empty to use the default AWS credentials provider chain.
     */
    @Setter
    // excluded from toString for security reasons
    @ToString.Exclude
    private String accessKey;

    /**
     * Leave empty to use the default AWS credentials provider chain.
     */
    @Setter
    // excluded from toString for security reasons
    @ToString.Exclude
    private String secretKey;

    @Setter
    private Duration s3Timeout = Duration.ofSeconds(30);

    /**
     * Message uploads with a body up to this size are buffered in memory before being uploaded to S3, so that
     * transient S3 errors can be retried (the raw request stream can only be read once). Larger uploads are
     * streamed without buffering and fail fast on the first S3 error - the client must retry the idempotent PUT.
     */
    @Setter
    private DataSize uploadRetryMemoryBufferThreshold = DataSize.ofMegabytes(1);

    /**
     * Emergency escape hatch: set to false to restore the pre-12.1.0 upload behavior, where every message body is
     * streamed directly to S3 through the retrying S3 client. With that behavior, an S3 retry cannot re-read the
     * request stream and fails with "Content input stream does not support mark/reset".
     */
    @Setter
    private boolean uploadRetryFixEnabled = true;

    public void setRegion(String region) {
        this.region = Region.of(region);
    }
}
