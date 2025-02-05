package ch.admin.bit.jeap.messageexchange.objectstorage;

import jakarta.validation.constraints.NotEmpty;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
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

    public void setRegion(String region) {
        this.region = Region.of(region);
    }
}
