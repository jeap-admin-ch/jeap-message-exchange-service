package ch.admin.bit.jeap.messageexchange.objectstorage.lifecycle;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

@RequiredArgsConstructor
@Slf4j
public class S3LifecycleConfigurationInitializer {

    private final S3LifecycleConfigurationFactory lifecycleConfigurationFactory;
    private final S3Client s3Client;

    private final Set<String> policyInitializedBucketNames = new CopyOnWriteArraySet<>();

    public void ensureLifecyclePolicyPresent(String bucketName) {
        if (policyInitializedBucketNames.contains(bucketName)) {
            log.debug("Lifecycle policy on bucket {} already initialized (cached)", bucketName);
            return;
        }

        updateLifecyclePoliciesOnBucket(bucketName);
        policyInitializedBucketNames.add(bucketName);
    }

    private GetBucketLifecycleConfigurationResponse getLifecycleConfigurationResponse(String bucketName) {
        try {
            return s3Client.getBucketLifecycleConfiguration(GetBucketLifecycleConfigurationRequest.builder()
                    .bucket(bucketName)
                    .build());
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                log.debug("Currently no lifecycle configuration on bucket {}", bucketName);
                return null;
            } else {
                log.error("Received error for getBucketLifecycleConfiguration", e);
                throw e;
            }
        }
    }

    /**
     * Creates or adds a lifecycle policy for MES messages if missing. Existing rules are preserved, only
     * rules created by this MES are added if missing. This is to ensure that manually created rules are not
     * overwritten. The full update of all MES lifecycle rules ensures that the PAS does not suffer concurrency issues
     * when multiple instances or threads are reading/updating/writing rules at the same time.
     */
    void updateLifecyclePoliciesOnBucket(String bucketName) {
        GetBucketLifecycleConfigurationResponse response = getLifecycleConfigurationResponse(bucketName);
        if (lifecycleConfigurationFactory.ruleExists(response)) {
            log.info("Lifecycle rule for bucket {} already exists", bucketName);
            return;
        }

        BucketLifecycleConfiguration updatedConfiguration = lifecycleConfigurationFactory
                .createOrUpdateBucketLifecycleConfiguration(response);
        log.info("Setting lifecycle config for bucket {} with rules {}", bucketName, updatedConfiguration.rules().stream().map(LifecycleRule::id).toList());
        s3Client.putBucketLifecycleConfiguration(PutBucketLifecycleConfigurationRequest.builder()
                .bucket(bucketName)
                .lifecycleConfiguration(updatedConfiguration)
                .build());
    }

    public Tag lifecyclePolicyTag() {
        return lifecycleConfigurationFactory.lifecyclePolicyTag();
    }
}
