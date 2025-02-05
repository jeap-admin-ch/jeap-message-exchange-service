package ch.admin.bit.jeap.messageexchange.objectstorage.lifecycle;

import ch.admin.bit.jeap.messageexchange.objectstorage.AbstractS3ObjectRepositoryTestBase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ExpirationStatus;
import software.amazon.awssdk.services.s3.model.GetBucketLifecycleConfigurationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketLifecycleConfigurationResponse;
import software.amazon.awssdk.services.s3.model.LifecycleRule;

import static org.assertj.core.api.Assertions.assertThat;

class S3LifecycleConfigurationInitializerTest extends AbstractS3ObjectRepositoryTestBase {

    private static final String TEST_POLICY = "TestPolicy";

    @Autowired
    private S3Client s3Client;

    @Test
    void updateLifecyclePoliciesOnBucket_shouldOnlyCreateSinglePolicyPerBucketForMultipleInvocations() {
        S3LifecycleConfigurationFactory lifecycleConfigurationFactory14days =
                new S3LifecycleConfigurationFactory(TEST_POLICY, 14);
        S3LifecycleConfigurationInitializer lifecycleConfigurationInitializer =
                new S3LifecycleConfigurationInitializer(lifecycleConfigurationFactory14days, s3Client);

        lifecycleConfigurationInitializer.updateLifecyclePoliciesOnBucket(TEST_BUCKET_NAME);
        lifecycleConfigurationInitializer.updateLifecyclePoliciesOnBucket(TEST_BUCKET_NAME);
        lifecycleConfigurationInitializer.updateLifecyclePoliciesOnBucket(TEST_BUCKET_2_NAME);
        lifecycleConfigurationInitializer.updateLifecyclePoliciesOnBucket(TEST_BUCKET_2_NAME);

        String ruleId = TEST_POLICY + "-14";
        assertHasSingleLifecyclePolicyWithRule(TEST_BUCKET_NAME, ruleId);
        assertHasSingleLifecyclePolicyWithRule(TEST_BUCKET_2_NAME, ruleId);
    }

    private void assertHasSingleLifecyclePolicyWithRule(String testBucketName, String ruleId) {
        assertThat(lifecycleConfig(testBucketName).rules())
                .filteredOn(rule -> rule.id().equals(ruleId))
                .hasSize(1)
                .first()
                .satisfies(rule -> {
                    assertThat(rule.id()).isEqualTo(ruleId);
                    assertThat(rule.expiration().days()).isEqualTo(14);
                    assertThat(rule.filter().prefix()).isNull();
                    assertThat(rule.filter().tag().key()).isEqualTo(TEST_POLICY);
                    assertThat(rule.filter().tag().value()).isEqualTo("14");
                    assertThat(rule.status()).isEqualTo(ExpirationStatus.ENABLED);
                });
    }

    @Test
    void updateLifecyclePoliciesOnBucket_shouldCreateOnePolicyPerExpirationDays() {
        S3LifecycleConfigurationFactory lifecycleConfigurationFactory14days =
                new S3LifecycleConfigurationFactory(TEST_POLICY, 14);
        S3LifecycleConfigurationInitializer lifecycleConfigurationInitializer14days =
                new S3LifecycleConfigurationInitializer(lifecycleConfigurationFactory14days, s3Client);
        S3LifecycleConfigurationFactory lifecycleConfigurationFactory28days =
                new S3LifecycleConfigurationFactory(TEST_POLICY, 28);
        S3LifecycleConfigurationInitializer lifecycleConfigurationInitializer28days =
                new S3LifecycleConfigurationInitializer(lifecycleConfigurationFactory28days, s3Client);

        lifecycleConfigurationInitializer14days.updateLifecyclePoliciesOnBucket(TEST_BUCKET_NAME);
        lifecycleConfigurationInitializer14days.updateLifecyclePoliciesOnBucket(TEST_BUCKET_NAME);
        lifecycleConfigurationInitializer28days.updateLifecyclePoliciesOnBucket(TEST_BUCKET_NAME);
        lifecycleConfigurationInitializer28days.updateLifecyclePoliciesOnBucket(TEST_BUCKET_NAME);

        assertThat(lifecycleConfig(TEST_BUCKET_NAME).rules()
                .stream().map(LifecycleRule::id).filter(id -> id.startsWith(TEST_POLICY)))
                .containsExactly(TEST_POLICY + "-14", TEST_POLICY + "-28");
    }

    private GetBucketLifecycleConfigurationResponse lifecycleConfig(String bucketName) {
        return s3Client.getBucketLifecycleConfiguration(GetBucketLifecycleConfigurationRequest.builder()
                .bucket(bucketName)
                .build());
    }
}
