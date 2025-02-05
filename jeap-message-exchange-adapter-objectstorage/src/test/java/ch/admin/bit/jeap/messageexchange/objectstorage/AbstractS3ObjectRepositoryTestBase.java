package ch.admin.bit.jeap.messageexchange.objectstorage;

import ch.admin.bit.jeap.messageexchange.domain.housekeeping.HousekeepingProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

@SpringBootTest(
        classes = ObjectStorageConfiguration.class,
        properties = {
                "jeap.messageexchange.objectstorage.connection.bucket-name-partner=test-bucket",
                "jeap.messageexchange.objectstorage.connection.bucket-name-internal=test-bucket"
        })
@Import(AbstractS3ObjectRepositoryTestBase.TestConfig.class)
public class AbstractS3ObjectRepositoryTestBase {

    protected static final String TEST_BUCKET_NAME = "test-bucket";
    protected static final String TEST_BUCKET_2_NAME = "test-bucket-2";

    protected static S3Client s3Client;

    public static LocalStackContainer localStack;

    static {
        localStack = createLocalStackContainer();
        localStack.start();
    }

    static class TestConfig {

        @Bean
        HousekeepingProperties housekeepingProperties() {
            HousekeepingProperties housekeepingProperties = new HousekeepingProperties();
            housekeepingProperties.setExpirationDays(14);
            return housekeepingProperties;
        }
    }

    @SuppressWarnings("resource")
    private static LocalStackContainer createLocalStackContainer() {
        return new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.1")
                .asCompatibleSubstituteFor("localstack/localstack"))
                .withEnv("DISABLE_EVENTS", "1") // Disable localstack features that require an internet connection
                .withEnv("SKIP_INFRA_DOWNLOADS", "1")
                .withEnv("SKIP_SSL_CERT_DOWNLOAD", "1");
    }

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("jeap.messageexchange.objectstorage.connection.region", () -> localStack.getRegion());
        registry.add("jeap.messageexchange.objectstorage.connection.access-key", () -> localStack.getAccessKey());
        registry.add("jeap.messageexchange.objectstorage.connection.secret-key", () -> localStack.getSecretKey());
        registry.add("jeap.messageexchange.objectstorage.connection.access-url", () -> localStack.getEndpointOverride(LocalStackContainer.Service.S3).toString());

        s3Client = S3Client.builder()
                .region(Region.of(localStack.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(localStack.getAccessKey(), localStack.getSecretKey())))
                .endpointOverride(localStack.getEndpointOverride(LocalStackContainer.Service.S3))
                .build();
        setupStorage();
    }

    private static void setupStorage() {
        createBucket(TEST_BUCKET_NAME);
        createBucket(TEST_BUCKET_2_NAME);
    }

    private static void createBucket(String bucketName) {
        CreateBucketRequest request = CreateBucketRequest.builder()
                .bucket(bucketName)
                .objectLockEnabledForBucket(true)
                .build();
        s3Client.createBucket(request);
    }
}
