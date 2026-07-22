package ch.admin.bit.jeap.messageexchange.objectstorage;

import ch.admin.bit.jeap.messageexchange.domain.housekeeping.HousekeepingProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.ProxyConfiguration;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.net.URI;

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

    private static final String RUSTFS_IMAGE = "rustfs/rustfs:1.0.0-beta.10";
    private static final int RUSTFS_PORT = 9000;
    private static final String RUSTFS_ACCESS_KEY = "dev";
    private static final String RUSTFS_SECRET_KEY = "devsecret";
    private static final String RUSTFS_REGION = "aws-global";

    protected static S3Client s3Client;

    public static GenericContainer<?> rustFs;

    static {
        rustFs = createRustFsContainer();
        rustFs.start();
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
    private static GenericContainer<?> createRustFsContainer() {
        return new GenericContainer<>(DockerImageName.parse(RUSTFS_IMAGE)
                .asCompatibleSubstituteFor("rustfs/rustfs"))
                .withExposedPorts(RUSTFS_PORT)
                .withEnv("RUSTFS_ACCESS_KEY", RUSTFS_ACCESS_KEY)
                .withEnv("RUSTFS_SECRET_KEY", RUSTFS_SECRET_KEY)
                .withCommand("/data");
    }

    private static URI getEndpoint() {
        return URI.create("http://" + rustFs.getHost() + ":" + rustFs.getMappedPort(RUSTFS_PORT));
    }

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("jeap.messageexchange.objectstorage.connection.region", () -> RUSTFS_REGION);
        registry.add("jeap.messageexchange.objectstorage.connection.access-key", () -> RUSTFS_ACCESS_KEY);
        registry.add("jeap.messageexchange.objectstorage.connection.secret-key", () -> RUSTFS_SECRET_KEY);
        registry.add("jeap.messageexchange.objectstorage.connection.access-url", () -> getEndpoint().toString());

        s3Client = S3Client.builder()
                .region(Region.of(RUSTFS_REGION))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(RUSTFS_ACCESS_KEY, RUSTFS_SECRET_KEY)))
                .endpointOverride(getEndpoint())
                .forcePathStyle(true)
                .httpClientBuilder(UrlConnectionHttpClient.builder()
                        .proxyConfiguration(ProxyConfiguration.builder()
                                .useSystemPropertyValues(false)
                                .useEnvironmentVariablesValues(false)
                                .build()))
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
