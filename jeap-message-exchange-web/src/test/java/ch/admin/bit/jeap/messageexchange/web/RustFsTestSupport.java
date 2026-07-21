package ch.admin.bit.jeap.messageexchange.web;

import lombok.experimental.UtilityClass;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.ProxyConfiguration;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;

@UtilityClass
public final class RustFsTestSupport {

    public static final String RUSTFS_IMAGE = "rustfs/rustfs:1.0.0-beta.10";
    public static final int RUSTFS_PORT = 9000;
    public static final String RUSTFS_ACCESS_KEY = "dev";
    public static final String RUSTFS_SECRET_KEY = "devsecret";
    public static final String RUSTFS_REGION = "aws-global";

    /**
     * Creates a RustFS container providing an S3-compatible object storage for tests.
     */
    public static GenericContainer<?> createRustFsContainer() {
        return new GenericContainer<>(DockerImageName.parse(RUSTFS_IMAGE))
                .withExposedPorts(RUSTFS_PORT)
                .withEnv("RUSTFS_ACCESS_KEY", RUSTFS_ACCESS_KEY)
                .withEnv("RUSTFS_SECRET_KEY", RUSTFS_SECRET_KEY)
                .withCommand("/data");
    }

    public static URI getEndpoint(GenericContainer<?> rustFs) {
        return URI.create("http://" + rustFs.getHost() + ":" + rustFs.getMappedPort(RUSTFS_PORT));
    }

    /**
     * Creates an S3Client for the given RustFS container with proxy disabled.
     * This prevents the AWS SDK from using system/environment proxy settings
     * to connect to the RustFS container.
     */
    public static S3Client createS3Client(GenericContainer<?> rustFs) {
        return S3Client.builder()
                .endpointOverride(getEndpoint(rustFs))
                .region(Region.of(RUSTFS_REGION))
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(RUSTFS_ACCESS_KEY, RUSTFS_SECRET_KEY)))
                .httpClientBuilder(UrlConnectionHttpClient.builder()
                        .proxyConfiguration(ProxyConfiguration.builder()
                                .useSystemPropertyValues(false)
                                .useEnvironmentVariablesValues(false)
                                .build()))
                .build();
    }
}
