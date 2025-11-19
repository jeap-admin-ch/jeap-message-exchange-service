package ch.admin.bit.jeap.messageexchange.web;

import lombok.experimental.UtilityClass;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.ProxyConfiguration;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

@UtilityClass
public final class LocalStackTestSupport {

    public final String LOCAL_STACK_IMAGE = "localstack/localstack:4.10.0";

    /**
     * Creates a LocalStackContainer with standard configuration while at the same time
     * disabling features that require an internet connection.
     */
    public static LocalStackContainer createLocalStackContainer() {
        return new LocalStackContainer(DockerImageName.parse(LOCAL_STACK_IMAGE)
                .asCompatibleSubstituteFor("localstack/localstack"))
                .withEnv("DISABLE_EVENTS", "1")
                .withEnv("SKIP_INFRA_DOWNLOADS", "1")
                .withEnv("SKIP_SSL_CERT_DOWNLOAD", "1");
    }

    /**
     * Creates an S3Client for the given LocalStackContainer with proxy disabled.
     * This prevents the AWS SDK from using system/environment proxy settings
     * to connect to the LocalStackContainer.
     */
    public static S3Client createS3Client(LocalStackContainer localStack) {
        return S3Client.builder()
                .endpointOverride(localStack.getEndpointOverride(LocalStackContainer.Service.S3))
                .region(Region.of(localStack.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(localStack.getAccessKey(), localStack.getSecretKey())))
                .httpClientBuilder(UrlConnectionHttpClient.builder()
                        .proxyConfiguration(ProxyConfiguration.builder()
                                .useSystemPropertyValues(false)
                                .useEnvironmentVariablesValues(false)
                                .build()))
                .build();
    }
}
