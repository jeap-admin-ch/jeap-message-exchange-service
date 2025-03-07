package ch.admin.bit.jeap.messageexchange.objectstorage;

import ch.admin.bit.jeap.messageexchange.domain.housekeeping.HousekeepingProperties;
import ch.admin.bit.jeap.messageexchange.objectstorage.lifecycle.S3LifecycleConfigurationFactory;
import ch.admin.bit.jeap.messageexchange.objectstorage.lifecycle.S3LifecycleConfigurationInitializer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.signer.AwsS3V4Signer;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.http.urlconnection.ProxyConfiguration;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

import static org.springframework.util.StringUtils.hasText;

@AutoConfiguration
@RequiredArgsConstructor
@Slf4j
@EnableConfigurationProperties
public class ObjectStorageConfiguration {

    private static final String MESSAGE_EXCHANGE_LIFECYCLE_POLICY = "MessageExchangeLifecyclePolicy";

    private final HousekeepingProperties housekeepingProperties;

    @Bean
    @ConfigurationProperties("jeap.messageexchange.objectstorage.connection")
    public S3ObjectStorageConnectionProperties s3ObjectStorageConnectionProperties() {
        return new S3ObjectStorageConnectionProperties();
    }

    @Bean
    @ConfigurationProperties("jeap.messageexchange.objectstorage.connection-fallback")
    @ConditionalOnProperty("jeap.messageexchange.objectstorage.connection-fallback.bucket-name-internal")
    public S3ObjectStorageConnectionProperties s3ObjectStorageFallbackConnectionProperties() {
        return new S3ObjectStorageConnectionProperties();
    }

    @Bean
    public ObjectStoreAdapter objectStoreAdapter(S3ObjectStorageRepository objectStorageRepository,
                                                 S3ObjectStorageConnectionProperties s3ObjectStorageConnectionProperties,
                                                 Optional<S3ObjectStorageFallbackRepository> objectStorageFallbackRepository,
                                                 Optional<S3ObjectStorageConnectionProperties> s3ObjectStorageFallbackConnectionProperties) {
        return new ObjectStoreAdapter(objectStorageRepository,
                s3ObjectStorageConnectionProperties.getBucketNameInternal(),
                s3ObjectStorageConnectionProperties.getBucketNamePartner(),
                objectStorageFallbackRepository.orElse(null),
                s3ObjectStorageFallbackConnectionProperties.map(S3ObjectStorageConnectionProperties::getBucketNameInternal).orElse(null),
                s3ObjectStorageFallbackConnectionProperties.map(S3ObjectStorageConnectionProperties::getBucketNamePartner).orElse(null));
    }

    @Bean
    public S3ObjectStorageRepository objectStorageRepository(S3Client s3Client, S3LifecycleConfigurationInitializer lifecycleConfigurationInitializer, S3ObjectStorageConnectionProperties s3ObjectStorageConnectionProperties) {
        return new S3ObjectStorageRepository(s3Client, s3ObjectStorageConnectionProperties, housekeepingProperties, lifecycleConfigurationInitializer);
    }

    @Bean
    @ConditionalOnBean(name = "s3ObjectStorageFallbackConnectionProperties")
    public S3ObjectStorageFallbackRepository objectStorageFallbackRepository(S3Client s3ClientFallbackRepository, S3ObjectStorageConnectionProperties s3ObjectStorageFallbackConnectionProperties) {
        return new S3ObjectStorageFallbackRepository(s3ClientFallbackRepository, s3ObjectStorageFallbackConnectionProperties);
    }

    @Bean
    public S3LifecycleConfigurationFactory s3LifecycleConfigurationFactory() {
        // Storage expiration is always one day after DB expiration to make sure no inconsistencies occur when
        // messages are deleted from the storage but not yet from the DB
        int s3ExpirationDays = housekeepingProperties.getExpirationDays() + 1;
        return new S3LifecycleConfigurationFactory(MESSAGE_EXCHANGE_LIFECYCLE_POLICY, s3ExpirationDays);
    }

    @Bean
    public S3LifecycleConfigurationInitializer s3LifecycleConfigurationInitializer(S3Client s3Client,
                                                                                   S3LifecycleConfigurationFactory lifecycleConfigurationFactory) {
        return new S3LifecycleConfigurationInitializer(lifecycleConfigurationFactory, s3Client);
    }

    @Bean
    @ConditionalOnMissingBean(AwsCredentialsProvider.class)
    DefaultCredentialsProvider awsCredentialsProvider() {
        return DefaultCredentialsProvider.create();
    }

    @Bean
    public S3Client s3Client(S3ObjectStorageConnectionProperties s3ObjectStorageConnectionProperties, AwsCredentialsProvider awsCredentialsProvider) {
        return createS3Client(s3ObjectStorageConnectionProperties, awsCredentialsProvider);
    }

    @Bean
    @ConditionalOnBean(name = "s3ObjectStorageFallbackConnectionProperties")
    public S3Client s3ClientFallbackRepository(S3ObjectStorageConnectionProperties s3ObjectStorageFallbackConnectionProperties, AwsCredentialsProvider awsCredentialsProvider) {
        return createS3Client(s3ObjectStorageFallbackConnectionProperties, awsCredentialsProvider);
    }

    private S3Client createS3Client(S3ObjectStorageConnectionProperties connectionProperties, AwsCredentialsProvider awsCredentialsProvider) {
        log.info("Initializing s3Client with connection properties {}", connectionProperties);

        ClientOverrideConfiguration.Builder overrideConfig = ClientOverrideConfiguration.builder();
        overrideConfig.advancedOptions(Map.of(SdkAdvancedClientOption.SIGNER, AwsS3V4Signer.create()));

        S3Configuration serviceConfiguration = S3Configuration.builder()
                .checksumValidationEnabled(false)
                .chunkedEncodingEnabled(true)
                .build();

        S3ClientBuilder s3ClientBuilder = S3Client.builder()
                .region(connectionProperties.getRegion())
                .forcePathStyle(true)
                .httpClientBuilder(UrlConnectionHttpClient.builder()
                        .proxyConfiguration(ProxyConfiguration.builder() // Configure proxy to work around the issue https://github.com/aws/aws-sdk-java-v2/issues/4728 which is coming with the aws sdk update
                                .useSystemPropertyValues(false)
                                .useEnvironmentVariablesValues(false)
                                .build())
                        .connectionTimeout(connectionProperties.getS3Timeout())
                        .socketTimeout(connectionProperties.getS3Timeout()))
                .credentialsProvider(createS3CredentialsProvider(awsCredentialsProvider, connectionProperties))
                .overrideConfiguration(overrideConfig.build())
                .serviceConfiguration(serviceConfiguration);

        String accessUrl = connectionProperties.getAccessUrl();
        if (hasText(accessUrl)) {
            log.info("Overriding endpoint in S3Client to {}", accessUrl);
            s3ClientBuilder = s3ClientBuilder.endpointOverride(retrieveEndpointURI(accessUrl));
        }
        return s3ClientBuilder.build();
    }

    private URI retrieveEndpointURI(String accessUrl) {
        if (accessUrl.startsWith("http:") || accessUrl.startsWith("https:")) {
            return URI.create(accessUrl);
        }
        return URI.create("https://" + accessUrl);
    }

    private AwsCredentialsProvider createS3CredentialsProvider(AwsCredentialsProvider awsCredentialsProvider, S3ObjectStorageConnectionProperties s3Props) {
        if (s3Props.getAccessKey() != null && s3Props.getSecretKey() != null) {
            log.info("Creating AwsCredentialsProvider using configured accessKey and secretKey");
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(s3Props.getAccessKey(), s3Props.getSecretKey()));
        }
        log.info("Using AwsCredentialsProvider bean for S3 access");
        return awsCredentialsProvider;
    }

}
