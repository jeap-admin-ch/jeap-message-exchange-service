package ch.admin.bit.jeap.messageexchange.web;

import ch.admin.bit.jeap.messageexchange.objectstorage.S3ObjectStorageFallbackRepository;
import ch.admin.bit.jeap.messageexchange.objectstorage.S3ObjectStorageRepository;
import ch.admin.bit.jeap.messaging.kafka.test.KafkaIntegrationTestBase;
import ch.admin.bit.jeap.security.resource.semanticAuthentication.SemanticApplicationRole;
import ch.admin.bit.jeap.security.resource.token.JeapAuthenticationContext;
import ch.admin.bit.jeap.security.test.jws.JwsBuilder;
import ch.admin.bit.jeap.security.test.jws.JwsBuilderFactory;
import ch.admin.bit.jeap.security.test.resource.configuration.JeapOAuth2IntegrationTestResourceConfiguration;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static ch.admin.bit.jeap.messageexchange.web.LocalStackTestSupport.createLocalStackContainer;
import static ch.admin.bit.jeap.messageexchange.web.LocalStackTestSupport.createS3Client;
import static ch.admin.bit.jeap.messageexchange.web.api.HeaderNames.HEADER_BP_ID;
import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT,
        properties = {
                "server.port=8535",
                "jeap.messageexchange.kafka.topic.message-received=message-received",
                "jeap.messaging.kafka.error-topic-name=error",
                "jeap.messaging.kafka.system-name=test",
                "jeap.messaging.kafka.service-name=test",
                "spring.application.name=junit",
                "jeap.security.oauth2.resourceserver.authorization-server.issuer=" + JwsBuilder.DEFAULT_ISSUER,
                "jeap.security.oauth2.resourceserver.authorization-server.jwk-set-uri=http://localhost:${server.port}/.well-known/jwks.json",
                "jeap.messageexchange.objectstorage.connection.bucket-name-partner=test-bucket-partner",
                "jeap.messageexchange.objectstorage.connection.bucket-name-internal=test-bucket-internal",
                "jeap.messageexchange.objectstorage.connection-fallback.bucket-name-partner=test-bucket-partner-fallback",
                "jeap.messageexchange.objectstorage.connection-fallback.bucket-name-internal=test-bucket-internal-fallback",
                "jeap.messageexchange.api.max-request-body-size-in-bytes=100"
        })
@ContextConfiguration(classes = {MessageExchangeApplication.class, JeapOAuth2IntegrationTestResourceConfiguration.class})
@Testcontainers
@AutoConfigureObservability
@SuppressWarnings("resource")
class MessageExchangeFallbackBucketInteractionTest extends KafkaIntegrationTestBase {

    @Container
    public static LocalStackContainer localStack = createLocalStackContainer();

    @Container
    public static LocalStackContainer localStackFallback = createLocalStackContainer();

    @LocalServerPort
    int serverPort;

    @MockitoSpyBean
    private S3ObjectStorageRepository objectStorageRepository;

    @MockitoSpyBean
    private S3ObjectStorageFallbackRepository objectStorageFallbackRepository;

    @Autowired
    private JwsBuilderFactory jwsBuilderFactory;

    @Autowired
    private ResourceLoader resourceLoader;

    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine");

    @BeforeAll
    static void startContainers() {
        postgres.start();
    }

    @AfterAll
    static void stopContainers() {
        postgres.stop();
    }

    private RequestSpecification request;

    private static final String MESSAGE_1_ID = UUID.randomUUID().toString();
    private static final String MESSAGE_2_ID = UUID.randomUUID().toString();

    private static final SemanticApplicationRole B2B_MESSAGE_IN_READ = SemanticApplicationRole.builder()
            .system("junit")
            .resource("b2bmessagein")
            .operation("read")
            .build();


    @DynamicPropertySource
    static void dynamicProperties(DynamicPropertyRegistry registry) {
        registry.add("jeap.messageexchange.objectstorage.connection.region", () -> localStack.getRegion());
        registry.add("jeap.messageexchange.objectstorage.connection.access-key", () -> localStack.getAccessKey());
        registry.add("jeap.messageexchange.objectstorage.connection.secret-key", () -> localStack.getSecretKey());
        registry.add("jeap.messageexchange.objectstorage.connection.accessUrl", () -> localStack.getEndpointOverride(LocalStackContainer.Service.S3));
        registry.add("jeap.messageexchange.objectstorage.connection-fallback.region", () -> localStackFallback.getRegion());
        registry.add("jeap.messageexchange.objectstorage.connection-fallback.access-key", () -> localStackFallback.getAccessKey());
        registry.add("jeap.messageexchange.objectstorage.connection-fallback.secret-key", () -> localStackFallback.getSecretKey());
        registry.add("jeap.messageexchange.objectstorage.connection-fallback.accessUrl", () -> localStackFallback.getEndpointOverride(LocalStackContainer.Service.S3));
        registry.add("spring.datasource.url", () -> postgres.getJdbcUrl());
        registry.add("spring.datasource.username", () -> postgres.getUsername());
        registry.add("spring.datasource.password", () -> postgres.getPassword());
    }

    @BeforeAll
    static void createBucket() {
        S3Client s3Client = createS3Client(localStack);
        CreateBucketRequest createPartnerBucketRequest = CreateBucketRequest.builder()
                .bucket("test-bucket-partner")
                .build();
        s3Client.createBucket(createPartnerBucketRequest);

        CreateBucketRequest createInternalBucketRequest = CreateBucketRequest.builder()
                .bucket("test-bucket-internal")
                .build();
        s3Client.createBucket(createInternalBucketRequest);

        s3Client.putObject(PutObjectRequest.builder().bucket("test-bucket-partner").key(MESSAGE_2_ID).build(), Path.of("src/test/resources/input2.xml"));
    }

    @BeforeAll
    static void createFallbackBucket() {
        S3Client s3Client = createS3Client(localStackFallback);
        CreateBucketRequest createPartnerBucketRequest = CreateBucketRequest.builder()
                .bucket("test-bucket-partner-fallback")
                .build();
        s3Client.createBucket(createPartnerBucketRequest);

        CreateBucketRequest createInternalBucketRequest = CreateBucketRequest.builder()
                .bucket("test-bucket-internal-fallback")
                .build();
        s3Client.createBucket(createInternalBucketRequest);

        s3Client.putObject(PutObjectRequest.builder().bucket("test-bucket-partner-fallback").key(MESSAGE_1_ID).build(), Path.of("src/test/resources/input.xml"));
    }

    @BeforeEach
    void prepareRestAssured() {
        request = new RequestSpecBuilder().setPort(serverPort).build();
    }

    @Test
    void getMessagesFromPartner_returnMessageFromFallbackBucket() throws Exception {
        String bpId = "myBpID";

        Response response = given()
                .spec(request)
                .contentType(ContentType.XML)
                .auth().oauth2(createAuthTokenForBpRoles(bpId, B2B_MESSAGE_IN_READ))
                .header(HEADER_BP_ID, bpId)
                .when()
                .get("/api/internal/v2/messages/" + MESSAGE_1_ID);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK.value());
        assertThat(response.getBody().asString()).isEqualTo(getXmlResource("input.xml"));

        verify(objectStorageRepository, times(1)).getObject("test-bucket-partner", MESSAGE_1_ID);
        verify(objectStorageFallbackRepository, times(1)).getObject("test-bucket-partner-fallback", MESSAGE_1_ID);
    }

    @Test
    void getMessagesFromPartner_returnMessageFromBucket() throws Exception {
        String bpId = "myBpID";

        Response response = given()
                .spec(request)
                .contentType(ContentType.XML)
                .auth().oauth2(createAuthTokenForBpRoles(bpId, B2B_MESSAGE_IN_READ))
                .header(HEADER_BP_ID, bpId)
                .when()
                .get("/api/internal/v2/messages/" + MESSAGE_2_ID);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK.value());
        assertThat(response.getBody().asString()).isEqualTo(getXmlResource("input2.xml"));

        verify(objectStorageRepository, times(1)).getObject("test-bucket-partner", MESSAGE_2_ID);
        verify(objectStorageFallbackRepository, never()).getObject(anyString(), anyString());
    }

    private String createAuthTokenForBpRoles(String bpId, SemanticApplicationRole... roles) {
        return jwsBuilderFactory.createValidForFixedLongPeriodBuilder(UUID.randomUUID().toString(), JeapAuthenticationContext.SYS)
                .withBusinessPartnerRoles(bpId, roles)
                .build()
                .serialize();
    }

    private String getXmlResource(String filename) throws IOException {
        Resource resource = resourceLoader.getResource("classpath:" + filename);
        return new String(Files.readAllBytes(resource.getFile().toPath()));
    }

}
