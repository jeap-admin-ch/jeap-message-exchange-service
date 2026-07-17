package ch.admin.bit.jeap.messageexchange.web;

import ch.admin.bit.jeap.messaging.kafka.test.KafkaIntegrationTestBase;
import ch.admin.bit.jeap.security.resource.semanticAuthentication.SemanticApplicationRole;
import ch.admin.bit.jeap.security.resource.token.JeapAuthenticationContext;
import ch.admin.bit.jeap.security.test.jws.JwsBuilder;
import ch.admin.bit.jeap.security.test.jws.JwsBuilderFactory;
import ch.admin.bit.jeap.security.test.resource.configuration.JeapOAuth2IntegrationTestResourceConfiguration;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.specification.RequestSpecification;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.Tag;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static ch.admin.bit.jeap.messageexchange.web.LocalStackTestSupport.createLocalStackContainer;
import static ch.admin.bit.jeap.messageexchange.web.LocalStackTestSupport.createS3Client;
import static ch.admin.bit.jeap.messageexchange.web.api.HeaderNames.HEADER_BP_ID;
import static ch.admin.bit.jeap.messageexchange.web.api.HeaderNames.HEADER_MESSAGE_TYPE;
import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that with the legacy tag compatibility disabled (all instances run &gt;= 11.0.0), uploaded objects
 * carry only the lifecycle tag. LEGACY-TAG-FALLBACK: remove with the contract story (JEAP-7252).
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT,
        properties = {
                "server.port=8313",
                "jeap.messageexchange.kafka.topic.message-received=message-received",
                "jeap.messaging.kafka.error-topic-name=error",
                "jeap.messaging.kafka.system-name=test",
                "jeap.messaging.kafka.service-name=test",
                "spring.application.name=junit",
                "jeap.security.oauth2.resourceserver.authorization-server.issuer=" + JwsBuilder.DEFAULT_ISSUER,
                "jeap.security.oauth2.resourceserver.authorization-server.jwk-set-uri=http://localhost:${server.port}/.well-known/jwks.json",
                "jeap.messageexchange.objectstorage.connection.bucket-name-partner=test-bucket-partner",
                "jeap.messageexchange.objectstorage.connection.bucket-name-internal=test-bucket-internal",
                "jeap.messageexchange.api.max-request-body-size-in-bytes=100",
                "jeap.messageexchange.malwarescan.enabled=false",
                "jeap.messageexchange.legacy-tag-compatibility.enabled=false"
        })
@ContextConfiguration(classes = {MessageExchangeApplication.class, JeapOAuth2IntegrationTestResourceConfiguration.class})
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SuppressWarnings("resource")
class MessageExchangeLegacyTagCompatibilityDisabledTest extends KafkaIntegrationTestBase {

    private static final String BP_ID = "myBpID";
    private static final String MESSAGE_TYPE = "myMessageType";
    private static final String BUCKET_NAME_PARTNER = "test-bucket-partner";
    private static final String BUCKET_NAME_INTERNAL = "test-bucket-internal";

    @Container
    public static LocalStackContainer localStack = createLocalStackContainer();

    @LocalServerPort
    int serverPort;

    @Autowired
    private JwsBuilderFactory jwsBuilderFactory;

    @Autowired
    private TestEventConsumer testEventConsumer;

    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

    private static S3Client s3Client;

    private RequestSpecification request;

    private static final SemanticApplicationRole B2B_MESSAGE_IN_WRITE = SemanticApplicationRole.builder()
            .system("junit")
            .resource("b2bmessagein")
            .operation("write")
            .build();

    @BeforeAll
    static void startContainers() {
        postgres.start();
    }

    @AfterAll
    static void stopContainers() {
        postgres.stop();
    }

    @DynamicPropertySource
    static void dynamicProperties(DynamicPropertyRegistry registry) {
        registry.add("jeap.messageexchange.objectstorage.connection.region", () -> localStack.getRegion());
        registry.add("jeap.messageexchange.objectstorage.connection.access-key", () -> localStack.getAccessKey());
        registry.add("jeap.messageexchange.objectstorage.connection.secret-key", () -> localStack.getSecretKey());
        registry.add("jeap.messageexchange.objectstorage.connection.accessUrl", () -> localStack.getEndpoint());
        registry.add("spring.datasource.url", () -> postgres.getJdbcUrl());
        registry.add("spring.datasource.username", () -> postgres.getUsername());
        registry.add("spring.datasource.password", () -> postgres.getPassword());
    }

    @BeforeAll
    static void createBucket() {
        s3Client = createS3Client(localStack);
        s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET_NAME_PARTNER).build());
        s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET_NAME_INTERNAL).build());
    }

    @BeforeEach
    void prepareRestAssured() {
        request = new RequestSpecBuilder().setPort(serverPort).build();
    }

    @Test
    void putMessageFromPartner_legacyTagCompatibilityDisabled_objectCarriesOnlyLifecycleTag() {
        UUID messageId = UUID.randomUUID();

        given()
                .spec(request)
                .contentType(MediaType.APPLICATION_XML_VALUE)
                .auth().oauth2(createAuthTokenForBpRoles(BP_ID, B2B_MESSAGE_IN_WRITE))
                .header(HEADER_BP_ID, BP_ID)
                .header(HEADER_MESSAGE_TYPE, MESSAGE_TYPE)
                .body("<valid/>")
                .when()
                .put("/api/partner/v4/messages/" + messageId)
                .then()
                .statusCode(HttpStatus.CREATED.value());

        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .until(() -> testEventConsumer.hasMessageWithIdempotenceId(messageId));

        List<Tag> tagSet = s3Client.getObjectTagging(GetObjectTaggingRequest.builder()
                .bucket(BUCKET_NAME_PARTNER)
                .key(messageId.toString())
                .build()).tagSet();
        assertThat(tagSet).hasSize(1);
        assertThat(tagSet.getFirst().key()).isEqualTo("MessageExchangeLifecyclePolicy");
    }

    private String createAuthTokenForBpRoles(String bpId, SemanticApplicationRole... roles) {
        return jwsBuilderFactory.createValidForFixedLongPeriodBuilder(UUID.randomUUID().toString(), JeapAuthenticationContext.SYS)
                .withBusinessPartnerRoles(bpId, roles)
                .build()
                .serialize();
    }
}
