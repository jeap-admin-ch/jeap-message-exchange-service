package ch.admin.bit.jeap.messageexchange.web;

import ch.admin.bit.jeap.messaging.kafka.test.KafkaIntegrationTestBase;
import ch.admin.bit.jeap.security.resource.semanticAuthentication.SemanticApplicationRole;
import ch.admin.bit.jeap.security.resource.token.JeapAuthenticationContext;
import ch.admin.bit.jeap.security.test.jws.JwsBuilder;
import ch.admin.bit.jeap.security.test.jws.JwsBuilderFactory;
import ch.admin.bit.jeap.security.test.resource.configuration.JeapOAuth2IntegrationTestResourceConfiguration;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.util.Map;
import java.util.UUID;

import static ch.admin.bit.jeap.messageexchange.web.RustFsTestSupport.createRustFsContainer;
import static ch.admin.bit.jeap.messageexchange.web.RustFsTestSupport.createS3Client;
import static ch.admin.bit.jeap.messageexchange.web.api.HeaderNames.HEADER_BP_ID;
import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that error responses rendered by Spring Boot's error controller stay generic. The JWK set URI points to a
 * port nothing listens on, so decoding the bearer token fails with an exception whose message names the JWK set URI.
 * Spring Security does not answer such a failure with 401 but passes it on, and it ends up as a 500 response of the
 * error controller - which must contain neither that message nor the request path.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT,
        properties = {
                "server.port=8317",
                "jeap.messageexchange.kafka.topic.message-received=message-received",
                "jeap.messaging.kafka.error-topic-name=error",
                "jeap.messaging.kafka.system-name=test",
                "jeap.messaging.kafka.service-name=test",
                "spring.application.name=junit",
                "jeap.security.oauth2.resourceserver.authorization-server.issuer=" + JwsBuilder.DEFAULT_ISSUER,
                "jeap.security.oauth2.resourceserver.authorization-server.jwk-set-uri=" + MessageExchangeErrorResponseTest.UNREACHABLE_JWK_SET_URI,
                "jeap.messageexchange.objectstorage.connection.bucket-name-partner=test-bucket-partner",
                "jeap.messageexchange.objectstorage.connection.bucket-name-internal=test-bucket-internal",
                "jeap.messageexchange.malwarescan.enabled=false"
        })
@ContextConfiguration(classes = {MessageExchangeApplication.class, JeapOAuth2IntegrationTestResourceConfiguration.class})
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SuppressWarnings("resource")
class MessageExchangeErrorResponseTest extends KafkaIntegrationTestBase {

    static final String UNREACHABLE_JWK_SET_URI = "http://localhost:1/unreachable-jwks";
    private static final String PARTNER_MESSAGES_PATH = "/api/partner/v4/messages";

    @Container
    public static GenericContainer<?> rustFs = createRustFsContainer();

    static PostgreSQLContainer postgres = new PostgreSQLContainer(DockerImageName.parse("postgres:17-alpine").asCompatibleSubstituteFor("postgres"));

    @LocalServerPort
    int serverPort;

    @Autowired
    private JwsBuilderFactory jwsBuilderFactory;

    private RequestSpecification request;

    private static final SemanticApplicationRole B2B_MESSAGE_OUT_READ = SemanticApplicationRole.builder()
            .system("junit")
            .resource("b2bmessageout")
            .operation("read")
            .build();

    @BeforeAll
    static void startContainers() {
        postgres.start();
        S3Client s3Client = createS3Client(rustFs);
        s3Client.createBucket(CreateBucketRequest.builder().bucket("test-bucket-partner").build());
        s3Client.createBucket(CreateBucketRequest.builder().bucket("test-bucket-internal").build());
    }

    @AfterAll
    static void stopContainers() {
        postgres.stop();
    }

    @DynamicPropertySource
    static void dynamicProperties(DynamicPropertyRegistry registry) {
        registry.add("jeap.messageexchange.objectstorage.connection.region", () -> RustFsTestSupport.RUSTFS_REGION);
        registry.add("jeap.messageexchange.objectstorage.connection.access-key", () -> RustFsTestSupport.RUSTFS_ACCESS_KEY);
        registry.add("jeap.messageexchange.objectstorage.connection.secret-key", () -> RustFsTestSupport.RUSTFS_SECRET_KEY);
        registry.add("jeap.messageexchange.objectstorage.connection.accessUrl", () -> RustFsTestSupport.getEndpoint(rustFs));
        registry.add("spring.datasource.url", () -> postgres.getJdbcUrl());
        registry.add("spring.datasource.username", () -> postgres.getUsername());
        registry.add("spring.datasource.password", () -> postgres.getPassword());
    }

    @BeforeEach
    void prepareRestAssured() {
        request = new RequestSpecBuilder().setPort(serverPort).build();
    }

    @Test
    void getMessages_whenJwkSetUnavailable_thenServerErrorResponseIsGeneric() {
        String bpId = "myBpID";

        Response response = given()
                .spec(request)
                .accept(MediaType.APPLICATION_JSON_VALUE)
                .auth().oauth2(createAuthTokenForBpRoles(bpId, B2B_MESSAGE_OUT_READ))
                .header(HEADER_BP_ID, bpId)
                .when()
                .get(PARTNER_MESSAGES_PATH);

        assertThat(response.statusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR.value());
        Map<String, Object> body = response.jsonPath().getMap("$");
        assertThat(body)
                .containsEntry("status", HttpStatus.INTERNAL_SERVER_ERROR.value())
                .containsEntry("error", HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase())
                .doesNotContainKeys("message", "path", "errors", "exception", "trace");
        assertThat(response.asString())
                .doesNotContain("unreachable-jwks")
                .doesNotContain("Jwt")
                .doesNotContain(PARTNER_MESSAGES_PATH);
    }

    private String createAuthTokenForBpRoles(String bpId, SemanticApplicationRole... roles) {
        return jwsBuilderFactory.createValidForFixedLongPeriodBuilder(UUID.randomUUID().toString(), JeapAuthenticationContext.SYS)
                .withBusinessPartnerRoles(bpId, roles)
                .build()
                .serialize();
    }
}
