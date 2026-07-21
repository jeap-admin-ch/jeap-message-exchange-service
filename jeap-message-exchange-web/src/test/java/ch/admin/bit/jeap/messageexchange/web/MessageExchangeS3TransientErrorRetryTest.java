package ch.admin.bit.jeap.messageexchange.web;

import ch.admin.bit.jeap.messaging.kafka.test.KafkaIntegrationTestBase;
import ch.admin.bit.jeap.security.resource.semanticAuthentication.SemanticApplicationRole;
import ch.admin.bit.jeap.security.resource.token.JeapAuthenticationContext;
import ch.admin.bit.jeap.security.test.jws.JwsBuilder;
import ch.admin.bit.jeap.security.test.jws.JwsBuilderFactory;
import ch.admin.bit.jeap.security.test.resource.configuration.JeapOAuth2IntegrationTestResourceConfiguration;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import io.restassured.builder.RequestSpecBuilder;
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
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.util.UUID;

import static ch.admin.bit.jeap.messageexchange.web.api.HeaderNames.HEADER_BP_ID;
import static ch.admin.bit.jeap.messageexchange.web.api.HeaderNames.HEADER_MESSAGE_TYPE;
import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static io.restassured.RestAssured.given;

/**
 * Reproduces the production error
 * <pre>
 * java.lang.IllegalStateException: Content input stream does not support mark/reset, and was already read once.
 * </pre>
 * The MES streams the servlet request input stream directly to S3 ({@code ControllerStreams.getRequestContent} /
 * {@code S3ObjectStorageRepository.putObject}). When the first PUT attempt fails with a retryable error (e.g. a
 * transient 503 from the object storage), the AWS SDK retries the request, but the servlet input stream cannot be
 * re-read. The retry then fails with the above IllegalStateException, the real cause is masked, and the client
 * receives a 500 instead of the message being stored by the successful retry.
 * <p>
 * This test simulates the object storage with WireMock: the first PUT of the message content fails with a
 * transient 503 (SlowDown), every subsequent attempt succeeds. Expected behavior: storing the message succeeds
 * (via SDK retry or an equivalent recovery), i.e. the API responds with 201 and the object storage has received
 * a second, complete PUT attempt.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT,
        properties = {
                "server.port=8315",
                "jeap.messageexchange.kafka.topic.message-received=message-received",
                "jeap.messaging.kafka.error-topic-name=error",
                "jeap.messaging.kafka.system-name=test",
                "jeap.messaging.kafka.service-name=test",
                "spring.application.name=junit",
                "jeap.security.oauth2.resourceserver.authorization-server.issuer=" + JwsBuilder.DEFAULT_ISSUER,
                "jeap.security.oauth2.resourceserver.authorization-server.jwk-set-uri=http://localhost:${server.port}/.well-known/jwks.json",
                "jeap.messageexchange.objectstorage.connection.bucket-name-partner=test-bucket-partner",
                "jeap.messageexchange.objectstorage.connection.bucket-name-internal=test-bucket-internal",
                "jeap.messageexchange.objectstorage.connection.region=aws-global",
                "jeap.messageexchange.objectstorage.connection.access-key=test",
                "jeap.messageexchange.objectstorage.connection.secret-key=test",
                // Avoid S3 lifecycle configuration calls at startup, they are irrelevant for this scenario
                "jeap.messageexchange.housekeeping.enabled=false"
        })
@ContextConfiguration(classes = {MessageExchangeApplication.class, JeapOAuth2IntegrationTestResourceConfiguration.class})
class MessageExchangeS3TransientErrorRetryTest extends KafkaIntegrationTestBase {

    private static final String TRANSIENT_ERROR_SCENARIO = "transient-s3-error";
    private static final String FIRST_ATTEMPT_FAILED = "first-attempt-failed";

    private static final WireMockServer S3_MOCK = new WireMockServer(wireMockConfig().dynamicPort());

    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

    @LocalServerPort
    int serverPort;

    @Autowired
    private JwsBuilderFactory jwsBuilderFactory;

    private RequestSpecification request;

    private static final SemanticApplicationRole B2B_MESSAGE_OUT_WRITE = SemanticApplicationRole.builder()
            .system("junit")
            .resource("b2bmessageout")
            .operation("write")
            .build();

    @BeforeAll
    static void startContainers() {
        postgres.start();
    }

    @AfterAll
    static void stopContainers() {
        postgres.stop();
        S3_MOCK.stop();
    }

    @DynamicPropertySource
    static void dynamicProperties(DynamicPropertyRegistry registry) {
        S3_MOCK.start();
        stubS3Api();
        registry.add("jeap.messageexchange.objectstorage.connection.access-url", S3_MOCK::baseUrl);
        registry.add("spring.datasource.url", () -> postgres.getJdbcUrl());
        registry.add("spring.datasource.username", () -> postgres.getUsername());
        registry.add("spring.datasource.password", () -> postgres.getPassword());
    }

    private static void stubS3Api() {
        // Bucket access check at application startup
        S3_MOCK.stubFor(head(urlPathMatching("/test-bucket-(internal|partner)"))
                .willReturn(aResponse().withStatus(200)));

        // First PUT of a message content fails with a transient, retryable S3 error
        S3_MOCK.stubFor(put(urlPathMatching("/test-bucket-(internal|partner)/.+"))
                .inScenario(TRANSIENT_ERROR_SCENARIO)
                .whenScenarioStateIs(Scenario.STARTED)
                .willSetStateTo(FIRST_ATTEMPT_FAILED)
                .willReturn(aResponse()
                        .withStatus(503)
                        .withHeader("Content-Type", "application/xml")
                        .withBody("""
                                <?xml version="1.0" encoding="UTF-8"?>
                                <Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message></Error>""")));

        // Any further PUT attempt succeeds
        S3_MOCK.stubFor(put(urlPathMatching("/test-bucket-(internal|partner)/.+"))
                .inScenario(TRANSIENT_ERROR_SCENARIO)
                .whenScenarioStateIs(FIRST_ATTEMPT_FAILED)
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("ETag", "\"d41d8cd98f00b204e9800998ecf8427e\"")));
    }

    @BeforeEach
    void prepareRestAssured() {
        request = new RequestSpecBuilder().setPort(serverPort).build();
    }

    @Test
    void putMessage_whenFirstS3PutAttemptFailsWithTransientError_thenMessageIsStoredOnRetry() {
        UUID messageId = UUID.randomUUID();

        given()
                .spec(request)
                .contentType(MediaType.APPLICATION_XML_VALUE)
                .auth().oauth2(createAuthTokenForUserRoles(B2B_MESSAGE_OUT_WRITE))
                .header(HEADER_BP_ID, "myBpID")
                .header(HEADER_MESSAGE_TYPE, "myMessageType")
                .param("topicName", "topicName")
                .body("<content>transient error test</content>")
                .when()
                .put("/api/internal/v3/messages/" + messageId)
                .then()
                .statusCode(HttpStatus.CREATED.value());

        // The first attempt received the 503, the retry must have delivered the content
        S3_MOCK.verify(2, putRequestedFor(urlPathMatching("/test-bucket-(internal|partner)/" + messageId)));
    }

    private String createAuthTokenForUserRoles(SemanticApplicationRole... userroles) {
        return jwsBuilderFactory.createValidForFixedLongPeriodBuilder(UUID.randomUUID().toString(), JeapAuthenticationContext.SYS)
                .withUserRoles(userroles)
                .build()
                .serialize();
    }
}
