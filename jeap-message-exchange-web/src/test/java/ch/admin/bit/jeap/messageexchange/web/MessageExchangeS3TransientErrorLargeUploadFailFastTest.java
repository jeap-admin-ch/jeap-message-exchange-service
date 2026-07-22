package ch.admin.bit.jeap.messageexchange.web;

import ch.admin.bit.jeap.messaging.kafka.test.KafkaIntegrationTestBase;
import ch.admin.bit.jeap.security.resource.semanticAuthentication.SemanticApplicationRole;
import ch.admin.bit.jeap.security.resource.token.JeapAuthenticationContext;
import ch.admin.bit.jeap.security.test.jws.JwsBuilder;
import ch.admin.bit.jeap.security.test.jws.JwsBuilderFactory;
import ch.admin.bit.jeap.security.test.resource.configuration.JeapOAuth2IntegrationTestResourceConfiguration;
import com.github.tomakehurst.wiremock.WireMockServer;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.specification.RequestSpecification;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.UUID;

import static ch.admin.bit.jeap.messageexchange.web.api.HeaderNames.HEADER_BP_ID;
import static ch.admin.bit.jeap.messageexchange.web.api.HeaderNames.HEADER_MESSAGE_TYPE;
import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Companion to {@link MessageExchangeS3TransientErrorRetryTest} for message bodies above the memory buffer
 * threshold: those are streamed to S3 without buffering, so a transient S3 error cannot be retried by the SDK
 * (the one-shot request stream cannot be re-read). Expected behavior: the upload fails fast on the first
 * attempt with the actual S3 error - no retry, and in particular no misleading
 * "Content input stream does not support mark/reset" IllegalStateException.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT,
        properties = {
                "server.port=8316",
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
                // force the streaming (non-buffered) upload path for the test message body
                "jeap.messageexchange.objectstorage.connection.upload-retry-memory-buffer-threshold=16B",
                // Avoid S3 lifecycle configuration calls at startup, they are irrelevant for this scenario
                "jeap.messageexchange.housekeeping.enabled=false"
        })
@ContextConfiguration(classes = {MessageExchangeApplication.class, JeapOAuth2IntegrationTestResourceConfiguration.class})
@ExtendWith(OutputCaptureExtension.class)
class MessageExchangeS3TransientErrorLargeUploadFailFastTest extends KafkaIntegrationTestBase {

    private static final WireMockServer S3_MOCK = new WireMockServer(wireMockConfig().dynamicPort());

    static PostgreSQLContainer postgres = new PostgreSQLContainer(DockerImageName.parse("postgres:17-alpine").asCompatibleSubstituteFor("postgres"));

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

        // Every PUT of a message content fails with a transient S3 error - only a single attempt is expected
        S3_MOCK.stubFor(put(urlPathMatching("/test-bucket-(internal|partner)/.+"))
                .willReturn(aResponse()
                        .withStatus(503)
                        .withHeader("Content-Type", "application/xml")
                        .withBody("""
                                <?xml version="1.0" encoding="UTF-8"?>
                                <Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message></Error>""")));
    }

    @BeforeEach
    void prepareRestAssured() {
        request = new RequestSpecBuilder().setPort(serverPort).build();
    }

    @Test
    void putLargeMessage_whenS3PutFailsWithTransientError_thenFailsFastWithActualCause(CapturedOutput output) {
        UUID messageId = UUID.randomUUID();

        given()
                .spec(request)
                .contentType(MediaType.APPLICATION_XML_VALUE)
                .auth().oauth2(createAuthTokenForUserRoles(B2B_MESSAGE_OUT_WRITE))
                .header(HEADER_BP_ID, "myBpID")
                .header(HEADER_MESSAGE_TYPE, "myMessageType")
                .param("topicName", "topicName")
                .body("<content>" + "x".repeat(100) + "</content>") // above the 16B buffer threshold
                .when()
                .put("/api/internal/v3/messages/" + messageId)
                .then()
                .statusCode(HttpStatus.INTERNAL_SERVER_ERROR.value());

        // fail fast: exactly one attempt, no retry that cannot re-read the request stream
        S3_MOCK.verify(1, putRequestedFor(urlPathMatching("/test-bucket-(internal|partner)/.*" + messageId)));
        // the actual S3 error surfaces instead of the misleading mark/reset IllegalStateException
        assertThat(output).doesNotContain("Content input stream does not support mark/reset");
        assertThat(output).contains("Status Code: 503");
    }

    private String createAuthTokenForUserRoles(SemanticApplicationRole... userroles) {
        return jwsBuilderFactory.createValidForFixedLongPeriodBuilder(UUID.randomUUID().toString(), JeapAuthenticationContext.SYS)
                .withUserRoles(userroles)
                .build()
                .serialize();
    }
}
