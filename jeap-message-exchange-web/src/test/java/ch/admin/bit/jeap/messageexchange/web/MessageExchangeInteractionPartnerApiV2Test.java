package ch.admin.bit.jeap.messageexchange.web;

import ch.admin.bit.jeap.messageexchange.domain.Message;
import ch.admin.bit.jeap.messageexchange.domain.database.MessageRepository;
import ch.admin.bit.jeap.messageexchange.event.message.received.B2BMessageReceivedEvent;
import ch.admin.bit.jeap.messageexchange.event.message.received.MessageReference;
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
import org.awaitility.Awaitility;
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
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

import static ch.admin.bit.jeap.messageexchange.web.api.HeaderNames.HEADER_BP_ID;
import static ch.admin.bit.jeap.messageexchange.web.api.HeaderNames.HEADER_MESSAGE_TYPE;
import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT,
        properties = {
                "server.port=8304",
                "jeap.messageexchange.kafka.topic.message-received=message-received",
                "jeap.messaging.kafka.error-topic-name=error",
                "jeap.messaging.kafka.system-name=test",
                "jeap.messaging.kafka.service-name=test",
                "spring.application.name=junit",
                "jeap.security.oauth2.resourceserver.authorization-server.issuer=" + JwsBuilder.DEFAULT_ISSUER,
                "jeap.security.oauth2.resourceserver.authorization-server.jwk-set-uri=http://localhost:${server.port}/.well-known/jwks.json",
                "jeap.messageexchange.objectstorage.connection.bucket-name-partner=test-bucket-partner",
                "jeap.messageexchange.objectstorage.connection.bucket-name-internal=test-bucket-internal",
                "jeap.messageexchange.api.max-request-body-size-in-bytes=100"
        })
@ContextConfiguration(classes = {MessageExchangeApplication.class, JeapOAuth2IntegrationTestResourceConfiguration.class})
@Testcontainers
@AutoConfigureObservability
@SuppressWarnings("resource")
class MessageExchangeInteractionPartnerApiV2Test extends KafkaIntegrationTestBase {

    @Container
    public static LocalStackContainer localStack = createLocalStackContainer();

    @LocalServerPort
    int serverPort;

    @MockitoSpyBean
    private S3ObjectStorageRepository objectStorageRepository;

    @Autowired
    private JwsBuilderFactory jwsBuilderFactory;

    @Autowired
    private ResourceLoader resourceLoader;

    @Autowired
    private TestEventConsumer testEventConsumer;

    @Autowired
    private MessageRepository messageRepository;

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

    private static final SemanticApplicationRole B2B_MESSAGE_IN_WRITE = SemanticApplicationRole.builder()
            .system("junit")
            .resource("b2bmessagein")
            .operation("write")
            .build();

    private static final SemanticApplicationRole B2B_MESSAGE_IN_READ = SemanticApplicationRole.builder()
            .system("junit")
            .resource("b2bmessagein")
            .operation("read")
            .build();

    private static final SemanticApplicationRole B2B_MESSAGE_OUT_WRITE = SemanticApplicationRole.builder()
            .system("junit")
            .resource("b2bmessageout")
            .operation("write")
            .build();

    private static final SemanticApplicationRole B2B_MESSAGE_OUT_READ = SemanticApplicationRole.builder()
            .system("junit")
            .resource("b2bmessageout")
            .operation("read")
            .build();

    private static LocalStackContainer createLocalStackContainer() {
        return new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.1")
                .asCompatibleSubstituteFor("localstack/localstack"))
                .withEnv("DISABLE_EVENTS", "1") // Disable localstack features that require an internet connection
                .withEnv("SKIP_INFRA_DOWNLOADS", "1")
                .withEnv("SKIP_SSL_CERT_DOWNLOAD", "1");
    }

    @DynamicPropertySource
    static void dynamicProperties(DynamicPropertyRegistry registry) {
        registry.add("jeap.messageexchange.objectstorage.connection.region", () -> localStack.getRegion());
        registry.add("jeap.messageexchange.objectstorage.connection.access-key", () -> localStack.getAccessKey());
        registry.add("jeap.messageexchange.objectstorage.connection.secret-key", () -> localStack.getSecretKey());
        registry.add("jeap.messageexchange.objectstorage.connection.accessUrl", () -> localStack.getEndpointOverride(LocalStackContainer.Service.S3));
        registry.add("spring.datasource.url", () -> postgres.getJdbcUrl());
        registry.add("spring.datasource.username", () -> postgres.getUsername());
        registry.add("spring.datasource.password", () -> postgres.getPassword());
    }

    @BeforeAll
    static void createBucket() {
        S3Client s3Client = S3Client.builder()
                .endpointOverride(localStack.getEndpointOverride(LocalStackContainer.Service.S3)) // LocalStack endpoint
                .region(Region.of(localStack.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(localStack.getAccessKey(), localStack.getSecretKey())))
                .build();
        CreateBucketRequest createPartnerBucketRequest = CreateBucketRequest.builder()
                .bucket("test-bucket-partner")
                .build();
        s3Client.createBucket(createPartnerBucketRequest);

        CreateBucketRequest createInternalBucketRequest = CreateBucketRequest.builder()
                .bucket("test-bucket-internal")
                .build();
        s3Client.createBucket(createInternalBucketRequest);
    }

    @BeforeEach
    void prepareRestAssured() {
        request = new RequestSpecBuilder().setPort(serverPort).build();
    }

    @Test
    void putMessageFromPartner_thenWaitForNotification_thenShouldBeAbleToGetMessage() throws Exception {
        UUID messageId = UUID.randomUUID();
        String bpId = "myBpID";
        String messageType = "myMessageType";
        String xmlContent = getXmlResource("input.xml");

        given()
                .spec(request)
                .contentType(ContentType.XML)
                .auth().oauth2(createAuthTokenForBpRoles(bpId, B2B_MESSAGE_IN_WRITE))
                .header(HEADER_BP_ID, bpId)
                .header(HEADER_MESSAGE_TYPE, messageType)
                .body(xmlContent)
                .when()
                .put("/api/partner/v2/messages/" + messageId)
                .then()
                .statusCode(HttpStatus.CREATED.value());

        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .until(() -> testEventConsumer.hasMessageWithIdempotenceId(messageId));

        B2BMessageReceivedEvent message = testEventConsumer.getMessageByIdempotenceId(messageId);
        MessageReference messageReference = message.getReferences().getMessageReference();
        assertThat(messageReference.getBpId()).isEqualTo(bpId);
        assertThat(messageReference.getMessageId()).isEqualTo(messageId.toString());
        assertThat(messageReference.getType()).isEqualTo(messageType);
        assertThat(message.getIdentity().getIdempotenceId()).isEqualTo(messageId.toString());

        Response response = given()
                .spec(request)
                .contentType(ContentType.XML)
                .auth().oauth2(createAuthTokenForUserRoles(B2B_MESSAGE_IN_READ))
                .when()
                .get("/api/internal/v2/messages/" + messageId);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK.value());
        assertThat(response.getBody().asString()).isEqualTo(xmlContent);
        assertMetricsPresent();
    }

    @Test
    void putMessageFromPartner_whenInvalidXml_thenExpectBadRequest() throws Exception {
        UUID messageId = UUID.randomUUID();
        String bpId = "myBpID";
        String messageType = "myMessageType";
        String xmlContent = getXmlResource("invalid.not-xml");

        given()
                .spec(request)
                .contentType(ContentType.XML)
                .auth().oauth2(createAuthTokenForBpRoles(bpId, B2B_MESSAGE_IN_WRITE))
                .header(HEADER_BP_ID, bpId)
                .header(HEADER_MESSAGE_TYPE, messageType)
                .body(xmlContent)
                .when()
                .put("/api/partner/v2/messages/" + messageId)
                .then()
                .statusCode(HttpStatus.BAD_REQUEST.value())
                .body(containsString("Invalid XML"));

        // Make sure the invalid message has not been saved to S3
        Response response = given()
                .spec(request)
                .contentType(ContentType.XML)
                .auth().oauth2(createAuthTokenForUserRoles(B2B_MESSAGE_IN_READ))
                .when()
                .get("/api/internal/v2/messages/" + messageId);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND.value());
    }

    @Test
    void putMessageFromPartner_inputTooLarge_returnsBadRequest() throws Exception {

        UUID messageId = UUID.randomUUID();
        String bpId = "myBpID";
        String messageType = "myMessageType";
        String xmlContent = getXmlResource("input-too-large.xml");

        given()
                .spec(request)
                .contentType(ContentType.XML)
                .auth().oauth2(createAuthTokenForBpRoles(bpId, B2B_MESSAGE_IN_WRITE))
                .header(HEADER_BP_ID, bpId)
                .header(HEADER_MESSAGE_TYPE, messageType)
                .body(xmlContent)
                .when()
                .put("/api/partner/v2/messages/" + messageId)
                .then()
                .statusCode(HttpStatus.BAD_REQUEST.value());

        verify(objectStorageRepository, never()).putObject(anyString(), eq(messageId.toString()), any());
    }

    @Test
    void putMessageFromInternal_thenWaitForMessageInDb_thenShouldBeAbleToGetMessage() throws Exception {
        UUID messageId = UUID.randomUUID();
        String bpId = "myBpID";
        String messageType = "myMessageType";
        String topicName = "topicName";
        String xmlContent = getXmlResource("input.xml");

        given()
                .spec(request)
                .contentType(ContentType.XML)
                .auth().oauth2(createAuthTokenForUserRoles(B2B_MESSAGE_OUT_WRITE))
                .header(HEADER_BP_ID, bpId)
                .header(HEADER_MESSAGE_TYPE, messageType)
                .param("topicName", topicName)
                .body(xmlContent)
                .when()
                .put("/api/internal/v2/messages/" + messageId)
                .then()
                .statusCode(HttpStatus.CREATED.value());

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .until(() -> messageRepository.findByMessageId(messageId).isPresent());

        Optional<Message> optionalMessage = messageRepository.findByMessageId(messageId);

        assertThat(optionalMessage).isPresent();

        Message savedMessage = optionalMessage.get();

        assertThat(savedMessage.getSequenceId()).isNotNull();
        assertThat(savedMessage.getMessageId()).isEqualTo(messageId);
        assertThat(savedMessage.getBpId()).isEqualTo(bpId);
        assertThat(savedMessage.getTopicName()).isEqualTo(topicName);
        assertThat(savedMessage.getMessageType()).isEqualTo(messageType);

        Response response = given()
                .spec(request)
                .contentType(ContentType.XML)
                .auth().oauth2(createAuthTokenForBpRoles(bpId, B2B_MESSAGE_OUT_READ))
                .header(HEADER_BP_ID, bpId)
                .when()
                .get("/api/partner/v2/messages/" + messageId);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK.value());
        assertThat(response.getBody().asString()).isEqualTo(xmlContent);

    }

    @Test
    void putMessagesFromInternal_thenWaitForMessagesInDb_thenShouldBeAbleToGetNextMessage() throws Exception {
        UUID message1Id = UUID.randomUUID();
        UUID message2Id = UUID.randomUUID();
        String bpId = "myBpID";
        String messageType = "myMessageType";
        String topicName = "topicName";
        String xmlContent1 = getXmlResource("input.xml");
        String xmlContent2 = "<content>test for message 2</content>";

        given()
                .spec(request)
                .contentType(ContentType.XML)
                .auth().oauth2(createAuthTokenForUserRoles(B2B_MESSAGE_OUT_WRITE))
                .header(HEADER_BP_ID, bpId)
                .header(HEADER_MESSAGE_TYPE, messageType)
                .param("topicName", topicName)
                .body(xmlContent1)
                .when()
                .put("/api/internal/v2/messages/" + message1Id)
                .then()
                .statusCode(HttpStatus.CREATED.value());

        given()
                .spec(request)
                .contentType(ContentType.XML)
                .auth().oauth2(createAuthTokenForUserRoles(B2B_MESSAGE_OUT_WRITE))
                .header(HEADER_BP_ID, bpId)
                .header(HEADER_MESSAGE_TYPE, messageType)
                .param("topicName", topicName)
                .body(xmlContent2)
                .when()
                .put("/api/internal/v2/messages/" + message2Id)
                .then()
                .statusCode(HttpStatus.CREATED.value());

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .until(() -> messageRepository.findByMessageId(message1Id).isPresent() && messageRepository.findByMessageId(message2Id).isPresent());

        Response response = given()
                .spec(request)
                .contentType(ContentType.XML)
                .auth().oauth2(createAuthTokenForBpRoles(bpId, B2B_MESSAGE_OUT_READ))
                .header(HEADER_BP_ID, bpId)
                .when()
                .get("/api/partner/v2/messages/" + message1Id + "/next");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK.value());
        assertThat(response.getBody().asString()).isEqualTo(xmlContent2);
    }


    @Test
    void putMessageFromInternal_inputTooLarge_returnsBadRequest() throws Exception {

        UUID messageId = UUID.randomUUID();
        String bpId = "myBpID";
        String messageType = "myMessageType";
        String topicName = "topicName";
        String xmlContent = getXmlResource("input-too-large.xml");

        given()
                .spec(request)
                .contentType(ContentType.XML)
                .auth().oauth2(createAuthTokenForUserRoles(B2B_MESSAGE_OUT_WRITE))
                .header(HEADER_BP_ID, bpId)
                .header(HEADER_MESSAGE_TYPE, messageType)
                .param("topicName", topicName)
                .body(xmlContent)
                .when()
                .put("/api/internal/v2/messages/" + messageId)
                .then()
                .statusCode(HttpStatus.BAD_REQUEST.value());

        verify(objectStorageRepository, never()).putObject(anyString(), eq(messageId.toString()), any());
    }

    private String createAuthTokenForBpRoles(String bpId, SemanticApplicationRole... roles) {
        return jwsBuilderFactory.createValidForFixedLongPeriodBuilder(UUID.randomUUID().toString(), JeapAuthenticationContext.SYS)
                .withBusinessPartnerRoles(bpId, roles)
                .build()
                .serialize();
    }

    private String createAuthTokenForUserRoles(SemanticApplicationRole... userroles) {
        return jwsBuilderFactory.createValidForFixedLongPeriodBuilder(UUID.randomUUID().toString(), JeapAuthenticationContext.SYS)
                .withUserRoles(userroles)
                .build()
                .serialize();
    }

    private String getXmlResource(String filename) throws IOException {
        Resource resource = resourceLoader.getResource("classpath:" + filename);
        return new String(Files.readAllBytes(resource.getFile().toPath()));
    }

    void assertMetricsPresent() {
        given()
                .spec(request)
                .auth().basic("prometheus", "test")
                .get("/actuator/prometheus")
                .then()
                .assertThat()
                .statusCode(200)
                .body(containsString("jeap_mes_partner_controller_send_message"))
                .body(containsString("jeap_mes_internal_controller_get_message"))
                .body(containsString("jeap_mes_repository_get_next_message_id"))
                .body(containsString("jeap_mes_objectstore_put"))
                .body(containsString("jeap_mes_objectstore_get"));
    }
}
