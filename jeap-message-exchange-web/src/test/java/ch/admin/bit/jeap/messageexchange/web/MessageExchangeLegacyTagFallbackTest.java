package ch.admin.bit.jeap.messageexchange.web;

import ch.admin.bit.jeap.messageexchange.event.message.received.B2BMessageReceivedEvent;
import ch.admin.bit.jeap.messageexchange.event.message.received.MessageReference;
import ch.admin.bit.jeap.messageexchange.malware.api.MalwareScanResult;
import ch.admin.bit.jeap.messageexchange.malware.api.MalwareScanResultNotifier;
import ch.admin.bit.jeap.messaging.kafka.test.KafkaIntegrationTestBase;
import ch.admin.bit.jeap.security.resource.semanticAuthentication.SemanticApplicationRole;
import ch.admin.bit.jeap.security.resource.token.JeapAuthenticationContext;
import ch.admin.bit.jeap.security.test.jws.JwsBuilder;
import ch.admin.bit.jeap.security.test.jws.JwsBuilderFactory;
import ch.admin.bit.jeap.security.test.resource.configuration.JeapOAuth2IntegrationTestResourceConfiguration;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static ch.admin.bit.jeap.messageexchange.web.RustFsTestSupport.createRustFsContainer;
import static ch.admin.bit.jeap.messageexchange.web.RustFsTestSupport.createS3Client;
import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for the backwards compatibility with messages stored by MES &lt; 11.0.0: their metadata and
 * scan status live in S3 object tags (and the database row - if one exists at all - lacks the metadata columns).
 * Verifies that such messages are delivered based on the tag scan status, that scan results for them are handled
 * via the read-only tag fallback, and that the database is backfilled so subsequent reads use the database.
 * LEGACY-TAG-FALLBACK: remove with the contract story (JEAP-7252).
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT,
        properties = {
                "server.port=8312",
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
                "jeap.messageexchange.malwarescan.enabled=true"
        })
@ContextConfiguration(classes = {MessageExchangeApplication.class, JeapOAuth2IntegrationTestResourceConfiguration.class, MessageExchangeLegacyTagFallbackTest.TestConfig.class})
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ActiveProfiles("legacy-fallback-test")
@SuppressWarnings({"resource", "SameParameterValue"})
class MessageExchangeLegacyTagFallbackTest extends KafkaIntegrationTestBase {

    private static final String BP_ID = "myBpID";
    private static final String MESSAGE_TYPE = "myMessageType";
    private static final String PARTNER_TOPIC = "myPartnerTopic";
    private static final String PARTNER_EXTERNAL_REFERENCE = "myPartnerExternalReference";
    private static final String BUCKET_NAME_PARTNER = "test-bucket-partner";
    private static final String BUCKET_NAME_INTERNAL = "test-bucket-internal";
    private static final String XML_CONTENT = "<legacy>content</legacy>";

    @Container
    public static GenericContainer<?> rustFs = createRustFsContainer();

    @LocalServerPort
    int serverPort;

    @Autowired
    private JwsBuilderFactory jwsBuilderFactory;

    @Autowired
    private TestEventConsumer testEventConsumer;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    @SuppressWarnings("unused")
    private TestMalwareScanResultNotifier malwareScanResultNotifier;

    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

    private static S3Client s3Client;

    private RequestSpecification request;

    private static final SemanticApplicationRole B2B_MESSAGE_IN_READ = SemanticApplicationRole.builder()
            .system("junit")
            .resource("b2bmessagein")
            .operation("read")
            .build();

    @BeforeAll
    static void startContainers() {
        postgres.start();
        createBucket();
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

    static void createBucket() {
        s3Client = createS3Client(rustFs);
        s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET_NAME_PARTNER).build());
        s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET_NAME_INTERNAL).build());
    }

    @BeforeEach
    void prepareRestAssured() {
        request = new RequestSpecBuilder().setPort(serverPort).build();
    }

    @ParameterizedTest
    @CsvSource({
            "SCAN_PENDING,     403, Legacy SCAN_PENDING must block delivery while scanning is enabled",
            "SCAN_FAILED,      403, Legacy SCAN_FAILED must block delivery while scanning is enabled",
            "NO_THREATS_FOUND, 200, Legacy NO_THREATS_FOUND should be delivered",
            "NOT_SCANNED,      200, Legacy NOT_SCANNED should be delivered",
            "THREATS_FOUND,    403, Legacy THREATS_FOUND must never be delivered"
    })
    void getMessage_legacyMessage_deliveryGatedByTagScanStatus(String scanStatus, int expectedHttpStatus, String description) {
        UUID messageId = UUID.randomUUID();
        createLegacyMessage(messageId, scanStatus, true);

        Response response = getMessageAsInternal(messageId);

        assertThat(response.getStatusCode())
                .as(description)
                .isEqualTo(expectedHttpStatus);
        if (expectedHttpStatus == 200) {
            assertThat(response.getBody().asString()).isEqualTo(XML_CONTENT);
        }
    }

    @Test
    void getMessage_taglessObjectWithoutDatabaseRecord_scanningEnabled_shouldNotDeliver() {
        // An object without metadata tags was stored by MES >= 11.0.0; if its database record is unexpectedly
        // missing (e.g. the upload crashed between storing the object and the record), its scan is still pending
        UUID messageId = UUID.randomUUID();
        putLegacyObject(messageId, Map.of("MessageExchangeLifecyclePolicy", "15"));

        Response response = getMessageAsInternal(messageId);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN.value());
    }

    @Test
    void getMessage_legacyMessageWithoutScanStatusTag_shouldDeliver() {
        // objects stored before malware scanning was introduced carry metadata tags but no scanStatus tag
        UUID messageId = UUID.randomUUID();
        putLegacyObject(messageId, Map.of(
                "MessageExchangeLifecyclePolicy", "15",
                "bpId", BP_ID,
                "messageType", MESSAGE_TYPE,
                "saveTimeInMillis", String.valueOf(System.currentTimeMillis())
        ));
        insertLegacyDatabaseRow(messageId);

        Response response = getMessageAsInternal(messageId);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK.value());
        assertThat(response.getBody().asString()).isEqualTo(XML_CONTENT);
    }

    @Test
    void scanResult_legacyMessageWithLegacyDatabaseRow_publishesEventFromTagsAndBackfillsRow() {
        UUID messageId = UUID.randomUUID();
        createLegacyMessage(messageId, "SCAN_PENDING", true);
        List<Tag> tagsBeforeScanResult = getObjectTags(messageId);

        triggerScanResultAndAwaitEvent(messageId, MalwareScanResult.NO_THREATS_FOUND);

        // event data comes from the legacy S3 tags
        B2BMessageReceivedEvent message = testEventConsumer.getMessageByIdempotenceId(messageId);
        MessageReference messageReference = message.getReferences().getMessageReference();
        assertThat(messageReference.getBpId()).isEqualTo(BP_ID);
        assertThat(messageReference.getMessageId()).isEqualTo(messageId.toString());
        assertThat(messageReference.getType()).isEqualTo(MESSAGE_TYPE);
        assertThat(messageReference.getPartnerTopic()).isEqualTo(PARTNER_TOPIC);
        assertThat(messageReference.getPartnerExternalReference()).isEqualTo(PARTNER_EXTERNAL_REFERENCE);
        assertThat(message.getIdentity().getIdempotenceId()).isEqualTo(messageId + "_NO_THREATS_FOUND");

        // the scanStatus tag is updated exactly like MES < 11.0.0 did (transitional, JEAP-7252); all other
        // tags stay unchanged
        assertThat(getObjectTags(messageId))
                .anyMatch(tag -> tag.key().equals("scanStatus") && tag.value().equals("NO_THREATS_FOUND"))
                .containsAll(tagsBeforeScanResult.stream().filter(tag -> !tag.key().equals("scanStatus")).toList())
                .hasSameSizeAs(tagsBeforeScanResult);

        // the database row is backfilled with the metadata from the tags and the new scan status
        Map<String, Object> row = selectInboundMessageRow(messageId);
        assertThat(row)
                .containsEntry("scanStatus", "NO_THREATS_FOUND")
                .containsEntry("messageType", MESSAGE_TYPE)
                .containsEntry("partnerTopic", PARTNER_TOPIC)
                .containsEntry("partnerExternalReference", PARTNER_EXTERNAL_REFERENCE)
                .containsEntry("contentType", MediaType.APPLICATION_XML_VALUE);

        // after the backfill, the message is delivered based on the database scan status
        Response response = getMessageAsInternal(messageId);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK.value());
        assertThat(response.getBody().asString()).isEqualTo(XML_CONTENT);
    }

    @Test
    void scanResult_legacyMessageWithoutDatabaseRow_createsRowAndPublishesEvent() {
        // messages stored before the inbound_message table was introduced have no database row at all
        UUID messageId = UUID.randomUUID();
        createLegacyMessage(messageId, "SCAN_PENDING", false);

        triggerScanResultAndAwaitEvent(messageId, MalwareScanResult.NO_THREATS_FOUND);

        Map<String, Object> row = selectInboundMessageRow(messageId);
        assertThat(row)
                .containsEntry("bpId", BP_ID)
                .containsEntry("scanStatus", "NO_THREATS_FOUND")
                .containsEntry("messageType", MESSAGE_TYPE);
        assertThat(((Number) row.get("contentLength")).intValue()).isEqualTo(XML_CONTENT.getBytes(StandardCharsets.UTF_8).length);

        Response response = getMessageAsInternal(messageId);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK.value());
    }

    @Test
    void scanResult_legacyMessageWithThreatsFound_blocksDeliveryViaDatabase() {
        UUID messageId = UUID.randomUUID();
        createLegacyMessage(messageId, "SCAN_PENDING", true);

        triggerScanResultAndAwaitEvent(messageId, MalwareScanResult.THREATS_FOUND);

        Map<String, Object> row = selectInboundMessageRow(messageId);
        assertThat(row).containsEntry("scanStatus", "THREATS_FOUND");

        Response response = getMessageAsInternal(messageId);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN.value());
    }

    @Test
    void scanResult_legacyMessageNotScanned_doesNotPublishEventAgainButBackfillsRow() {
        // NOT_SCANNED means the received event was already published when the message was uploaded with
        // scanning disabled - a clean scan result must not publish it again
        UUID messageId = UUID.randomUUID();
        createLegacyMessage(messageId, "NOT_SCANNED", true);

        malwareScanResultNotifier.triggerScanResult(messageId.toString(), BUCKET_NAME_PARTNER, MalwareScanResult.NO_THREATS_FOUND);

        // deterministic wait: the scan result has been fully processed once the database row is backfilled
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .until(() -> "NO_THREATS_FOUND".equals(selectInboundMessageRow(messageId).get("scanStatus")));

        assertThat(testEventConsumer.hasMessageWithIdempotenceId(messageId)).isFalse();
    }

    // Healing of pending scan statuses from the S3 object tags (LEGACY-TAG-FALLBACK, JEAP-7252)

    @Test
    void getMessage_scanResultProcessedByOldInstance_healsScanStatusFromTagAndDelivers() {
        // Rolling deployment scenario: an MES < 11.0.0 instance processed the scan result and updated the
        // scanStatus tag, but the database row (written by an 11.x instance at upload) still says SCAN_PENDING
        UUID messageId = UUID.randomUUID();
        createLegacyMessage(messageId, "NO_THREATS_FOUND", false);
        insertDatabaseRowWithScanStatus(messageId, "SCAN_PENDING");

        Response response = getMessageAsInternal(messageId);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK.value());
        assertThat(response.getBody().asString()).isEqualTo(XML_CONTENT);
        assertThat(selectInboundMessageRow(messageId)).containsEntry("scanStatus", "NO_THREATS_FOUND");
    }

    private void createLegacyMessage(UUID messageId, String scanStatus, boolean withDatabaseRow) {
        // simulates the state written by MES < 11.0.0: metadata tags on the S3 object,
        // minimal database row without metadata columns
        putLegacyObject(messageId, Map.of(
                "MessageExchangeLifecyclePolicy", "15",
                "bpId", BP_ID,
                "messageType", MESSAGE_TYPE,
                "partnerTopic", PARTNER_TOPIC,
                "partnerExternalReference", PARTNER_EXTERNAL_REFERENCE,
                "scanStatus", scanStatus,
                "saveTimeInMillis", String.valueOf(System.currentTimeMillis())
        ));
        if (withDatabaseRow) {
            insertLegacyDatabaseRow(messageId);
        }
    }

    private void putLegacyObject(UUID messageId, Map<String, String> tags) {
        List<Tag> tagSet = new ArrayList<>();
        tags.forEach((key, value) -> tagSet.add(Tag.builder().key(key).value(value).build()));
        s3Client.putObject(PutObjectRequest.builder()
                        .bucket(BUCKET_NAME_PARTNER)
                        .key(messageId.toString())
                        .contentType(MediaType.APPLICATION_XML_VALUE)
                        .tagging(Tagging.builder().tagSet(tagSet).build())
                        .build(),
                RequestBody.fromString(XML_CONTENT, StandardCharsets.UTF_8));
    }

    private void insertLegacyDatabaseRow(UUID messageId) {
        jdbcTemplate.update("""
                        INSERT INTO inbound_message("messageId","bpId","contentLength","createdAt") \
                        VALUES (:messageId,:bpId,:contentLength,NOW())""",
                Map.of("messageId", messageId.toString(),
                        "bpId", BP_ID,
                        "contentLength", XML_CONTENT.getBytes(StandardCharsets.UTF_8).length));
    }

    private void insertDatabaseRowWithScanStatus(UUID messageId, String scanStatus) {
        jdbcTemplate.update("""
                        INSERT INTO inbound_message("messageId","bpId","contentLength","createdAt","messageType","partnerTopic","partnerExternalReference","contentType","scanStatus") \
                        VALUES (:messageId,:bpId,:contentLength,NOW(),:messageType,:partnerTopic,:partnerExternalReference,:contentType,:scanStatus)""",
                Map.of("messageId", messageId.toString(),
                        "bpId", BP_ID,
                        "contentLength", XML_CONTENT.getBytes(StandardCharsets.UTF_8).length,
                        "messageType", MESSAGE_TYPE,
                        "partnerTopic", PARTNER_TOPIC,
                        "partnerExternalReference", PARTNER_EXTERNAL_REFERENCE,
                        "contentType", MediaType.APPLICATION_XML_VALUE,
                        "scanStatus", scanStatus));
    }

    private Map<String, Object> selectInboundMessageRow(UUID messageId) {
        return jdbcTemplate.queryForMap("SELECT * FROM inbound_message WHERE \"messageId\" = :messageId",
                Map.of("messageId", messageId.toString()));
    }

    private List<Tag> getObjectTags(UUID messageId) {
        return s3Client.getObjectTagging(GetObjectTaggingRequest.builder()
                .bucket(BUCKET_NAME_PARTNER)
                .key(messageId.toString())
                .build()).tagSet();
    }

    private void triggerScanResultAndAwaitEvent(UUID messageId, MalwareScanResult scanResult) {
        malwareScanResultNotifier.triggerScanResult(messageId.toString(), BUCKET_NAME_PARTNER, scanResult);
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .until(() -> testEventConsumer.hasMessageWithIdempotenceId(messageId));
    }

    private Response getMessageAsInternal(UUID messageId) {
        return given()
                .spec(request)
                .accept(MediaType.APPLICATION_XML_VALUE)
                .auth().oauth2(createAuthTokenForUserRoles(B2B_MESSAGE_IN_READ))
                .when()
                .get("/api/internal/v3/messages/" + messageId);
    }

    private String createAuthTokenForUserRoles(SemanticApplicationRole... userroles) {
        return jwsBuilderFactory.createValidForFixedLongPeriodBuilder(UUID.randomUUID().toString(), JeapAuthenticationContext.SYS)
                .withUserRoles(userroles)
                .build()
                .serialize();
    }

    @TestConfiguration
    static class TestConfig {

        @Bean
        @Profile("legacy-fallback-test")
        public TestMalwareScanResultNotifier malwareScanResultNotifier() {
            return new TestMalwareScanResultNotifier();
        }

    }

    static class TestMalwareScanResultNotifier extends MalwareScanResultNotifier {
        public void triggerScanResult(String key, String bucketName, MalwareScanResult scanResult) {
            notifyListeners(key, bucketName, scanResult);
        }
    }
}
