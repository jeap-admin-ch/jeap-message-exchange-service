package ch.admin.bit.jeap.messageexchange.web.rest.model;

import au.com.dius.pact.provider.junit5.HttpTestTarget;
import au.com.dius.pact.provider.junit5.PactVerificationContext;
import au.com.dius.pact.provider.junit5.PactVerificationInvocationContextProvider;
import au.com.dius.pact.provider.junitsupport.AllowOverridePactUrl;
import au.com.dius.pact.provider.junitsupport.IgnoreNoPactsToVerify;
import au.com.dius.pact.provider.junitsupport.Provider;
import au.com.dius.pact.provider.junitsupport.State;
import au.com.dius.pact.provider.junitsupport.loader.PactBroker;
import ch.admin.bit.jeap.messageexchange.domain.MessageContent;
import ch.admin.bit.jeap.messageexchange.kafka.MessageExchangeServiceContractsValidator;
import ch.admin.bit.jeap.messageexchange.objectstorage.S3ObjectStorageRepository;
import ch.admin.bit.jeap.messageexchange.persistence.JdbcMessageRepository;
import ch.admin.bit.jeap.messageexchange.web.MessageExchangeApplication;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static ch.admin.bit.jeap.messageexchange.test.Pacticipants.MESSAGE_EXCHANGE;
import static org.mockito.Mockito.when;

@SpringBootTest(classes = MessageExchangeApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT,
        properties = {
                "jeap.security.oauth2.resourceserver.authorization-server.issuer=http://localhost/auth",
                "jeap.security.oauth2.resourceserver.authorization-server.jwk-set-uri=http://localhost:1235/jme-message-exchange-service/.well-known/jwks.json"
        })
@ActiveProfiles({"pact-provider-test"})
@Provider(MESSAGE_EXCHANGE)
@PactBroker
@IgnoreNoPactsToVerify
@AllowOverridePactUrl
public class PactProviderTestBase {

    @MockitoBean
    JdbcMessageRepository jdbcMessageRepository;

    @MockitoBean
    S3ObjectStorageRepository s3ObjectStorageRepository;

    @MockitoBean
    DataSource dataSource;

    @MockitoBean
    MessageExchangeServiceContractsValidator messageExchangeServiceContractsValidator;

    @BeforeEach
    void setUp(PactVerificationContext context) {
        // If there are no pacts there will be no context.
        if (context != null) {
            context.setTarget(new HttpTestTarget("localhost", 1235, "/"));
        }
    }

    @TestTemplate
    @ExtendWith(PactVerificationInvocationContextProvider.class)
    void testPacts(PactVerificationContext context) {
        // If there are no pacts there will be no context.
        if (context != null) {
            context.verifyInteraction();
        }
    }

    @SneakyThrows
    @State("A message has been received from a business partner with messageId=610b64cc-4211-4625-a11e-8e8bfe616876")
    void initStateTaskWithMessageFromBp() {
        byte[] xml = "<partnermessage/>".getBytes(StandardCharsets.UTF_8);
        InputStream xmlStream = new ByteArrayInputStream(xml);
        MessageContent messageContent = new MessageContent(xmlStream, xml.length);
        Optional<MessageContent> content = Optional.of(messageContent);
        when(s3ObjectStorageRepository.getObjectWithTags("bazg-jme-messageexchange-partner-obs-dev", "610b64cc-4211-4625-a11e-8e8bfe616876"))
                .thenReturn(content);
    }
}
