package ch.admin.bit.jeap.messageexchange.web.api;

import ch.admin.bit.jeap.messageexchange.domain.Message;
import ch.admin.bit.jeap.messageexchange.domain.MessageContent;
import ch.admin.bit.jeap.messageexchange.domain.MessageExchangeService;
import ch.admin.bit.jeap.messageexchange.domain.exception.MalwareScanFailedOrBlockedException;
import ch.admin.bit.jeap.messageexchange.domain.exception.MismatchedContentTypeException;
import ch.admin.bit.jeap.messageexchange.domain.malwarescan.ScanStatus;
import ch.admin.bit.jeap.security.resource.semanticAuthentication.SemanticApplicationRole;
import ch.admin.bit.jeap.security.resource.token.JeapAuthenticationToken;
import ch.admin.bit.jeap.security.test.resource.JeapAuthenticationTestTokenBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.io.ByteArrayInputStream;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static ch.admin.bit.jeap.messageexchange.web.api.HeaderNames.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.authentication;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = MessageInternalV3Controller.class, properties = {
        "jeap.messageexchange.api.media-types=application/xml,application/yaml",
})
@ActiveProfiles("controller-test")
@ContextConfiguration(classes = RestApiTestContext.class)
@AutoConfigureMockMvc
class MessageInternalV3ControllerITTest {

    @MockitoBean
    @SuppressWarnings("unused")
    private MessageExchangeService messageExchangeService;

    @Autowired
    private MockMvc mockMvc;

    @Captor
    private ArgumentCaptor<Message> messageCaptor;

    @Captor
    private ArgumentCaptor<MessageContent> messageContentCaptor;

    private static final String XML_CONTENT = "<content>test</content>";

    private static final SemanticApplicationRole B2B_MESSAGE_OUT_WRITE = SemanticApplicationRole.builder()
            .system("junit")
            .resource("b2bmessageout")
            .operation("write")
            .build();

    private static final SemanticApplicationRole B2B_MESSAGE_IN_READ = SemanticApplicationRole.builder()
            .system("junit")
            .resource("b2bmessagein")
            .operation("read")
            .build();

    @Test
    void putNewMessageFromInternalApplication_whenValid_thenReturnsCreated() throws Exception {
        UUID messageId = UUID.randomUUID();

        mockMvc.perform(
                        put("/api/internal/v3/messages/{messageId}", messageId)
                                .header(HEADER_BP_ID, "bpId")
                                .param("topicName", "topicName")
                                .param("groupId", "groupId")
                                .header(HEADER_MESSAGE_TYPE, "messageType")
                                .header(HEADER_PARTNER_TOPIC, "partnerTopic")
                                .contentType(MediaType.APPLICATION_XML_VALUE)
                                .content(XML_CONTENT)
                                .with(csrf()) // needed because in this test no bearer token is provided, just the final Spring authentication
                                .with(authentication(createAuthenticationForUserRoles(B2B_MESSAGE_OUT_WRITE))))
                .andExpect(status().isCreated());

        Message message = Message.builder()
                .messageId(messageId)
                .bpId("bpId")
                .topicName("topicName")
                .groupId("groupId")
                .messageType("messageType")
                .partnerTopic("partnerTopic")
                .contentType(MediaType.APPLICATION_XML_VALUE)
                .build();

        verify(messageExchangeService, times(1))
                .saveNewMessageFromInternalApplication(messageCaptor.capture(), messageContentCaptor.capture());

        Message value = messageCaptor.getValue();
        assertThat(value.getMessageId()).isEqualTo(message.getMessageId());

        MessageContent messageContent = messageContentCaptor.getValue();
        assertThat(messageContent.contentLength()).isEqualTo(XML_CONTENT.getBytes(UTF_8).length);
    }

    @Test
    void putNewMessageFromInternalApplicationWithAllHeaders_whenValid_thenReturnsCreated() throws Exception {
        UUID messageId = UUID.randomUUID();

        Map<String, String> metadata = Map.of("myKey", "myValue", "foo", "bar");
        String metadataBase64 = Base64.getEncoder().encodeToString(new ObjectMapper().writeValueAsString(metadata).getBytes());

        mockMvc.perform(
                        put("/api/internal/v3/messages/{messageId}", messageId)
                                .header(HEADER_BP_ID, "bpId")
                                .param("topicName", "topicName")
                                .param("groupId", "groupId")
                                .header(HEADER_MESSAGE_TYPE, "messageType")
                                .header(HEADER_PARTNER_TOPIC, "partnerTopic")
                                .header(HEADER_PARTNER_EXTERNAL_REFERENCE, "partnerExternalReferenceTest")
                                .header(HEADER_MES_METADATA, metadataBase64)
                                .contentType(MediaType.APPLICATION_XML_VALUE)
                                .content(XML_CONTENT)
                                .with(csrf()) // needed because in this test no bearer token is provided, just the final Spring authentication
                                .with(authentication(createAuthenticationForUserRoles(B2B_MESSAGE_OUT_WRITE))))
                .andExpect(status().isCreated());

        Message message = Message.builder()
                .messageId(messageId)
                .bpId("bpId")
                .topicName("topicName")
                .groupId("groupId")
                .messageType("messageType")
                .partnerTopic("partnerTopic")
                .partnerExternalReference("partnerExternalReferenceTest")
                .metadata(metadata)
                .contentType(MediaType.APPLICATION_XML_VALUE)
                .build();

        verify(messageExchangeService, times(1))
                .saveNewMessageFromInternalApplication(messageCaptor.capture(), messageContentCaptor.capture());

        Message value = messageCaptor.getValue();
        assertThat(value.getMessageId()).isEqualTo(message.getMessageId());

        MessageContent messageContent = messageContentCaptor.getValue();
        assertThat(messageContent.contentLength()).isEqualTo(XML_CONTENT.getBytes(UTF_8).length);
    }

    @Test
    void putNewMessageFromInternalApplication_whenMetadataBase64Invalid_thenReturnsBadRequest() throws Exception {
        UUID messageId = UUID.randomUUID();

        mockMvc.perform(
                        put("/api/internal/v3/messages/{messageId}", messageId)
                                .header(HEADER_BP_ID, "bpId")
                                .param("topicName", "topicName")
                                .param("groupId", "groupId")
                                .header(HEADER_MESSAGE_TYPE, "messageType")
                                .header(HEADER_MES_METADATA, "fooBar")
                                .contentType(MediaType.APPLICATION_XML_VALUE)
                                .content(XML_CONTENT)
                                .with(csrf()) // needed because in this test no bearer token is provided, just the final Spring authentication
                                .with(authentication(createAuthenticationForUserRoles(B2B_MESSAGE_OUT_WRITE))))
                .andExpect(status().isBadRequest());
    }

    @Test
    void putNewMessageFromInternalApplication_whenMetadataInvalid_thenReturnsBadRequest() throws Exception {
        UUID messageId = UUID.randomUUID();
        String metadataBase64 = Base64.getEncoder().encodeToString("fooBar".getBytes());

        mockMvc.perform(
                        put("/api/internal/v3/messages/{messageId}", messageId)
                                .header(HEADER_BP_ID, "bpId")
                                .param("topicName", "topicName")
                                .param("groupId", "groupId")
                                .header(HEADER_MESSAGE_TYPE, "messageType")
                                .header(HEADER_MES_METADATA, metadataBase64)
                                .contentType(MediaType.APPLICATION_XML_VALUE)
                                .content(XML_CONTENT)
                                .with(csrf()) // needed because in this test no bearer token is provided, just the final Spring authentication
                                .with(authentication(createAuthenticationForUserRoles(B2B_MESSAGE_OUT_WRITE))))
                .andExpect(status().isBadRequest());
    }

    @Test
    void putNewMessageFromInternalApplication_whenValidContentTypeYaml_thenReturnsCreated() throws Exception {
        UUID messageId = UUID.randomUUID();

        mockMvc.perform(
                        put("/api/internal/v3/messages/{messageId}", messageId)
                                .header(HEADER_BP_ID, "bpId")
                                .param("topicName", "topicName")
                                .param("groupId", "groupId")
                                .header(HEADER_MESSAGE_TYPE, "messageType")
                                .header(HEADER_PARTNER_TOPIC, "partnerTopic")
                                .contentType(MediaType.APPLICATION_YAML_VALUE)
                                .content(XML_CONTENT)
                                .with(csrf()) // needed because in this test no bearer token is provided, just the final Spring authentication
                                .with(authentication(createAuthenticationForUserRoles(B2B_MESSAGE_OUT_WRITE))))
                .andExpect(status().isCreated());

        Message message = Message.builder()
                .messageId(messageId)
                .bpId("bpId")
                .topicName("topicName")
                .groupId("groupId")
                .messageType("messageType")
                .partnerTopic("partnerTopic")
                .contentType(MediaType.APPLICATION_YAML_VALUE)
                .build();

        verify(messageExchangeService, times(1))
                .saveNewMessageFromInternalApplication(messageCaptor.capture(), messageContentCaptor.capture());

        Message value = messageCaptor.getValue();
        assertThat(value.getMessageId()).isEqualTo(message.getMessageId());

        MessageContent messageContent = messageContentCaptor.getValue();
        assertThat(messageContent.contentLength()).isEqualTo(XML_CONTENT.getBytes(UTF_8).length);
    }

    @Test
    void putNewMessageFromInternalApplication_onlyRequiredFields_thenReturnsCreated() throws Exception {
        UUID messageId = UUID.randomUUID();

        mockMvc.perform(
                        put("/api/internal/v3/messages/{messageId}", messageId)
                                .header(HEADER_BP_ID, "bpId")
                                .param("topicName", "topicName")
                                .header(HEADER_MESSAGE_TYPE, "messageType")
                                .contentType(MediaType.APPLICATION_XML_VALUE)
                                .content(XML_CONTENT)
                                .with(csrf()) // needed because in this test no bearer token is provided, just the final Spring authentication
                                .with(authentication(createAuthenticationForUserRoles(B2B_MESSAGE_OUT_WRITE))))
                .andExpect(status().isCreated());

        Message message = Message.builder()
                .messageId(messageId)
                .bpId("bpId")
                .topicName("topicName")
                .messageType("messageType")
                .contentType(MediaType.APPLICATION_XML_VALUE)
                .build();

        verify(messageExchangeService, times(1))
                .saveNewMessageFromInternalApplication(messageCaptor.capture(), messageContentCaptor.capture());

        Message value = messageCaptor.getValue();
        assertThat(value.getMessageId()).isEqualTo(message.getMessageId());

        MessageContent messageContent = messageContentCaptor.getValue();
        assertThat(messageContent.contentLength()).isEqualTo(XML_CONTENT.getBytes(UTF_8).length);
    }

    @Test
    void putNewMessageFromInternalApplication_contentTypeHeaderNotSet_thenReturnsBadRequest() throws Exception {
        UUID messageId = UUID.randomUUID();
        mockMvc.perform(
                        put("/api/internal/v3/messages/{messageId}", messageId)
                                .header(HEADER_BP_ID, "bpId")
                                .param("topicName", "topicName")
                                .header(HEADER_MESSAGE_TYPE, "messageType")
                                .content(XML_CONTENT)
                                .with(csrf()) // needed because in this test no bearer token is provided, just the final Spring authentication
                                .with(authentication(createAuthenticationForUserRoles(B2B_MESSAGE_OUT_WRITE))))
                .andExpect(status().isBadRequest());
    }

    @Test
    void putNewMessageFromInternalApplication_contentTypeHeaderNotSupported_thenReturnsNotAcceptable() throws Exception {
        UUID messageId = UUID.randomUUID();
        mockMvc.perform(
                        put("/api/internal/v3/messages/{messageId}", messageId)
                                .header(HEADER_BP_ID, "bpId")
                                .param("topicName", "topicName")
                                .header(HEADER_MESSAGE_TYPE, "messageType")
                                .content(XML_CONTENT)
                                .contentType(MediaType.APPLICATION_JSON_VALUE) // unsupported content type
                                .with(csrf()) // needed because in this test no bearer token is provided, just the final Spring authentication
                                .with(authentication(createAuthenticationForUserRoles(B2B_MESSAGE_OUT_WRITE))))
                .andExpect(status().isNotAcceptable());
    }

    @Test
    void putNewMessageFromInternalApplication_missingRequiredField_thenReturnsBadRequest() throws Exception {
        UUID messageId = UUID.randomUUID();
        mockMvc.perform(
                        put("/api/internal/v3/messages/{messageId}", messageId)
                                .header(HEADER_BP_ID, "bpId")
                                .param("topicName", "topicName")
                                .contentType(MediaType.APPLICATION_XML_VALUE)
                                .content(XML_CONTENT)
                                .with(csrf()) // needed because in this test no bearer token is provided, just the final Spring authentication
                                .with(authentication(createAuthenticationForUserRoles(B2B_MESSAGE_OUT_WRITE))))
                .andExpect(status().isBadRequest());

        verify(messageExchangeService, never()).saveNewMessageFromInternalApplication(any(Message.class), any(MessageContent.class));

    }

    @Test
    void putNewMessageFromInternalApplication_noAuthorization_thenReturnsUnauthorized() throws Exception {
        UUID messageId = UUID.randomUUID();
        mockMvc.perform(
                        put("/api/internal/v3/messages/{messageId}", messageId)
                                .header(HEADER_BP_ID, "bpId")
                                .param("topicName", "topicName")
                                .header(HEADER_MESSAGE_TYPE, "messageType")
                                .contentType(MediaType.APPLICATION_XML_VALUE)
                                .content(XML_CONTENT)
                                .with(csrf())) // needed because in this test no bearer token is provided, just the final Spring authentication
                .andExpect(status().isUnauthorized());

        verify(messageExchangeService, never()).saveNewMessageFromInternalApplication(any(Message.class), any(MessageContent.class));

    }

    @Test
    void putNewMessageFromInternalApplication_wrongPermission_thenReturnsForbidden() throws Exception {
        UUID messageId = UUID.randomUUID();
        mockMvc.perform(
                        put("/api/internal/v3/messages/{messageId}", messageId)
                                .header(HEADER_BP_ID, "bpId")
                                .param("topicName", "topicName")
                                .header(HEADER_MESSAGE_TYPE, "messageType")
                                .contentType(MediaType.APPLICATION_XML_VALUE)
                                .content(XML_CONTENT)
                                .with(csrf()) // needed because in this test no bearer token is provided, just the final Spring authentication
                                .with(authentication(createAuthenticationForUserRoles(B2B_MESSAGE_IN_READ))))
                .andExpect(status().isForbidden());

        verify(messageExchangeService, never()).saveNewMessageFromInternalApplication(any(Message.class), any(MessageContent.class));

    }


    @Test
    void getMessageFromPartner_messageExists_thenReturnsMessage() throws Exception {
        UUID messageId = UUID.randomUUID();
        String xmlContent = "<content>test</content>";
        byte[] xmlContentBytes = xmlContent.getBytes();
        MessageContent messageContent = new MessageContent(new ByteArrayInputStream(xmlContentBytes), xmlContentBytes.length);
        when(messageExchangeService.getMessageFromPartner(messageId, MediaType.APPLICATION_XML_VALUE)).thenReturn(Optional.of(messageContent));
        mockMvc.perform(
                        get("/api/internal/v3/messages/{messageId}", messageId)
                                .accept(MediaType.APPLICATION_XML_VALUE)
                                .with(authentication(createAuthenticationForUserRoles(B2B_MESSAGE_IN_READ))))
                .andExpect(status().isOk())
                .andExpect(content().string(xmlContent));
    }

    @Test
    void getMessageFromPartner_messageExistsWithYamlContentType_thenReturnsMessage() throws Exception {
        UUID messageId = UUID.randomUUID();
        String xmlContent = "<content>test</content>";
        byte[] xmlContentBytes = xmlContent.getBytes();
        MessageContent messageContent = new MessageContent(new ByteArrayInputStream(xmlContentBytes), xmlContentBytes.length);
        when(messageExchangeService.getMessageFromPartner(messageId, MediaType.APPLICATION_YAML_VALUE)).thenReturn(Optional.of(messageContent));
        mockMvc.perform(
                        get("/api/internal/v3/messages/{messageId}", messageId)
                                .accept(MediaType.APPLICATION_YAML_VALUE)
                                .with(authentication(createAuthenticationForUserRoles(B2B_MESSAGE_IN_READ))))
                .andExpect(status().isOk())
                .andExpect(content().string(xmlContent));
    }

    @Test
    void getMessageFromPartner_acceptHeaderNotSet_thenReturnsBadRequest() throws Exception {
        UUID messageId = UUID.randomUUID();
        mockMvc.perform(
                        get("/api/internal/v3/messages/{messageId}", messageId)
                                .with(authentication(createAuthenticationForUserRoles(B2B_MESSAGE_IN_READ))))
                .andExpect(status().isBadRequest());
    }

    @Test
    void getMessageFromPartner_acceptHeaderMismatching_thenReturnsNotAcceptable() throws Exception {
        UUID messageId = UUID.randomUUID();
        when(messageExchangeService.getMessageFromPartner(messageId, MediaType.APPLICATION_JSON_VALUE))
                .thenThrow(MismatchedContentTypeException.requestedContentTypeIncorrect(messageId, MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE));
        mockMvc.perform(
                        get("/api/internal/v3/messages/{messageId}", messageId)
                                .accept(MediaType.APPLICATION_JSON_VALUE)
                                .with(authentication(createAuthenticationForUserRoles(B2B_MESSAGE_IN_READ))))
                .andExpect(status().isNotAcceptable());
    }

    @Test
    void getMessageFromPartner_messageNotExists_thenReturnsNotFound() throws Exception {
        mockMvc.perform(
                        get("/api/internal/v3/messages/{messageId}", UUID.randomUUID())
                                .accept(MediaType.APPLICATION_XML_VALUE)
                                .with(authentication(createAuthenticationForUserRoles(B2B_MESSAGE_IN_READ))))
                .andExpect(status().isNotFound());
    }

    @Test
    void getMessageFromPartner_notAuthorized_thenReturnsForbidden() throws Exception {
        mockMvc.perform(
                        get("/api/internal/v3/messages/{messageId}", UUID.randomUUID())
                                .accept(MediaType.APPLICATION_XML_VALUE)
                                .with(authentication(createAuthenticationForUserRoles(B2B_MESSAGE_OUT_WRITE))))
                .andExpect(status().isForbidden());
    }

    @Test
    void getMessageFromPartner_noAuthorization_thenReturnsUnauthorized() throws Exception {
        mockMvc.perform(
                        get("/api/internal/v3/messages/{messageId}", UUID.randomUUID())
                )
                .andExpect(status().isUnauthorized());
    }

    @Test
    void getMessageFromPartner_messageScanFailed_thenReturnsForbidden() throws Exception {
        UUID messageId = UUID.randomUUID();
        doThrow(MalwareScanFailedOrBlockedException.malwareScanFailedOrBlockedException(messageId, ScanStatus.SCAN_FAILED))
                .when(messageExchangeService).getMessageFromPartner(messageId, MediaType.APPLICATION_XML_VALUE);
        mockMvc.perform(
                        get("/api/internal/v3/messages/{messageId}", messageId)
                                .accept(MediaType.APPLICATION_XML_VALUE)
                                .with(authentication(createAuthenticationForUserRoles(B2B_MESSAGE_IN_READ))))
                .andExpect(status().isForbidden());
    }

    private JeapAuthenticationToken createAuthenticationForUserRoles(SemanticApplicationRole... userroles) {
        return JeapAuthenticationTestTokenBuilder.create()
                .withUserRoles(userroles)
                .build();
    }
}
