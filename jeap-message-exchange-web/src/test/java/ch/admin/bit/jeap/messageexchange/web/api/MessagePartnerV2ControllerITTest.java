package ch.admin.bit.jeap.messageexchange.web.api;

import ch.admin.bit.jeap.messageexchange.domain.MessageContent;
import ch.admin.bit.jeap.messageexchange.domain.MessageExchangeService;
import ch.admin.bit.jeap.messageexchange.domain.NextMessageResultDto;
import ch.admin.bit.jeap.messageexchange.domain.dto.MessageSearchResultDto;
import ch.admin.bit.jeap.messageexchange.domain.xml.InvalidXMLInputException;
import ch.admin.bit.jeap.security.resource.semanticAuthentication.SemanticApplicationRole;
import ch.admin.bit.jeap.security.resource.token.JeapAuthenticationToken;
import ch.admin.bit.jeap.security.test.resource.JeapAuthenticationTestTokenBuilder;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.security.core.Authentication;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

import javax.xml.stream.XMLStreamException;
import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static ch.admin.bit.jeap.messageexchange.web.api.DeprecatedHeaderNames.HEADER_MESSAGE_ID_OLD;
import static ch.admin.bit.jeap.messageexchange.web.api.HeaderNames.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.Mockito.*;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.authentication;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SuppressWarnings("deprecation")
@WebMvcTest(controllers = {MessagePartnerV2Controller.class})
@ActiveProfiles("controller-test")
@ContextConfiguration(classes = RestApiTestContext.class)
@AutoConfigureMockMvc
class MessagePartnerV2ControllerITTest {

    @MockitoBean
    private MessageExchangeService messageExchangeService;

    @Autowired
    private MockMvc mockMvc;
    private static final String XML_CONTENT = "<content>test</content>";

    private static final SemanticApplicationRole B2B_MESSAGE_OUT_READ = SemanticApplicationRole.builder()
            .system("junit")
            .resource("b2bmessageout")
            .operation("read")
            .build();
    private static final SemanticApplicationRole B2B_MESSAGE_IN_WRITE = SemanticApplicationRole.builder()
            .system("junit")
            .resource("b2bmessagein")
            .operation("write")
            .build();

    @Test
    void putNewMessage_whenValid_thenReturnsCreated() throws Exception {
        UUID messageId = UUID.randomUUID();
        String bpId = UUID.randomUUID().toString();
        String messageType = "messageType";

        performPutMessage(messageId.toString(), bpId, messageType, XML_CONTENT, createAuthenticationForBpRoles(bpId, B2B_MESSAGE_IN_WRITE))
                .andExpect(status().isCreated());

        verify(messageExchangeService, times(1))
                .saveNewMessageFromPartner(eq(messageId), eq(bpId), eq("messageType"), any());
    }

    @Test
    void putNewMessage_whenAuthorizedForAllBpIds_thenAccessGranted() throws Exception {
        UUID messageId = UUID.randomUUID();
        String requestBpId = UUID.randomUUID().toString(); // any business partner id
        String messageType = "messageType";

        performPutMessage(messageId.toString(), requestBpId, messageType, XML_CONTENT,
                createAuthenticationForUserRoles(B2B_MESSAGE_IN_WRITE))
                .andExpect(status().isCreated());
    }

    @Test
    void putNewMessage_whenNotAuthorizedBpRole_thenReturnsForbidden() throws Exception {
        UUID messageId = UUID.randomUUID();
        String bpId = UUID.randomUUID().toString();
        String messageType = "messageType";

        performPutMessage(messageId.toString(), bpId, messageType, XML_CONTENT, createAuthenticationForBpRoles(bpId, B2B_MESSAGE_OUT_READ))
                .andExpect(status().isForbidden());
    }

    @Test
    void putNewMessage_whenNotAuthorizedUserRole_thenReturnsForbidden() throws Exception {
        UUID messageId = UUID.randomUUID();
        String bpId = UUID.randomUUID().toString();
        String messageType = "messageType";

        performPutMessage(messageId.toString(), bpId, messageType, XML_CONTENT, createAuthenticationForUserRoles(B2B_MESSAGE_OUT_READ))
                .andExpect(status().isForbidden());
    }

    @Test
    void putNewMessage_whenNoAuthorization_thenReturnsUnauthorized() throws Exception {
        UUID messageId = UUID.randomUUID();
        String bpId = UUID.randomUUID().toString();
        String messageType = "messageType";

        performPutMessage(messageId.toString(), bpId, messageType, XML_CONTENT, null)
                .andExpect(status().isUnauthorized());
    }

    @Test
    void putNewMessage_whenBpIdWrong_thenReturnsForbidden() throws Exception {
        UUID messageId = UUID.randomUUID();
        String bpId = UUID.randomUUID().toString();
        String messageType = "messageType";

        performPutMessage(messageId.toString(), bpId, messageType, XML_CONTENT, createAuthenticationForBpRoles("dummy", B2B_MESSAGE_IN_WRITE))
                .andExpect(status().isForbidden());

    }

    @Test
    void putNewMessage_whenMessageIdNotValid_thenReturnsBadRequest() throws Exception {
        String messageId = "dummy";
        String bpId = UUID.randomUUID().toString();
        String messageType = "messageType";

        performPutMessage(messageId, bpId, messageType, XML_CONTENT, createAuthenticationForBpRoles(bpId, B2B_MESSAGE_IN_WRITE))
                .andExpect(status().isBadRequest());
    }

    @Test
    void putNewMessage_whenXmlInvalid_thenReturnsBadRequest() throws Exception {
        UUID messageId = UUID.randomUUID();
        String bpId = UUID.randomUUID().toString();
        String messageType = "messageType";
        String xmlContent = "dummy";
        doThrow(InvalidXMLInputException.invalid(UUID.randomUUID(), bpId, mock(XMLStreamException.class)))
                .when(messageExchangeService).saveNewMessageFromPartner(eq(messageId), eq(bpId), eq(messageType), any());

        performPutMessage(messageId.toString(), bpId, messageType, xmlContent, createAuthenticationForBpRoles(bpId, B2B_MESSAGE_IN_WRITE))
                .andExpect(status().isBadRequest());
    }

    @SneakyThrows
    private ResultActions performPutMessage(String messageId, String bpId, String messageType, String xmlContent, Authentication authentication) {
        return mockMvc.perform(
                put("/api/partner/v2/messages/{messageId}", messageId)
                        .header(HEADER_BP_ID, bpId)
                        .header(HEADER_MESSAGE_TYPE, messageType)
                        .contentType(MediaType.APPLICATION_XML_VALUE)
                        .content(xmlContent)
                        .with(csrf()) // needed because in this test no bearer token is provided, just the final Spring authentication
                        .with(authentication(authentication)));
    }

    @Test
    void getMessage_bpIdMissing_thenReturnsBadRequest() throws Exception {
        UUID messageId = UUID.randomUUID();

        mockMvc.perform(
                        get("/api/partner/v2/messages/{messageId}", messageId)
                                .contentType(MediaType.APPLICATION_XML_VALUE)
                                .with(authentication(createAuthenticationForBpRoles(UUID.randomUUID().toString(), B2B_MESSAGE_OUT_READ))))
                .andExpect(status().isBadRequest());

        verify(messageExchangeService, never()).getMessageFromInternalApplication(anyString(), any(UUID.class));
    }

    @Test
    void getMessage_notAuthorizedBpRole_thenReturnsForbidden() throws Exception {
        UUID messageId = UUID.randomUUID();
        String bpId = UUID.randomUUID().toString();
        mockMvc.perform(
                        get("/api/partner/v2/messages/{messageId}", messageId)
                                .header(HEADER_BP_ID, bpId)
                                .contentType(MediaType.APPLICATION_XML_VALUE)
                                .with(authentication(createAuthenticationForBpRoles(bpId, B2B_MESSAGE_IN_WRITE))))
                .andExpect(status().isForbidden());

        verify(messageExchangeService, never()).getMessageFromInternalApplication(anyString(), any(UUID.class));
    }

    @Test
    void getMessage_notAuthorizedUserRole_thenReturnsForbidden() throws Exception {
        UUID messageId = UUID.randomUUID();
        String bpId = UUID.randomUUID().toString();
        mockMvc.perform(
                        get("/api/partner/v2/messages/{messageId}", messageId)
                                .header(HEADER_BP_ID, bpId)
                                .contentType(MediaType.APPLICATION_XML_VALUE)
                                .with(authentication(createAuthenticationForUserRoles(B2B_MESSAGE_IN_WRITE))))
                .andExpect(status().isForbidden());

        verify(messageExchangeService, never()).getMessageFromInternalApplication(anyString(), any(UUID.class));
    }

    @Test
    void getMessage_wrongBpId_thenReturnsForbidden() throws Exception {
        UUID messageId = UUID.randomUUID();
        mockMvc.perform(
                        get("/api/partner/v2/messages/{messageId}", messageId)
                                .header(HEADER_BP_ID, UUID.randomUUID())
                                .contentType(MediaType.APPLICATION_XML_VALUE)
                                .with(authentication(createAuthenticationForBpRoles("dummy", B2B_MESSAGE_OUT_READ))))
                .andExpect(status().isForbidden());

        verify(messageExchangeService, never()).getMessageFromInternalApplication(anyString(), any(UUID.class));
    }

    @Test
    void getMessage_withoutAuthentication_thenReturnsUnauthorized() throws Exception {
        UUID messageId = UUID.randomUUID();
        mockMvc.perform(
                        get("/api/partner/v2/messages/{messageId}", messageId)
                                .header(HEADER_BP_ID, UUID.randomUUID().toString())
                                .contentType(MediaType.APPLICATION_XML_VALUE))
                .andExpect(status().isUnauthorized());

        verify(messageExchangeService, never()).getMessageFromInternalApplication(anyString(), any(UUID.class));
    }

    @Test
    void getMessage_whenAuthorizedForAllBpIds_thenAccessGranted() throws Exception {
        UUID messageId = UUID.randomUUID();
        String requestBpId = UUID.randomUUID().toString(); // any business partner id
        MessageContent messageContent = new MessageContent(new ByteArrayInputStream("123".getBytes(UTF_8)), 3);
        when(messageExchangeService.getMessageFromInternalApplication(requestBpId, messageId))
                .thenReturn(Optional.of(messageContent));

        mockMvc.perform(
                        get("/api/partner/v2/messages/{messageId}", messageId)
                                .header(HEADER_BP_ID, requestBpId)
                                .contentType(MediaType.APPLICATION_XML_VALUE)
                                .with(authentication(createAuthenticationForUserRoles(B2B_MESSAGE_OUT_READ))))
                .andExpect(status().isOk());
    }

    @Test
    void getMessage_whenPresent_thenReturnsContent() throws Exception {
        UUID messageId = UUID.randomUUID();
        String bpId = UUID.randomUUID().toString();
        MessageContent messageContent = new MessageContent(new ByteArrayInputStream("123".getBytes(UTF_8)), 3);
        when(messageExchangeService.getMessageFromInternalApplication(bpId, messageId))
                .thenReturn(Optional.of(messageContent));

        mockMvc.perform(
                        get("/api/partner/v2/messages/{messageId}", messageId)
                                .header(HEADER_BP_ID, bpId)
                                .contentType(MediaType.APPLICATION_XML_VALUE)
                                .with(authentication(createAuthenticationForBpRoles(bpId, B2B_MESSAGE_OUT_READ))))
                .andExpect(status().isOk());

        verify(messageExchangeService, times(1))
                .getMessageFromInternalApplication(bpId, messageId);
    }

    @Test
    void getMessage_whenNotPresent_thenReturnsNotFound() throws Exception {
        UUID messageId = UUID.randomUUID();
        String bpId = UUID.randomUUID().toString();

        mockMvc.perform(
                        get("/api/partner/v2/messages/{messageId}", messageId)
                                .header(HEADER_BP_ID, bpId)
                                .contentType(MediaType.APPLICATION_XML_VALUE)
                                .with(authentication(createAuthenticationForBpRoles(bpId, B2B_MESSAGE_OUT_READ))))
                .andExpect(status().isNotFound());

        verify(messageExchangeService, times(1))
                .getMessageFromInternalApplication(bpId, messageId);
    }

    @Test
    void getMessages_bpIdMissing_thenReturnsBadRequest() throws Exception {
        mockMvc.perform(
                        get("/api/partner/v2/messages")
                                .contentType(MediaType.APPLICATION_XML_VALUE)
                                .with(authentication(createAuthenticationForBpRoles(UUID.randomUUID().toString(), B2B_MESSAGE_OUT_READ))))
                .andExpect(status().isBadRequest());

        verify(messageExchangeService, never()).getMessageFromInternalApplication(anyString(), any(UUID.class));
    }

    @Test
    void getMessages_notAuthorizedBpRole_thenReturnsForbidden() throws Exception {
        String bpId = UUID.randomUUID().toString();
        mockMvc.perform(
                        get("/api/partner/v2/messages")
                                .header(HEADER_BP_ID, bpId)
                                .contentType(MediaType.APPLICATION_XML_VALUE)
                                .with(authentication(createAuthenticationForBpRoles(bpId, B2B_MESSAGE_IN_WRITE))))
                .andExpect(status().isForbidden());

        verify(messageExchangeService, never()).getMessageFromInternalApplication(anyString(), any(UUID.class));
    }

    @Test
    void getMessages_notAuthorizedUserRole_thenReturnsForbidden() throws Exception {
        String bpId = UUID.randomUUID().toString();
        mockMvc.perform(
                        get("/api/partner/v2/messages")
                                .header(HEADER_BP_ID, bpId)
                                .contentType(MediaType.APPLICATION_XML_VALUE)
                                .with(authentication(createAuthenticationForUserRoles(B2B_MESSAGE_IN_WRITE))))
                .andExpect(status().isForbidden());

        verify(messageExchangeService, never()).getMessageFromInternalApplication(anyString(), any(UUID.class));
    }

    @Test
    void getMessages_wrongBpId_thenReturnsForbidden() throws Exception {
        mockMvc.perform(
                        get("/api/partner/v2/messages")
                                .header(HEADER_BP_ID, UUID.randomUUID())
                                .contentType(MediaType.APPLICATION_XML_VALUE)
                                .with(authentication(createAuthenticationForBpRoles("dummy", B2B_MESSAGE_OUT_READ))))
                .andExpect(status().isForbidden());

        verify(messageExchangeService, never()).getMessageFromInternalApplication(anyString(), any(UUID.class));
    }

    @Test
    void getMessages_withoutAuthentication_thenReturnsUnauthorized() throws Exception {
        mockMvc.perform(
                        get("/api/partner/v2/messages")
                                .header(HEADER_BP_ID, UUID.randomUUID().toString())
                                .contentType(MediaType.APPLICATION_XML_VALUE))
                .andExpect(status().isUnauthorized());

        verify(messageExchangeService, never()).getMessageFromInternalApplication(anyString(), any(UUID.class));
    }

    @Test
    void getMessages_whenRequestOkWithTrailingSlash_thenReturnsResults() throws Exception {
        String bpId = UUID.randomUUID().toString();
        UUID message1Id = UUID.randomUUID();
        UUID message2Id = UUID.randomUUID();
        when(messageExchangeService.getMessages(bpId, null, null, null, null, 1000))
                .thenReturn(List.of(new MessageSearchResultDto(message1Id, "type1", null, null),
                        new MessageSearchResultDto(message2Id, "type2", "groupdId2", "partnerTopic2")));

        mockMvc.perform(
                        get("/api/partner/v2/messages/")
                                .header(HEADER_BP_ID, bpId)
                                .contentType(MediaType.APPLICATION_XML_VALUE)
                                .with(authentication(createAuthenticationForBpRoles(bpId, B2B_MESSAGE_OUT_READ))))
                .andExpect(status().isOk())
                .andExpect(content().string("<messages><message><messageId>" + message1Id + "</messageId><messageType>type1</messageType></message><message><messageId>" + message2Id + "</messageId><messageType>type2</messageType></message></messages>"));

        verify(messageExchangeService, times(1)).getMessages(bpId, null, null, null, null, 1000);
    }

    @Test
    void getMessages_whenRequestWithAllParamsOk_thenReturnsResults() throws Exception {
        String bpId = UUID.randomUUID().toString();
        UUID messageId = UUID.randomUUID();
        String topicName = "topicName";
        String groupId = "groupId";
        UUID lastMessageId = UUID.randomUUID();
        String partnerTopic = "partnerTopic";
        int size = 3;

        when(messageExchangeService.getMessages(bpId, topicName, groupId, lastMessageId, partnerTopic, size))
                .thenReturn(List.of(new MessageSearchResultDto(messageId, "type", null, null)));

        mockMvc.perform(
                        get("/api/partner/v2/messages")
                                .header(HEADER_BP_ID, bpId)
                                .param("topicName", topicName)
                                .param("groupId", groupId)
                                .param("lastMessageId", lastMessageId.toString())
                                .header(HEADER_PARTNER_TOPIC, partnerTopic)
                                .param("size", String.valueOf(size))
                                .contentType(MediaType.APPLICATION_XML_VALUE)
                                .with(authentication(createAuthenticationForBpRoles(bpId, B2B_MESSAGE_OUT_READ))))
                .andExpect(status().isOk())
                .andExpect(content().string("<messages><message><messageId>" + messageId + "</messageId><messageType>type</messageType></message></messages>"));

        verify(messageExchangeService, times(1)).getMessages(bpId, topicName, groupId, lastMessageId, partnerTopic, size);
    }

    @Test
    void getMessages_whenRequestWithAllParamsOk_lastMessageIdUppercaseID_thenReturnsResults() throws Exception {
        String bpId = UUID.randomUUID().toString();
        UUID messageId = UUID.randomUUID();
        String topicName = "topicName";
        String groupId = "groupId";
        UUID lastMessageID = UUID.randomUUID();
        String partnerTopic = "partnerTopic";
        int size = 3;

        when(messageExchangeService.getMessages(bpId, topicName, groupId, lastMessageID, partnerTopic, size))
                .thenReturn(List.of(new MessageSearchResultDto(messageId, "type", "groupdId", "partnerTopic")));

        mockMvc.perform(
                        get("/api/partner/v2/messages")
                                .header(HEADER_BP_ID, bpId)
                                .param("topicName", topicName)
                                .param("groupId", groupId)
                                .param("lastMessageID", lastMessageID.toString())
                                .header(HEADER_PARTNER_TOPIC, partnerTopic)
                                .param("size", String.valueOf(size))
                                .contentType(MediaType.APPLICATION_XML_VALUE)
                                .with(authentication(createAuthenticationForBpRoles(bpId, B2B_MESSAGE_OUT_READ))))
                .andExpect(status().isOk())
                .andExpect(content().string("<messages><message><messageId>" + messageId + "</messageId><messageType>type</messageType></message></messages>"));

        verify(messageExchangeService, times(1)).getMessages(bpId, topicName, groupId, lastMessageID, partnerTopic, size);
    }

    @Test
    void getMessages_whenRequestOkButNoResults_thenReturnsEmptyList() throws Exception {
        String bpId = UUID.randomUUID().toString();
        when(messageExchangeService.getMessages(bpId, null, null, null, null, 1000)).thenReturn(List.of());

        mockMvc.perform(
                        get("/api/partner/v2/messages")
                                .header(HEADER_BP_ID, bpId)
                                .contentType(MediaType.APPLICATION_XML_VALUE)
                                .with(authentication(createAuthenticationForBpRoles(bpId, B2B_MESSAGE_OUT_READ))))
                .andExpect(status().isOk())
                .andExpect(content().string("<messages/>"));

        verify(messageExchangeService, times(1)).getMessages(bpId, null, null, null, null, 1000);
    }

    @Test
    void getNextMessage_bpIdMissing_thenReturnsBadRequest() throws Exception {
        UUID messageId = UUID.randomUUID();

        mockMvc.perform(
                        get("/api/partner/v2/messages/{messageId}/next", messageId)
                                .contentType(MediaType.APPLICATION_XML_VALUE)
                                .with(authentication(createAuthenticationForBpRoles(UUID.randomUUID().toString(), B2B_MESSAGE_OUT_READ))))
                .andExpect(status().isBadRequest());

        verify(messageExchangeService, never()).getMessageFromInternalApplication(anyString(), any(UUID.class));
    }

    @Test
    void getNextMessage_notAuthorizedBpRole_thenReturnsForbidden() throws Exception {
        UUID messageId = UUID.randomUUID();
        String bpId = UUID.randomUUID().toString();
        mockMvc.perform(
                        get("/api/partner/v2/messages/{messageId}/next", messageId)
                                .header(HEADER_BP_ID, bpId)
                                .contentType(MediaType.APPLICATION_XML_VALUE)
                                .with(authentication(createAuthenticationForBpRoles(bpId, B2B_MESSAGE_IN_WRITE))))
                .andExpect(status().isForbidden());

        verify(messageExchangeService, never()).getMessageFromInternalApplication(anyString(), any(UUID.class));
    }

    @Test
    void getNextMessage_notAuthorizedUserRole_thenReturnsForbidden() throws Exception {
        UUID messageId = UUID.randomUUID();
        String bpId = UUID.randomUUID().toString();
        mockMvc.perform(
                        get("/api/partner/v2/messages/{messageId}/next", messageId)
                                .header(HEADER_BP_ID, bpId)
                                .contentType(MediaType.APPLICATION_XML_VALUE)
                                .with(authentication(createAuthenticationForUserRoles(B2B_MESSAGE_IN_WRITE))))
                .andExpect(status().isForbidden());

        verify(messageExchangeService, never()).getMessageFromInternalApplication(anyString(), any(UUID.class));
    }

    @Test
    void getNextMessage_wrongBpId_thenReturnsForbidden() throws Exception {
        UUID messageId = UUID.randomUUID();
        mockMvc.perform(
                        get("/api/partner/v2/messages/{messageId}/next", messageId)
                                .header(HEADER_BP_ID, UUID.randomUUID())
                                .contentType(MediaType.APPLICATION_XML_VALUE)
                                .with(authentication(createAuthenticationForBpRoles("dummy", B2B_MESSAGE_OUT_READ))))
                .andExpect(status().isForbidden());

        verify(messageExchangeService, never()).getMessageFromInternalApplication(anyString(), any(UUID.class));
    }

    @Test
    void getNextMessage_withoutAuthentication_thenReturnsUnauthorized() throws Exception {
        UUID messageId = UUID.randomUUID();
        mockMvc.perform(
                        get("/api/partner/v2/messages/{messageId}/next", messageId)
                                .header(HEADER_BP_ID, UUID.randomUUID().toString())
                                .contentType(MediaType.APPLICATION_XML_VALUE))
                .andExpect(status().isUnauthorized());

        verify(messageExchangeService, never()).getMessageFromInternalApplication(anyString(), any(UUID.class));
    }

    @Test
    void getNextMessage_whenRequestOk_thenReturnsResult() throws Exception {
        String bpId = UUID.randomUUID().toString();
        UUID messageId = UUID.randomUUID();
        byte[] bytes = "<content>test</content>".getBytes(UTF_8);
        MessageContent message = new MessageContent(new ByteArrayInputStream(bytes), bytes.length);
        UUID messageIdFromDatabase = UUID.randomUUID();
        NextMessageResultDto nextMessageResultDto = new NextMessageResultDto(messageIdFromDatabase, message);
        when(messageExchangeService.getNextMessageFromInternalApplication(messageId, bpId, null, null)).thenReturn(Optional.of(nextMessageResultDto));

        mockMvc.perform(
                        get("/api/partner/v2/messages/{messageId}/next", messageId)
                                .header(HEADER_BP_ID, bpId)
                                .contentType(MediaType.APPLICATION_XML_VALUE)
                                .with(authentication(createAuthenticationForBpRoles(bpId, B2B_MESSAGE_OUT_READ))))
                .andExpect(status().isOk())
                .andExpect(content().string("<content>test</content>"))
                .andExpect(header().string(HEADER_MESSAGE_ID, messageIdFromDatabase.toString()))
                //TODO: JEAP-5099 remove old header
                .andExpect(header().string(HEADER_MESSAGE_ID_OLD, messageIdFromDatabase.toString()));

        verify(messageExchangeService, times(1)).getNextMessageFromInternalApplication(messageId, bpId, null, null);
    }

    private JeapAuthenticationToken createAuthenticationForBpRoles(String bpId, SemanticApplicationRole... roles) {
        return JeapAuthenticationTestTokenBuilder.create()
                .withBusinessPartnerRoles(bpId, roles)
                .build();
    }

    private JeapAuthenticationToken createAuthenticationForUserRoles(SemanticApplicationRole... roles) {
        return JeapAuthenticationTestTokenBuilder.create()
                .withUserRoles(roles)
                .build();
    }
}
