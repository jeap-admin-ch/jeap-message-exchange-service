package ch.admin.bit.jeap.messageexchange.web.api;

import ch.admin.bit.jeap.messageexchange.domain.Message;
import ch.admin.bit.jeap.messageexchange.domain.MessageContent;
import ch.admin.bit.jeap.messageexchange.domain.MessageExchangeService;
import ch.admin.bit.jeap.security.resource.semanticAuthentication.SemanticApplicationRole;
import ch.admin.bit.jeap.security.resource.token.JeapAuthenticationToken;
import ch.admin.bit.jeap.security.test.resource.JeapAuthenticationTestTokenBuilder;
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
import java.util.Optional;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.authentication;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = MessageInternalController.class)
@ActiveProfiles("controller-test")
@ContextConfiguration(classes = RestApiTestContext.class)
@AutoConfigureMockMvc
//TODO: JEAP-5099 delete class
class MessageInternalControllerOldHeaderITTest {

    @MockitoBean
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
                        put("/api/internal/v2/messages/{messageId}", messageId)
                                .header("bpId", "bpId")
                                .param("topicName", "topicName")
                                .param("groupId", "groupId")
                                .header("messageType", "messageType")
                                .header("partnerTopic", "partnerTopic")
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
                        put("/api/internal/v2/messages/{messageId}", messageId)
                                .header("bpId", "bpId")
                                .param("topicName", "topicName")
                                .header("messageType", "messageType")
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
                .build();

        verify(messageExchangeService, times(1))
                .saveNewMessageFromInternalApplication(messageCaptor.capture(), messageContentCaptor.capture());

        Message value = messageCaptor.getValue();
        assertThat(value.getMessageId()).isEqualTo(message.getMessageId());

        MessageContent messageContent = messageContentCaptor.getValue();
        assertThat(messageContent.contentLength()).isEqualTo(XML_CONTENT.getBytes(UTF_8).length);
    }

    @Test
    void putNewMessageFromInternalApplication_missingRequiredField_thenReturnsBadRequest() throws Exception {
        UUID messageId = UUID.randomUUID();

        mockMvc.perform(
                        put("/api/internal/v2/messages/{messageId}", messageId)
                                .header("bpId", "bpId")
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
                        put("/api/internal/v2/messages/{messageId}", messageId)
                                .header("bpId", "bpId")
                                .param("topicName", "topicName")
                                .header("messageType", "messageType")
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
                        put("/api/internal/v2/messages/{messageId}", messageId)
                                .header("bpId", "bpId")
                                .param("topicName", "topicName")
                                .header("messageType", "messageType")
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
        when(messageExchangeService.getMessageFromPartner(messageId)).thenReturn(Optional.of(messageContent));
        mockMvc.perform(
                        get("/api/internal/v2/messages/{messageId}", messageId)
                                .contentType(MediaType.APPLICATION_XML_VALUE)
                                .with(authentication(createAuthenticationForUserRoles(B2B_MESSAGE_IN_READ))))
                .andExpect(status().isOk())
                .andExpect(content().string(xmlContent));
    }

    @Test
    void getMessageFromPartner_messageNotExists_thenReturnsNotFound() throws Exception {
        mockMvc.perform(
                        get("/api/internal/v2/messages/{messageId}", UUID.randomUUID())
                                .contentType(MediaType.APPLICATION_XML_VALUE)
                                .with(authentication(createAuthenticationForUserRoles(B2B_MESSAGE_IN_READ))))
                .andExpect(status().isNotFound());
    }

    @Test
    void getMessageFromPartner_notAuthorized_thenReturnsForbidden() throws Exception {
        mockMvc.perform(
                        get("/api/internal/v2/messages/{messageId}", UUID.randomUUID())
                                .contentType(MediaType.APPLICATION_XML_VALUE)
                                .with(authentication(createAuthenticationForUserRoles(B2B_MESSAGE_OUT_WRITE))))
                .andExpect(status().isForbidden());
    }

    @Test
    void getMessageFromPartner_noAuthorization_thenReturnsUnauthorized() throws Exception {
        mockMvc.perform(
                        get("/api/internal/v2/messages/{messageId}", UUID.randomUUID())
                                .contentType(MediaType.APPLICATION_XML_VALUE)
                )
                .andExpect(status().isUnauthorized());
    }

    private JeapAuthenticationToken createAuthenticationForUserRoles(SemanticApplicationRole... userroles) {
        return JeapAuthenticationTestTokenBuilder.create()
                .withUserRoles(userroles)
                .build();
    }
}
