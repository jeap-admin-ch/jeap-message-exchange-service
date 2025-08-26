package ch.admin.bit.jeap.messageexchange.domain.sent;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "jeap.messageexchange.messagesent")
public class MessageSentProperties {

    /**
     * Enable or disable the message sent event. It's disabled by default.
     */
    private boolean enabled;
}
