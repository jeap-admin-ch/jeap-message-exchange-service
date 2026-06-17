package ch.admin.bit.jeap.messageexchange.web.api.mdc;

import org.slf4j.MDC;

import java.io.Closeable;
import java.util.UUID;

/**
 * A wrapper that puts the messageId and/or bpId into the {@link MDC} to automatically add it to all log statements
 */
public class MessageIdBpIdMdcCloseable implements Closeable {

    private static final String MESSAGE_ID = "messageId";
    private static final String BP_ID = "bpId";

    private MessageIdBpIdMdcCloseable() {
    }

    public static MessageIdBpIdMdcCloseable mdcMessageIdAndBpId(UUID messageId, String bpId) {
        MDC.put(MESSAGE_ID, messageId.toString());
        MDC.put(BP_ID, bpId);
        return new MessageIdBpIdMdcCloseable();
    }

    public static MessageIdBpIdMdcCloseable mdcMessageId(UUID messageId) {
        MDC.put(MESSAGE_ID, messageId.toString());
        return new MessageIdBpIdMdcCloseable();
    }

    public static MessageIdBpIdMdcCloseable mdcBpId(String bpId) {
        MDC.put(BP_ID, bpId);
        return new MessageIdBpIdMdcCloseable();
    }

    public void close() {
        MDC.remove(MESSAGE_ID);
        MDC.remove(BP_ID);
    }
}
