package ch.admin.bit.jeap.messageexchange.plugin.api.listener;

import java.util.UUID;

public interface MessageReceivedListener {

    MessageResult onMessageReceived(UUID messageId, String bpId, String type);

}
