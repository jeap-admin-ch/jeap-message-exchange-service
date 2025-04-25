package ch.admin.bit.jeap.messageexchange.test;

import lombok.experimental.UtilityClass;

@UtilityClass
public class Pacticipants {

    public static final String MESSAGE_EXCHANGE_PROPERTY_NAME = "message-exchange-service";
    public static final String MESSAGE_EXCHANGE = "${" + MESSAGE_EXCHANGE_PROPERTY_NAME + "}";


    public static void setMessageExchangePacticipant(String messageExchangePacticipant) {
        System.setProperty(MESSAGE_EXCHANGE_PROPERTY_NAME, messageExchangePacticipant);
    }

}
