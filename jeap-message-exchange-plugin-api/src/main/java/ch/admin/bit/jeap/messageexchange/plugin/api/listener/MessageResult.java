package ch.admin.bit.jeap.messageexchange.plugin.api.listener;


import ch.admin.bit.jeap.messaging.avro.AvroMessage;

public record MessageResult(String topicName, AvroMessage message){
}
