package com.jpmc.nkb.kafka.consumer.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

public class KafkaConsumer implements MessageListener<String, String>{
    @Override
    public void onMessage(ConsumerRecord<String, String> record) {
        System.out.println(record.key() + " = " + record.value() + " partition " + record.partition() + " offset " + record.offset());
    }
}