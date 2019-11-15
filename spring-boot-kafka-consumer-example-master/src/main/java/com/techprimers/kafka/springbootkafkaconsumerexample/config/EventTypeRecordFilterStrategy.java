package com.techprimers.kafka.springbootkafkaconsumerexample.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class EventTypeRecordFilterStrategy implements RecordFilterStrategy<String, String> {
    private final List<String> interestList;
    private final String eventType="eventType";

    public EventTypeRecordFilterStrategy(List<String> interestList) {
        this.interestList=interestList;
    }

    @Override
    public boolean filter(ConsumerRecord<String, String> consumerRecord) {
        final Optional<String> any = Stream.of(consumerRecord.headers().toArray())
                .filter(header -> eventType.equalsIgnoreCase(header.key()))
                .map(header -> new String((byte[]) header.value()))
                .filter(interestList::contains)
                .findAny();

        return any.isPresent()?false:true;
    }
}