package com.techprimers.kafka.springbootkafkaproducerexample.resource;

import com.techprimers.kafka.springbootkafkaproducerexample.model.User;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("kafka")
public class UserResource {

    @Autowired
    private KafkaTemplate<String, User> kafkaTemplate;

    private static final String TOPIC = "nkb";

    @GetMapping("/publish/{name}")
    public String post(@PathVariable("name") final String name, @RequestParam("eventType") final String eventType) {
        List<Header> headers = new ArrayList<>();
        headers.add(new RecordHeader("eventType", eventType.getBytes()));
        ProducerRecord<String, User> bar = new ProducerRecord<>(TOPIC, 0, "111", new User(name, "Technology", 12000L), headers);
        kafkaTemplate.send(bar);

        return "Published successfully";
    }
}
