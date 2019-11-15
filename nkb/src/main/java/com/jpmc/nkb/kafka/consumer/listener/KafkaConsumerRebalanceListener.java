package com.jpmc.nkb.kafka.consumer.listener;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class KafkaConsumerRebalanceListener implements ConsumerAwareRebalanceListener {
        public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        System.out.println(partitions);
        final Map<TopicPartition, Long> map = partitions.stream().map(partition -> Pair.of(partition, consumer.position(partition)))
                .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
        String topic = map.keySet().stream().findAny().get().topic();

        TopicPartition topicPartition = new TopicPartition(topic, 0);
        consumer.seek(topicPartition,120);
//        consumer.seekToBeginning(partitions);
    }
}