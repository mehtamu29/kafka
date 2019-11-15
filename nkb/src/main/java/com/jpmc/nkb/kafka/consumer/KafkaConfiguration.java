package com.jpmc.nkb.kafka.consumer;

import com.jpmc.nkb.kafka.consumer.listener.EventTypeRecordFilterStrategy;
import com.jpmc.nkb.kafka.consumer.listener.KafkaConsumer;
import com.jpmc.nkb.kafka.consumer.listener.KafkaConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.listener.adapter.FilteringMessageListenerAdapter;
import org.springframework.kafka.support.Acknowledgment;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfiguration {
    @Value("${spring.kafka.bootstrap-servers}")
    private String broker;
    @Value("${spring.kafka.topic}")
    private String topic;
    @Value("${spring.kafka.group}")
    private String group;
    @Value("${spring.kafka.interestList}")
    private String[] interestList;

    @Bean
    public static PropertySourcesPlaceholderConfigurer placeHolderConfigurer() {
        final PropertySourcesPlaceholderConfigurer property = new PropertySourcesPlaceholderConfigurer();
        property.setLocation(new ClassPathResource("/nkb.properties"));
        return property;
    }
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(config);
    }
    @Bean
    public KafkaConsumer kafkaConsumer() {
        return new KafkaConsumer();
    }
    @Bean
    public EventTypeRecordFilterStrategy eventTypeRecordFilterStrategy() {
        return new EventTypeRecordFilterStrategy(interestList);
    }
    @Bean
    public KafkaConsumerRebalanceListener kafkaConsumerRebalanceListener(){
        return new KafkaConsumerRebalanceListener();
    }
    @Bean
    public ContainerProperties containerProperties() {
        final ContainerProperties properties = new ContainerProperties(topic);
        properties.setConsumerRebalanceListener(kafkaConsumerRebalanceListener());
        properties.setMessageListener(filteringMessageListenerAdapter());
        return properties;
    }
    @Bean
    public KafkaMessageListenerContainer messageListenerContainer(){
        KafkaMessageListenerContainer<String, String> container = new KafkaMessageListenerContainer<>(consumerFactory(), containerProperties());
        container.start();
        return container;
    }
    @Bean
    public FilteringMessageListenerAdapter<String, String> filteringMessageListenerAdapter(){
        return new FilteringMessageListenerAdapter(kafkaConsumer(),eventTypeRecordFilterStrategy());
    }
}