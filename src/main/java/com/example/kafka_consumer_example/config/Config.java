package com.example.kafka_consumer_example.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class Config {

    @Bean
    public Map<String, Object> consumerConfig() {
        Map<String, Object> properties = new HashMap<>();

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.kafka_producer_example.model,com.example.kafka_consumer_example.dto");
//        properties.put(JsonDeserializer.TYPE_MAPPINGS, "user:com.example.kafka_consumer_example.dto.User");
//        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "user-consumer-group");
        //since the package type is coming from producer so don't use type info and since type info is not used we have to give a default value type
        properties.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, "false");
//        properties.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.example.kafka_consumer_example.dto.User");

        return properties;
    }

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfig());
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Object>> containerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();

        //Below property creates this many consumers
        factory.setConcurrency(3);
        factory.setConsumerFactory(consumerFactory());

        return factory;
    }


}
