package com.example.kafka_consumer_example.service;

import com.example.kafka_consumer_example.dto.Greeting;
import com.example.kafka_consumer_example.dto.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

@Service
public class KafkaConsumerExample {


    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerExample.class);

    //    Below annotation value will read the messages only from partition 2 of 'demo-topic' topic.
//    @KafkaListener(topics = "demo-topic", groupId = "demo-consumer-group", topicPartitions = {@TopicPartition(topic = "demo-topic", partitions = {"2"})})
//    @RetryableTopic(attempts = "4", backoff = @Backoff(delay = 1000, maxDelay = 10000, multiplier = 1.5), exclude = {RuntimeException.class, NullPointerException.class})
    //-> This will tell consume will retry 3(n-1) times for the records there was error and also will internally create n-1 topics for retry.
    //exclude -> to exclude the specific exceptions from retry mechanism. backoff -> to specify the delay/ time interval in which the retry should be performed.
    @KafkaListener(topics = "user", groupId = "user-consumer-group", containerFactory = "containerFactory", properties = {"spring.json.value.default.type=com.example.kafka_consumer_example.dto.User"})
    public void consume(User message) {
        log.info("Message consumed is: {}", message);
    }

//    @DltHandler
//    public void consumeFromDeadLetterTopic(User message) {
//        System.out.println("Message consumed is from DLT: " + message);
//    }

    @KafkaListener(topics = "greeting-1", groupId = "greeting-consumer-group", containerFactory = "containerFactory", properties = {"spring.json.value.default.type=com.example.kafka_consumer_example.dto.Greeting"})
    public void consumeGreeting(@Payload Greeting message,
                                @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long ts) {
        LocalDateTime date =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.systemDefault());
        log.info("Greeting consumed is: {} on partition {}, topic {}, and date is {}", message, partition, topic, date);
    }

}
