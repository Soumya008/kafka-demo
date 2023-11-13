package com.example.kafka.demo.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TestTopicConsumer {

    private Logger logger = LoggerFactory.getLogger(TestTopicConsumer.class);

    @KafkaListener(topics = "test-topic", groupId = "test-consumer", containerFactory = "kafkaListenerContainerFactory")
    public void consumeMessage(ConsumerRecord<String, Object> record) {
        logger.info("Received Message in group test-consumer, topic test-topic is :: {}", record.value());
    }

    @KafkaListener(topics = "output-topic", groupId = "test-consumer", containerFactory = "kafkaListenerContainerFactory")
    public void consumeOutputStreamMessage(ConsumerRecord<String, Object> record) {
        logger.info("Received Message in group test-consumer, topic output-topic is :: {}", record.value());
    }
}
