package com.example.kafka.demo.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TestTopicConsumer {

    private Logger logger = LoggerFactory.getLogger(TestTopicConsumer.class);

    @KafkaListener(topics = "test-topic", groupId = "test-consumer")
    public void consumeMessage(String message) {
        logger.info("Received Message in group test-consumer, topic test-topic is :: {}", message);
    }
}
