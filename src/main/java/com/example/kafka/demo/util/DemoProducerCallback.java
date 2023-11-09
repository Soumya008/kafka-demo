package com.example.kafka.demo.util;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DemoProducerCallback implements Callback {

    private Logger logger = LoggerFactory.getLogger(DemoProducerCallback.class);

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {

        logger.info("Message published to topic {} in partition {} for offset {}.", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
    }
}
