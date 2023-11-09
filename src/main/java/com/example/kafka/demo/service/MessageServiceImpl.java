package com.example.kafka.demo.service;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.stream.IntStream;

@Service
public class MessageServiceImpl implements MessageService {

    private Logger logger = LoggerFactory.getLogger(MessageServiceImpl.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Override
    public void sendFnf(String topic, Object payload) {
        IntStream.rangeClosed(1, 10).forEach(i-> {
            String data = payload + "-" + i;
            kafkaTemplate.send(topic, data);
            logger.info("Message {} published to topic {}.", data, topic);
        });
    }

    @Override
    public void sendSync(String topic, Object payload) {
        IntStream.rangeClosed(1, 10).forEach(i-> {
            SendResult<String, String> sendResult = null;
            String data;
            try {
                data = payload + "-" + i;
                sendResult = kafkaTemplate.send(topic, data).get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            RecordMetadata recordMetadata = sendResult.getRecordMetadata();
            logger.info("Message {} published to topic {} in partition {} for offset {}.", data, recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
        });
    }

    @Override
    public void sendAsync(String topic, Object payload) {
        IntStream.rangeClosed(1, 10).forEach(i-> {
            String data;
            data = payload.toString() + "-" + i;
            ListenableFuture<SendResult<String, String>> lf = kafkaTemplate.send(topic, data);
            lf.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

                @Override
                public void onSuccess(final SendResult<String, String> message) {
                    logger.info("sent message= " + message + " with offset= " + message.getRecordMetadata().offset());
                }

                @Override
                public void onFailure(final Throwable throwable) {
                    logger.error("unable to send message= " + data, throwable);
                }
            });
        });
    }
}
