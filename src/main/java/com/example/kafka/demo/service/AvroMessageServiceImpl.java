package com.example.kafka.demo.service;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.example.kafka.demo.model.User;

@Service("avroMessageService")
@Qualifier("avroMessageService")
public class AvroMessageServiceImpl implements MessageService {

    private Logger logger = LoggerFactory.getLogger(AvroMessageServiceImpl.class);

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Override
    public void sendFnf(String topic, Object payload) {
        kafkaTemplate.send(topic, (User) payload);
        logger.info("Message {} published to topic {}.", payload, topic);
    }

    @Override
    public void sendSync(String topic, Object payload) {
        SendResult<String, Object> sendResult = null;
        try {
            sendResult = kafkaTemplate.send(topic, payload).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        RecordMetadata recordMetadata = sendResult.getRecordMetadata();
        logger.info("Message {} published to topic {} in partition {} for offset {}.", payload, recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());

    }

    @Override
    public void sendAsync(String topic, Object payload) {
        ListenableFuture<SendResult<String, Object>> lf = kafkaTemplate.send(topic, payload);
        lf.addCallback(new ListenableFutureCallback<>() {

            @Override
            public void onSuccess(final SendResult<String, Object> message) {
                logger.info("sent message= " + message + " with offset= " + message.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(final Throwable throwable) {
                logger.error("unable to send message= " + payload, throwable);
            }
        });
    }
}
