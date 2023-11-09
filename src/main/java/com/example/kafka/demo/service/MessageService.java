package com.example.kafka.demo.service;

public interface MessageService {
    /**
     * Publish to Kafka broker and forget. Do not wait for the acknowledgement from broker.
     * @param topic
     * @param payload
     */
    void sendFnf(String topic, Object payload);

    /**
     * Publish to Kafka broker and wait for the response of Kafka broker. This is a synchronous call,
     * will take comparatively more time than the FNF / Async method.
     * @param topic
     * @param payload
     */
    void sendSync(String topic, Object payload);

    /**
     * Publish to kafka broker, acknowledgement will come as a callback. It will
     * not wait for the acknowledgement and can publish next message to the broker.
     * @param topic
     * @param payload
     */
    void sendAsync(String topic, Object payload);
}
