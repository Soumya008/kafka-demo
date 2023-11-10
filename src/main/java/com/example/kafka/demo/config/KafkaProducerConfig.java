package com.example.kafka.demo.config;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;

    @Value(value = "${spring.kafka.batch.size}")
    private int batchSize;

    @Value(value = "${spring.kafka.linger.ms}")
    private int lingerMs;

    @Value(value = "${spring.kafka.properties.schema.registry.url}")
    private String schemaRegistryUri;

    @Value(value = "${spring.kafka.properties.basic.auth.credentials.source}")
    private String schemaRegistryCredentialSource;

    @Value(value = "${spring.kafka.properties.schema.registry.basic.auth.user.info}")
    private String schemaRegistryCredential;

    @Value(value = "${spring.kafka.avro.enabled}")
    private boolean avroEnabled;

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
          bootstrapAddress);
        configProps.put(
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
          StringSerializer.class);
        configProps.put(
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                avroEnabled ? KafkaAvroSerializer.class : StringSerializer.class);
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);

        if(avroEnabled) {
            configProps.put("schema.registry.url", schemaRegistryUri);
            configProps.put("basic.auth.credentials.source", schemaRegistryCredentialSource);
            configProps.put("schema.registry.basic.auth.user.info", schemaRegistryCredential);
        }
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}