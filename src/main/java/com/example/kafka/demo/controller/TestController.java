package com.example.kafka.demo.controller;

import com.example.kafka.demo.model.User;
import com.example.kafka.demo.processor.WordCountProcessor;
import com.example.kafka.demo.service.MessageService;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.*;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/test")
public class TestController {

    @Value(value = "${spring.kafka.avro.enabled}")
    private boolean avroEnabled;

    private MessageService messageService;

    private MessageService avroMessageService;

    private StreamsBuilderFactoryBean factoryBean;

    public TestController(@Qualifier("defaultMessageService") MessageService messageService,
                          @Qualifier("avroMessageService") MessageService avroMessageService,
                          StreamsBuilderFactoryBean factoryBean,
                          WordCountProcessor wordCountProcessor) {
        this.messageService = messageService;
        this.avroMessageService = avroMessageService;
        this.factoryBean = factoryBean;
    }

    private Logger logger = LoggerFactory.getLogger(TestController.class);

    @PostMapping("/message/publish/fnf")
    public Map<String, Object> sendMessageFF(@RequestBody Map<String, Object> request) {
        String topic = request.get("topic").toString();
        Object payload = request.get("payload");
        Date d1 = new Date();

        if(avroEnabled) {
            Map<String, Object> data = (Map<String, Object>) payload;
            User user = new User();
            user.setAge(Integer.parseInt("" + data.get("age")));
            user.setName("" + data.get("name"));
            avroMessageService.sendFnf(topic, user);
        }
        else {
            messageService.sendFnf(topic, payload);
        }

        logger.info("Total time taken :: {} ms.", (new Date().getTime() - d1.getTime()));

        Map<String, Object> responseBody = new HashMap<>();
        responseBody.put("code", HttpStatus.OK.value());
        responseBody.put("status", HttpStatus.OK.name());
        return responseBody;
    }

    @PostMapping("/message/publish/synchronous")
    public Map<String, Object> sendMessage(@RequestBody Map<String, Object> request) {
        String topic = request.get("topic").toString();
        Object payload = request.get("payload");
        Date d1 = new Date();
        if(avroEnabled) {
            Map<String, Object> data = (Map<String, Object>) payload;
            User user = new User();
            user.setAge(Integer.parseInt("" + data.get("age")));
            user.setName("" + data.get("name"));
            avroMessageService.sendSync(topic, user);
        }
        else {
            messageService.sendSync(topic, payload);
        }
        logger.info("Total time taken :: {} ms.", (new Date().getTime() - d1.getTime()));

        Map<String, Object> responseBody = new HashMap<>();
        responseBody.put("code", HttpStatus.OK.value());
        responseBody.put("status", HttpStatus.OK.name());
        return responseBody;
    }

    @PostMapping("/message/publish/asynchronous")
    public Map<String, Object> sendMessageAsync(@RequestBody Map<String, Object> request) {
        String topic = request.get("topic").toString();
        Object payload = request.get("payload");
        Date d1 = new Date();
        if(avroEnabled) {
            Map<String, Object> data = (Map<String, Object>) payload;
            User user = new User();
            user.setAge(Integer.parseInt("" + data.get("age")));
            user.setName("" + data.get("name"));
            avroMessageService.sendAsync(topic, user);
        }
        else {
            messageService.sendAsync(topic, payload);
        }
        logger.info("Total time taken :: {} ms.", (new Date().getTime() - d1.getTime()));

        Map<String, Object> responseBody = new HashMap<>();
        responseBody.put("code", HttpStatus.OK.value());
        responseBody.put("status", HttpStatus.OK.name());
        return responseBody;
    }

    @GetMapping("/word-count/{word}")
    public Long getWordCount(@PathVariable String word) {
        KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
        ReadOnlyKeyValueStore<String, Long> counts = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType("counts", QueryableStoreTypes.keyValueStore())
        );
        return counts.get(word);
    }

    @PostMapping("/word-count/publish")
    public boolean publishWordCount(@RequestBody String word) {
        messageService.sendSync("input-topic", word);
        return true;
    }
}
