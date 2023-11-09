package com.example.kafka.demo.controller;

import com.example.kafka.demo.service.MessageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/test")
public class TestController {

    private KafkaTemplate<String, String> kafkaTemplate;
    private MessageService messageService;

    public TestController(KafkaTemplate<String, String> kafkaTemplate, MessageService messageService) {
        this.kafkaTemplate = kafkaTemplate;
        this.messageService = messageService;
    }

    private Logger logger = LoggerFactory.getLogger(TestController.class);

    @PostMapping("/message/publish/fnf")
    public Map<String, Object> sendMessageFF(@RequestBody Map<String, Object> request) {
        String topic = request.get("topic").toString();
        Object payload = request.get("payload");
        Date d1 = new Date();

        messageService.sendFnf(topic, payload);
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

        messageService.sendSync(topic, payload);
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

        messageService.sendAsync(topic, payload);
        logger.info("Total time taken :: {} ms.", (new Date().getTime() - d1.getTime()));

        Map<String, Object> responseBody = new HashMap<>();
        responseBody.put("code", HttpStatus.OK.value());
        responseBody.put("status", HttpStatus.OK.name());
        return responseBody;
    }
}
