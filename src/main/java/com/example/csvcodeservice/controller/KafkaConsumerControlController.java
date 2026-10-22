
package com.example.csvcodeservice.controller;

import com.example.csvcodeservice.service.KafkaConsumerControlService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/kafka/consumer")
public class KafkaConsumerControlController {

    private final KafkaConsumerControlService controlService;

    public KafkaConsumerControlController(
            KafkaConsumerControlService controlService) {
        this.controlService = controlService;
    }

    @PostMapping("/pause")
    public String pauseConsumer() {
        controlService.pauseConsumer();
        return "Kafka consumer paused";
    }

    @PostMapping("/resume")
    public String resumeConsumer() {
        controlService.resumeConsumer();
        return "Kafka consumer resumed";
    }
}
