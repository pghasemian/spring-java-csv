
package com.example.csvcodeservice.controller;

import com.example.csvcodeservice.dto.KafkaLagDto;
import com.example.csvcodeservice.service.KafkaLagService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/kafka")
public class KafkaLagController {

    private final KafkaLagService kafkaLagService;

    public KafkaLagController(KafkaLagService kafkaLagService) {
        this.kafkaLagService = kafkaLagService;
    }

    @GetMapping("/lag")
    public KafkaLagDto getLag() {
        return kafkaLagService.getLag();
    }
}
