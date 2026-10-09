package com.example.csvcodeservice.service;

import com.example.csvcodeservice.entity.CodeEntity;
import com.example.csvcodeservice.repository.CodeRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);

    private final CodeRepository codeRepository;
    private final ObjectMapper objectMapper;

    public KafkaConsumerService(
            CodeRepository codeRepository,
            ObjectMapper objectMapper
    ) {
        this.codeRepository = codeRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "codes", groupId = "code-consumer-group")
    public void consume(String message) {

        try {

            CodeEntity entity = objectMapper.readValue(message, CodeEntity.class);

            if (codeRepository.existsByCode(entity.getCode())) {
                log.warn("Duplicate code skipped: {}", entity.getCode());
                return;
            }

            codeRepository.save(entity);

            log.info("Code consumed and saved to database. code={}", entity.getCode());

        } catch (Exception e) {

            log.error("Could not process Kafka message", e);
        }
    }
}