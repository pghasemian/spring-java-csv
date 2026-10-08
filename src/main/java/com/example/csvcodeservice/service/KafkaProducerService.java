package com.example.csvcodeservice.service;

import com.example.csvcodeservice.entity.CodeEntity;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);

    private static final String TOPIC = "codes";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate,
                                ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void send(CodeEntity entity, int partitionId) {

        try {
            String message = objectMapper.writeValueAsString(entity);

            kafkaTemplate.send(TOPIC, partitionId, entity.getCode(),
                    message);

            log.info(
                    "Code sent to Kafka. code={}, partition={}",
                    entity.getCode(), partitionId);

        } catch (JsonProcessingException e) {

            log.error(
                    "Could not serialize code: {}",
                    entity.getCode(),
                    e
            );

            throw new RuntimeException("Could not serialize code");
        }
    }
}