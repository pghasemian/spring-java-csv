
package com.example.csvcodeservice.service;

import com.example.csvcodeservice.entity.CodeEntity;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
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

    private final Counter messagesSent;
    private final Counter messagesFailed;

    public KafkaProducerService(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry) {

        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;

        this.messagesSent = Counter.builder("app.kafka.producer.messages")
                .description("Number of messages successfully sent to Kafka")
                .tag("result", "success")
                .register(meterRegistry);

        this.messagesFailed = Counter.builder("app.kafka.producer.messages")
                .description("Number of messages that failed to send to Kafka")
                .tag("result", "failure")
                .register(meterRegistry);
    }

    public void send(CodeEntity entity, int partitionId) {
        final String message;

        try {
            message = objectMapper.writeValueAsString(entity);
        } catch (JsonProcessingException e) {
            messagesFailed.increment();

            log.error("Could not serialize code: {}", entity.getCode(), e);

            throw new RuntimeException("Could not serialize code", e);
        }

        kafkaTemplate.send(TOPIC, partitionId, entity.getCode(), message)
                .whenComplete((result, exception) -> {
                    if (exception == null) {
                        messagesSent.increment();

                        log.info("Code sent to Kafka. code={}, partition={}", entity.getCode(), partitionId);
                    } else {
                        messagesFailed.increment();

                        log.error("Failed to send code to Kafka. code={}, partition={}",
                                entity.getCode(), partitionId, exception);
                    }
                });
    }
}