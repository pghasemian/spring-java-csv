
package com.example.csvcodeservice.service;

import com.example.csvcodeservice.entity.CodeEntity;
import com.example.csvcodeservice.repository.CodeRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);

    private final CodeRepository codeRepository;
    private final ObjectMapper objectMapper;

    private final Counter messagesProcessed;
    private final Counter messagesFailed;

    public KafkaConsumerService(
            CodeRepository codeRepository,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry
    ) {
        this.codeRepository = codeRepository;
        this.objectMapper = objectMapper;

        this.messagesProcessed =
                Counter.builder("app.kafka.consumer.messages")
                        .description("Number of Kafka messages processed successfully")
                        .tag("result", "success")
                        .register(meterRegistry);

        this.messagesFailed =
                Counter.builder("app.kafka.consumer.messages")
                        .description("Number of Kafka messages that failed processing")
                        .tag("result", "failure")
                        .register(meterRegistry);
    }

    @KafkaListener(id = "codesListener", topics = "codes", groupId = "code-consumer-group")
    public void consume(String message, Acknowledgment acknowledgment) {
        try {
            CodeEntity entity = objectMapper.readValue(message, CodeEntity.class);

            if (codeRepository.existsByCode(entity.getCode())) {
                log.warn("Duplicate code skipped: {}", entity.getCode());

                acknowledgment.acknowledge();
                messagesProcessed.increment();
                return;
            }

            codeRepository.save(entity);

            log.info("Code consumed and saved to database. code={}", entity.getCode());

            acknowledgment.acknowledge();
            messagesProcessed.increment();

        } catch (Exception e) {
            messagesFailed.increment();

            log.error("Could not process Kafka message", e);

            throw new RuntimeException("Failed to process Kafka message", e);
        }
    }
}
