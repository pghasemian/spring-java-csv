
package com.example.csvcodeservice.service;

import com.example.csvcodeservice.entity.CodeEntity;
import com.example.csvcodeservice.repository.CodeRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class KafkaConsumerServiceTest {

    private CodeRepository codeRepository;
    private ObjectMapper objectMapper;
    private Acknowledgment acknowledgment;
    private KafkaConsumerService consumerService;
    private MeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        codeRepository = mock(CodeRepository.class);
        objectMapper = mock(ObjectMapper.class);
        acknowledgment = mock(Acknowledgment.class);
        meterRegistry = new SimpleMeterRegistry();

        consumerService = new KafkaConsumerService(
                codeRepository,
                objectMapper,
                meterRegistry
        );
    }

    @Test
    void shouldSaveNewCodeAndAcknowledgeMessage() throws Exception {
        CodeEntity entity = new CodeEntity();
        entity.setCode("TEST001");

        when(objectMapper.readValue("valid-json", CodeEntity.class))
                .thenReturn(entity);
        when(codeRepository.existsByCode("TEST001")).thenReturn(false);

        consumerService.consume("valid-json", acknowledgment);

        verify(codeRepository).save(entity);
        verify(acknowledgment).acknowledge();

        assertEquals(1.0,
                meterRegistry.counter("app.kafka.consumer.messages", "result", "success").count());

        assertEquals(0.0,
                meterRegistry.counter("app.kafka.consumer.messages", "result", "failure").count());
    }

    @Test
    void shouldAcknowledgeDuplicateWithoutSavingAgain() throws Exception {
        CodeEntity entity = new CodeEntity();
        entity.setCode("TEST001");

        when(objectMapper.readValue("valid-json", CodeEntity.class))
                .thenReturn(entity);
        when(codeRepository.existsByCode("TEST001")).thenReturn(true);

        consumerService.consume("valid-json", acknowledgment);

        verify(codeRepository, never()).save(any(CodeEntity.class));
        verify(acknowledgment).acknowledge();

        assertEquals(1.0,
                meterRegistry.counter("app.kafka.consumer.messages", "result", "success").count());
    }

    @Test
    void shouldNotAcknowledgeWhenDeserializationFails() throws Exception {
        when(objectMapper.readValue("invalid-json", CodeEntity.class))
                .thenThrow(new JsonProcessingException("Invalid JSON") {
                });

        assertThrows(RuntimeException.class, () -> consumerService.consume("invalid-json", acknowledgment));

        verify(acknowledgment, never()).acknowledge();

        assertEquals(1.0,
                meterRegistry.counter("app.kafka.consumer.messages", "result", "failure").count());
    }
}
