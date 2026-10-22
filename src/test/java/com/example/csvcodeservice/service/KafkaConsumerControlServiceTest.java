
package com.example.csvcodeservice.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class KafkaConsumerControlServiceTest {

    private KafkaListenerEndpointRegistry registry;
    private MessageListenerContainer container;
    private KafkaConsumerControlService controlService;

    @BeforeEach
    void setUp() {
        registry = mock(KafkaListenerEndpointRegistry.class);
        container = mock(MessageListenerContainer.class);

        controlService = new KafkaConsumerControlService(registry);
    }

    @Test
    void shouldPauseConsumer() {
        when(registry.getListenerContainer("codesListener"))
                .thenReturn(container);

        controlService.pauseConsumer();

        verify(container).pause();
    }

    @Test
    void shouldResumeConsumer() {
        when(registry.getListenerContainer("codesListener"))
                .thenReturn(container);

        controlService.resumeConsumer();

        verify(container).resume();
    }

    @Test
    void shouldThrowExceptionWhenListenerIsNotFound() {
        when(registry.getListenerContainer("codesListener"))
                .thenReturn(null);

        assertThrows(
                IllegalStateException.class,
                () -> controlService.pauseConsumer()
        );
    }
}
