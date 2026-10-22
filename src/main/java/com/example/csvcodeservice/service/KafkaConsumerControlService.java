
package com.example.csvcodeservice.service;

import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerControlService {

    private final KafkaListenerEndpointRegistry registry;

    public KafkaConsumerControlService(
            KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    public void pauseConsumer() {
        MessageListenerContainer container =
                getListenerContainer();

        container.pause();
    }

    public void resumeConsumer() {
        MessageListenerContainer container =
                getListenerContainer();

        container.resume();
    }

    private MessageListenerContainer getListenerContainer() {
        MessageListenerContainer container =
                registry.getListenerContainer("codesListener");

        if (container == null) {
            throw new IllegalStateException(
                    "Kafka listener 'codesListener' was not found"
            );
        }

        return container;
    }
}
