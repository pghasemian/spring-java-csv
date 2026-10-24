
package com.example.csvcodeservice.service;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.ConsumerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class KafkaOffsetRangeConsumerTest {

    @SuppressWarnings("unchecked")
    private final ConsumerFactory<String, String> consumerFactory =
            mock(ConsumerFactory.class);

    @SuppressWarnings("unchecked")
    private final Consumer<String, String> consumer =
            mock(Consumer.class);

    private final KafkaOffsetRangeConsumer service =
            new KafkaOffsetRangeConsumer(consumerFactory);

    @Test
    void shouldRejectNegativePartition() {
        assertThrows(
                IllegalArgumentException.class,
                () -> service.readRange("codes", -1, 0, 10)
        );

        verifyNoInteractions(consumerFactory);
    }

    @Test
    void shouldRejectInvalidOffsetRange() {
        assertThrows(
                IllegalArgumentException.class,
                () -> service.readRange("codes", 1, 10, 5)
        );

        verifyNoInteractions(consumerFactory);
    }

    @Test
    void shouldRejectRangeBeyondEndOfPartition() {
        TopicPartition partition = new TopicPartition("codes", 1);

        when(consumerFactory.createConsumer()).thenReturn(consumer);
        when(consumer.beginningOffsets(List.of(partition)))
                .thenReturn(Map.of(partition, 0L));
        when(consumer.endOffsets(List.of(partition)))
                .thenReturn(Map.of(partition, 10L));

        assertThrows(
                IllegalArgumentException.class,
                () -> service.readRange("codes", 1, 10, 20)
        );

        verify(consumer).close();
    }

    @Test
    void shouldReadOnlyRequestedPartitionAndOffsetRange() {
        TopicPartition partition = new TopicPartition("codes", 1);
        when(consumer.beginningOffsets(List.of(partition)))
                .thenReturn(Map.of(partition, 0L));
        when(consumerFactory.createConsumer()).thenReturn(consumer);
        when(consumer.endOffsets(List.of(partition)))
                .thenReturn(Map.of(partition, 30L));
        when(consumer.position(partition))
                .thenReturn(18L, 23L);

        ConsumerRecord<String, String> record18 =
                new ConsumerRecord<>("codes", 1, 18L, "key18", "value18");
        ConsumerRecord<String, String> record19 =
                new ConsumerRecord<>("codes", 1, 19L, "key19", "value19");
        ConsumerRecord<String, String> record20 =
                new ConsumerRecord<>("codes", 1, 20L, "key20", "value20");
        ConsumerRecord<String, String> record21 =
                new ConsumerRecord<>("codes", 1, 21L, "key21", "value21");
        ConsumerRecord<String, String> record22 =
                new ConsumerRecord<>("codes", 1, 22L, "key22", "value22");

        ConsumerRecords<String, String> records =
                new ConsumerRecords<>(Map.of(
                        partition,
                        List.of(record18, record19, record20, record21, record22)
                ));

        when(consumer.poll(any(Duration.class))).thenReturn(records);

        var result = service.readRange("codes", 1, 18, 22);

        assertEquals(5, result.size());
        assertEquals(
                List.of(18L, 19L, 20L, 21L, 22L),
                result.stream()
                        .map(KafkaOffsetRangeConsumer.OffsetRecord::offset)
                        .toList()
        );

        verify(consumer).assign(List.of(partition));
        verify(consumer).seek(partition, 18L);
        verify(consumer).close();
    }
}
