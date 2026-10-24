
package com.example.csvcodeservice.service;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class KafkaOffsetRangeConsumer {

    private final ConsumerFactory<String, String> consumerFactory;

    public KafkaOffsetRangeConsumer(
            ConsumerFactory<String, String> consumerFactory) {
        this.consumerFactory = consumerFactory;
    }


    public List<OffsetRecord> readRange(
            String topic,
            int partitionNumber,
            long startOffset,
            long endOffset) {

        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Topic must not be blank");
        }
        if (partitionNumber < 0) {
            throw new IllegalArgumentException("Partition must not be negative");
        }
        if (startOffset < 0 || endOffset < startOffset) {
            throw new IllegalArgumentException("Invalid offset range");
        }

        TopicPartition partition =
                new TopicPartition(topic, partitionNumber);

        try (Consumer<String, String> consumer =
                     consumerFactory.createConsumer()) {

            consumer.assign(List.of(partition));

            long beginningOffset =
                    consumer.beginningOffsets(List.of(partition)).get(partition);
            long endOffsetExclusive =
                    consumer.endOffsets(List.of(partition)).get(partition);

            if (startOffset < beginningOffset) {
                throw new IllegalArgumentException(
                        "Start offset is no longer available");
            }

            if (startOffset >= endOffsetExclusive
                    || endOffset >= endOffsetExclusive) {
                throw new IllegalArgumentException(
                        "Requested offset range is not available");
            }

            consumer.seek(partition, startOffset);

            List<OffsetRecord> records = new ArrayList<>();

            while (consumer.position(partition) <= endOffset) {
                var polledRecords = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record
                        : polledRecords.records(partition)) {

                    if (record.offset() > endOffset) {
                        break;
                    }

                    if (record.offset() >= startOffset) {
                        records.add(new OffsetRecord(
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                record.key(),
                                record.value(),
                                record.timestamp()
                        ));
                    }
                }
            }

            return records;
        }
    }


    public record OffsetRecord(
            String topic,
            int partition,
            long offset,
            String key,
            String value,
            long timestamp) {
    }
}
