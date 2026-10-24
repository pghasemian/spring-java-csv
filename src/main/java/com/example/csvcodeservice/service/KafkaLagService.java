
package com.example.csvcodeservice.service;

import com.example.csvcodeservice.dto.KafkaLagDto;
import com.example.csvcodeservice.dto.PartitionLagDto;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class KafkaLagService {

    private static final Logger log =
            LoggerFactory.getLogger(KafkaLagService.class);

    private final String bootstrapServers;
    private final String topic;
    private final String consumerGroup;
    private final MeterRegistry meterRegistry;

    private final Map<Integer, AtomicLong> partitionLags =
            new ConcurrentHashMap<>();

    public KafkaLagService(
            @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
            String bootstrapServers,
            @Value("${app.kafka.topic:codes}")
            String topic,
            @Value("${app.kafka.consumer-group:code-consumer-group}")
            String consumerGroup,
            MeterRegistry meterRegistry) {

        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.consumerGroup = consumerGroup;
        this.meterRegistry = meterRegistry;
    }

    public KafkaLagDto getLag() {
        KafkaLagDto lag = fetchLag();
        updateMetrics(lag);
        return lag;
    }

    @Scheduled(fixedDelayString = "${app.kafka.lag.refresh-ms:10000}")
    public void refreshLagMetrics() {
        try {
            getLag();
        } catch (Exception e) {
            log.warn("Failed to refresh Kafka consumer lag metrics", e);
        }
    }

    private KafkaLagDto fetchLag() {
        Properties properties = new Properties();
        properties.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers
        );

        try (AdminClient adminClient = AdminClient.create(properties)) {

            var topicDescription = adminClient
                    .describeTopics(Set.of(topic))
                    .allTopicNames()
                    .get(10, TimeUnit.SECONDS)
                    .get(topic);

            Map<TopicPartition, OffsetAndMetadata> committedOffsets =
                    adminClient.listConsumerGroupOffsets(consumerGroup)
                            .partitionsToOffsetAndMetadata()
                            .get(10, TimeUnit.SECONDS);

            Map<TopicPartition, OffsetSpec> offsetRequests =
                    new HashMap<>();

            for (var partitionInfo : topicDescription.partitions()) {
                TopicPartition partition = new TopicPartition(
                        topic,
                        partitionInfo.partition()
                );

                offsetRequests.put(partition, OffsetSpec.latest());
            }

            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>
                    endOffsets = adminClient.listOffsets(offsetRequests)
                    .all()
                    .get(10, TimeUnit.SECONDS);

            List<PartitionLagDto> partitions =
                    offsetRequests.keySet().stream()
                            .sorted((a, b) ->
                                    Integer.compare(
                                            a.partition(),
                                            b.partition()
                                    ))
                            .map(partition -> {
                                OffsetAndMetadata committed =
                                        committedOffsets.get(partition);

                                long currentOffset = committed == null
                                        ? 0L
                                        : committed.offset();

                                long logEndOffset =
                                        endOffsets.get(partition).offset();

                                return calculatePartitionLag(
                                        partition.partition(),
                                        currentOffset,
                                        logEndOffset
                                );
                            })
                            .toList();

            return new KafkaLagDto(
                    topic,
                    consumerGroup,
                    partitions
            );

        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to retrieve Kafka consumer lag",
                    e
            );
        }
    }

    private void updateMetrics(KafkaLagDto lag) {
        for (PartitionLagDto partition : lag.partitions()) {
            int partitionId = partition.partition();

            AtomicLong lagValue = partitionLags.computeIfAbsent(
                    partitionId,
                    id -> {
                        AtomicLong value = new AtomicLong();

                        Gauge.builder(
                                        "app.kafka.consumer.lag",
                                        value,
                                        AtomicLong::get
                                )
                                .tag("topic", topic)
                                .tag("consumer_group", consumerGroup)
                                .tag("partition", String.valueOf(id))
                                .description("Kafka consumer lag per partition")
                                .register(meterRegistry);

                        return value;
                    }
            );

            lagValue.set(partition.lag());
        }
    }

    static PartitionLagDto calculatePartitionLag(
            int partition,
            long currentOffset,
            long logEndOffset) {

        long lag = Math.max(
                0L,
                logEndOffset - currentOffset
        );

        return new PartitionLagDto(
                partition,
                currentOffset,
                logEndOffset,
                lag
        );
    }
}
