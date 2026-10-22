
package com.example.csvcodeservice.service;

import com.example.csvcodeservice.dto.KafkaLagDto;
import com.example.csvcodeservice.dto.PartitionLagDto;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Service
public class KafkaLagService {

    private final String bootstrapServers;
    private final String topic;
    private final String consumerGroup;

    public KafkaLagService(
            @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
            String bootstrapServers,
            @Value("${app.kafka.topic:codes}")
            String topic,
            @Value("${app.kafka.consumer-group:code-consumer-group}")
            String consumerGroup) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.consumerGroup = consumerGroup;
    }

    public KafkaLagDto getLag() {
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
