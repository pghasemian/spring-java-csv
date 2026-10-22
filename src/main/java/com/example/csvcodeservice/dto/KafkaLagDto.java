package com.example.csvcodeservice.dto;


import java.util.List;

public record KafkaLagDto(
        String topic,
        String consumerGroup,
        List<PartitionLagDto> partitions
) {
}
