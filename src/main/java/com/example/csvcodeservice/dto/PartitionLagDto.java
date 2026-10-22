package com.example.csvcodeservice.dto;

public record PartitionLagDto(
        int partition,
        long currentOffset,
        long logEndOffset,
        long lag
) {
}
