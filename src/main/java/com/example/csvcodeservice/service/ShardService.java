
package com.example.csvcodeservice.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class ShardService {

    private final int shardCount;

    public ShardService(
            @Value("${app.kafka.partition-count:3}")
            int shardCount) {

        if (shardCount <= 0) {
            throw new IllegalArgumentException(
                    "Shard count must be greater than zero"
            );
        }

        this.shardCount = shardCount;
    }

    public int getShardId(String key) {
        return calculateShardId(key, shardCount);
    }

    static int calculateShardId(String key, int shardCount) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException(
                    "Shard key must not be null or blank"
            );
        }

        if (shardCount <= 0) {
            throw new IllegalArgumentException(
                    "Shard count must be greater than zero"
            );
        }

        return Math.floorMod(key.hashCode(), shardCount);
    }

    public int getShardCount() {
        return shardCount;
    }
}
