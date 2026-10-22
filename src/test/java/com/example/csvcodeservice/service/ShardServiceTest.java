
package com.example.csvcodeservice.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ShardServiceTest {

    @Test
    void shouldMapSameKeyToSameShard() {
        ShardService shardService = new ShardService(3);

        int firstResult = shardService.getShardId("ABC001");
        int secondResult = shardService.getShardId("ABC001");

        assertEquals(firstResult, secondResult);
    }

    @Test
    void shouldReturnShardWithinConfiguredRange() {
        ShardService shardService = new ShardService(3);

        int shardId = shardService.getShardId("ABC001");

        assertTrue(shardId >= 0);
        assertTrue(shardId < 3);
    }

    @Test
    void shouldRejectZeroShardCount() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new ShardService(0)
        );
    }

    @Test
    void shouldRejectBlankKey() {
        ShardService shardService = new ShardService(3);

        assertThrows(
                IllegalArgumentException.class,
                () -> shardService.getShardId(" ")
        );
    }


    @Test
    void changingShardCountCanChangeKeyMapping() {
        String key = "ABC001";

        int shardWithThreeShards =
                ShardService.calculateShardId(key, 3);

        int shardWithFourShards =
                ShardService.calculateShardId(key, 4);

        assertTrue(shardWithThreeShards >= 0);
        assertTrue(shardWithThreeShards < 3);

        assertTrue(shardWithFourShards >= 0);
        assertTrue(shardWithFourShards < 4);
    }


    @Test
    void shouldMeasureKeyRemappingWhenShardCountChanges() {
        int changedMappings = 0;
        int totalKeys = 100;

        for (int i = 0; i < totalKeys; i++) {
            String key = "CODE_" + i;

            int oldShard = ShardService.calculateShardId(key, 3);
            int newShard = ShardService.calculateShardId(key, 4);

            if (oldShard != newShard) {
                changedMappings++;
            }
        }

        System.out.println(
                "Keys remapped from 3 to 4 shards: "
                        + changedMappings + "/" + totalKeys
        );

        assertTrue(changedMappings > 0);
    }

}
