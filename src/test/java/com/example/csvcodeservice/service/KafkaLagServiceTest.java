
package com.example.csvcodeservice.service;

import com.example.csvcodeservice.dto.PartitionLagDto;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaLagServiceTest {

    @Test
    void shouldCalculatePositiveLag() {
        PartitionLagDto result =
                KafkaLagService.calculatePartitionLag(0, 21, 23);

        assertEquals(0, result.partition());
        assertEquals(21, result.currentOffset());
        assertEquals(23, result.logEndOffset());
        assertEquals(2, result.lag());
    }

    @Test
    void shouldReturnZeroLagWhenOffsetsAreEqual() {
        PartitionLagDto result =
                KafkaLagService.calculatePartitionLag(1, 21, 21);

        assertEquals(0, result.lag());
    }

    @Test
    void shouldReturnZeroLagWhenCurrentOffsetExceedsLogEndOffset() {
        PartitionLagDto result =
                KafkaLagService.calculatePartitionLag(2, 22, 21);

        assertEquals(0, result.lag());
    }
}
