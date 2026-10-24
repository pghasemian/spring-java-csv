
package com.example.csvcodeservice.controller;

import com.example.csvcodeservice.service.KafkaOffsetRangeConsumer;
import com.example.csvcodeservice.service.KafkaOffsetRangeConsumer.OffsetRecord;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/kafka")
public class KafkaOffsetRangeController {

    private final KafkaOffsetRangeConsumer offsetRangeConsumer;

    public KafkaOffsetRangeController(
            KafkaOffsetRangeConsumer offsetRangeConsumer) {
        this.offsetRangeConsumer = offsetRangeConsumer;
    }

    @GetMapping("/offset-range")
    public ResponseEntity<List<OffsetRecord>> readOffsetRange(
            @RequestParam(defaultValue = "codes") String topic,
            @RequestParam int partition,
            @RequestParam long startOffset,
            @RequestParam long endOffset) {

        return ResponseEntity.ok(
                offsetRangeConsumer.readRange(
                        topic,
                        partition,
                        startOffset,
                        endOffset
                )
        );
    }
}
