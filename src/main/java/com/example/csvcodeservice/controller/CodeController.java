package com.example.csvcodeservice.controller;

import com.example.csvcodeservice.dto.CodeResponse;
import com.example.csvcodeservice.service.CodeService;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/codes")
public class CodeController {

    private final CodeService codeService;

    public CodeController(CodeService codeService) {
        this.codeService = codeService;
    }

    @PostMapping(value = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<Map<String, Object>> upload(
            @RequestParam("file") MultipartFile file) {
        int count = codeService.upload(file);
        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(Map.of(
                        "message", "CSV published to Kafka successfully",
                        "recordsPublished", count
                ));
    }

    @GetMapping
    public ResponseEntity<List<CodeResponse>> findAll() {
        return ResponseEntity.ok(codeService.findAll());
    }

    @GetMapping("/{code}")
    public ResponseEntity<CodeResponse> findByCode(@PathVariable String code) {
        return ResponseEntity.ok(codeService.findByCode(code));
    }

    @DeleteMapping
    public ResponseEntity<Void> deleteAll() {
        codeService.deleteAll();
        return ResponseEntity.noContent().build();
    }
}