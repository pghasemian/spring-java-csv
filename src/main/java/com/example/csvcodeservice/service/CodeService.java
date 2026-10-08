package com.example.csvcodeservice.service;

import com.example.csvcodeservice.dto.CodeResponse;
import com.example.csvcodeservice.entity.CodeEntity;
import com.example.csvcodeservice.exception.CsvValidationException;
import com.example.csvcodeservice.exception.ResourceNotFoundException;
import com.example.csvcodeservice.repository.CodeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

@Service
public class CodeService {

    private static final Logger log = LoggerFactory.getLogger(CodeService.class);

    private final CodeRepository codeRepository;
    private final CsvParserService csvParserService;

    public CodeService(
            CodeRepository codeRepository,
            CsvParserService csvParserService
    ) {
        this.codeRepository = codeRepository;
        this.csvParserService = csvParserService;
    }

    @Transactional
    public int upload(MultipartFile file) {

        log.info("Starting CSV upload");

        List<CodeEntity> entities = csvParserService.parse(file);

        log.info("CSV parsed successfully. {} records found", entities.size());

        validateAgainstDatabase(entities);

        codeRepository.saveAll(entities);

        log.info("CSV upload completed successfully. {} records imported", entities.size());

        return entities.size();
    }

    @Transactional(readOnly = true)
    public List<CodeResponse> findAll() {

        log.info("Fetching all codes");

        List<CodeResponse> result =
                codeRepository.findAll()
                        .stream()
                        .map(CodeResponse::from)
                        .toList();

        log.info("Found {} codes", result.size());
        return result;
    }

    @Transactional(readOnly = true)
    public CodeResponse findByCode(String code) {

        log.info("Finding code: {}", code);

        CodeEntity entity =
                codeRepository.findByCode(code)
                        .orElseThrow(() -> {
                            log.warn("Code not found: {}", code);
                            return new ResourceNotFoundException("Code not found: " + code);
                        });

        log.info("Code found: {}", code);

        return CodeResponse.from(entity);
    }

    @Transactional
    public void deleteAll() {

        long count = codeRepository.count();

        codeRepository.deleteAllInBatch();

        log.info("Deleted all codes. {} records were removed", count);
    }

    private void validateAgainstDatabase(
            List<CodeEntity> entities
    ) {

        for (CodeEntity entity : entities) {

            if (codeRepository.existsByCode(
                    entity.getCode()
            )) {

                log.warn("Code already exists in database: {}", entity.getCode());

                throw new CsvValidationException("Code already exists: " + entity.getCode());
            }
        }
    }
}