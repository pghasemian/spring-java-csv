package com.example.csvcodeservice.service;

import com.example.csvcodeservice.dto.CodeResponse;
import com.example.csvcodeservice.entity.CodeEntity;
import com.example.csvcodeservice.exception.CsvValidationException;
import com.example.csvcodeservice.exception.ResourceNotFoundException;
import com.example.csvcodeservice.repository.CodeRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

@Service
public class CodeService {

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

        List<CodeEntity> entities = csvParserService.parse(file);

        validateAgainstDatabase(entities);

        codeRepository.saveAll(entities);

        return entities.size();
    }

    @Transactional(readOnly = true)
    public List<CodeResponse> findAll() {

        return codeRepository.findAll()
                .stream()
                .map(CodeResponse::from)
                .toList();
    }

    @Transactional(readOnly = true)
    public CodeResponse findByCode(String code) {

        CodeEntity entity = codeRepository.findByCode(code)
                .orElseThrow(() ->
                        new ResourceNotFoundException("Code not found: " + code)
                );

        return CodeResponse.from(entity);
    }

    @Transactional
    public void deleteAll() {

        codeRepository.deleteAllInBatch();
    }

    private void validateAgainstDatabase(
            List<CodeEntity> entities
    ) {

        for (CodeEntity entity : entities) {

            if (codeRepository.existsByCode(entity.getCode())) {

                throw new CsvValidationException("Code already exists: "
                        + entity.getCode()
                );
            }
        }
    }
}