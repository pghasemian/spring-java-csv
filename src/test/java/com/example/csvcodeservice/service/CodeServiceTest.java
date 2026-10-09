
package com.example.csvcodeservice.service;

import com.example.csvcodeservice.dto.CodeResponse;
import com.example.csvcodeservice.entity.CodeEntity;
import com.example.csvcodeservice.exception.CsvValidationException;
import com.example.csvcodeservice.exception.ResourceNotFoundException;
import com.example.csvcodeservice.repository.CodeRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.multipart.MultipartFile;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CodeServiceTest {

    @Mock
    private CodeRepository codeRepository;

    @Mock
    private CsvParserService csvParserService;

    @Mock
    private KafkaProducerService kafkaProducerService;

    @InjectMocks
    private CodeService codeService;

    private CodeEntity entity;

    @BeforeEach
    void setUp() {

        entity = new CodeEntity();

        entity.setSource("TestSource");
        entity.setCodeListCode("TestList");
        entity.setCode("TEST001");
        entity.setDisplayValue("Test Value");
        entity.setLongDescription("Test description");
        entity.setFromDate(LocalDate.of(2026, 1, 1));
        entity.setToDate(LocalDate.of(2026, 12, 31));
        entity.setSortingPriority(1);
    }

    @Test
    void shouldUploadData() {

        MultipartFile file = new MockMultipartFile(
                "file",
                "test.csv",
                "text/csv",
                "test".getBytes()
        );

        when(csvParserService.parse(file))
                .thenReturn(List.of(entity));

        when(codeRepository.existsByCode("TEST001"))
                .thenReturn(false);

        int result = codeService.upload(file);

        assertEquals(1, result);

        verify(csvParserService).parse(file);

        verify(codeRepository)
                .existsByCode("TEST001");

        verify(kafkaProducerService)
                .send(entity, 0);

        verify(codeRepository, never())
                .saveAll(anyList());
    }

    @Test
    void shouldRejectExistingCode() {

        MultipartFile file = new MockMultipartFile(
                "file",
                "test.csv",
                "text/csv",
                "test".getBytes()
        );

        when(csvParserService.parse(file))
                .thenReturn(List.of(entity));

        when(codeRepository.existsByCode("TEST001"))
                .thenReturn(true);

        CsvValidationException exception = assertThrows(
                CsvValidationException.class,
                () -> codeService.upload(file)
        );

        assertEquals(
                "Code already exists: TEST001",
                exception.getMessage()
        );

        verify(kafkaProducerService, never())
                .send(any(CodeEntity.class), anyInt());
    }

    @Test
    void shouldFindAll() {

        when(codeRepository.findAll())
                .thenReturn(List.of(entity));

        List<CodeResponse> result = codeService.findAll();

        assertEquals(1, result.size());

        CodeResponse response = result.getFirst();

        assertEquals("TestSource", response.source());
        assertEquals("TestList", response.codeListCode());
        assertEquals("TEST001", response.code());
        assertEquals("Test Value", response.displayValue());
    }

    @Test
    void shouldFindByCode() {

        when(codeRepository.findByCode("TEST001"))
                .thenReturn(Optional.of(entity));

        CodeResponse result = codeService.findByCode("TEST001");

        assertEquals("TEST001", result.code());
        assertEquals("Test Value", result.displayValue());

        verify(codeRepository).findByCode("TEST001");
    }

    @Test
    void shouldThrowWhenCodeDoesNotExist() {

        when(codeRepository.findByCode("UNKNOWN"))
                .thenReturn(Optional.empty());

        ResourceNotFoundException exception = assertThrows(
                ResourceNotFoundException.class,
                () -> codeService.findByCode("UNKNOWN")
        );

        assertEquals(
                "Code not found: UNKNOWN",
                exception.getMessage()
        );
    }

    @Test
    void shouldDeleteAll() {

        codeService.deleteAll();

        verify(codeRepository).deleteAllInBatch();
    }
}
