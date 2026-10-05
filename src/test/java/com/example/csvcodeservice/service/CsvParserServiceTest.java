package com.example.csvcodeservice.service;

import com.example.csvcodeservice.entity.CodeEntity;
import com.example.csvcodeservice.exception.CsvValidationException;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockMultipartFile;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CsvParserServiceTest {

    private final CsvParserService csvParserService =
            new CsvParserService();

    @Test
    void shouldParseValidCsv() {

        String csv = """
                source,codeListCode,code,displayValue,longDescription,fromDate,toDate,sortingPriority
                ZIB,ZIB001,123,Test value,Description,01-01-2019,,1
                ZIB,ZIB001,456,Another value,Description 2,01-01-2020,31-12-2025,2
                """;

        MockMultipartFile file = new MockMultipartFile(
                "file",
                "test.csv",
                "text/csv",
                csv.getBytes(StandardCharsets.UTF_8)
        );

        List<CodeEntity> result =
                csvParserService.parse(file);

        assertEquals(2, result.size());

        CodeEntity first = result.get(0);

        assertEquals("ZIB", first.getSource());
        assertEquals("ZIB001", first.getCodeListCode());
        assertEquals("123", first.getCode());
        assertEquals("Test value", first.getDisplayValue());
        assertEquals("Description", first.getLongDescription());
        assertEquals(1, first.getSortingPriority());

        assertNotNull(first.getFromDate());
        assertNull(first.getToDate());
    }

    @Test
    void shouldRejectDuplicateCodes() {

        String csv = """
                source,codeListCode,code,displayValue,longDescription,fromDate,toDate,sortingPriority
                ZIB,ZIB001,123,First,Description,01-01-2019,,1
                ZIB,ZIB001,123,Second,Description,01-01-2020,,2
                """;

        MockMultipartFile file = new MockMultipartFile(
                "file",
                "test.csv",
                "text/csv",
                csv.getBytes(StandardCharsets.UTF_8)
        );

        assertThrows(
                CsvValidationException.class,
                () -> csvParserService.parse(file)
        );
    }

    @Test
    void shouldRejectInvalidDate() {

        String csv = """
                source,codeListCode,code,displayValue,longDescription,fromDate,toDate,sortingPriority
                ZIB,ZIB001,123,Test,Description,invalid-date,,1
                """;

        MockMultipartFile file = new MockMultipartFile(
                "file",
                "test.csv",
                "text/csv",
                csv.getBytes(StandardCharsets.UTF_8)
        );

        assertThrows(
                CsvValidationException.class,
                () -> csvParserService.parse(file)
        );
    }

    @Test
    void shouldRejectEmptyFile() {

        MockMultipartFile file = new MockMultipartFile(
                "file",
                "empty.csv",
                "text/csv",
                new byte[0]
        );

        assertThrows(
                CsvValidationException.class,
                () -> csvParserService.parse(file)
        );
    }
}