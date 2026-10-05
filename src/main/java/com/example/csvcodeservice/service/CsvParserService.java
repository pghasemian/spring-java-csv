package com.example.csvcodeservice.service;

import com.example.csvcodeservice.entity.CodeEntity;
import com.example.csvcodeservice.exception.CsvValidationException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Service
public class CsvParserService {

    private static final DateTimeFormatter DATE_FORMAT =
            DateTimeFormatter.ofPattern("dd-MM-yyyy");

    private static final List<String> EXPECTED_HEADERS = List.of(
            "source",
            "codeListCode",
            "code",
            "displayValue",
            "longDescription",
            "fromDate",
            "toDate",
            "sortingPriority"
    );

    public List<CodeEntity> parse(MultipartFile file) {

        if (file == null || file.isEmpty()) {
            throw new CsvValidationException(
                    "CSV file must not be empty"
            );
        }

        try (
                Reader reader = new InputStreamReader(
                        file.getInputStream(),
                        StandardCharsets.UTF_8
                );

                CSVParser parser = CSVFormat.DEFAULT.builder()
                        .setHeader()
                        .setSkipHeaderRecord(true)
                        .setIgnoreEmptyLines(true)
                        .setTrim(true).get()
                        .parse(reader)
        ) {

            validateHeaders(parser.getHeaderMap());

            List<CodeEntity> result = new ArrayList<>();
            Set<String> codes = new HashSet<>();

            for (CSVRecord record : parser) {

                CodeEntity entity = parseRecord(record);

                if (!codes.add(entity.getCode())) {
                    throw new CsvValidationException(
                            "Duplicate code found in CSV: "
                                    + entity.getCode()
                    );
                }

                result.add(entity);
            }

            if (result.isEmpty()) {
                throw new CsvValidationException(
                        "CSV file does not contain any data"
                );
            }

            return result;

        } catch (IOException e) {
            throw new CsvValidationException(
                    "Could not read CSV file"
            );
        }
    }

    private CodeEntity parseRecord(CSVRecord record) {

        CodeEntity entity = new CodeEntity();

        entity.setSource(
                required(record, "source")
        );

        entity.setCodeListCode(
                required(record, "codeListCode")
        );

        entity.setCode(
                required(record, "code")
        );

        entity.setDisplayValue(
                required(record, "displayValue")
        );

        entity.setLongDescription(
                nullable(record, "longDescription")
        );

        entity.setFromDate(
                parseDate(
                        required(record, "fromDate"),
                        "fromDate"
                )
        );

        String toDate = nullable(record, "toDate");

        if (toDate != null) {
            entity.setToDate(
                    parseDate(toDate, "toDate")
            );
        }

        String sortingPriority =
                nullable(record, "sortingPriority");

        if (sortingPriority != null) {

            try {
                entity.setSortingPriority(
                        Integer.valueOf(sortingPriority)
                );

            } catch (NumberFormatException e) {

                throw new CsvValidationException(
                        "Invalid sortingPriority at row "
                                + record.getRecordNumber()
                );
            }
        }

        return entity;
    }

    private String required(
            CSVRecord record,
            String column
    ) {

        String value = nullable(record, column);

        if (value == null) {
            throw new CsvValidationException(
                    "Column '" + column
                            + "' cannot be empty at row "
                            + record.getRecordNumber()
            );
        }

        return value;
    }

    private String nullable(
            CSVRecord record,
            String column
    ) {

        String value = record.get(column);

        if (value == null || value.isBlank()) {
            return null;
        }

        return value.trim();
    }

    private LocalDate parseDate(
            String value,
            String field
    ) {

        try {

            return LocalDate.parse(
                    value,
                    DATE_FORMAT
            );

        } catch (DateTimeParseException e) {

            throw new CsvValidationException(
                    "Invalid date '" + value
                            + "' for field " + field
            );
        }
    }

    private void validateHeaders(
            java.util.Map<String, Integer> headers
    ) {

        List<String> actualHeaders =
                new ArrayList<>(headers.keySet());

        if (!actualHeaders.equals(EXPECTED_HEADERS)) {

            throw new CsvValidationException(
                    "Invalid CSV headers. Expected: "
                            + EXPECTED_HEADERS
            );
        }
    }
}