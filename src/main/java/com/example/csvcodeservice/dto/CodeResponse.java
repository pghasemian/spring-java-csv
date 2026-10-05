package com.example.csvcodeservice.dto;

import com.example.csvcodeservice.entity.CodeEntity;

import java.time.LocalDate;

public record CodeResponse(
        String source,
        String codeListCode,
        String code,
        String displayValue,
        String longDescription,
        LocalDate fromDate,
        LocalDate toDate,
        Integer sortingPriority
) {

    public static CodeResponse from(CodeEntity entity) {
        return new CodeResponse(
                entity.getSource(),
                entity.getCodeListCode(),
                entity.getCode(),
                entity.getDisplayValue(),
                entity.getLongDescription(),
                entity.getFromDate(),
                entity.getToDate(),
                entity.getSortingPriority()
        );
    }
}