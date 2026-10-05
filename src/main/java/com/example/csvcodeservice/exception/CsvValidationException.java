package com.example.csvcodeservice.exception;

public class CsvValidationException extends RuntimeException {

    public CsvValidationException(String message) {
        super(message);
    }
}