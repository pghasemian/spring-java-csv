package com.example.csvcodeservice.controller;

import com.example.csvcodeservice.dto.CodeResponse;
import com.example.csvcodeservice.service.CodeService;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.LocalDate;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(CodeController.class)
class CodeControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private CodeService codeService;

    @Test
    void shouldUploadCsv() throws Exception {

        MockMultipartFile file =
                new MockMultipartFile(
                        "file",
                        "codes.csv",
                        "text/csv",
                        "source,codeListCode,code,displayValue\n"
                                .getBytes()
                );

        when(codeService.upload(any()))
                .thenReturn(18);

        mockMvc.perform(
                        multipart("/api/codes/upload")
                                .file(file)
                )
                .andExpect(status().isCreated())
                .andExpect(
                        jsonPath("$.message")
                                .value("CSV uploaded successfully")
                )
                .andExpect(
                        jsonPath("$.recordsImported")
                                .value(18)
                );

        verify(codeService).upload(any());
    }

    @Test
    void shouldFindAllCodes() throws Exception {

        CodeResponse response =
                new CodeResponse(
                        "TestSource",
                        "TestList",
                        "TEST001",
                        "Test Value",
                        "Test description",
                        LocalDate.of(2026, 1, 1),
                        LocalDate.of(2026, 12, 31),
                        1
                );

        when(codeService.findAll())
                .thenReturn(List.of(response));

        mockMvc.perform(
                        get("/api/codes")
                )
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(
                        jsonPath("$[0].code")
                                .value("TEST001")
                )
                .andExpect(
                        jsonPath("$[0].displayValue")
                                .value("Test Value")
                );

        verify(codeService).findAll();
    }

    @Test
    void shouldFindCodeByCode() throws Exception {

        CodeResponse response =
                new CodeResponse(
                        "TestSource",
                        "TestList",
                        "TEST001",
                        "Test Value",
                        "Test description",
                        LocalDate.of(2026, 1, 1),
                        LocalDate.of(2026, 12, 31),
                        1
                );

        when(codeService.findByCode("TEST001"))
                .thenReturn(response);

        mockMvc.perform(
                        get("/api/codes/TEST001")
                )
                .andExpect(status().isOk())
                .andExpect(
                        jsonPath("$.code")
                                .value("TEST001")
                )
                .andExpect(
                        jsonPath("$.displayValue")
                                .value("Test Value")
                );

        verify(codeService)
                .findByCode("TEST001");
    }

    @Test
    void shouldDeleteAllCodes() throws Exception {

        doNothing()
                .when(codeService)
                .deleteAll();

        mockMvc.perform(
                        delete("/api/codes")
                )
                .andExpect(status().isNoContent());

        verify(codeService)
                .deleteAll();
    }
}