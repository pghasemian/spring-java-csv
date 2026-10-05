package com.example.csvcodeservice.entity;

import jakarta.persistence.*;

import java.time.LocalDate;

@Entity
@Table(
        name = "codes",
        uniqueConstraints = {
                @UniqueConstraint(
                        name = "uk_codes_code",
                        columnNames = "code"
                )
        }
)
public class CodeEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String source;

    @Column(nullable = false)
    private String codeListCode;

    @Column(nullable = false, unique = true)
    private String code;

    @Column(nullable = false)
    private String displayValue;

    @Column(length = 2000)
    private String longDescription;

    @Column(nullable = false)
    private LocalDate fromDate;

    private LocalDate toDate;

    private Integer sortingPriority;

    public CodeEntity() {
    }

    public Long getId() {
        return id;
    }

    public String getSource() {
        return source;
    }

    public String getCodeListCode() {
        return codeListCode;
    }

    public String getCode() {
        return code;
    }

    public String getDisplayValue() {
        return displayValue;
    }

    public String getLongDescription() {
        return longDescription;
    }

    public LocalDate getFromDate() {
        return fromDate;
    }

    public LocalDate getToDate() {
        return toDate;
    }

    public Integer getSortingPriority() {
        return sortingPriority;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public void setCodeListCode(String codeListCode) {
        this.codeListCode = codeListCode;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public void setDisplayValue(String displayValue) {
        this.displayValue = displayValue;
    }

    public void setLongDescription(String longDescription) {
        this.longDescription = longDescription;
    }

    public void setFromDate(LocalDate fromDate) {
        this.fromDate = fromDate;
    }

    public void setToDate(LocalDate toDate) {
        this.toDate = toDate;
    }

    public void setSortingPriority(Integer sortingPriority) {
        this.sortingPriority = sortingPriority;
    }
}