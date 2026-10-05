package com.example.csvcodeservice.repository;

import com.example.csvcodeservice.entity.CodeEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface CodeRepository
        extends JpaRepository<CodeEntity, Long> {

    Optional<CodeEntity> findByCode(String code);

    boolean existsByCode(String code);
}