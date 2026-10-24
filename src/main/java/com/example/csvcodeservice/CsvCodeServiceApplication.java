package com.example.csvcodeservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication @EnableScheduling
public class CsvCodeServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(CsvCodeServiceApplication.class, args);
	}

}
