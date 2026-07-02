package com.kafka.dlqservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DlqServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(DlqServiceApplication.class, args);
    }

}