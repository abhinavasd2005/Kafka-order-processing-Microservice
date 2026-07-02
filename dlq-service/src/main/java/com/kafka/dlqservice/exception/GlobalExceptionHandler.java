package com.kafka.dlqservice.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(DlqEntryNotFoundException.class)
    public ResponseEntity<String> handleNotFound(
            DlqEntryNotFoundException ex
    ) {

        return ResponseEntity
                .status(HttpStatus.NOT_FOUND)
                .body(ex.getMessage());

    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<String> handleIllegalState(
            IllegalStateException ex
    ) {

        return ResponseEntity
                .badRequest()
                .body(ex.getMessage());

    }

}