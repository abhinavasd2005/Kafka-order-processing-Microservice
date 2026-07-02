package com.kafka.dlqservice.exception;

public class DlqEntryNotFoundException extends RuntimeException {

    public DlqEntryNotFoundException(Long id) {

        super("DLQ entry not found with id : " + id);

    }

}