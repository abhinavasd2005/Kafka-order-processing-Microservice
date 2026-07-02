package com.kafka.dlqservice.dto;

import com.kafka.dlqservice.entity.FailureType;

import java.time.LocalDateTime;
import java.util.UUID;

public record DlqResponse(

        Long id,

        UUID orderId,

        FailureType failureType,

        String failureReason,

        boolean replayed,

        LocalDateTime createdAt

) {
}