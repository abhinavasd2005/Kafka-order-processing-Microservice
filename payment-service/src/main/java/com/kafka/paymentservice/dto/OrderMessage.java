package com.kafka.paymentservice.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

public record OrderMessage(

        @NotNull
        UUID orderId,

        @NotBlank
        String customerId,

        @NotNull
        @Positive
        BigDecimal amount,

        @NotEmpty
        List<String> items

) {
}