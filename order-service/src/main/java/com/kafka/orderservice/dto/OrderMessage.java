package com.kafka.orderservice.dto;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

public record OrderMessage(

        UUID orderId,

        String customerId,

        BigDecimal amount,

        List<String> items

) {
}