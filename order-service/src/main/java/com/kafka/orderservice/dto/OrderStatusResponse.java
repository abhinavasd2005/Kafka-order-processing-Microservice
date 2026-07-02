package com.kafka.orderservice.dto;

import com.kafka.orderservice.entity.OrderStatus;

import java.time.LocalDateTime;
import java.util.UUID;

public record OrderStatusResponse(

        UUID orderId,

        OrderStatus status,

        LocalDateTime createdAt,

        LocalDateTime updatedAt

) {
}