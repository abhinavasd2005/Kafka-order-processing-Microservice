package com.kafka.paymentservice.dto;

import com.kafka.paymentservice.entity.OrderStatus;

import java.util.UUID;

public record OrderStatusUpdateEvent(

        UUID orderId,

        OrderStatus status

) {
}