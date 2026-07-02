package com.kafka.orderservice.dto;

import com.kafka.orderservice.entity.OrderStatus;

import java.util.UUID;

public record OrderStatusUpdateEvent(

        UUID orderId,

        OrderStatus status

) {
}