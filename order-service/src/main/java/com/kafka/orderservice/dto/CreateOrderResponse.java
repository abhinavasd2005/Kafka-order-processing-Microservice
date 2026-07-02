package com.kafka.orderservice.dto;

import com.kafka.orderservice.entity.OrderStatus;

import java.util.UUID;

public record CreateOrderResponse(

        UUID orderId,

        OrderStatus status

) {
}