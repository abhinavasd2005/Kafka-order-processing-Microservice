package com.kafka.orderservice.controller;

import com.kafka.orderservice.dto.CreateOrderRequest;
import com.kafka.orderservice.dto.CreateOrderResponse;
import com.kafka.orderservice.dto.OrderStatusResponse;
import com.kafka.orderservice.service.OrderService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    @PostMapping
    public ResponseEntity<CreateOrderResponse> createOrder(
            @Valid
            @RequestBody
            CreateOrderRequest request
    ) {

        return ResponseEntity
                .status(HttpStatus.ACCEPTED)
                .body(orderService.createOrder(request));

    }

    @GetMapping("/{orderId}")
    public ResponseEntity<OrderStatusResponse> getOrderStatus(
            @PathVariable UUID orderId
    ) {

        return ResponseEntity.ok(
                orderService.getOrderStatus(orderId)
        );

    }

}