package com.kafka.orderservice.service;

import com.kafka.orderservice.dto.CreateOrderRequest;
import com.kafka.orderservice.dto.CreateOrderResponse;
import com.kafka.orderservice.dto.OrderMessage;
import com.kafka.orderservice.dto.OrderStatusResponse;
import com.kafka.orderservice.entity.Order;
import com.kafka.orderservice.entity.OrderStatus;
import com.kafka.orderservice.exception.OrderNotFoundException;
import com.kafka.orderservice.kafka.producer.OrderProducer;
import com.kafka.orderservice.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
@Transactional
public class OrderService {

    private final OrderRepository orderRepository;
    private final OrderProducer orderProducer;

    public CreateOrderResponse createOrder(CreateOrderRequest request) {

        UUID orderId = UUID.randomUUID();

        Order order = new Order();

        order.setOrderId(orderId);
        order.setStatus(OrderStatus.PENDING);

        orderRepository.save(order);

        OrderMessage orderMessage = new OrderMessage(
                orderId,
                request.customerId(),
                request.amount(),
                request.items()
        );

        orderProducer.publish(orderMessage);

        log.info("Order {} created successfully", orderId);

        return new CreateOrderResponse(
                orderId,
                OrderStatus.PENDING
        );

    }

    @Transactional(readOnly = true)
    public OrderStatusResponse getOrderStatus(UUID orderId) {

        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));

        return new OrderStatusResponse(
                order.getOrderId(),
                order.getStatus(),
                order.getCreatedAt(),
                order.getUpdatedAt()
        );

    }

    public void updateStatus(UUID orderId, OrderStatus status) {

        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));

        order.setStatus(status);

        orderRepository.save(order);

        log.info("Order {} updated to {}", orderId, status);

    }

}