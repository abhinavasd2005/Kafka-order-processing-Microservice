package com.kafka.orderservice.kafka.consumer;

import com.kafka.orderservice.dto.OrderStatusUpdateEvent;
import com.kafka.orderservice.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderStatusUpdateListener {

    private final OrderService orderService;

    @KafkaListener(
            topics = "${app.kafka.topics.order-status-updates}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consume(OrderStatusUpdateEvent event) {

        log.info(
                "Received status update for order {} : {}",
                event.orderId(),
                event.status()
        );

        orderService.updateStatus(
                event.orderId(),
                event.status()
        );

    }

}