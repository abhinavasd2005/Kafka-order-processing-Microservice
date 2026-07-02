package com.kafka.paymentservice.kafka.producer;

import com.kafka.paymentservice.dto.OrderStatusUpdateEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class StatusUpdateProducer {

    @Value("${app.kafka.topics.order-status-updates}")
    private String statusTopic;

    private final KafkaTemplate<String, OrderStatusUpdateEvent> kafkaTemplate;

    public void publish(OrderStatusUpdateEvent event) {

        log.info(
                "Publishing status {} for order {}",
                event.status(),
                event.orderId()
        );

        kafkaTemplate.send(
                statusTopic,
                event.orderId().toString(),
                event
        );

    }

}