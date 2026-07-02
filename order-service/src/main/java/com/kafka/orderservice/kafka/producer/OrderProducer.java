package com.kafka.orderservice.kafka.producer;

import com.kafka.orderservice.dto.OrderMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderProducer {

    @Value("${app.kafka.topics.orders}")
    private String ordersTopic;

    private final KafkaTemplate<String, OrderMessage> kafkaTemplate;

    public void publish(OrderMessage orderMessage) {

        log.info("Publishing order {} to Kafka topic '{}'",
                orderMessage.orderId(),
                ordersTopic);

        kafkaTemplate.send(
                ordersTopic,
                orderMessage.orderId().toString(),
                orderMessage
        );

    }

}