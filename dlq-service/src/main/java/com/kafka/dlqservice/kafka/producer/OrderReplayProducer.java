package com.kafka.dlqservice.kafka.producer;

import com.kafka.dlqservice.dto.OrderMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderReplayProducer {

    private final KafkaTemplate<String, OrderMessage> kafkaTemplate;

    @Value("${app.kafka.topics.orders}")
    private String ordersTopic;

    public void replay(OrderMessage orderMessage) {

        kafkaTemplate.send(
                ordersTopic,
                orderMessage.orderId().toString(),
                orderMessage
        );

        log.info(
                "Replayed order {} to topic {}",
                orderMessage.orderId(),
                ordersTopic
        );

    }

}