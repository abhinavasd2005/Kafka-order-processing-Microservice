package com.kafka.paymentservice.kafka.consumer;

import com.kafka.paymentservice.dto.OrderMessage;
import com.kafka.paymentservice.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderListener {

    private final PaymentService paymentService;

    @KafkaListener(
            topics = "${app.kafka.topics.orders}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consume(
            ConsumerRecord<String, OrderMessage> record
    ) {

        OrderMessage orderMessage = record.value();

        log.info("Received order {}", orderMessage.orderId());

        paymentService.processPayment(
                orderMessage,
                record.headers()
        );

    }

}