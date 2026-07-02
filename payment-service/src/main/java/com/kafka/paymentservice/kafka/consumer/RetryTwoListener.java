package com.kafka.paymentservice.kafka.consumer;

import com.kafka.paymentservice.dto.OrderMessage;
import com.kafka.paymentservice.service.PaymentService;
import com.kafka.paymentservice.util.KafkaHeaderUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
@Slf4j
public class RetryTwoListener {

    private final PaymentService paymentService;

    @KafkaListener(
            topics = "${app.kafka.topics.orders-retry-2}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consume(
            ConsumerRecord<String, OrderMessage> record
    ) throws InterruptedException {

        OrderMessage orderMessage = record.value();

        LocalDateTime nextRetryAt =
                KafkaHeaderUtil.getNextRetryAt(record.headers());

        if (nextRetryAt != null &&
                nextRetryAt.isAfter(LocalDateTime.now())) {

            long delay = Duration.between(
                    LocalDateTime.now(),
                    nextRetryAt
            ).toMillis();

            log.info(
                    "Waiting {} ms before retrying order {}",
                    delay,
                    orderMessage.orderId()
            );

            Thread.sleep(delay);

        }

        paymentService.processPayment(
                orderMessage,
                record.headers()
        );

    }

}