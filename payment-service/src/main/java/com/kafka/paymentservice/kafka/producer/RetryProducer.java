package com.kafka.paymentservice.kafka.producer;

import com.kafka.paymentservice.dto.OrderMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
@Slf4j
public class RetryProducer {

    private final KafkaTemplate<String, OrderMessage> kafkaTemplate;

    @Value("${app.kafka.topics.orders-retry-1}")
    private String retryTopicOne;

    @Value("${app.kafka.topics.orders-retry-2}")
    private String retryTopicTwo;

    @Value("${app.kafka.topics.orders-dlq}")
    private String dlqTopic;

    public void sendToRetry(
            OrderMessage orderMessage,
            int retryCount
    ) {

        if (retryCount == 1) {

            publish(
                    retryTopicOne,
                    orderMessage,
                    1,
                    LocalDateTime.now().plusSeconds(30),
                    null,
                    null
            );

        } else {

            publish(
                    retryTopicTwo,
                    orderMessage,
                    2,
                    LocalDateTime.now().plusMinutes(2),
                    null,
                    null
            );

        }

    }

    public void sendToDlq(
            OrderMessage orderMessage,
            String failureType,
            String failureReason
    ) {

        publish(
                dlqTopic,
                orderMessage,
                2,
                null,
                failureType,
                failureReason
        );

    }

    private void publish(
            String topic,
            OrderMessage orderMessage,
            int retryCount,
            LocalDateTime nextRetryAt,
            String failureType,
            String failureReason
    ) {

        Message<OrderMessage> message =
                MessageBuilder.withPayload(orderMessage)
                        .setHeader(
                                KafkaHeaders.TOPIC,
                                topic
                        )
                        .setHeader(
                                KafkaHeaders.KEY,
                                orderMessage.orderId().toString()
                        )
                        .setHeader(
                                "retryCount",
                                String.valueOf(retryCount)
                        )
                        .setHeader(
                                "nextRetryAt",
                                nextRetryAt == null ? "" : nextRetryAt.toString()
                        )
                        .setHeader(
                                "failureType",
                                failureType == null ? "" : failureType
                        )
                        .setHeader(
                                "failureReason",
                                failureReason == null ? "" : failureReason
                        )
                        .build();

        log.info(
                "Publishing order {} to topic {}",
                orderMessage.orderId(),
                topic
        );

        kafkaTemplate.send(message);

    }

}