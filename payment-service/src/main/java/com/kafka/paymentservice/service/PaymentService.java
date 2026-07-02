package com.kafka.paymentservice.service;

import com.kafka.paymentservice.dto.OrderMessage;
import com.kafka.paymentservice.dto.OrderStatusUpdateEvent;
import com.kafka.paymentservice.entity.OrderStatus;
import com.kafka.paymentservice.entity.PaymentResult;
import com.kafka.paymentservice.entity.PaymentStatus;
import com.kafka.paymentservice.kafka.producer.RetryProducer;
import com.kafka.paymentservice.kafka.producer.StatusUpdateProducer;
import com.kafka.paymentservice.repository.PaymentStatusRepository;
import com.kafka.paymentservice.util.KafkaHeaderUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
@Slf4j
@Transactional
public class PaymentService {

    private final PaymentStatusRepository paymentStatusRepository;
    private final PaymentSimulator paymentSimulator;
    private final StatusUpdateProducer statusUpdateProducer;
    private final RetryProducer retryProducer;

    public void processPayment(
            OrderMessage orderMessage,
            Headers headers
    ) {

        if (paymentStatusRepository.existsByOrderIdAndStatus(
                orderMessage.orderId(),
                OrderStatus.PAID
        )) {

            log.info("Order {} already processed. Skipping.",
                    orderMessage.orderId());

            return;
        }

        int retryCount = KafkaHeaderUtil.getRetryCount(headers);


        PaymentResult paymentResult = paymentSimulator.processPayment();

        switch (paymentResult) {

            case SUCCESS -> handleSuccess(orderMessage);

            case TRANSIENT_FAILURE -> handleTransientFailure(
                    orderMessage,
                    retryCount
            );

            case PERMANENT_FAILURE -> handlePermanentFailure(
                    orderMessage
            );

        }

    }

    private void handleSuccess(OrderMessage orderMessage) {

        PaymentStatus paymentStatus = new PaymentStatus();

        paymentStatus.setOrderId(orderMessage.orderId());
        paymentStatus.setStatus(OrderStatus.PAID);

        paymentStatusRepository.save(paymentStatus);

        statusUpdateProducer.publish(

                new OrderStatusUpdateEvent(

                        orderMessage.orderId(),

                        OrderStatus.PAID

                )

        );

        log.info("Payment successful for order {}",
                orderMessage.orderId());

    }

    private void handleTransientFailure(
            OrderMessage orderMessage,
            int retryCount
    ) {

        log.warn(
                "Transient failure for order {} (Retry {})",
                orderMessage.orderId(),
                retryCount
        );

        if (retryCount < 2) {

            retryProducer.sendToRetry(
                    orderMessage,
                    retryCount + 1
            );

        } else {

            retryProducer.sendToDlq(
                    orderMessage,
                    "TRANSIENT_FAILURE",
                    "Maximum retry attempts exceeded"
            );

        }

    }

    private void handlePermanentFailure(
            OrderMessage orderMessage
    ) {

        log.error("Permanent failure for order {}",
                orderMessage.orderId());

        retryProducer.sendToDlq(
                orderMessage,
                "PERMANENT_FAILURE",
                "Invalid payment method"
        );

    }

}