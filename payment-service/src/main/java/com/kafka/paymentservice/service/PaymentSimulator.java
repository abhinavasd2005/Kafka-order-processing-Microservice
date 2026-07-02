package com.kafka.paymentservice.service;

import com.kafka.paymentservice.entity.PaymentResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

@Component
public class PaymentSimulator {

    public PaymentResult processPayment() {

        int random = ThreadLocalRandom.current().nextInt(100);

        if (random < 50) {
            return PaymentResult.SUCCESS;
        }

        if (random < 85) {
            return PaymentResult.TRANSIENT_FAILURE;
        }

        return PaymentResult.PERMANENT_FAILURE;
    }

}