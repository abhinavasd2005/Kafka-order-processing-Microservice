package com.kafka.paymentservice.repository;

import com.kafka.paymentservice.entity.OrderStatus;
import com.kafka.paymentservice.entity.PaymentStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface PaymentStatusRepository extends JpaRepository<PaymentStatus, UUID> {

    boolean existsByOrderIdAndStatus(UUID orderId, OrderStatus status);

}