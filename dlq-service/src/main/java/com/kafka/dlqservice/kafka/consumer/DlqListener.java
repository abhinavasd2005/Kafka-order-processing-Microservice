package com.kafka.dlqservice.kafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.dlqservice.dto.OrderMessage;
import com.kafka.dlqservice.entity.DlqEntry;
import com.kafka.dlqservice.entity.FailureType;
import com.kafka.dlqservice.repository.DlqEntryRepository;
import com.kafka.dlqservice.util.KafkaHeaderUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class DlqListener {

    private final DlqEntryRepository dlqEntryRepository;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${app.kafka.topics.orders-dlq}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consume(
            ConsumerRecord<String, OrderMessage> record
    ) throws JsonProcessingException {

        OrderMessage orderMessage = record.value();

        DlqEntry entry = new DlqEntry();

        entry.setOrderId(orderMessage.orderId());

        entry.setPayload(
                objectMapper.writeValueAsString(orderMessage)
        );

        entry.setFailureReason(
                KafkaHeaderUtil.getFailureReason(record.headers())
        );

        entry.setFailureType(
                FailureType.valueOf(
                        KafkaHeaderUtil.getFailureType(record.headers())
                )
        );

        dlqEntryRepository.save(entry);

        log.warn(
                "Order {} stored in DLQ",
                orderMessage.orderId()
        );

    }

}