package com.kafka.dlqservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.dlqservice.dto.DlqResponse;
import com.kafka.dlqservice.dto.OrderMessage;
import com.kafka.dlqservice.entity.DlqEntry;
import com.kafka.dlqservice.entity.FailureType;
import com.kafka.dlqservice.exception.DlqEntryNotFoundException;
import com.kafka.dlqservice.kafka.producer.OrderReplayProducer;
import com.kafka.dlqservice.repository.DlqEntryRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class DlqService {

    private final DlqEntryRepository repository;
    private final OrderReplayProducer replayProducer;
    private final ObjectMapper objectMapper;

    public List<DlqResponse> getAllEntries() {

        return repository.findAll()

                .stream()

                .map(entry -> new DlqResponse(

                        entry.getId(),

                        entry.getOrderId(),

                        entry.getFailureType(),

                        entry.getFailureReason(),

                        entry.isReplayed(),

                        entry.getCreatedAt()

                ))

                .toList();

    }
    @Transactional
    public void replay(Long id) throws JsonProcessingException {

        DlqEntry entry = repository.findById(id)

                .orElseThrow(() ->
                        new DlqEntryNotFoundException(id));

        if (entry.getFailureType() == FailureType.PERMANENT_FAILURE) {

            throw new IllegalStateException(
                    "Permanent failures cannot be replayed."
            );

        }
        if (entry.isReplayed()) {
            throw new IllegalStateException(
                    "This DLQ entry has already been replayed."
            );
        }

        OrderMessage orderMessage = objectMapper.readValue(

                entry.getPayload(),

                OrderMessage.class

        );

        replayProducer.replay(orderMessage);

        entry.setReplayed(true);

        repository.save(entry);

    }

}