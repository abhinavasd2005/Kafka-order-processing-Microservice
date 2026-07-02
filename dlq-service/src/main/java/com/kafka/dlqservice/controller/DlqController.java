package com.kafka.dlqservice.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafka.dlqservice.dto.DlqResponse;
import com.kafka.dlqservice.service.DlqService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/dlq")
@RequiredArgsConstructor
public class DlqController {

    private final DlqService dlqService;

    @GetMapping
    public ResponseEntity<List<DlqResponse>> getAllEntries() {

        return ResponseEntity.ok(
                dlqService.getAllEntries()
        );

    }

    @PostMapping("/{id}/replay")
    public ResponseEntity<String> replay(
            @PathVariable Long id
    ) throws JsonProcessingException {

        dlqService.replay(id);

        return ResponseEntity.ok(
                "Replay initiated successfully."
        );

    }

}