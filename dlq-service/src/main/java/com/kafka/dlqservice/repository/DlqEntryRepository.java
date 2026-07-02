package com.kafka.dlqservice.repository;

import com.kafka.dlqservice.entity.DlqEntry;
import org.springframework.data.jpa.repository.JpaRepository;

public interface DlqEntryRepository extends JpaRepository<DlqEntry, Long> {
}