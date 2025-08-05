package com.pradeep.kma.audit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.pradeep.kma.audit.Constants.INTERNAL_AUDIT_TOPIC;

@Component
@Slf4j
public class AuditTrackerListener {


    private final AuditHelper auditHelper;

    ExecutorService executorService = Executors.newCachedThreadPool();

    public AuditTrackerListener(AuditHelper auditHelper) {
        this.auditHelper = auditHelper;
    }

    @KafkaListener(topics = INTERNAL_AUDIT_TOPIC, groupId = "audit-consumer-group")
    public void listen(String payload) {
        log.info("✅ Consumed Audit message: {}", payload);
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            objectMapper.registerModule(new JavaTimeModule());
            final AuditRecord message = objectMapper.readValue(payload, AuditRecord.class);
            executorService.submit(() -> processAuditMessage(message));
        } catch (JsonProcessingException e) {
            log.error("Error deserializing audit message: {}", e.getMessage(), e);
        }
    }

    private void processAuditMessage(AuditRecord message) {
        switch (message.status()) {
            case MESSAGE_SENT -> auditHelper.updatePublishedAuditInfo(message);
            case MESSAGE_ACKNOWLEDGED -> auditHelper.updateAcknowledgedAuditInfo(message);
            case MESSAGE_CONSUMED -> auditHelper.updateConsumedAuditInfo(message);
            case MESSAGE_PROCESSED -> auditHelper.updateProcessedAuditInfo(message);
            default -> log.warn("Unknown audit event type: {}", message.status());
        }
    }
}
