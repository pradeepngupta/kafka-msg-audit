package com.pradeep.kma.audit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
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
            final AuditRecord message = objectMapper.readValue(payload, AuditRecord.class);
            executorService.submit(() -> processAuditMessageWithRetry(message));
        } catch (JsonProcessingException e) {
            log.error("Error deserializing audit message: {}", e.getMessage(), e);
        }
    }

    private void processAuditMessage(AuditRecord message) {
        log.info("Processing audit message with AuditId: {} and status: {}", message.auditId(), message.status());
        switch (message.status()) {
            case MESSAGE_SENT -> auditHelper.updatePublishedAuditInfo(message);
            case MESSAGE_ACKNOWLEDGED -> auditHelper.updateAcknowledgedAuditInfo(message);
            case MESSAGE_CONSUMED -> auditHelper.updateConsumedAuditInfo(message);
            case MESSAGE_PROCESSED -> auditHelper.updateProcessedAuditInfo(message);
            default -> log.warn("Unknown audit event type: {}", message.status());
        }
    }

    private void processAuditMessageWithRetry(AuditRecord message) {
        int maxRetries = 10;
        int attempt = 0;
        do {
            try {
                processAuditMessage(message);
                return; // Success
            } catch (DataIntegrityViolationException | ObjectOptimisticLockingFailureException ex) {
                attempt++;
                log.warn("Retry {}/{} for auditId {} for status {} due to DataIntegrityViolationException / ObjectOptimisticLockingFailureException: {}", attempt, maxRetries, message.auditId(), message.status(), ex.getMessage());
                if (attempt >= maxRetries) throw ex;
                try { Thread.sleep(100L * attempt); } catch (InterruptedException ignored) {
                    // Restore interrupted status
                    Thread.currentThread().interrupt();
                    log.error("Thread interrupted while waiting to retry processing audit message: {}", message.auditId());
                }
            } catch( Exception ex) {
                log.error("Error to update audit {} for the status {}: {}",  message.auditId(), message.status(), ex.getMessage(), ex);
            }
        } while (attempt < maxRetries);
    }
}
