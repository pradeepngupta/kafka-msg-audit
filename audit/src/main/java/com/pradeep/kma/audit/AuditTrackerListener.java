package com.pradeep.kma.audit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class AuditTrackerListener {


    private final AuditHelper auditHelper;

    public AuditTrackerListener(AuditHelper auditHelper) {
        this.auditHelper = auditHelper;
    }

    @KafkaListener(topics = "internal-audit-topic", groupId = "audit-consumer-group")
    public void listen(String payload) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        AuditRecord message = objectMapper.readValue(payload, AuditRecord.class);

        switch (message.status()) {
            case MESSAGE_SENT -> auditHelper.updatePublishedAuditInfo(message);
            case MESSAGE_ACKNOWLEDGED -> auditHelper.updateAcknowledgedAuditInfo(message);
            case MESSAGE_CONSUMED -> auditHelper.updateConsumedAuditInfo(message);
            case MESSAGE_PROCESSED -> auditHelper.updateProcessedAuditInfo(message);
            default -> log.warn("Unknown audit event type: {}", message.status());
        }
        log.info("Received audit message: {}", message);
    }
}
