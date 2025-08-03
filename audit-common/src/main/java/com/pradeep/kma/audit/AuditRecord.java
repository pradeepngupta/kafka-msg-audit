package com.pradeep.kma.audit;

import java.time.Instant;

public record AuditRecord(
        String auditId,
        String topic,
        String partition,
        long offset,
        MessageStatus status,
        String messageKey,
        String payload,
        Instant ts,
        String producerApp,
        String consumerGroup
) {

}
