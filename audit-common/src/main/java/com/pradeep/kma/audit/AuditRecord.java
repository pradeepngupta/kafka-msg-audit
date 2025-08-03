package com.pradeep.kma.audit;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.time.Instant;

public record AuditRecord(
        String auditId,
        String topic,
        String partition,
        long offset,
        MessageStatus status,
        String messageKey,
        String payload,
        @JsonSerialize(using = CustomDateSerializer.class)
        Instant ts,
        String producerApp,
        String consumerGroup
) {

}
