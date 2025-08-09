package com.pradeep.kma.audit;

import java.util.Date;

public record AuditRecord(
        String auditId,
        String topic,
        String partition,
        long offset,
        MessageStatus status,
        String messageKey,
        String payload,
        Date date,
        String producerApp
) {

}
