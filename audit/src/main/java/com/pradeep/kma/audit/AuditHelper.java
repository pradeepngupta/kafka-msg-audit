package com.pradeep.kma.audit;

import com.pradeep.kma.audit.datamodel.MsgAudit;
import com.pradeep.kma.audit.repository.MsgAuditRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class AuditHelper {
    private final MsgAuditRepository repo;

    public AuditHelper(MsgAuditRepository repo) {
        this.repo = repo;
    }

    public void updatePublishedAuditInfo(AuditRecord message) {
        log.info("Logging PublishedAudit for message with AuditId {} & key {}", message.auditId(), message.messageKey());
        MsgAudit audit = new MsgAudit();
        audit.setAuditId(message.auditId());
        audit.setMessageKey(message.messageKey());
        audit.setTopicName(message.topic());
        if (message.partition() != null)
            audit.setPartition(Integer.parseInt(message.partition()));
        audit.setOffset(message.offset());
        audit.setTsPub(message.date());
        audit.setPayload(message.payload()); // <-- store payload here
        audit.setStatus("PUB");

        repo.save(audit);
    }

    public void updateAcknowledgedAuditInfo(AuditRecord message) {
        log.info("Logging AcknowledgedAudit for message with AuditId {} & key {}", message.auditId(), message.messageKey());

        repo.findByAuditId(message.auditId()).ifPresent(audit -> {
            if (message.partition() != null)
                audit.setPartition(Integer.parseInt(message.partition()));
            audit.setTsAck(message.date());
            audit.setStatus("ACK");
            repo.save(audit);
        });
    }

    public void updateConsumedAuditInfo(AuditRecord message) {
        log.info("Logging ConsumedAudit for message with AuditId {} & key {}", message.auditId(), message.messageKey());

        repo.findByAuditId(message.auditId()).ifPresent(audit -> {
            audit.setTsCom(message.date());
            audit.setStatus("COM");
            repo.save(audit);
        });
    }

    public void updateProcessedAuditInfo(AuditRecord message) {
        log.info("Logging ProcessedAudit for message with AuditId {} & key {}", message.auditId(), message.messageKey());

        repo.findByAuditId(message.auditId()).ifPresent(audit -> {
            audit.setTsPrc(message.date());
            audit.setStatus("PRC");
            repo.save(audit);
        });
    }
}
