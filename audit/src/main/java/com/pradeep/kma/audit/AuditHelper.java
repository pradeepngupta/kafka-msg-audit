package com.pradeep.kma.audit;

import com.pradeep.kma.audit.datamodel.MsgAudit;
import com.pradeep.kma.audit.repository.MsgAuditRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;

@Slf4j
@Service
public class AuditHelper {

    public enum AuditStatus {
        PUB, ACK, COM, PRC
    }

    private final MsgAuditRepository repo;

    public AuditHelper(MsgAuditRepository repo) {
        this.repo = repo;
    }

    @Transactional
    public void updatePublishedAuditInfo(AuditRecord message) {
        log.info("Logging PublishedAudit for message with AuditId {} & key {}", message.auditId(), message.messageKey());
        updateAuditInfo(message, AuditStatus.PUB);
        log.info("Updated PublishedAudit for message with AuditId {} & key {}", message.auditId(), message.messageKey());
    }

    @Transactional
    public void updateAcknowledgedAuditInfo(AuditRecord message) {
        log.info("Logging AcknowledgedAudit for message with AuditId {} & key {}", message.auditId(), message.messageKey());
        updateAuditInfo(message, AuditStatus.ACK);
        log.info("Updated AcknowledgedAudit for message with AuditId {} & key {}", message.auditId(), message.messageKey());

    }

    @Transactional
    public void updateConsumedAuditInfo(AuditRecord message) {
        log.info("Logging ConsumedAudit for message with AuditId {} & key {}", message.auditId(), message.messageKey());
        updateAuditInfo(message, AuditStatus.COM);
        log.info("Updated ConsumedAudit for message with AuditId {} & key {}", message.auditId(), message.messageKey());

    }

    @Transactional
    public void updateProcessedAuditInfo(AuditRecord message) {
        log.info("Logging ProcessedAudit for message with AuditId {} & key {}", message.auditId(), message.messageKey());
        updateAuditInfo(message, AuditStatus.PRC);
        log.info("Updated ProcessedAudit for message with AuditId {} & key {}", message.auditId(), message.messageKey());

    }

    private void updateAuditInfo(AuditRecord message, AuditStatus status) {
        MsgAudit audit = repo.findByAuditId(message.auditId()).orElseGet(() -> {
                MsgAudit mAudit = new MsgAudit();
                mAudit.setAuditId(message.auditId());
                mAudit.setMessageKey(message.messageKey());
                mAudit.setTopicName(message.topic());
                if (message.partition() != null)
                    mAudit.setPartition(Integer.parseInt(message.partition()));
                mAudit.setOffset(message.offset());
                updateTs(mAudit, message.date(), status);
                mAudit.setStatus(status.name());
                return repo.save(mAudit);
        });

        if (canTransition(audit.getStatus(), status.name())) {
            if (message.partition() != null)
                audit.setPartition(Integer.parseInt(message.partition()));
            audit.setStatus(status.name());
        } else {
            log.warn("Cannot transition from {} to {} for AuditId {}", audit.getStatus(), status.name(), message.auditId());
        }
        updateTs(audit, message.date(), status);
        repo.save(audit);
    }

    private void updateTs(MsgAudit mAudit, Date date, AuditStatus status) {
        switch (status) {
            case PUB -> mAudit.setTsPub(date);
            case ACK -> mAudit.setTsAck(date);
            case COM -> mAudit.setTsCom(date);
            case PRC -> mAudit.setTsPrc(date);
        }
    }



    private boolean canTransition(String current, String next) {
        if (current == null) return true;
        try {
            return AuditStatus.valueOf(next).ordinal() > AuditStatus.valueOf(current).ordinal();
        } catch (Exception e) {
            return false;
        }
    }
}
