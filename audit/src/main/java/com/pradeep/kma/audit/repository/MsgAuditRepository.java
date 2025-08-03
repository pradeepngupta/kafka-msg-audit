package com.pradeep.kma.audit.repository;

import com.pradeep.kma.audit.datamodel.MsgAudit;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface MsgAuditRepository extends JpaRepository<MsgAudit, Long> {
    Optional<MsgAudit> findByAuditId(String auditId);

}

