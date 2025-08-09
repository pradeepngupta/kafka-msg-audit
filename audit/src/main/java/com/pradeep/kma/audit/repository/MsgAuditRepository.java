package com.pradeep.kma.audit.repository;

import com.pradeep.kma.audit.datamodel.MsgAudit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface MsgAuditRepository extends JpaRepository<MsgAudit, Long> {
    Optional<MsgAudit> findByAuditId(String auditId);

}

