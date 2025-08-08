package com.pradeep.kma.audit.datamodel;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Date;

@Entity
@Setter
@Getter
@ToString
public class MsgAudit {

    @Version
    private Long version;

    @Id
    @GeneratedValue
    @Column(unique = true, nullable = false)
    private Long id;

    @Column(unique = true, nullable = false)
    private String auditId; // Unique identifier for the audit record

    private String messageKey;
    private String topicName;
    @Column(name = "\"partition\"")
    private int partition;
    @Column(name = "\"offset\"")
    private long offset;

    @Column(name = "payload", columnDefinition = "CLOB") // or TEXT
    private String payload;

    private Date tsPub;
    private Date tsAck;
    private Date tsCom;
    private Date tsPrc;
    private String status;
    private String consumerGroup;
}
