package com.pradeep.kma.audit.datamodel;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Date;

@Entity
@Setter
@Getter
@ToString
public class MsgAudit {
    @Id
    @GeneratedValue
    private Long id;

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
