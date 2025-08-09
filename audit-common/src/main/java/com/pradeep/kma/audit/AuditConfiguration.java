package com.pradeep.kma.audit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

@Configuration
public class AuditConfiguration {
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public AuditConfiguration(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Bean
    public AuditRecordSenderService auditRecordSenderService() {
        return new AuditRecordSenderService(kafkaTemplate);
    }

}
