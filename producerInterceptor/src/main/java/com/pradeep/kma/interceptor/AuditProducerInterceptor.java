package com.pradeep.kma.interceptor;

import com.pradeep.kma.audit.AuditRecord;
import com.pradeep.kma.audit.AuditRecordSenderService;
import com.pradeep.kma.audit.MessageStatus;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.pradeep.kma.audit.Constants.*;

@Component
@Slf4j
public class AuditProducerInterceptor implements ProducerInterceptor<String, String> {

    private final AuditRecordSenderService auditRecordSenderService;

    @Value("${spring.application.name: Default-Producer}")
    private String appName;

    private final ConcurrentHashMap<String, List<ProducerRecord<String, String>>> pendingAckRecords = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, List<RecordMetadata>> unverifiedAckRecords = new ConcurrentHashMap<>();

    public AuditProducerInterceptor(AuditRecordSenderService auditRecordSenderService) {
        this.auditRecordSenderService = auditRecordSenderService;
    }


    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        // This method is invoked before the record is sent to Kafka.
        // You can modify the record here, for example, add a header or change the key/value.

        if (EXCLUDED_TOPICS.contains(producerRecord.topic())) {
            return producerRecord; // Skip audit
        }

        // Add audit_id header
        String auditId = UUID.randomUUID().toString();
        producerRecord.headers().add(AUDIT_ID, auditId.getBytes(StandardCharsets.UTF_8));

        List<ProducerRecord<String, String>> producerRecords = pendingAckRecords.get(producerRecord.topic());
        if (producerRecords == null) {
            pendingAckRecords.put(producerRecord.topic(), List.of(producerRecord));
        } else {
            producerRecords.add(producerRecord);
        }

        auditRecordSenderService.publishAuditRecordToKafka(
                new AuditRecord(
                        auditId,
                        producerRecord.topic(),
                        null,
                        0,
                        MessageStatus.MESSAGE_SENT,
                        producerRecord.key(),
                        producerRecord.value(),
                        Instant.now(),
                        appName, null
                ),
                auditId
        );

        return producerRecord;
    }


    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // This method is invoked when Kafka responds with an acknowledgement for a sent record.

        if (EXCLUDED_TOPICS.contains(metadata.topic())) {
            return; // Skip audit
        }

        if (exception == null) {
            List<ProducerRecord<String, String>> producerRecords = pendingAckRecords.get(metadata.topic());

            if (producerRecords == null || producerRecords.isEmpty()) {
                log.warn("No pending records found for topic: {}", metadata.topic());
                return; // No pending records to process
            }
            if (producerRecords.size() > 1) {
                log.info("Multiple pending records found for topic: {}", metadata.topic());
                List<RecordMetadata> recordMetadataList = unverifiedAckRecords.get(metadata.topic());
                if (recordMetadataList == null) {
                    unverifiedAckRecords.put(metadata.topic(), List.of(metadata));
                } else {
                    recordMetadataList.add(metadata);
                }
            } else {
                log.info("Single pending record found for topic: {}", metadata.topic());
                auditRecordSenderService.publishAuditRecordToKafka(
                        new AuditRecord(
                                Arrays.toString(producerRecords.get(0).headers().lastHeader(AUDIT_ID).value()),
                                metadata.topic(),
                                String.valueOf(metadata.partition()),
                                metadata.offset(),
                                MessageStatus.MESSAGE_ACKNOWLEDGED,
                                producerRecords.get(0).key(),
                                producerRecords.get(0).value(),
                                Instant.now(),
                                appName, null
                        ),
                        Arrays.toString(producerRecords.get(0).headers().lastHeader(AUDIT_ID).value())
                );
            }
        }
    }

    @Override
    public void close() {
        auditRecordSenderService.close();

        log.info("pendingAckRecords: {}", pendingAckRecords);
        log.info("unverifiedAckRecords: {}", unverifiedAckRecords);
        // clean-up if required
        pendingAckRecords.clear();
        unverifiedAckRecords.clear();

        log.info("AuditProducerInterceptor closed and cleared pending records.");
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // configure if needed
    }
}
