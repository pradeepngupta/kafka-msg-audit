package com.pradeep.kma.interceptor;

import com.pradeep.kma.audit.AuditRecord;
import com.pradeep.kma.audit.AuditRecordSenderService;
import com.pradeep.kma.audit.MessageStatus;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.pradeep.kma.audit.Constants.AUDIT_ID;
import static com.pradeep.kma.audit.Constants.EXCLUDED_TOPICS;

@Component
@Slf4j
public class AuditConsumerInterceptor implements ConsumerInterceptor<String, String> {
    private final AuditRecordSenderService auditRecordSenderService;
    @Value("${spring.application.name: Default-Consumer}")
    private String appName;

    private final ConcurrentHashMap<String, ConsumerRecord<String, String>> pendingCommittedRecords = new ConcurrentHashMap<>();

    public AuditConsumerInterceptor(AuditRecordSenderService auditRecordSenderService) {
        this.auditRecordSenderService = auditRecordSenderService;
    }


    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> consumerRecords) {
        consumerRecords.forEach(consumerRecord -> {
            // Log the consumerRecord details
            log.info("Consumed consumerRecord: topic={}, partition={}, offset={}, key={}, value={}",
                    consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key(), consumerRecord.value());

            // Here you can add logic to send an audit message if needed
            if (!EXCLUDED_TOPICS.contains(consumerRecord.topic())) {
                pendingCommittedRecords.putIfAbsent(getKey(consumerRecord), consumerRecord);
                String auditId = consumerRecord.headers().lastHeader(AUDIT_ID) != null ?
                        new String(consumerRecord.headers().lastHeader(AUDIT_ID).value()) : null;
                auditRecordSenderService.publishAuditRecordToKafka(
                        new AuditRecord(
                                auditId,
                                consumerRecord.topic(),
                                String.valueOf(consumerRecord.partition()),
                                consumerRecord.offset(),
                                MessageStatus.MESSAGE_CONSUMED,
                                consumerRecord.key(),
                                consumerRecord.value(),
                                Instant.now(),
                                appName, null
                        ),
                        auditId
                );

            }
        });
        return consumerRecords;
    }

    private String getKey(ConsumerRecord<String, String> consumerRecord) {
        return getKey(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
    }

    private String getKey(String topic, int partition, long offset) {
        return topic + "-" + partition + "-" + offset;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // This method is invoked when offsets are committed.
        // You can log the commit or perform additional actions if needed.
        offsets.forEach((tp, om) -> {
            log.info("Committing offset: topic={}, partition={}, offset={}", tp.topic(), tp.partition(), om.offset());
            ConsumerRecord<String, String> consumerRecord = pendingCommittedRecords.remove(getKey(tp.topic(), tp.partition(), om.offset()));
            if (consumerRecord != null) {
                log.info("Removing pending committed record: topic={}, partition={}, offset={}",
                        consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
                String auditId = consumerRecord.headers().lastHeader(AUDIT_ID) != null ?
                        new String(consumerRecord.headers().lastHeader(AUDIT_ID).value()) : null;
                auditRecordSenderService.publishAuditRecordToKafka(
                        new AuditRecord(
                                auditId,
                                tp.topic(),
                                String.valueOf(tp.partition()),
                                om.offset(),
                                MessageStatus.MESSAGE_PROCESSED,
                                consumerRecord.key(),
                                consumerRecord.value(),
                                Instant.now(),
                                appName, null
                        ),
                        auditId
                );
            } else {
                log.warn("No pending committed record found for topic={}, partition={}, offset={}",
                        tp.topic(), tp.partition(), om.offset());
            }

        });

    }

    @Override
    public void close() {
        // This method is invoked when the interceptor is closed.
        log.info("Closing AuditConsumerInterceptor and clearing pending committed records.");
        log.info("Pending committed records: {}", pendingCommittedRecords);
        pendingCommittedRecords.clear();
        log.info("Pending committed records cleared.");
        auditRecordSenderService.close();
    }

    @Override
    public void configure(Map<String, ?> map) {
        // No specific configuration needed for this interceptor
        log.info("Configuring AuditConsumerInterceptor with properties: {}", map);
    }
}
