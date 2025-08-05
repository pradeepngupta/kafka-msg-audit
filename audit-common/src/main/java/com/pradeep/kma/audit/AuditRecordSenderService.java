package com.pradeep.kma.audit;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.pradeep.kma.audit.Constants.INTERNAL_AUDIT_TOPIC;

@Service
@Slf4j
public class AuditRecordSenderService {
    private final Executor executor = Executors.newCachedThreadPool();
    private final KafkaTemplate<String, String> kafkaTemplate;

    public AuditRecordSenderService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishAuditRecordToKafka(String json, String auditId) {
        executor.execute(() -> kafkaTemplate.send(INTERNAL_AUDIT_TOPIC, auditId, json)
        );
    }

    public void close() {
        log.info("Shutting down executor service...");
        try {
            boolean terminated = ((ExecutorService) executor).awaitTermination(5, TimeUnit.SECONDS);
            if (!terminated) {
                log.warn("Executor service did not terminate in the specified time.");
                ((ExecutorService) executor).shutdownNow(); // Force shutdown if not terminated
            } else {
                log.info("Executor service terminated successfully.");
            }
        } catch (InterruptedException e) {
            log.warn("Executor service termination interrupted: {}", e.getMessage(), e);
            Thread.currentThread().interrupt(); // Restore interrupted status
        }
    }
}
