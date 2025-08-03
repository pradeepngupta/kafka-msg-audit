package com.pradeep.kma.audit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class AuditRecordSenderService {
    private final Executor executor = Executors.newCachedThreadPool();
    private final KafkaTemplate<String, String> kafkaTemplate;

    public AuditRecordSenderService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishAuditRecordToKafka(AuditRecord auditRecord, String auditId) {
        executor.execute(() -> {
            ObjectWriter ow = new ObjectMapper().writer();
            String json;
            try {
                json = ow.writeValueAsString(auditRecord);
                kafkaTemplate.send("audit-interceptor-topic", auditId, json);
            } catch (JsonProcessingException e) {
                log.error("Error serializing audit record: {}", e.getMessage(), e);
            }
        }
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
