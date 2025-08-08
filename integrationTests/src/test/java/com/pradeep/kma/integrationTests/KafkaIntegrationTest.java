package com.pradeep.kma.integrationTests;

import com.pradeep.kma.audit.*;
import com.pradeep.kma.audit.datamodel.MsgAudit;
import com.pradeep.kma.audit.repository.MsgAuditRepository;
import com.pradeep.kma.consumer.ConsumerMessages;
import com.pradeep.kma.consumer.KafkaConsumerSimulator;
import com.pradeep.kma.producer.KafkaProducerSimulator;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.pradeep.kma.audit.Constants.INTERNAL_AUDIT_TOPIC;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@EmbeddedKafka(partitions = 1, topics = {"demo-topic", INTERNAL_AUDIT_TOPIC})
@SpringBootTest
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
@ImportAutoConfiguration(classes = {
        KafkaAutoConfiguration.class
})
@ContextConfiguration(classes = { AuditConfiguration.class, SpringContextBridge.class,
        KafkaProducerSimulator.class, ConsumerMessages.class, KafkaConsumerSimulator.class, AuditDataConfiguration.class, AuditTrackerListener.class, AuditHelper.class })
@Slf4j
class KafkaIntegrationTest {

    private final KafkaProducerSimulator kafkaProducerSimulator;

    private final ConsumerMessages consumerMessages;

    private final MsgAuditRepository repo;

    @Autowired
    public KafkaIntegrationTest(KafkaProducerSimulator kafkaProducerSimulator, ConsumerMessages consumerMessages, MsgAuditRepository repo) {
        this.kafkaProducerSimulator = kafkaProducerSimulator;
        this.consumerMessages = consumerMessages;
        this.repo = repo;
    }

    @BeforeEach
    void setup() {
        repo.deleteAll();
    }

    @Test
    void testMessageFlow() {
        // 1. Send message
        String key = "test-key-1";
        String msg = "Test message 1";
        kafkaProducerSimulator.send(key, msg);

        // 2. Give time for consumer and audit listener
        await().atMost(500, TimeUnit.MILLISECONDS).until(() -> consumerMessages.messageReceived(msg));

        await().atMost(1000, TimeUnit.MILLISECONDS).until(() -> repo.count() > 0);
        System.out.println("Audit count: " + repo.count());
        System.out.println("Audit records: " + repo.findAll());
        assertEquals(1, repo.count());
        testAudits(List.of(key));
    }

    @Test
    void testMessagesFlow() {
        List<String> msgList = new ArrayList<>();
        List<String> keyList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            // 1. Send message
            String key = "test-key-"+i;
            String msg = "Test message "+i;
            kafkaProducerSimulator.send( key, msg);
            msgList.add(msg);
            keyList.add(key);
        }

        // 2. Give time for consumer and audit listener

        for (String msg: msgList)
            await().atMost(500, TimeUnit.MILLISECONDS).until(()->consumerMessages.messageReceived(msg));

        await().atMost(1000, TimeUnit.MILLISECONDS).until(() -> repo.count() > 0);
        assertEquals(100, repo.count());

        testAudits(keyList);
    }

    private void testAudits(List<String> keys) {
        await().atMost(30, TimeUnit.SECONDS).pollInterval(Duration.ofMillis(500)).untilAsserted(() -> {
            List<MsgAudit> audits = repo.findAll();
            assertEquals(keys.size(), audits.size());
            for (MsgAudit audit : audits) {
                assertEquals("PRC", audit.getStatus());
                assertNotNull(audit.getTsPub(), "tsPub should not be null");
                assertNotNull(audit.getTsAck(), "tsAck should not be null");
                assertNotNull(audit.getTsCom(), "tsCom should not be null");
                assertNotNull(audit.getTsPrc(), "tsPrc should not be null");
            }
        });

        List<MsgAudit> audits = repo.findAll();
        for (int i = 0; i < audits.size(); i++) {
            MsgAudit audit = audits.get(i);
            log.info("*** Audit Message *** {}", audit);
            assertEquals(keys.get(i), audit.getMessageKey());
            assertTrue(audit.getTopicName().length() > 1);
            assertEquals(0, audit.getPartition());
//            assertEquals(i, audit.getOffset());
            assertTrue(Instant.now().isAfter(audit.getTsPub().toInstant()));
            assertTrue(Instant.now().isAfter(audit.getTsPrc().toInstant()));
            assertEquals("PRC", audit.getStatus());
        }
    }

}
