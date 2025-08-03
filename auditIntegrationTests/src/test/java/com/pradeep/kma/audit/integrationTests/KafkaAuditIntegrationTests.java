package com.pradeep.kma.audit.integrationTests;

import com.pradeep.kma.audit.*;
import com.pradeep.kma.audit.datamodel.MsgAudit;
import com.pradeep.kma.audit.repository.MsgAuditRepository;
import com.pradeep.kma.consumer.ConsumerMessages;
import com.pradeep.kma.consumer.KafkaConsumerSimulator;
import com.pradeep.kma.producer.KafkaProducerSimulator;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@EmbeddedKafka(partitions = 1, topics = {"demo-topic"},
        brokerProperties = {
        "auto.create.topics.enable=true"})
@SpringBootTest
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ImportAutoConfiguration(classes = {
        KafkaAutoConfiguration.class
})
@ContextConfiguration(classes = { AuditConfiguration.class, SpringContextBridge.class,
        KafkaProducerSimulator.class, ConsumerMessages.class, KafkaConsumerSimulator.class, AuditDataConfiguration.class, AuditTrackerListener.class, AuditHelper.class})
@Slf4j
class KafkaAuditIntegrationTests {
    private final KafkaProducerSimulator kafkaProducerSimulator;

    private final ConsumerMessages consumerMessages;

    private final MsgAuditRepository repo;

    @Autowired
    public KafkaAuditIntegrationTests(KafkaProducerSimulator kafkaProducerSimulator, ConsumerMessages consumerMessages, MsgAuditRepository repo) {
        this.kafkaProducerSimulator = kafkaProducerSimulator;
        this.consumerMessages = consumerMessages;
        this.repo = repo;
    }

    @BeforeAll
    void setup() {
        repo.deleteAll();
    }

    @Test
    void testMessageFlow() {
        // 1. Send message
        String key = "test-key-1";
        String msg = "Test message 1";
        kafkaProducerSimulator.send( key, msg);

        // 2. Give time for consumer and audit listener
        await().atMost(500, TimeUnit.MILLISECONDS).until(()-> consumerMessages.messageReceived(msg));
        assertEquals(1, repo.count());
        testAudits(List.of(key));
    }

    private void testAudits(List<String> keys) {
        await().atMost(2, TimeUnit.SECONDS).pollInterval(Duration.ofMillis(200)).untilAsserted(() -> {
            List<MsgAudit> audits = repo.findAll();
            assertEquals(keys.size(), audits.size());
            for (int i = 0; i < audits.size(); i++) {
                MsgAudit audit = audits.get(i);

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
            assertEquals(i, audit.getOffset());
            assertTrue(Instant.now().isAfter(audit.getTsPub()));
            assertTrue(Instant.now().isAfter(audit.getTsAck()));
            assertTrue(Instant.now().isAfter(audit.getTsCom()));
            assertTrue(Instant.now().isAfter(audit.getTsPrc()));
            assertEquals("PRC", audit.getStatus());

            //assertTrue(audit.getTsPub().isBefore(audit.getTsAck()));
            assertTrue(audit.getTsAck().isBefore(audit.getTsCom()));
            assertTrue(audit.getTsCom().isBefore(audit.getTsPrc()));
        }
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

        assertEquals(100, repo.count());

        testAudits(keyList);
    }
}
