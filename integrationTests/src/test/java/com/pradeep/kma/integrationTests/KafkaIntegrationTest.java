package com.pradeep.kma.integrationTests;

import com.pradeep.kma.consumer.ConsumerMessages;
import com.pradeep.kma.producer.KafkaProducerSimulator;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@EmbeddedKafka(partitions = 1, topics = {"demo-topic"})
@SpringBootTest
@TestPropertySource(properties = {
    "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
class KafkaIntegrationTest {

    private final KafkaProducerSimulator kafkaProducerSimulator;

    private final ConsumerMessages consumerMessages;

    @Autowired
    public KafkaIntegrationTest(KafkaProducerSimulator kafkaProducerSimulator, ConsumerMessages consumerMessages) {
        this.kafkaProducerSimulator = kafkaProducerSimulator;
        this.consumerMessages = consumerMessages;
    }

    @Test
    void testMessageFlow() {
        // 1. Send message
        String key = "test-key-1";
        String msg = "Test message 1";
        kafkaProducerSimulator.send( key, msg);

        // 2. Give time for consumer and audit listener
        await().atMost(500, TimeUnit.MILLISECONDS).until(()-> consumerMessages.messageReceived(msg));
    }

    @Test
    void testMessagesFlow() {
        List<String> msgList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            // 1. Send message
            String key = "test-key-"+i;
            String msg = "Test message "+i;
            kafkaProducerSimulator.send( key, msg);
            msgList.add(msg);
        }

        // 2. Give time for consumer and audit listener

        for (String msg: msgList)
            await().atMost(500, TimeUnit.MILLISECONDS).until(()->consumerMessages.messageReceived(msg));

    }
}
