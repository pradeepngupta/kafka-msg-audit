package com.pradeep.kma.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class KafkaConsumerSimulator {

    private final ConsumerMessages consumerMessages;

    @Autowired
    public KafkaConsumerSimulator(ConsumerMessages consumerMessages) {
        this.consumerMessages = consumerMessages;
    }

    @KafkaListener(topics = "demo-topic", groupId = "real-consumer-group")
    public void consume(String message) {
        log.info("✅ Consumed message: {}", message);
        consumerMessages.messageList.add(message);
    }


}
