package com.pradeep.kma.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducerSimulator {

     private final KafkaTemplate<String, String> kafkaTemplate;

     @Autowired
    public KafkaProducerSimulator(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(String key, String msg) {
        kafkaTemplate.send("demo-topic", key, msg);
    }
}
