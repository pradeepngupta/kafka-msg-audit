package com.pradeep.kma.consumer;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class ConsumerMessages {
    List<String> messageList = new ArrayList<>();

    public boolean messageReceived(String message) {
        return messageList.contains(message);
    }

}
