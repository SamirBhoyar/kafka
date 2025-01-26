package com.lbg.kafka_producer_consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class Consumer {
    @KafkaListener(topics = {"fruits"},groupId = "abc")
    public void consumerMessage(String message){
        System.out.println("TopicMsg:"+message);
    }
}
