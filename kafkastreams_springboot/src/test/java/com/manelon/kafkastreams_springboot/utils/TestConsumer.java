package com.manelon.kafkastreams_springboot.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.manelon.model.UserId;

import com.manelon.model.UserEnriched;

@Component
public class TestConsumer {
    
    private List<ConsumerRecord<UserId, UserEnriched>> enrichedUsers = new ArrayList<ConsumerRecord<UserId, UserEnriched>>();

    public List<ConsumerRecord<UserId, UserEnriched>> getEnrichedUsers() {
        return enrichedUsers;
    }

    @KafkaListener(topics = "${topics.processedUsers}", groupId = "Test")
    public void consume(ConsumerRecord<UserId, UserEnriched> record) {
        enrichedUsers.add(record);
    }

    

}


