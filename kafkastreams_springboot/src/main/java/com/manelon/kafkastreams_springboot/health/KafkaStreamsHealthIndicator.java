package com.manelon.kafkastreams_springboot.health;

import org.apache.kafka.streams.ThreadMetadata;
import org.apache.kafka.streams.KafkaStreams.State;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health.Builder;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

@Component
public class KafkaStreamsHealthIndicator extends AbstractHealthIndicator {

    //We expect only one StreamsBuilderFactoryBean in the application
    @Autowired
    private StreamsBuilderFactoryBean streamsBuilder;

    @Override
    protected void doHealthCheck(Builder builder) throws Exception {
       
        var state = streamsBuilder.getKafkaStreams().state();

        

        

        //If CREATED, RUNING or REBALANCING
         if (state == State.CREATED || state.isRunningOrRebalancing()) {
            builder.up().withDetail("state", state.name());
            //TODO:Add more details (threas, tasks, etc)
            // var metadata = streamsBuilder.getKafkaStreams().metadataForLocalThreads();
            // for (ThreadMetadata threadMetadata : metadata) {
            //     threadMetadata.activeTasks()
            // }
            return;
         }

        // ERROR, NOT_RUNNING, PENDING_SHUTDOWN, 
        builder.down().withDetail("state", state.name()).build();
        

    }

}
