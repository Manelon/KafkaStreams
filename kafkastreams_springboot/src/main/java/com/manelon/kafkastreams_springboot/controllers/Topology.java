package com.manelon.kafkastreams_springboot.controllers;

import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Topology {
    //I know... I can simply write autowire here, but I prefer use constructor injection
    private StreamsBuilderFactoryBean streamsFactory;
    
    public Topology(StreamsBuilderFactoryBean streamsFactory){
        this.streamsFactory = streamsFactory;
    }

    @GetMapping("/topology")
    public ResponseEntity<String> getTopology() {
        //TODO: integrate this with https://zz85.github.io/kafka-streams-viz/ (It should be better a webpage than a rest api)
        return ResponseEntity.ok(streamsFactory.getTopology().describe().toString());
    }

    
}
