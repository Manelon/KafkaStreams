package com.manelon.kafkastreams_simple;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.CommandLineRunner;

@SpringBootApplication
public class KafkastreamsSimpleApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(KafkastreamsSimpleApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		System.out.println("Application running");
		
	}

}
