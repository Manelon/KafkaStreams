package com.manelon.kafkastreams_springboot;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import com.manelon.kafkastreams_springboot.config.Topics;
import com.manelon.model.User;
import com.manelon.model.UserEnriched;
import com.manelon.model.UserId;

@SpringBootApplication
@EnableKafkaStreams
@EnableConfigurationProperties(Topics.class)
public class KafkastreamsSpringbootApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkastreamsSpringbootApplication.class, args);
	}

	@Bean 
	public KStream<UserId, User> handleStream(StreamsBuilder builder, Topics topicsConfiguration) {
		//I don't like the way springboot handle the creation of the topology.
		//I need the StreamsBuilder, but I don't need to return the string builder
		//The good thing, is with this approach we can registrer serveral streams 
		KStream<UserId, User> inputStream = builder.stream(topicsConfiguration.getUsers());
		KStream<UserId, UserEnriched> outptStream = inputStream
			.filter((id, user) -> (id.getId() > 0), Named.as("Filering_negative_ids"))
			.mapValues((user) -> (new UserEnriched(
				user.getId(),
				user.getFirstName(),
				user.getLastName(),
				user.getFirstName() + ' ' + user.getLastName(),
				user.getEMail(),
				user.getPhoneNumber(),
				user.getAddress()
			 )), Named.as("Calculating_FullName"));
		outptStream.to(topicsConfiguration.getProcessedUsers());
		return inputStream;
	}
}
