package com.manelon.kafkastreams_simple;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class SimpleTopologyTest {
	private static final String INPUT_TOPIC = "users";
	private static final String OUTPUT_TOPIC = "users_in_capital_letters";

	private static TopologyTestDriver testDriver;

	private static TestInputTopic<Long, String> inputTopic;
	private static TestOutputTopic<Long, String> outputTopic;
	

	private static Serde<String> stringSerde = new Serdes.StringSerde();
	private static Serde<Long> longSerde = new Serdes.LongSerde();

	@BeforeAll
	public static void setup() {

		StreamsBuilder builder = new StreamsBuilder();

		// create a simple stream
		KStream<Long, String> inputStream = builder.stream(INPUT_TOPIC);
		KStream<Long, String> outptStream = inputStream
			.filter((id, user) -> (id >= 0), Named.as("Filering_negative_ids"))
			.mapValues((id, user) -> (user.toUpperCase()), Named.as("Converting_user_to_upper_case"));
		outptStream.to(OUTPUT_TOPIC);
		
		//setup test driver
		Properties props = new Properties();
		props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
		props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

		Topology topology = builder.build();

		System.out.println(topology.describe().toString());

		testDriver = new TopologyTestDriver(topology, props);

		// setup test topics
		inputTopic = testDriver.createInputTopic(INPUT_TOPIC, longSerde.serializer(), stringSerde.serializer());
		outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, longSerde.deserializer(), stringSerde.deserializer());


	}

	@AfterAll
	public static void tearDown() {
		testDriver.close();
	}

	@Test
	void should_filter_user_when_id_is_negative () {
		inputTopic.pipeInput( 1l, "James T. Kirk");
		inputTopic.pipeInput(-1l, "ingnore this user");
		inputTopic.pipeInput(-2l, "Another user to ignore");
		inputTopic.pipeInput( 2l, "Spock");

		var results = outputTopic.readKeyValuesToList();
		assertEquals(2, results.size());

	}

	@Test
	void user_should_have_capital_letters_when_is_processed () {
		inputTopic.pipeInput( 1l, "Leonard McCoy");
		inputTopic.pipeInput( 2l, "Montgomery Scott");

		assertEquals("LEONARD MCCOY", outputTopic.readValue());
		assertEquals("MONTGOMERY SCOTT", outputTopic.readValue());

		assertTrue(outputTopic.isEmpty());

	}

}
