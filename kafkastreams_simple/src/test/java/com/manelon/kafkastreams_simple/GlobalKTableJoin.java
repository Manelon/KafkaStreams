package com.manelon.kafkastreams_simple;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.manelon.model.starfleet.Personnel;
import com.manelon.model.starfleet.PersonnelEnriched;
import com.manelon.model.starfleet.Starship;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

class GlobalKTableJoin {
  	private static final String SCHEMA_REGISTRY_SCOPE = SimpleAvroTopologyTest.class.getName();
	private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;

	private static final String PERSONNEL_TOPIC = "personnel";
    private static final String STARSHIP_TOPIC = "starships";
	private static final String OUTPUT_TOPIC = "personnel-ships";

	private static TopologyTestDriver testDriver;

	private static TestInputTopic<Integer, Personnel> personnelTopic;
    private static TestInputTopic<Integer, Starship> starshipTopic;
	private static TestOutputTopic<Integer, PersonnelEnriched> outputTopic;

    private static Serde<Integer> integerSerde = new Serdes.IntegerSerde();

    @BeforeAll
	@SuppressWarnings("resource")
	public static void setup() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<Integer, Personnel> personnel = builder.stream(PERSONNEL_TOPIC);
        GlobalKTable<Integer, Starship> starship = builder.globalTable(STARSHIP_TOPIC);

        KStream<Integer, PersonnelEnriched> outputStream = personnel.join(starship, 
                                                                        (leftKey,leftValue) -> leftValue.getStarship() , 
                                                                        (leftValue, rightValue) -> {
                                                                            return new PersonnelEnriched(
                                                                                leftValue.getName(),
                                                                                leftValue.getStarship(),
                                                                                rightValue.getName(),
                                                                                leftValue.getRank()
                                                                            );
                                                                        });
        outputStream.to(OUTPUT_TOPIC);
        
        
        //setup test driver
		Properties props = new Properties();
		props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
		props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);

		Topology topology = builder.build();

		//Serde props
		// Configure Serdes to use the same mock schema registry URL
		Map<String,String> serdeProps = Map.of(
			AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL
		);

		// Create Serdes used for test record keys and values
		Serde<Personnel> personnelSerde = new SpecificAvroSerde<>();
		Serde<Starship> starshipSerde = new SpecificAvroSerde<>();
		Serde<PersonnelEnriched> personalEnrichedSerde = new SpecificAvroSerde<>();

		personnelSerde.configure(serdeProps, false);
		starshipSerde.configure(serdeProps, false);
		personalEnrichedSerde.configure(serdeProps, false);
		System.out.println(topology.describe().toString());


		testDriver = new TopologyTestDriver(topology, props);

		// setup test topics
        personnelTopic = testDriver.createInputTopic(PERSONNEL_TOPIC, integerSerde.serializer(), personnelSerde.serializer());
        starshipTopic = testDriver.createInputTopic(STARSHIP_TOPIC, integerSerde.serializer(), starshipSerde.serializer());

        outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, integerSerde.deserializer(), personalEnrichedSerde.deserializer());

    }

    @AfterAll
	public static void tearDown() {
		testDriver.close();
	}

	/*
	 * Learning how to join a Stream with a GlobalKTable
	 * https://kafka.apache.org/34/documentation/streams/developer-guide/dsl-api.html#kstream-globalktable-join
	 */
	@Test
	void should_join_person_with_ship () {
		starshipTopic.pipeInput(1, new Starship("Enterprise", "NCC-1701"));
		starshipTopic.pipeInput(2, new Starship("Excelsior", "NX-2000"));
		starshipTopic.pipeInput(3, new Starship("Reliant", "NCC-1864"));
		starshipTopic.pipeInput(4, new Starship("Pasteur", "NCC-58925"));
		starshipTopic.pipeInput(5, new Starship("Nebula", "NCC-2001"));
		starshipTopic.pipeInput(6, new Starship("Defiant", "NCC-75633"));

		personnelTopic.pipeInput(1, new Personnel("James T. Kirk", 1, "Captain"));
		personnelTopic.pipeInput(2, new Personnel("Spock", 1, "Commander"));
		personnelTopic.pipeInput(3, new Personnel("Montgomery Scott", 1, "Lieutenant Commander"));
		personnelTopic.pipeInput(4, new Personnel("Uhura", 1, "Lieutenant"));
		personnelTopic.pipeInput(5, new Personnel("Sulu", 1, "Lieutenant"));

		personnelTopic.pipeInput(6, new Personnel("Styles", 2, "Captain"));
		personnelTopic.pipeInput(7, new Personnel("Clark Terrel", 3, "Captain"));

		assertEquals(7, outputTopic.getQueueSize());
		var output = outputTopic.readKeyValuesToMap();

		var kirk = output.get(1);
		assertEquals("Enterprise", kirk.getStarshipName());
		assertEquals("Captain", kirk.getRank());

		var terrel = output.get(7);
		assertEquals("Reliant", terrel.getStarshipName());
		assertEquals("Captain", terrel.getRank());

		var scotty = output.get(3);
		assertEquals("Enterprise", scotty.getStarshipName());
		assertEquals("Lieutenant Commander", scotty.getRank());


	}

}
