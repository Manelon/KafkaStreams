package com.manelon.kafkastreams_springboot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.manelon.kafkastreams_springboot.config.Topics;
import com.manelon.model.User;
import com.manelon.model.UserEnriched;
import com.manelon.model.UserId;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class KafkaStreamsTest {
    private static final String SCHEMA_REGISTRY_SCOPE = KafkaStreamsTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;

    private static final String INPUT_TOPIC = "users";
    private static final String OUTPUT_TOPIC = "processed_users";

    private static TopologyTestDriver testDriver;

    private static TestInputTopic<UserId, User> inputTopic;
    private static TestOutputTopic<UserId, UserEnriched> outputTopic;

    @BeforeAll
    public static void init() {
        StreamsBuilder builder = new StreamsBuilder();
        var topics = new Topics(INPUT_TOPIC, OUTPUT_TOPIC);
        new KafkastreamsSpringbootApplication().handleStream(builder, topics);

        Properties props = new Properties();
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);

        Topology topology = builder.build();

        // Serde props
        // Configure Serdes to use the same mock schema registry URL
        Map<String, String> serdeProps = Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);

        // Create Serdes used for test record keys and values
        Serde<UserId> userIdSerde = new SpecificAvroSerde<>();
        Serde<User> userSerde = new SpecificAvroSerde<>();
        Serde<UserEnriched> userEnrichedSerde = new SpecificAvroSerde<>();

        userIdSerde.configure(serdeProps, true);
        userSerde.configure(serdeProps, false);
        userEnrichedSerde.configure(serdeProps, false);
        System.out.println(topology.describe().toString());

        testDriver = new TopologyTestDriver(topology, props);

        // setup test topics
        inputTopic = testDriver.createInputTopic(INPUT_TOPIC, userIdSerde.serializer(), userSerde.serializer());
        outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, userIdSerde.deserializer(),
                userEnrichedSerde.deserializer());

    }

    @Test
    void should_filter_user_when_id_is_negative() {
        inputTopic.pipeInput(new UserId(1),
                new User(1, "James T.", "Kirk", "kirk@enterprise.com", "999-999-999", "Iowa"));
        inputTopic.pipeInput(new UserId(0), new User(0, "Ignore", "Me", "dummy.com", null, null));
        inputTopic.pipeInput(new UserId(-1), new User(-1, "Ignore", "Again", "dummy.com", null, null));
        inputTopic.pipeInput(new UserId(2),
                new User(2, "Sock", "son of Sarek", "spock@enterprise,com", "999-999-999i", "Vulcan"));

        var results = outputTopic.readKeyValuesToList();
        assertEquals(2, results.size());

    }

    @Test
    void user_should_have_capital_letters_when_is_processed() {
        inputTopic.pipeInput(new UserId(1), new User(1, "Leonard", "McCoy", "bones@enterprise,com", "112", "sickbay"));
        inputTopic.pipeInput(new UserId(1),
                new User(2, "Montgomery", "Scott", "miracleworker@enterprise,com", "123-456-789", "Scotland"));

        assertEquals("Leonard McCoy", outputTopic.readValue().getFullName());
        assertEquals("Montgomery Scott", outputTopic.readValue().getFullName());

        assertTrue(outputTopic.isEmpty());

    }
}
