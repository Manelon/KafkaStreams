package com.manelon.kafkastreams_simple;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
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
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import com.manelon.kafkastreams_simple.utils.avro.AvroDecimalConverter;
import com.manelon.model.Vulcan;

/**
 * This test checks how well works the deserilization of some of the avro's logical types:
 * Demcimal
 * Date
 * Time
 * Timestamp
 * @see <a href="https://avro.apache.org/docs/1.10.2/spec.html#Logical+Types">Avro Logical Types reference</a>
 */
public class AvroLogicalTypesSupportTest {
	private static final String SCHEMA_REGISTRY_SCOPE = AvroLogicalTypesSupportTest.class.getName();
	private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;

	private static final String INPUT_TOPIC = "vulcans";
	private static final String OUTPUT_TOPIC = "processed_vulcans";

	private static TopologyTestDriver testDriver;

	private static TestInputTopic<Integer, Vulcan> inputTopic;
	private static TestOutputTopic<Integer, Vulcan> outputTopic;
	

	// private static Serde<UserId> stringSerde = new Serdes.StringSerde();
	// private static Serde<Long> longSerde = new Serdes.LongSerde();

	@BeforeAll
	@SuppressWarnings("resource")
	public static void setup() {

		StreamsBuilder builder = new StreamsBuilder();

		// create a simple stream
		KStream<Integer, Vulcan> inputStream = builder.stream(INPUT_TOPIC);
		KStream<Integer, Vulcan> outptStream = inputStream;
		outptStream.to(OUTPUT_TOPIC);
		
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
		Serde<Integer> integerSerde = Serdes.Integer();
		Serde<Vulcan> vulcanSerde = new SpecificAvroSerde<>();
		

		integerSerde.configure(serdeProps, true);
		vulcanSerde.configure(serdeProps, false);

		System.out.println(topology.describe().toString());


		testDriver = new TopologyTestDriver(topology, props);

		// setup test topics
		inputTopic = testDriver.createInputTopic(INPUT_TOPIC, integerSerde.serializer(), vulcanSerde.serializer());
		outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, integerSerde.deserializer(), vulcanSerde.deserializer());


	}

	@AfterAll
	public static void tearDown() {
		testDriver.close();
	}

	@Test
	void should_serialize_and_deserialze_logical_types_with_specific_record () {
		
		LocalDateTime birthday = LocalDateTime.of(1967, 11, 17, 0, 59);
		Instant birthdayTS = birthday.toInstant(ZoneId.systemDefault().getRules().getOffset(birthday));

		// for decimal conversion check: https://github.com/apache/avro/blob/master/lang/java/avro/src/test/java/org/apache/avro/TestDecimalConversion.java
		var vulcan = new Vulcan();
		vulcan.setName("Sarek");
		AvroDecimalConverter.setDecimal(vulcan, "BankBalance", new BigDecimal("3.14"));
		vulcan.setDateOfBirth(birthday.toLocalDate());
		vulcan.setTimeOfBirthMiliseconds(birthday.toLocalTime());
		vulcan.setTimeOfBirthMicroseconds(birthday.toLocalTime());
		vulcan.setBirthTimestamMiliseconds(birthdayTS);
		vulcan.setBirthTimestampMicroseconds(birthdayTS);
		inputTopic.pipeInput( 1, vulcan);
		
		vulcan = new Vulcan("Spock", AvroDecimalConverter.decimalToBytes(new BigDecimal("17.01"), Vulcan.SCHEMA$.getField("BankBalance").schema()), birthday.toLocalDate(), birthday.toLocalTime(), birthday.toLocalTime(), birthdayTS, birthdayTS);

		inputTopic.pipeInput(2,vulcan);

		var sarek = outputTopic.readValue();

		assertEquals("Sarek", sarek.getName());
		assertEquals("3.14", AvroDecimalConverter.getDecimal(sarek, "BankBalance").toString());
		assertEquals(birthday.toLocalDate(), sarek.getDateOfBirth());
		assertEquals(birthday.toLocalTime(), sarek.getTimeOfBirthMiliseconds());
		assertEquals(birthday.toLocalTime(), sarek.getTimeOfBirthMicroseconds());
		assertEquals(birthdayTS, sarek.getBirthTimestamMiliseconds());
		assertEquals(birthdayTS, sarek.getBirthTimestampMicroseconds());

		var spock = outputTopic.readValue();

		assertEquals("Spock", spock.getName());
		assertEquals("17.01", AvroDecimalConverter.getDecimal(spock, "BankBalance").toString());
		assertEquals(birthday.toLocalDate(), spock.getDateOfBirth());
		assertEquals(birthday.toLocalTime(), spock.getTimeOfBirthMiliseconds());
		assertEquals(birthday.toLocalTime(), spock.getTimeOfBirthMicroseconds());
		assertEquals(birthdayTS, spock.getBirthTimestamMiliseconds());
		assertEquals(birthdayTS, spock.getBirthTimestampMicroseconds());


	}

}
