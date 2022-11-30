package com.manelon.kafkastreams_simple;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
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
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import com.manelon.kafkastreams_simple.utils.avro.AvroDateConverter;
import com.manelon.kafkastreams_simple.utils.avro.AvroDecimalConverter;
import com.manelon.kafkastreams_simple.utils.avro.AvroInstantConverter;
import com.manelon.kafkastreams_simple.utils.avro.AvroTimeConverter;
import com.manelon.model.Vulcan;

/**
 * This test checks how well works the deserilization of some of the avro's logical types:
 * Demcimal
 * Date
 * Time
 * Timestamp
 * @see <a href="https://avro.apache.org/docs/1.10.2/spec.html#Logical+Types">Avro Logical Types reference</a>
 */
public class AvroLogicalTypesSupportWithGenericRecordsTest {
	private static final String SCHEMA_REGISTRY_SCOPE = AvroLogicalTypesSupportTest.class.getName();
	private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;

	private static final String INPUT_TOPIC = "vulcans";
	private static final String OUTPUT_TOPIC = "processed_vulcans";

	private static TopologyTestDriver testDriver;

	private static TestInputTopic<Integer, GenericRecord> inputTopic;
	private static TestOutputTopic<Integer, GenericRecord> outputTopic;
	

	// private static Serde<UserId> stringSerde = new Serdes.StringSerde();
	// private static Serde<Long> longSerde = new Serdes.LongSerde();

	@BeforeAll
	@SuppressWarnings("resource")
	public static void setup() {

		StreamsBuilder builder = new StreamsBuilder();

		// create a simple stream
		KStream<Integer, GenericRecord> inputStream = builder.stream(INPUT_TOPIC);
		KStream<Integer, GenericRecord> outptStream = inputStream;
		outptStream.to(OUTPUT_TOPIC);
		
		//setup test driver
		Properties props = new Properties();
		props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
		props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);

		Topology topology = builder.build();

		//Serde props
		// Configure Serdes to use the same mock schema registry URL
		Map<String,String> serdeProps = Map.of(
			AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL
		);

		// Create Serdes used for test record keys and values
		Serde<Integer> integerSerde = Serdes.Integer();
		GenericAvroSerde vulcanSerde = new GenericAvroSerde();
		

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
	void should_serialize_and_deserialze_logical_types_with_generic_record () {
		
		LocalDateTime birthday = LocalDateTime.of(1967, 11, 17, 0, 59);
		Instant birthdayTS = birthday.toInstant(ZoneId.systemDefault().getRules().getOffset(birthday));

		// for decimal conversion check: https://github.com/apache/avro/blob/master/lang/java/avro/src/test/java/org/apache/avro/TestDecimalConversion.java
		var vulcan = new GenericRecordBuilder(Vulcan.SCHEMA$)
                                    .set("Name", "Sarek")
                                    .set("BankBalance", AvroDecimalConverter.decimalToBytes(BigDecimal.valueOf(3.14), Vulcan.SCHEMA$.getField("BankBalance").schema()))
                                    .set("DateOfBirth", AvroDateConverter.toAvro(birthday.toLocalDate()))
                                    .set("TimeOfBirthMiliseconds", AvroTimeConverter.LocalTimeToAvro(Vulcan.SCHEMA$, "TimeOfBirthMiliseconds", birthday.toLocalTime()))
                                    .set("TimeOfBirthMicroseconds", AvroTimeConverter.LocalTimeToAvro(Vulcan.SCHEMA$, "TimeOfBirthMicroseconds", birthday.toLocalTime()))
                                    .set("BirthTimestamMiliseconds", AvroInstantConverter.InstantToAvro(Vulcan.SCHEMA$, "BirthTimestamMiliseconds", birthdayTS))
                                    .set("BirthTimestampMicroseconds", AvroInstantConverter.InstantToAvro(Vulcan.SCHEMA$, "BirthTimestampMicroseconds", birthdayTS))
                                    .build();

		inputTopic.pipeInput( 1, vulcan);
		
		vulcan = new GenericRecordBuilder(Vulcan.SCHEMA$)
									.set("Name", "Spock")
                                    .set("BankBalance", AvroDecimalConverter.decimalToBytes(BigDecimal.valueOf(17.01), Vulcan.SCHEMA$.getField("BankBalance").schema()))
                                    .set("DateOfBirth", AvroDateConverter.toAvro(birthday.toLocalDate()))
                                    .set("TimeOfBirthMiliseconds", AvroTimeConverter.LocalTimeToAvro(Vulcan.SCHEMA$, "TimeOfBirthMiliseconds", birthday.toLocalTime()))
                                    .set("TimeOfBirthMicroseconds", AvroTimeConverter.LocalTimeToAvro(Vulcan.SCHEMA$, "TimeOfBirthMicroseconds", birthday.toLocalTime()))
                                    .set("BirthTimestamMiliseconds", AvroInstantConverter.InstantToAvro(Vulcan.SCHEMA$, "BirthTimestamMiliseconds", birthdayTS))
                                    .set("BirthTimestampMicroseconds", AvroInstantConverter.InstantToAvro(Vulcan.SCHEMA$, "BirthTimestampMicroseconds", birthdayTS))
                                    .build();
		// vulcan.put("Name", "Spock");
		// AvroDecimalConerter.setDecimal(vulcan, "BankBalance", BigDecimal.valueOf(17.01));
        // vulcan.put("DateOfBirth", AvroDateConverter.toAvro(birthday.toLocalDate()));
		// AvroTimeConverter.setLocalTime(vulcan, "TimeOfBirthMiliseconds", birthday.toLocalTime());
		// AvroTimeConverter.setLocalTime(vulcan, "TimeOfBirthMicroseconds", birthday.toLocalTime());
		// AvroInstantConverter.setInstant(vulcan, "BirthTimestamMiliseconds", birthdayTS);
		// AvroInstantConverter.setInstant(vulcan, "BirthTimestampMicroseconds", birthdayTS);

		inputTopic.pipeInput(2,vulcan);

		var sarek = outputTopic.readValue();

		assertEquals("Sarek", sarek.get("Name"));
		assertEquals("3.14", AvroDecimalConverter.getDecimal(sarek, "BankBalance").toString());
		assertEquals(birthday.toLocalDate(), AvroDateConverter.fromAvro((int)sarek.get("DateOfBirth")));
		assertEquals(birthday.toLocalTime(), AvroTimeConverter.getLocalTime(sarek,"TimeOfBirthMiliseconds"));
		assertEquals(birthday.toLocalTime(), AvroTimeConverter.getLocalTime(sarek, "TimeOfBirthMicroseconds"));
		assertEquals(birthdayTS, AvroInstantConverter.getInstant(sarek,"BirthTimestamMiliseconds"));
		assertEquals(birthdayTS, AvroInstantConverter.getInstant(sarek,"BirthTimestampMicroseconds"));

		var spock = outputTopic.readValue();

		assertEquals("Spock", spock.get("Name"));	
		assertEquals("17.01", AvroDecimalConverter.getDecimal(spock, "BankBalance").toString());
		assertEquals(birthday.toLocalDate(), AvroDateConverter.fromAvro((int)spock.get("DateOfBirth")));
		assertEquals(birthday.toLocalTime(), AvroTimeConverter.getLocalTime(spock,"TimeOfBirthMiliseconds"));
		assertEquals(birthday.toLocalTime(), AvroTimeConverter.getLocalTime(spock,"TimeOfBirthMicroseconds"));
		assertEquals(birthdayTS, AvroInstantConverter.getInstant(spock, "BirthTimestamMiliseconds"));
		assertEquals(birthdayTS, AvroInstantConverter.getInstant(spock,"BirthTimestampMicroseconds"));



	}

}
