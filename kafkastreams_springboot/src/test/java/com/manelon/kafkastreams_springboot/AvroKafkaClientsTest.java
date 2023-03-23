package com.manelon.kafkastreams_springboot;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

import com.manelon.model.User;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

@SpringBootTest
@EmbeddedKafka(topics = {"users"})
public class AvroKafkaClientsTest {

    @Autowired
    KafkaProperties kafkaProperties;
    // String schemaRegistryUrl = "mock://" + this.getClass().getSimpleName();

     @Value("${spring.kafka.properties.schema.registry.url}") 
     String schemaRegistryUrl;

    SchemaRegistryClient schemaRegistryClient = MockSchemaRegistry.getClientForScope(schemaRegistryUrl);

    @Test
    public void produce_anc_consume_avro_message() {
 
        Serde<Integer> integerSerde = Serdes.Integer();
        SpecificAvroSerde<User> userSerde = new SpecificAvroSerde<>(); //is not needed the instance of the mockschema registry because the serde is the same for the producer and the consumer
        Map<String, Object> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        serdeConfig.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true); 
        
        userSerde.configure(serdeConfig, false);

        


        
        var producerConfig = kafkaProperties.buildProducerProperties();
        producerConfig.put (AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
        producerConfig.put (AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        var producer = new KafkaProducer<Integer, User>(producerConfig, integerSerde.serializer(), userSerde.serializer());
        
        

        var user = new User(1, "Manel", "Rules", "Email", "Phone", "MK");
        producer.send(new ProducerRecord<>("users", user.getId(), user), (metadata, exception) -> {
            if (exception != null) {
                exception.printStackTrace();
            }
        });
        producer.flush();
        producer.close();

        var consumerConfig = kafkaProperties.buildConsumerProperties();
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        consumerConfig.put (AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
        consumerConfig.put (AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        var consumer = new KafkaConsumer<Integer, User>(consumerConfig, integerSerde.deserializer(), userSerde.deserializer());

        consumer.subscribe(List.of("users"));

        var results = consumer.poll(Duration.ofMillis(1000));
        Assertions.assertThat(results.count()).isEqualTo(1);

        var message = results.iterator().next();
        Assertions.assertThat(message.value()).isEqualTo(user);

        consumer.close();
    }
}
