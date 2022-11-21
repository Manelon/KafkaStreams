package com.manelon.kafkastreams_springboot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;

import com.manelon.kafkastreams_springboot.config.Topics;
import com.manelon.kafkastreams_springboot.consumers.TestConsumer;
import com.manelon.model.User;
import com.manelon.model.UserId;

@SpringBootTest (webEnvironment = WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 1,
         topics = {
				"${topics.users}",
                "${topics.processedUsers}" })
class KafkastreamsSpringbootApplicationTests {

  @Autowired
	private TestRestTemplate restTemplate;

	@LocalServerPort
	private int port;

  @Autowired
  private KafkaTemplate<UserId, User> producerTemplate;

  @Autowired
  private Topics topics;

  @Autowired
  private TestConsumer consumer;

  private String baseUrl;

  
 

 @BeforeEach
  void init () {
    // MockSchemaRegistry.validateAndMaybeGetMockScope(SchemaRegistryURL);
    baseUrl = "http://localhost:" + port + "/";
  }

  Logger log = LoggerFactory.getLogger(KafkastreamsSpringbootApplicationTests.class);

	@Test
	void topology_controller_should_return_the_topology() {

		String expectedTopology = """
Topologies:
   Sub-topology: 0
    Source: KSTREAM-SOURCE-0000000000 (topics: [users])
      --> Filering_negative_ids
    Processor: Filering_negative_ids (stores: [])
      --> Calculating_FullName
      <-- KSTREAM-SOURCE-0000000000
    Processor: Calculating_FullName (stores: [])
      --> KSTREAM-SINK-0000000003
      <-- Filering_negative_ids
    Sink: KSTREAM-SINK-0000000003 (topic: processed_users)
      <-- Calculating_FullName

	 """;
		assertEquals(expectedTopology, this.restTemplate.getForObject("http://localhost:" + port + "/topology", String.class));
		
	}

  @Test
  void health_should_return_kafka_streams_status (){

    var healthResponse = restTemplate.getForEntity(baseUrl+"actuator/health", String.class);
    assertEquals(HttpStatus.OK, healthResponse.getStatusCode());
    assertTrue(healthResponse.getBody().contains("kafkaStreams"));

    
  }

  @Test
  void kafka_streams_application_should_work () throws InterruptedException {
    producerTemplate.send(topics.getUsers(),new UserId(1), new User(1, "James T.", "Kirk", "kirk@enterprise.com", "999-999-999", "Iowa"));

    var usersProcessed = consumer.getEnrichedUsers();

    Thread.sleep(500);

    assertEquals(1, usersProcessed.size());
    
  }

  @Bean
  public SchemaRegistryClient schemaRegistryClient(@Value("${spring.kafka.properties.schema.registry.url}") String endpoint){
    SchemaRegistryClient client = MockSchemaRegistry.getClientForScope(endpoint);
    return client;
  }


  

}
