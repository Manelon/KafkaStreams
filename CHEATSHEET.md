# Helpfull console commands
- Generate sources without compiling: `mvn generate-sources`
- Execute only one test: `mvn test -Dtest="ClassName"` like `mvn test -Dtest="KafkastreamsSpringbootApplicationTests"`
- Execute test only from one class:  `mvn test -Dtest="ClassName#Method"` like `mvn test -Dtest="KafkastreamsSpringbootApplicationTests#kafka_streams_application_should_work"`

