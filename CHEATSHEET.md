# Helpfull console commands and other tips
- Generate sources without compiling: `mvn generate-sources`
- Execute only one test: `mvn test -Dtest="ClassName"` like `mvn test -Dtest=GlobalKTableJoin`
- Execute test only from one class:  `mvn test -Dtest="ClassName#Method"` like `mvn test -Dtest="KafkastreamsSpringbootApplicationTests#kafka_streams_application_should_work"`
- In order to fix dependency conflicts use the [dependency management section](https://maven.apache.org/guides/introduction/introduction-to-dependency-mechanism.html#Dependency_Management)
    ```xml
        <dependencyManagement>
		    <dependencies>
			    <dependency>
				    <groupId>org.scala-lang</groupId>
				    <artifactId>scala-library</artifactId>
				    <version>2.13.8</version>
			    </dependency>
		    </dependencies>
	    </dependencyManagement>
    ```
    Now no matters what is the version in the dependencies, maven will always resolver with the version 2.13.8 for the scala-library
