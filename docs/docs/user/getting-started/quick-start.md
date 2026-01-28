# Quick Start

This page shows how to add `streams-bootstrap` to a project and how to create and run a minimal application.

## Prerequisites

- Java 17
- Apache Kafka cluster (brokers reachable from the application)
- `streams-bootstrap-cli` dependency (see [Setup](setup.md) for Gradle/Maven snippets)

## Minimal Kafka Streams Application

Create a subclass of `KafkaStreamsApplication` and implement the required methods.

```java
import com.bakdata.kafka.streams.KafkaStreamsApplication;
import com.bakdata.kafka.streams.SerdeConfig;
import com.bakdata.kafka.streams.StreamsApp;
import com.bakdata.kafka.streams.StreamsTopicConfig;
import com.bakdata.kafka.streams.kstream.KStreamX;
import com.bakdata.kafka.streams.kstream.StreamsBuilderX;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes.StringSerde;

public class MyStreamsApplication extends KafkaStreamsApplication<StreamsApp> {

    public static void main(final String[] args) {
        new MyStreamsApplication().startApplication(args);
    }

    @Override
    public StreamsApp createApp() {
        return new StreamsApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input = builder.streamInput();
                // topology definition
                input.toOutputTopic();
            }

            @Override
            public String getUniqueAppId(final StreamsTopicConfig topics) {
                return "streams-bootstrap-app-" + topics.getOutputTopic();
            }

            @Override
            public SerdeConfig defaultSerializationConfig() {
                return new SerdeConfig(StringSerde.class, StringSerde.class);
            }
        };
    }
}
```

## Running the Application

### Via Command Line Interface

When packaged as a runnable JAR (for example, in a container), the `run` command is the default entrypoint:

```bash
java -jar my-streams-app.jar \
    run \
    --bootstrap-servers kafka:9092 \
    --input-topics input-topic \
    --output-topic output-topic \
    --schema-registry-url http://schema-registry:8081
```

Additional subcommands such as `clean` and `reset` are available for lifecycle management.

### From the `main` Method

In the `main` method, the application subclass starts the framework via:

```java
public static void main(final String[] args) {
    new MyStreamsApplication().startApplication(args);
}
```

This delegates configuration loading, lifecycle handling, and shutdown to `streams-bootstrap`.

---
