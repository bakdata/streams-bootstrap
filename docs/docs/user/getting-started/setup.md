# Setup

This page describes dependency setup, configuration options, commands, and Helm-based deployment for
`streams-bootstrap`.

## Dependencies

### Gradle

```gradle
implementation group: 'com.bakdata.kafka', name: 'streams-bootstrap-cli', version: '6.1.0'
```

With Kotlin DSL:

```gradle
implementation(group = "com.bakdata.kafka", name = "streams-bootstrap-cli", version = "6.1.0")
```

### Maven

```xml

<dependency>
    <groupId>com.bakdata.kafka</groupId>
    <artifactId>streams-bootstrap-cli</artifactId>
    <version>6.1.0</version>
</dependency>
```

For other build tools or versions, refer to the
[latest version in MvnRepository](https://mvnrepository.com/artifact/com.bakdata.kafka/streams-bootstrap/latest).

## Kafka Streams Applications

Create a subclass of `KafkaStreamsApplication` and provide a `StreamsApp` via `createApp()`.

Key responsibilities of `StreamsApp`:

- Define the topology in `buildTopology(StreamsBuilderX builder)`
- Provide a unique application id in `getUniqueAppId(StreamsTopicConfig topics)`
- Configure default key/value SerDes via `defaultSerializationConfig()`
- Optionally provide extra Kafka properties via `createKafkaProperties()`

A complete example is shown in the Quick Start page.

### Configuration Options (Streams)

The following CLI options are available:

- `--bootstrap-servers`, `--bootstrap-server`: List of Kafka bootstrap servers (comma-separated) (**required**)
- `--schema-registry-url`: The URL of the Schema Registry
- `--kafka-config`: Kafka Streams configuration (`<String=String>[,<String=String>...]`)
- `--input-topics`: List of input topics (comma-separated)
- `--input-pattern`: Pattern of input topics
- `--output-topic`: The output topic
- `--error-topic`: A topic to write errors to
- `--labeled-input-topics`: Additional labeled input topics for different message types (
  `<String=String>[,<String=String>...]`)
- `--labeled-input-patterns`: Additional labeled input patterns for different message types (
  `<String=String>[,<String=String>...]`)
- `--labeled-output-topics`: Additional labeled output topics for different message types (
  `<String=String>[,<String=String>...]`)
- `--application-id`: Unique application ID to use for Kafka Streams. Can also be provided by implementing
  `StreamsApp#getUniqueAppId()`
- `--volatile-group-instance-id`: Whether the group instance id is volatile, i.e., it changes on Streams shutdown

Additional commands:

- `clean`: Reset the Kafka Streams application. Additionally, delete the consumer group and all output and intermediate
  topics associated with the Kafka Streams application.
- `reset`: Clear all state stores, consumer group offsets, and internal topics associated with the Kafka Streams
  application.

## Kafka Producer Applications

Create a subclass of `KafkaProducerApplication`.

```java
import com.bakdata.kafka.producer.KafkaProducerApplication;
import com.bakdata.kafka.producer.ProducerApp;
import com.bakdata.kafka.producer.ProducerBuilder;
import com.bakdata.kafka.producer.ProducerRunnable;
import com.bakdata.kafka.producer.SerializerConfig;
import java.util.Map;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringSerializer;

public class MyProducerApplication extends KafkaProducerApplication<ProducerApp> {

    public static void main(final String[] args) {
        new MyProducerApplication().startApplication(args);
    }

    @Override
    public ProducerApp createApp() {
        return new ProducerApp() {
            @Override
            public ProducerRunnable buildRunnable(final ProducerBuilder builder) {
                return () -> {
                    try (final Producer<Object, Object> producer = builder.createProducer()) {
                        // producer logic
                    }
                };
            }

            @Override
            public SerializerConfig defaultSerializationConfig() {
                return new SerializerConfig(StringSerializer.class, StringSerializer.class);
            }

            @Override
            public Map<String, Object> createKafkaProperties() {
                return Map.of(
                        // additional Kafka properties
                );
            }
        };
    }
}
```

### Configuration Options (Producer)

The following CLI options are available:

- `--bootstrap-servers`, `--bootstrap-server`: List of Kafka bootstrap servers (comma-separated) (**required**)
- `--schema-registry-url`: The URL of the Schema Registry
- `--kafka-config`: Kafka producer configuration (`<String=String>[,<String=String>...]`)
- `--output-topic`: The output topic
- `--labeled-output-topics`: Additional labeled output topics (`<String=String>[,<String=String>...]`)

Additional commands:

- `clean`: Delete all output topics associated with the Kafka producer application.

## Helm Charts

For configuration and deployment to Kubernetes, use the provided Helm charts:

- Streams applications:
    - Chart: `charts/streams-app`
    - Example configuration: `charts/streams-app/values.yaml`
    - Cleanup job: `charts/streams-app-cleanup-job`

- Producer applications:
    - Chart: `charts/producer-app`
    - Example configuration: `charts/producer-app/values.yaml`
    - Cleanup job: `charts/producer-app-cleanup-job`

To configure your streams app, you can use
the [`values.yaml`](https://github.com/bakdata/streams-bootstrap/blob/master/charts/streams-app/values.yaml) as a
starting point.
We also provide a chart
to [clean](https://github.com/bakdata/streams-bootstrap/tree/master/charts/streams-app-cleanup-job) your streams app.

