[![Build Status](https://github.com/bakdata/streams-bootstrap/actions/workflows/build-and-publish.yaml/badge.svg?event=push)](https://github.com/bakdata/streams-bootstrap/actions/workflows/build-and-publish.yaml/badge.svg?event=push)
[![Sonarcloud status](https://sonarcloud.io/api/project_badges/measure?project=com.bakdata.kafka%3Astreams-bootstrap&metric=alert_status)](https://sonarcloud.io/dashboard?id=com.bakdata.kafka%3Astreams-bootstrap)
[![Code coverage](https://sonarcloud.io/api/project_badges/measure?project=com.bakdata.kafka%3Astreams-bootstrap&metric=coverage)](https://sonarcloud.io/dashboard?id=com.bakdata.kafka%3Astreams-bootstrap)
[![Maven](https://img.shields.io/maven-central/v/com.bakdata.kafka/streams-bootstrap-cli.svg)](https://search.maven.org/search?q=g:com.bakdata.kafka%20AND%20a:streams-bootstrap-cli&core=gav)

# streams-bootstrap

`streams-bootstrap` provides base classes and utility functions for Kafka Streams applications.

It provides a common way to

- configure Kafka Streams applications
- deploy streaming applications on Kubernetes via Helm charts
- reprocess data

Visit our [blogpost](https://medium.com/bakdata/continuous-nlp-pipelines-with-python-java-and-apache-kafka-f6903e7e429d)
and [demo](https://github.com/bakdata/common-kafka-streams-demo) for an overview and a demo application.  
The common configuration and deployments on Kubernetes are supported by
the [Streams Explorer](https://github.com/bakdata/streams-explorer), which makes it possible to explore and monitor data
pipelines in Apache Kafka.

## Getting Started

You can add streams-bootstrap via Maven Central.

#### Gradle

```gradle
implementation group: 'com.bakdata.kafka', name: 'streams-bootstrap-cli', version: '5.0.1'
```

With Kotlin DSL

```gradle
implementation(group = "com.bakdata.kafka", name = "streams-bootstrap-cli", version = "5.0.1")
```

#### Maven

```xml

<dependency>
    <groupId>com.bakdata.kafka</groupId>
    <artifactId>streams-bootstrap-cli</artifactId>
    <version>5.0.1</version>
</dependency>
```

For other build tools or versions, refer to
the [latest version in MvnRepository](https://mvnrepository.com/artifact/com.bakdata.kafka/streams-bootstrap/latest).

### Usage

#### Kafka Streams

Create a subclass of `KafkaStreamsApplication` and implement the abstract methods `buildTopology()`
and `getUniqueAppId()`. You can define the topology of your application in `buildTopology()`.

```java
import com.bakdata.kafka.streams.kstream.KStreamX;
import com.bakdata.kafka.streams.KafkaStreamsApplication;
import com.bakdata.kafka.streams.SerdeConfig;
import com.bakdata.kafka.streams.StreamsApp;
import com.bakdata.kafka.streams.StreamsTopicConfig;
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

          // your topology

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

        // Optionally you can define custom Kafka properties
        @Override
        public Map<String, Object> createKafkaProperties() {
          return Map.of(
                  // your config
          );
        }
      };
    }
}
```

The following configuration options are available:

- `--bootstrap-servers`, `--bootstrap-server`: List of Kafka bootstrap servers (comma-separated) (**required**)

- `--schema-registry-url`: The URL of the Schema Registry

- `--kafka-config`: Kafka Streams configuration (`<String=String>[,<String=String>...]`)

- `--input-topics`: List of input topics (comma-separated)

- `--input-pattern`: Pattern of input topics

- `--output-topic`: The output topic

- `--error-topic`: A topic to write errors to

- `--labeled-input-topics`: Additional labeled input topics if you need to specify multiple topics with different
  message types (`<String=String>[,<String=String>...]`)

- `--labeled-input-patterns`: Additional labeled input patterns if you need to specify multiple topics with different
  message types (`<String=String>[,<String=String>...]`)

- `--labeled-output-topics`: Additional labeled output topics if you need to specify multiple topics with different
  message types (`String=String>[,<String=String>...]`)

- `--application-id`: Unique application ID to use for Kafka Streams. Can also be provided by
  implementing `StreamsApp#getUniqueAppId()`

- `--volatile-group-instance-id`: Whether the group instance id is volatile, i.e., it will change on a Streams shutdown.

Additionally, the following commands are available:

- `clean`: Reset the Kafka Streams application. Additionally, delete the consumer group and all output and intermediate
  topics associated with the Kafka Streams application.

- `reset`: Clear all state stores, consumer group offsets, and internal topics associated with the Kafka Streams
  application.

#### Kafka producer

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
              // your producer
            }
          };
        }

        @Override
        public SerializerConfig defaultSerializationConfig() {
          return new SerializerConfig(StringSerializer.class, StringSerializer.class);
        }

        // Optionally you can define custom Kafka properties
        @Override
        public Map<String, Object> createKafkaProperties() {
          return Map.of(
                  // your config
          );
        }
      };
    }
}
```

The following configuration options are available:

- `--bootstrap-servers`, `--bootstrap-server`: List of Kafka bootstrap servers (comma-separated) (**required**)

- `--schema-registry-url`: The URL of the Schema Registry

- `--kafka-config`: Kafka producer configuration (`<String=String>[,<String=String>...]`)

- `--output-topic`: The output topic

- `--labeled-output-topics`: Additional labeled output topics (`String=String>[,<String=String>...]`)

Additionally, the following commands are available:

- `clean`: Delete all output topics associated with the Kafka Producer application.

### Helm Charts

For the configuration and deployment to Kubernetes, you can use
the [Helm Charts](https://github.com/bakdata/streams-bootstrap/tree/master/charts).

To configure your streams app, you can use
the [`values.yaml`](https://github.com/bakdata/streams-bootstrap/blob/master/charts/streams-app/values.yaml) as a
starting point.
We also provide a chart
to [clean](https://github.com/bakdata/streams-bootstrap/tree/master/charts/streams-app-cleanup-job) your streams app.

To configure your producer app, you can use
the [`values.yaml`](https://github.com/bakdata/streams-bootstrap/blob/master/charts/producer-app/values.yaml) as a
starting point.
We also provide a chart
to [clean](https://github.com/bakdata/streams-bootstrap/tree/master/charts/producer-app-cleanup-job) your producer app.

## Development

If you want to contribute to this project, you can simply clone the repository and build it via Gradle.
All dependencies should be included in the Gradle files, there are no external prerequisites.

```bash
> git clone git@github.com:bakdata/streams-bootstrap.git
> cd streams-bootstrap && ./gradlew build
```

Please note, that we have [code styles](https://github.com/bakdata/bakdata-code-styles) for Java.
They are basically the Google style guide, with some small modifications.

## Contributing

We are happy if you want to contribute to this project.
If you find any bugs or have suggestions for improvements, please open an issue.
We are also happy to accept your PRs.
Just open an issue beforehand and let us know what you want to do and why.

## License

This project is licensed under the MIT license.
Have a look at the [LICENSE](https://github.com/bakdata/streams-bootstrap/blob/master/LICENSE) for more details.
