[![Build Status](https://github.com/bakdata/streams-bootstrap/actions/workflows/build-and-publish.yaml/badge.svg?event=push)](https://github.com/bakdata/streams-bootstrap/actions/workflows/build-and-publish.yaml/badge.svg?event=push)
[![Sonarcloud status](https://sonarcloud.io/api/project_badges/measure?project=com.bakdata.kafka%3Astreams-bootstrap&metric=alert_status)](https://sonarcloud.io/dashboard?id=com.bakdata.kafka%3Astreams-bootstrap)
[![Code coverage](https://sonarcloud.io/api/project_badges/measure?project=com.bakdata.kafka%3Astreams-bootstrap&metric=coverage)](https://sonarcloud.io/dashboard?id=com.bakdata.kafka%3Astreams-bootstrap)
[![Maven](https://img.shields.io/maven-central/v/com.bakdata.kafka/streams-bootstrap.svg)](https://search.maven.org/search?q=g:com.bakdata.kafka%20AND%20a:streams-bootstrap&core=gav)

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
compile group: 'com.bakdata.kafka', name: 'streams-bootstrap', version: '2.1.1'
```

#### Maven

```xml

<dependency>
    <groupId>com.bakdata.kafka</groupId>
    <artifactId>streams-bootstrap</artifactId>
    <version>2.1.1</version>
</dependency>
```

For other build tools or versions, refer to
the [latest version in MvnRepository](https://mvnrepository.com/artifact/com.bakdata.kafka/streams-bootstrap/latest).

### Usage

#### Kafka Streams

Create a subclass of `KafkaStreamsApplication` and implement the abstract methods `buildTopology()`
and `getUniqueAppId()`. You can define the topology of your application in `buildTopology()`.

```java
import com.bakdata.kafka.KafkaStreamsApplication;
import java.util.Properties;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

public class StreamsBootstrapApplication extends KafkaStreamsApplication {
    public static void main(final String[] args) {
        startApplication(new StreamsBootstrapApplication(), args);
    }

    @Override
    public void buildTopology(final StreamsBuilder builder) {
        final KStream<String, String> input =
                builder.<String, String>stream(this.getInputTopics());

        // your topology

        input.to(this.getOutputTopic());
    }

    @Override
    public String getUniqueAppId() {
        return "streams-bootstrap-app";
    }

    // Optionally you can override the default streams bootstrap Kafka properties 
    @Override
    protected Properties createKafkaProperties() {
        final Properties kafkaProperties = super.createKafkaProperties();

        return kafkaProperties;
    }
}
```

The following configuration options are available:

- `--brokers`: List of Kafka brokers (comma-separated) (**required**)

- `--schema-registry-url`: The URL of the Schema Registry

- `--input-topics`: List of input topics (comma-separated)

- `--input-pattern`: Pattern of input topics

- `--output-topic`: The output topic

- `--error-topic`: A topic to write errors to

- `--streams-config`: Kafka Streams configuration (`<String=String>[,<String=String>...]`)

- `--extra-input-topics`: Additional named input topics if you need to specify multiple topics with different message
  types (`<String=String>[,<String=String>...]`)

- `--extra-input-patterns`: Additional named input patterns if you need to specify multiple topics with different
  message types (`<String=String>[,<String=String>...]`)

- `--extra-output-topics`: Additional named output topics if you need to specify multiple topics with different message
  types (`String=String>[,<String=String>...]`)

- `--volatile-group-instance-id`: Whether the group instance id is volatile, i.e., it will change on a Streams shutdown.

- `--clean-up`: Whether the state of the Kafka Streams app, i.e., offsets and state stores and auto-created topics,
  should be cleared instead of running the app

- `--delete-output`: Whether the output topics with their associated schemas and the consumer group should be deleted
  during the cleanup

- `--debug`: Configure logging to debug

#### Kafka producer

Create a subclass of `KafkaProducerApplication`.

```java
import com.bakdata.kafka.KafkaProducerApplication;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;

public class StreamsBootstrapApplication extends KafkaProducerApplication {
    public static void main(final String[] args) {
        startApplication(new StreamsBootstrapApplication(), args);
    }

    @Override
    protected void runApplication() {
        try (final KafkaProducer<Object, Object> producer = this.createProducer()) {
            // your producer
        }
    }

    // Optionally you can override the default streams bootstrap Kafka properties 
    @Override
    protected Properties createKafkaProperties() {
        final Properties kafkaProperties = super.createKafkaProperties();

        return kafkaProperties;
    }
}
```

The following configuration options are available:

- `--brokers`: List of Kafka brokers (comma-separated) (**required**)

- `--schema-registry-url`: The URL of the Schema Registry

- `--output-topic`: The output topic

- `--streams-config`: Kafka producer configuration (`<String=String>[,<String=String>...]`)

- `--extra-output-topics`: Additional named output topics (`String=String>[,<String=String>...]`)

- `--clean-up`: Whether the output topics and associated schemas of the producer app should be deleted instead of
  running the app

- `--debug`: Configure logging to debug

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
