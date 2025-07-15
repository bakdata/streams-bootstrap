# Testing Framework

## Overview

The `streams-bootstrap` Testing Framework provides a comprehensive set of tools for testing Kafka Streams and Producer
applications built with the `streams-bootstrap` library. This framework simplifies both unit and integration testing by
providing test abstractions that handle Kafka infrastructure setup, Schema Registry integration, and consumer group
verification.

The framework supports testing with real Kafka clusters using TestContainers, mock Schema Registry for schema-aware
testing, and utilities for verifying application behavior and consumer group states.

For information about creating Kafka Streams applications that you might want to test, see **Kafka Streams Applications
**.

---

## Core Testing Components

### KafkaTest Base Class

`KafkaTest` is an abstract base class that sets up a Kafka environment using TestContainers. It provides:

- Kafka container setup
- Access to bootstrap servers and Schema Registry
- Methods for waiting on consumer group states
- Integration with `TestSchemaRegistry`
- Creation of `KafkaTestClient` instances

### KafkaTestClient

`KafkaTestClient` is a fluent test client that simplifies:

- Producing data
- Consuming records
- Admin operations
- Topic creation and verification

### ConsumerGroupVerifier

Provides tools to:

- Check if a group is active or closed
- Get current group state
- Verify processing completion (lag = 0)
- Compute lag manually

---

## Unit Testing with `fluent-kafka-streams-tests`

The framework integrates with `fluent-kafka-streams-tests` for unit testing Kafka Streams topologies.

### Dependencies:

| Dependency                   | Purpose                                 |
|------------------------------|-----------------------------------------|
| `fluent-kafka-streams-tests` | Kafka Streams unit testing              |
| `junit-jupiter`              | JUnit 5 test runner                     |
| `testcontainers-kafka`       | Kafka container for integration testing |
| `mockito-core`               | Mocking framework                       |
| `assertj-core`               | Fluent assertions                       |
| `awaitility`                 | Polling/waiting utility                 |

---

## TestSchemaRegistry

`TestSchemaRegistry` provides built-in support for Schema Registry in tests using a mock implementation. It creates
isolated Schema Registry instances for testing schema-aware applications.

### Features:

- Random scoped mock URLs to avoid collisions
- Support for custom mock URLs
- Configurable schema providers
- Compatible with Confluent’s `MockSchemaRegistry`

### Example:

```java
// Random scope
TestSchemaRegistry registry=new TestSchemaRegistry();

// Custom scope
        TestSchemaRegistry registry=new TestSchemaRegistry("mock://custom-scope");

// Default providers
        SchemaRegistryClient client=registry.getSchemaRegistryClient();

// With custom providers
        List<SchemaProvider> providers=List.of(new ProtobufSchemaProvider());
        SchemaRegistryClient client=registry.getSchemaRegistryClient(providers);
```

---

## Integration Testing with TestContainers

For integration tests that require a real Kafka environment, the framework provides integration with TestContainers.

### Single Node Kafka Testing

KafkaTest provides a base class for integration tests with a single Kafka broker:

```java

@Testcontainers
public abstract class KafkaTest {
    @Container
    private final KafkaContainer kafkaCluster = newCluster();

    public static KafkaContainer newCluster() {
        return new KafkaContainer(DockerImageName.parse("apache/kafka")
                .withTag(AppInfoParser.getVersion()));
    }
}
```

This creates a Kafka container using TestContainers with the Apache Kafka image matching the client version.

### Multi-Node Cluster Testing

For testing with multi-node Kafka clusters, the framework provides `ApacheKafkaContainerCluster`:

- [] INSERT IMAGE

Example usage:

```java
ApacheKafkaContainerCluster cluster=new ApacheKafkaContainerCluster("3.4.0",3,2);
        cluster.start();
        String bootstrapServers=cluster.getBootstrapServers();
// Run tests...
        cluster.stop();
```

### Features:

- Configurable broker count
- Configurable replication factor for internal topics
- Uses KRaft (no ZooKeeper)
- Waits for all brokers to be ready before returning

---

## Utilities for Kafka Testing

### KafkaTestClient Operations

`KafkaTestClient` provides a fluent API for common Kafka operations in tests:

#### Topic Management

```java
KafkaTestClient client=newTestClient();

// Create topic with default settings (1 partition, 1 replica)
        client.createTopic("my-topic");

// Create topic with custom settings
        client.createTopic("my-topic",
        KafkaTestClient.defaultTopicSettings()
        .partitions(3)
        .replicationFactor((short)1)
        .build());

// Create topic with config
        Map<String, String> config=Map.of("cleanup.policy","compact");
        client.createTopic("my-topic",settings,config);

// Check if topic exists
        boolean exists=client.existsTopic("my-topic");
```

#### Data Production

```java
client.send()
        .withKeySerializer(new StringSerializer())
        .withValueSerializer(new StringSerializer())
        .to("topic-name",List.of(
        new SimpleProducerRecord<>("key1","value1"),
        new SimpleProducerRecord<>("key2","value2")
        ));
```

#### Data Consumption

```java
List<ConsumerRecord<String, String>>records=client.read()
        .withKeyDeserializer(new StringDeserializer())
        .withValueDeserializer(new StringDeserializer())
        .from("topic-name",Duration.ofSeconds(10));
```

---

## Administrative Operations

`KafkaTestClient` provides access to administrative operations through AdminClientX:

```java
try(AdminClientX admin=client.admin()){
        TopicClient topicClient=admin.getTopicClient();
        ConsumerGroupClient consumerGroupClient=admin.getConsumerGroupClient();
        }
```

---

## Test Configuration

### Runtime Configuration

The framework provides preconfigured settings for Kafka applications in test environments through the `KafkaTest` base
class.

```java
// No schema registry
RuntimeConfiguration config=createConfigWithoutSchemaRegistry();

// With schema registry
        RuntimeConfiguration config=createConfig();
```

### Constants

| Setting           | Value      | Purpose                  |
|-------------------|------------|--------------------------|
| `POLL_TIMEOUT`    | 10 seconds | Wait for consuming       |
| `SESSION_TIMEOUT` | 10 seconds | Consumer session timeout |

Custom test setup example:

```java
RuntimeConfiguration config=RuntimeConfiguration.create(getBootstrapServers())
        .withNoStateStoreCaching()
        .withSessionTimeout(SESSION_TIMEOUT);
```

---

## Consumer Group Verification

The framework provides utilities for verifying consumer group states:

```java
// Wait for application to become active
awaitActive(app);

// Wait for completion of processing
        awaitProcessing(app);

// Wait for app to shut down
        awaitClosed(app);
```

These methods ensure test reliability by validating consumer group behavior via `ConsumerGroupVerifier`.

---
