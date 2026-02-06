# Testing

The `streams-bootstrap` Testing tools provide a comprehensive set of utilities for testing Kafka Streams and Producer
applications. These tools simplify both unit and integration testing by providing test abstractions that handle
Kafka infrastructure setup, Schema Registry integration, and consumer group verification.

Testing is supported with real Kafka clusters using TestContainers, mock Schema Registry for schema-aware
testing, and utilities for verifying application behavior and consumer group states.

## Core Testing Components

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

## Unit Testing with `fluent-kafka-streams-tests`

Integration is provided with `fluent-kafka-streams-tests` for unit testing Kafka Streams topologies.```

## Utilities for Kafka Testing

### KafkaTestClient Operations

`KafkaTestClient` provides a fluent API for common Kafka operations in tests:

#### Topic Management

```java
KafkaTestClient client = newTestClient();

// Create topic with default settings (1 partition, 1 replica)
client.

createTopic("my-topic");

// Create topic with custom settings
client.

createTopic("my-topic",
        KafkaTestClient.defaultTopicSettings()
        .

partitions(3)
        .

replicationFactor(1)
        .

build());

// Create topic with config
Map<String, String> config = Map.of("cleanup.policy", "compact");
client.

createTopic("my-topic",settings, config);

// Check if topic exists
boolean exists = this.client.existsTopic("my-topic");
```

#### Data Production

```java
client.send()
        .

withKeySerializer(new StringSerializer())
        .

withValueSerializer(new StringSerializer())

to("topic-name",List.of(
        new SimpleProducerRecord<>("key1","value1"),
        new SimpleProducerRecord<>("key2","value2")
        ));
```

#### Data Consumption

```java
List<ConsumerRecord<String, String>> records = client.read()
        .withKeyDeserializer(new StringDeserializer())
        .withValueDeserializer(new StringDeserializer())
        .from("topic-name", Duration.ofSeconds(10));
```

## Administrative Operations

`KafkaTestClient` provides access to administrative operations through `AdminClientX`:

```java
try(AdminClientX admin = client.admin()){
TopicClient topicClient = this.admin.getTopicClient();
ConsumerGroupClient consumerGroupClient = this.admin.getConsumerGroupClient();
}
```

## Consumer Group Verification

The tools provide utilities for verifying consumer group states:

```java
// Wait for application to become active
awaitActive(app);

// Wait for completion of processing
awaitProcessing(app);

// Wait for app to shut down
awaitClosed(app);
```

These methods ensure test reliability by validating consumer group behavior via `ConsumerGroupVerifier`.
