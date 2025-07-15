# Kafka Streams Applications

## Purpose and Scope

This page describes how to create and configure Kafka Streams applications using the streams-bootstrap framework. It covers the core classes, configuration options, and lifecycle management for Kafka Streams applications specifically. For information about Kafka Producer applications, see Kafka Producer Applications. For details about application lifecycle management, see Application Lifecycle Management.

## Core Streams Classes

### KafkaStreamsApplication

`KafkaStreamsApplication<T extends StreamsApp>` is the abstract base class for creating Kafka Streams applications. It provides command-line options for configuring the application and methods for managing the application lifecycle.

**Key responsibilities:**

- Create and configure a StreamsApp instance
- Parse command-line arguments and environment variables
- Manage application lifecycle (run, clean, reset)
- Configure error handling and state listeners

### StreamsApp Interface

The `StreamsApp` interface defines the contract for implementing a Kafka Streams application:

## Creating a Kafka Streams Application

To create a Kafka Streams application, you need to:

1. Create a class that extends `KafkaStreamsApplication`
2. Implement the `createApp()` method to return a `StreamsApp` implementation
3. Define the topology in the `buildTopology()` method
4. Define a unique application ID in the `getUniqueAppId()` method
---

- [ ] Add link to hands on example or just remove and add as separate page

**Simplified Structure:**

### SimpleKafkaStreamsApplication

For simple use cases, the framework provides a `SimpleKafkaStreamsApplication<T>` implementation that takes a supplier function for creating your `StreamsApp` instance.

## Configuration Options

The `KafkaStreamsApplication` class provides several configuration options through command-line arguments:

| Option                      | Description                                                          | Default         |
|----------------------------|----------------------------------------------------------------------|-----------------|
| --bootstrap-servers        | List of Kafka bootstrap servers (comma-separated)                    | Required        |
| --schema-registry-url      | The URL of the Schema Registry                                       | None            |
| --kafka-config             | Kafka Streams config (<String=String>[,<String=String>...])          | None            |
| --input-topics             | List of input topics (comma-separated)                               | Empty list      |
| --input-pattern            | Pattern of input topics                                               | None            |
| --output-topic             | The output topic                                                     | None            |
| --error-topic              | A topic to write errors to                                           | None            |
| --labeled-input-topics     | Labeled input topics with different message types                    | None            |
| --labeled-input-patterns   | Additional labeled input patterns                                    | None            |
| --labeled-output-topics    | Additional labeled output topics with different message types        | None            |
| --application-id           | Unique application ID for Kafka Streams                              | Auto-generated  |
| --volatile-group-instance-id | Whether the group instance id is volatile                           | false           |

## Configuration Priority

Kafka configuration follows this order of precedence (highest to lowest):

1. Command-line parameters
2. Environment variables (prefixed with `KAFKA_`)
3. Configuration provided by the `StreamsApp.createKafkaProperties()` method
4. Framework default configuration

**Defaults set by framework:**

```
processing.guarantee=exactly_once_v2
producer.max.in.flight.requests.per.connection=1
producer.acks=all
producer.compression.type=gzip
```

## Application Lifecycle

### Running the Application

To run a Kafka Streams application:

```java
public static void main(final String[] args) {
    new MyStreamsApplication().startApplication(args);
}
```

The framework internally:

- Parses arguments
- Creates a `StreamsApp` instance
- Wraps it in `ConfiguredStreamsApp`
- Converts to `ExecutableStreamsApp`
- Runs via `StreamsRunner`
---
- [ ] add either diagram or more input on Streams app above for more context 

### Cleaning Up Resources

Built-in commands:

- `clean`: Deletes consumer group, output and intermediate topics

```bash
java -jar my-app.jar clean
```

- `reset`: Clears state stores, offsets, and internal topics

```bash
java -jar my-app.jar reset
```

**reset command**

- `reset()`
    - Reset state stores
    - Reset consumer offsets
    - Delete internal topics

**clean command**

- `clean()`
    - Delete output topics
    - Delete internal topics
    - Delete intermediate topics
    - Delete consumer group
    - Delete schemas

## Advanced Features

### Error Handling

You can override error handling:

```java
@Override
protected StreamsUncaughtExceptionHandler createUncaughtExceptionHandler() {
    return new MyCustomExceptionHandler();
}
```

### State Listeners

Monitor state transitions:

```java
@Override
protected StateListener createStateListener() {
    return new MyCustomStateListener();
}
```

### On-Start Hook

Execute logic after Kafka Streams has started:

```java
@Override
protected void onStreamsStart(final RunningStreams runningStreams) {
    // Custom setup
}
```

## Deployment

For deploying your Kafka Streams applications to Kubernetes, the framework provides Helm charts that can be found in the `charts/` directory. The configuration and deployment aspects are covered in detail in **Helm Charts Overview** and the **streams-app Chart**.
