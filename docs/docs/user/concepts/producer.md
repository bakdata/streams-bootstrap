# Producer applications

Producer applications generate data and send it to Kafka topics. They can be used to produce messages from various
sources, such as databases, files, or real-time events.

streams-bootstrap provides a structured way to build producer applications with consistent configuration handling,
command-line support, and lifecycle management.

---

## Application lifecycle

### Running an application

Producer applications are executed using the `ProducerRunner`, which runs the producer logic defined by the application.

Unlike Kafka Streams applications, producer applications typically:

- Run to completion and terminate automatically, or
- Run continuously when implemented as long-lived services

The execution model is fully controlled by the producer implementation and its runnable logic.

---

### Cleaning an application

Producer applications support a dedicated `clean` command.

```bash
java -jar my-producer-app.jar \
  --bootstrap-servers localhost:9092 \
  --output-topic my-topic \
  clean
```

The clean process can perform the following operations:

- Delete output topics
- Delete registered schemas from Schema Registry
- Execute custom cleanup hooks defined by the application

Applications can register custom cleanup logic by overriding `setupCleanUp`.

---

## Configuration

### Serialization configuration

Producer applications define key and value serialization using the `defaultSerializationConfig()` method in their
`ProducerApp` implementation.

```java

@Override
public SerializerConfig defaultSerializationConfig() {
    return new SerializerConfig(StringSerializer.class, SpecificAvroSerializer.class);
}
```

### Kafka properties

#### Base configuration

The following Kafka properties are configured by default for Producer applications in streams-bootstrap:

- `max.in.flight.requests.per.connection = 1`
- `acks = all`
- `compression.type = gzip`  

#### Custom Kafka properties

Kafka configuration can be customized by overriding `createKafkaProperties()`:

```java
@Override
public Map<String, Object> createKafkaProperties() {
    return Map.of(
            ProducerConfig.RETRIES_CONFIG, 3,
            ProducerConfig.BATCH_SIZE_CONFIG, 16384,
            ProducerConfig.LINGER_MS_CONFIG, 5
    );
}
```

These properties are merged with defaults and CLI-provided configuration.

---

### Lifecycle hooks

Producer applications can register cleanup logic via `setupCleanUp`. This method allows you to attach:

- **Cleanup hooks** – for general cleanup logic not tied to Kafka topics
- **Topic hooks** – for reacting to topic lifecycle events (e.g. deletion)

#### Clean up

Custom cleanup logic that is not tied to Kafka topics can be registered via cleanup hooks:

```java

@Override
public ProducerCleanUpConfiguration setupCleanUp(
        final AppConfiguration<ProducerTopicConfig> configuration) {

    return ProducerApp.super.setupCleanUp(configuration)
            .registerCleanHook(() -> {
                // Custom cleanup logic
            });
}
```

#### Topic hooks

Topic hooks should be used for topic-related cleanup or side effects, such as releasing external
resources associated with a topic or logging topic deletions:

```java
@Override
public ProducerCleanUpConfiguration setupCleanUp(
        final AppConfiguration<ProducerTopicConfig> configuration) {

    return ProducerApp.super.setupCleanUp(configuration)
            .registerTopicHook(new TopicHook() {

                @Override
                public void deleted(final String topic) {
                    // Called when a managed topic is deleted
                    System.out.println("Deleted topic: " + topic);
                }

                @Override
                public void close() {
                    // Optional cleanup for the hook itself
                }
            });
}
```

## Command line interface

Producer applications inherit standard CLI options from `KafkaApplication`. The following CLI options are producer-specific:

| Option                    | Description                                      | Default |
|---------------------------|--------------------------------------------------|---------|
| `--output-topic`          | Default output topic                             | -       |
| `--labeled-output-topics` | Named output topics (`label1=topic1,...`)        | -       |

---

## Deployment

TODO
