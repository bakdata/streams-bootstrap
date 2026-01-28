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

Producer-specific Kafka configuration can be customized by overriding `createKafkaProperties()`:

```java

@Override
public Map<String, Object> createKafkaProperties() {
    return Map.of(
            ProducerConfig.ACKS_CONFIG, "all",
            ProducerConfig.RETRIES_CONFIG, 3,
            ProducerConfig.BATCH_SIZE_CONFIG, 16384,
            ProducerConfig.LINGER_MS_CONFIG, 5
    );
}
```

These properties are merged with defaults and CLI-provided configuration.

---

### Lifecycle hooks

#### Clean up

Custom cleanup logic that is not tied to Kafka topics can be registered via cleanup hooks:

```java

@Override
public void setupCleanUp(final EffectiveAppConfiguration configuration) {
    configuration.addCleanupHook(() -> {
        // Custom cleanup logic
    });
}
```

Topic-related cleanup should be implemented using topic hooks.

---

## Command line interface

Producer applications inherit standard CLI options from `KafkaApplication`.

```text
--bootstrap-servers         Kafka bootstrap servers (comma-separated)          (Required)
--bootstrap-server          Alias for --bootstrap-servers                      (Required)
--schema-registry-url       URL of the Schema Registry                         (Optional)
--kafka-config              Additional Kafka config (key=value,...)            (Optional)
--output-topic              Default output topic                               (Optional)
--labeled-output-topics     Named output topics (label1=topic1,...)            (Optional)
```

---

## Deployment

TODO
