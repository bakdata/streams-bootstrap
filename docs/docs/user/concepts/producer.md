# Producer applications

Producer applications are applications that generate data and send it to Kafka topics. They can be used to produce
messages from various sources, such as databases, files, or real-time events.

The `streams-bootstrap` framework provides a structured way to build Kafka producer applications with built-in
configuration handling, command-line support, and resource lifecycle management.

---

## Application lifecycle

### Running an application

Producer applications are executed using the `ProducerRunner`, which runs the producer logic defined by the application.

Unlike Kafka Streams applications, producer applications typically:

- Run to completion and terminate automatically, or
- Run continuously when implemented as long-lived services

The execution model is fully controlled by the producer implementation and its runnable logic.

### Cleaning an application

Producer applications support a dedicated `clean` command that removes Kafka-related resources created by the
application.

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

Applications can register custom cleanup logic by overriding `setupCleanUp`:

```java

@Override
public void setupCleanUp(final EffectiveAppConfiguration configuration) {
    configuration.addCleanupHook(() -> {
        // Custom cleanup logic
    });
}
```

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

Common serializer combinations include:

| Key Serializer        | Value Serializer         | Use case                   |
|-----------------------|--------------------------|----------------------------|
| `StringSerializer`    | `StringSerializer`       | Simple string messages     |
| `StringSerializer`    | `SpecificAvroSerializer` | Avro with schema evolution |
| `StringSerializer`    | `GenericAvroSerializer`  | Dynamic Avro schemas       |
| `ByteArraySerializer` | `ByteArraySerializer`    | Binary data                |

### Custom Kafka properties

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

These properties are merged with the framework defaults and CLI-provided configuration.

### Schema Registry integration

When the `--schema-registry-url` option is provided:

- Schemas are registered automatically during application startup
- Schema cleanup is handled as part of the `clean` command
- Schema evolution is fully supported

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
