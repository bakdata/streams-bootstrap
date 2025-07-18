# Kafka Producer Applications

This document provides technical documentation for Kafka Producer Applications within the `streams-bootstrap` framework.
Kafka Producer Applications extend the `KafkaApplication` base class to create applications that produce messages to
Kafka topics with standardized CLI options, configuration management, and deployment patterns.

---

## Overview

Kafka Producer Applications in `streams-bootstrap` provide a structured framework for building applications that produce
data to Kafka topics. The framework handles:

- CLI argument parsing
- Kafka configuration
- Schema Registry integration
- Resource lifecycle management via the `KafkaApplication` base class

---

## Architecture

### Core Components and Responsibilities

- **KafkaApplication**: Abstract base with lifecycle methods
- **KafkaProducerApplication**: Implementation with producer-specific lifecycle
- **ProducerApp**: Interface defining producer logic and configuration
- **ConfiguredProducerApp**: Wraps app logic with configuration
- **ExecutableProducerApp**: Responsible for running and cleaning up
- **ProducerRunner**: Executes the producer
- **ProducerCleanUpRunner**: Handles resource cleanup
- **ProducerRunnable**: User-defined message production logic

---

## Creating a Kafka Producer Application

### Implementation Steps

1. **Extend `KafkaProducerApplication`** with your specific app type
2. **Implement `createApp()`** to define your producer logic
3. **Implement `buildRunnable()`** inside `ProducerApp`
4. **Configure serialization** using `defaultSerializationConfig()`

---

## Implementation Example

### Basic Producer Application

```java
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
                        producer.send(new ProducerRecord<>(
                                builder.getTopics().getOutputTopic(), "key", "value"
                        ));
                    }
                };
            }

            @Override
            public SerializerConfig defaultSerializationConfig() {
                return new SerializerConfig(StringSerializer.class, StringSerializer.class);
            }
        };
    }
}
```

---

## Using `SimpleKafkaProducerApplication`

For programmatic usage without custom CLI:

```java
try(final KafkaProducerApplication<?> app=new SimpleKafkaProducerApplication<>(()->
        new ProducerApp(){
@Override
public ProducerRunnable buildRunnable(final ProducerBuilder builder){
        return()->{
        try(final Producer<Object, Object> producer=builder.createProducer()){
// Producer logic
        }
        };
        }

@Override
public SerializerConfig defaultSerializationConfig(){
        return new SerializerConfig(StringSerializer.class,StringSerializer.class);
        }
        }
        )){
        app.setBootstrapServers("localhost:9092");
        app.setOutputTopic("output-topic");
        app.run();
        }
```

---

## Configuration Options

### CLI Arguments

Kafka Producer applications inherit configuration options from KafkaApplication and support the following CLI arguments:

```text
--bootstrap-servers         Kafka bootstrap servers (comma-separated)          (Required)
--bootstrap-server          Alias for --bootstrap-servers                      (Required)
--schema-registry-url       URL for Avro schema registry                       (Optional)
--kafka-config              Additional Kafka config (key=value,...)            (Optional)
--output-topic              Main Kafka topic to produce to                     (Optional)
--labeled-output-topics     Named output topics (label1=topic1,...)            (Optional)
```

---

## Environment Variable Support

The framework automatically parses environment variables with the `APP_ prefix` (configurable via `ENV_PREFIX`).
Environment variables are converted to CLI arguments:

```text
APP_BOOTSTRAP_SERVERS       → --bootstrap-servers
APP_SCHEMA_REGISTRY_URL     → --schema-registry-url
APP_OUTPUT_TOPIC            → --output-topic
```

Additionally, Kafka-specific environment variables with the `KAFKA_` prefix are automatically added to the Kafka
configuration.

---

## Configuration Precedence

Kafka properties are merged in the following order (later values override earlier ones):

1. Base configuration
2. App config from ProducerApp.createKafkaProperties()
3. Environment variables (`KAFKA_`)
4. Runtime args (--bootstrap-servers, etc.)
5. Serialization config from ProducerApp.defaultSerializationConfig()
6. CLI overrides via --kafka-config

---

## Serialization Configuration

Producer applications specify key and value serializers via the defaultSerializationConfig() method in your ProducerApp
implementation:

```java
@Override
public SerializerConfig defaultSerializationConfig(){
        return new SerializerConfig(StringSerializer.class,SpecificAvroSerializer.class);
        }
```

Common serializer configurations:

| Key Serializer      | Value Serializer       | Use Case               |
|---------------------|------------------------|------------------------|
| StringSerializer    | StringSerializer       | Simple string messages |
| StringSerializer    | SpecificAvroSerializer | Avro schema evolution  |
| StringSerializer    | GenericAvroSerializer  | Dynamic Avro schemas   |
| ByteArraySerializer | ByteArraySerializer    | Binary data            |

---

## Custom Kafka Properties

Override createKafkaProperties() to add custom producer configuration:

```java
@Override
public Map<String, Object> createKafkaProperties(){
        return Map.of(
        ProducerConfig.ACKS_CONFIG,"all",
        ProducerConfig.RETRIES_CONFIG,3,
        ProducerConfig.BATCH_SIZE_CONFIG,16384,
        ProducerConfig.LINGER_MS_CONFIG,5
        );
        }
```

---

## Schema Registry Integration

When `--schema-registry-url` is set:

- Schema registration happens automatically
- Schema cleanup is handled during clean
- Schema evolution is supported

```java
props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,schemaRegistryUrl);
```

---

## Resource Cleanup

### Command

The clean command provides comprehensive resource cleanup for producer applications:

```bash
java -jar my-producer-app.jar --bootstrap-servers localhost:9092 --output-topic my-topic clean
```

### Operations

- Delete output topics
- Delete schemas
- Run custom cleanup hooks via ProducerApp.setupCleanUp()

### Custom Hook

Producer applications can register custom cleanup logic:

```java
@Override
public void setupCleanUp(final EffectiveAppConfiguration configuration){
        configuration.addCleanupHook(()->{
// Your cleanup logic
        });
        }
```

---

## Kubernetes Deployment

### Helm Charts

- `producer-app`: Main deployment
- `producer-app-cleanup-job`: Cleanup job

### Deployment Modes

| Mode       | Use Case              | Resource Type      |
|------------|-----------------------|--------------------|
| Deployment | Long-running producer | apps/v1/Deployment |
| Job        | One-time run          | batch/v1/Job       |
| CronJob    | Scheduled job         | batch/v1/CronJob   |

### Configuration

Values from `values.yaml` are converted into:

- CLI arguments
- Environment variables
- Kafka properties
