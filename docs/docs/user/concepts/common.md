# Common concepts

## Application types

The **streams-bootstrap** framework uses a three-layer application type hierarchy:
**App → ConfiguredApp → ExecutableApp**

---

### App

The **App** represents your application logic implementation. Each application type has its own `App` interface:

- **StreamsApp** – for Kafka Streams applications
- **ProducerApp** – for producer applications
- **ConsumerApp** – for consumer applications
- **ConsumerProducerApp** – for consumer–producer applications

You implement the appropriate interface to define your application's behavior.

---

### ConfiguredApp

A **ConfiguredApp** pairs an `App` with its configuration. Examples include:

- `ConfiguredConsumerApp<T extends ConsumerApp>`
- `ConfiguredProducerApp<T extends ProducerApp>`

This layer handles Kafka property creation, combining:

- base configuration
- app-specific configuration
- environment variables
- runtime configuration

---

### ExecutableApp

An **ExecutableApp** is a `ConfiguredApp` with runtime configuration applied, making it ready to execute.  
It can create:

- a **Runner** for running the application
- a **CleanUpRunner** for cleanup operations

The `KafkaApplication` base class orchestrates the creation of these components through methods such as:

- `createConfiguredApp()`
- `createExecutableApp()`

---

### Usage Pattern

1. You implement an **App**.
2. The framework wraps it in a **ConfiguredApp**, applying the configuration.
3. Runtime configuration is then applied to create an **ExecutableApp**, which can be:

- **run**, or
- **cleaned up**.

---

## Application lifecycle

Applications built with streams-bootstrap follow a defined lifecycle with specific states and transitions.

The framework manages this lifecycle through the KafkaApplication base class and provides several extension points for
customization.

| Phase          | Description                                                              | Entry Point                                              |
|----------------|--------------------------------------------------------------------------|----------------------------------------------------------|
| Initialization | Parse CLI arguments, inject environment variables, configure application | `startApplication()` or `startApplicationWithoutExit()`  |
| Preparation    | Execute pre-run/pre-clean hooks                                          | `onApplicationStart()`, `prepareRun()`, `prepareClean()` |
| Execution      | Run main application logic or cleanup operations                         | `run()`, `clean()`, `reset()`                            |
| Shutdown       | Stop runners, close resources, cleanup                                   | `stop()`, `close()`                                      |

### Running an application

Applications built with streams-bootstrap can be started in two primary ways:

- **Via Command Line Interface**: When packaged as a runnable JAR (for example, in a container),
  the `run` command is the default entrypoint. An example invocation:

  ```bash
  java -jar example-app.jar \
      run \
      --bootstrap-servers kafka:9092 \
      --input-topics input-topic \
      --output-topic output-topic \
      --schema-registry-url http://schema-registry:8081
  ```

- **Programmatically**: The application subclass calls `startApplication(args)` on startup. Example for a Kafka Streams
  application:

```java
  public static void main(final String[] args) {
    new MyStreamsApplication().startApplication(args);
}
  ```

### Cleaning an application

The framework provides a built-in mechanism to clean up all resources associated with an application.

When the cleanup operation is triggered, the following resources are removed:

| Resource Type       | Description                                               | Streams Apps | Producer Apps |
|---------------------|-----------------------------------------------------------|--------------|---------------|
| Output Topics       | The main output topic of the application                  | ✓            | ✓             |
| Intermediate Topics | Topics for stream operations like `through()`             | ✓            | N/A           |
| Internal Topics     | Topics for state stores or repartitioning (Kafka Streams) | ✓            | N/A           |
| Consumer Groups     | Consumer group metadata                                   | ✓            | N/A           |
| Schema Registry     | All registered schemas                                    | ✓            | ✓             |

Cleanup can be triggered:

- **Via Command Line**: Helm cleanup jobs
- **Programmatically**:

```java
// For streams applications
try(StreamsCleanUpRunner cleanUpRunner = streamsApp.createCleanUpRunner()){
        cleanUpRunner.

clean();
}

// For producer applications
        try(
CleanUpRunner cleanUpRunner = producerApp.createCleanUpRunner()){
        cleanUpRunner.

clean();
}
```

The framework ensures that cleanup operations are idempotent, meaning they can be safely retried without causing
additional issues.

## Configuration

Kafka properties are applied in the following order (later values override earlier ones):

1. Base configuration
2. App config from .createKafkaProperties()
3. Environment variables (`KAFKA_`)
4. Runtime args (--bootstrap-servers, etc.)
5. Serialization config from ProducerApp.defaultSerializationConfig() or StreamsApp.defaultSerializationConfig()
6. CLI overrides via --kafka-config

The framework automatically parses environment variables with the `APP_ prefix` (configurable via `ENV_PREFIX`).
Environment variables are converted to CLI arguments:

```text
APP_BOOTSTRAP_SERVERS       →      --bootstrap-servers
APP_SCHEMA_REGISTRY_URL     →      --schema-registry-url
APP_OUTPUT_TOPIC            →      --output-topic
```

Additionally, Kafka-specific environment variables with the `KAFKA_` prefix are automatically added to the Kafka
configuration.

## Command line interface

The framework provides a unified command-line interface for application configuration.

### CLI Commands

- `run`: Run the application
- `clean`: Delete topics and consumer groups
- `reset`: Reset internal state and offsets (for Streams apps)

### Common CLI Configuration Options

- `--bootstrap-servers`: Kafka bootstrap servers (required)
- `--schema-registry-url`: URL for Avro serialization
- `--kafka-config`: Key-value Kafka configuration
- `--output-topic`: Main output topic
- `--labeled-output-topics`: Named output topics
- `--input-topics`: Input topics (for Streams apps)
- `--input-pattern`: Input topic pattern (for Streams apps)
- `--application-id`: Unique app ID (for Streams apps)
