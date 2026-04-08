# Common concepts

## Application types

In streams-bootstrap, there are three application types:

- **App**
- **ConfiguredApp**
- **ExecutableApp**

---

### App

The **App** represents your application logic. Each application type has its own `App` interface:

- **StreamsApp** – for Kafka Streams applications
- **ProducerApp** – for producer applications
- **ConsumerApp** – for consumer applications
- **ConsumerProducerApp** – for consumer–producer applications

You implement the appropriate interface to define your application's behavior.

---

### ConfiguredApp

A **ConfiguredApp** pairs an `App` with its configuration. Examples include:

- `ConfiguredStreamsApp<T extends StreamsApp>`
- `ConfiguredProducerApp<T extends ProducerApp>`
- `ConfiguredConsumerApp<T extends ConsumerApp>`
- `ConfiguredConsumerProducerApp<T extends ConsumerProducerApp>`

This layer handles Kafka property creation, combining:

- base configuration
- app-specific configuration
- user configuration
- runtime configuration, e.g., brokers and schema registry

---

### ExecutableApp

An **ExecutableApp** is a `ConfiguredApp` with runtime configuration applied, making it ready to execute.  
It can create:

- a **Runner** for running the application
- a **CleanUpRunner** for cleanup operations

---

### Usage Pattern

1. You implement an **App**.
2. The system wraps it in a **ConfiguredApp**, applying the configuration.
3. Runtime configuration is then applied to create an **ExecutableApp**, which can:

- **run**, i.e., execute the app
- **clean**, i.e., delete all resources associated with the application
- **reset** (only for consuming applications), i.e., reset application state as if it was launched for the first time
  without deleting any resources

---

## Application lifecycle

Applications built with streams-bootstrap follow a defined lifecycle with specific states and transitions.

The lifecycle is managed through the KafkaApplication base class and provides several extension points for
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

- **Programmatically**: You can create a `Runner` from an `ExecutableApp` to run it directly.

```java
// For streams applications
try (StreamsRunner runner = streamsApp.createRunner()) {
     runner.run();
}

// For producer applications
try (Runner runner = producerApp.createRunner()) {
    runner.run();
}
```

### Cleaning an application

A built-in mechanism is provided to clean up all resources associated with an application.

When the cleanup operation is triggered, the following resources are removed:

| Resource Type       | Description                                               | Streams Apps | Producer Apps | Consumer Apps | Consumer-Producer Apps |
|---------------------|-----------------------------------------------------------|--------------|---------------|---------------|------------------------|
| Output Topics       | Topics the application produces to                        | ✓            | ✓             | N/A           | ✓                      |
| Intermediate Topics | Topics the applications produces to and consumes from     | ✓            | N/A           | N/A           | N/A                    |
| Internal Topics     | Topics for state stores or repartitioning (Kafka Streams) | ✓            | N/A           | N/A           | N/A                    |
| Consumer Groups     | Consumer group metadata                                   | ✓            | N/A           | ✓             | ✓                      |

Cleanup can be triggered:

- **Via Command Line**: When packaged as a runnable JAR, the `clean` command can be used.

  ```bash
  java -jar example-app.jar \
      clean \
      --bootstrap-servers kafka:9092 \
      --output-topic output-topic
  ```
- **Programmatically**:

```java
// For streams applications
try(StreamsCleanUpRunner cleanUpRunner = streamsApp.createCleanUpRunner()) {
    cleanUpRunner.clean();
}

// For producer applications
try(CleanUpRunner cleanUpRunner = producerApp.createCleanUpRunner()) {
    cleanUpRunner.clean();
}
```

Cleanup operations are idempotent, meaning they can be safely retried without causing
additional issues.

## Configuration

Kafka properties are applied in the following order (later values override earlier ones):

1. Base configuration
2. App config from .createKafkaProperties()
3. Kafka-specific environment variables with the `KAFKA_` prefix
4. Runtime args (`--bootstrap-servers`, `--schema-registry`, `--kafka-config`)
5. Serialization config
6. Group ID configuration

Environment variables with the `APP_ prefix` (configurable via `ENV_PREFIX`) are automatically parsed.
Environment variables are converted to CLI arguments:

```text
APP_BOOTSTRAP_SERVERS       →      --bootstrap-servers
APP_SCHEMA_REGISTRY_URL     →      --schema-registry-url
APP_OUTPUT_TOPIC            →      --output-topic
```

### Common CLI Configuration Options

- `--bootstrap-servers`: Kafka bootstrap servers (required)
- `--schema-registry-url`: URL for the Schema Registry. When this option is provided schema cleanup is handled as part
  of the `clean` command
- `--kafka-config`: Key-value Kafka configuration
