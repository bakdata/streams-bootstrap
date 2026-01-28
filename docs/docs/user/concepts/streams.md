# Streams applications

Streams apps are applications that process data in real-time as it flows through Kafka topics.
They can be used to filter, transform, aggregate, or enrich data streams.
Streams apps can also produce new messages to other topics based on the processed data.

---

## Application lifecycle

### Running an application

Kafka Streams applications are started via the `KafkaStreamsApplication` entry point:

```java
public static void main(final String[] args) {
    new MyStreamsApplication().startApplication(args);
}
```

When an application is started, the framework performs the following steps:

- Parse command-line arguments and environment variables
- Create a `StreamsApp` instance
- Wrap it in a `ConfiguredStreamsApp`
- Convert it into an `ExecutableStreamsApp`
- Start execution using the `StreamsRunner`

---

### Resetting an application

Streams applications support a dedicated `reset` operation that clears processing state while preserving the
application definition and configuration. This is useful for reprocessing input data from the beginning.

When a reset is triggered, the following resources are affected:

| Resource         | Action                                    |
|------------------|-------------------------------------------|
| State stores     | Cleared locally, changelog topics deleted |
| Internal topics  | Deleted (e.g. repartition topics)         |
| Consumer offsets | Reset to earliest for input topics        |
| Output topics    | Preserved                                 |

Triggering a reset via CLI:

```bash
java -jar my-streams-app.jar reset
```

Triggering a reset programmatically:

```java
try(StreamsCleanUpRunner cleanUpRunner = streamsApp.createCleanUpRunner()){
        cleanUpRunner.

reset();
}
```

After a reset, the application can be started again and will reprocess all input data.

---

### Cleaning an application

The `clean` command performs everything that `reset` does and additionally removes the Kafka consumer groups created by
the application.

```bash
java -jar my-streams-app.jar clean
```

---

## Configuration

### Topics

Streams applications support flexible topic configuration:

- `--input-topics`: Comma-separated list of input topics
- `--input-pattern`: Regex pattern for input topics
- `--output-topic`: Default output topic
- `--error-topic`: Topic for error records
- `--labeled-input-topics`: Named input topics with different message types
- `--labeled-input-patterns`: Additional labeled input topic patterns
- `--labeled-output-topics`: Named output topics with different message types

---

### Application ID

- `--application-id`: Unique Kafka Streams application ID

---

### Kafka properties

Additional Kafka Streams configuration can be supplied using:

- `--kafka-config <key=value,...>`

The framework applies the following defaults:

```text
processing.guarantee=exactly_once_v2
producer.max.in.flight.requests.per.connection=1
producer.acks=all
producer.compression.type=gzip
```

---

### Lifecycle hooks

#### Setup

TODO

#### Clean up

TODO

---

### Execution options

#### On start

Custom logic can be executed once Kafka Streams has fully started:

```java

@Override
private void onStreamsStart(final RunningStreams runningStreams) {
    // Custom startup logic
}
```

#### Application server

TODO

#### State listener

TODO

#### Uncaught exception handler

TODO

#### Closing options

TODO

---

## Command line interface

Streams applications inherit standard CLI options from `KafkaStreamsApplication`.

| Option                         | Description                               | Default        |
|--------------------------------|-------------------------------------------|----------------|
| `--bootstrap-servers`          | Kafka bootstrap servers (comma-separated) | Required       |
| `--schema-registry-url`        | URL of the Schema Registry                | None           |
| `--application-id`             | Kafka Streams application ID              | Auto-generated |
| `--volatile-group-instance-id` | Use volatile group instance ID            | false          |

---

## Deployment

TODO

---

## Kafka Streams extensions

The framework provides several extensions that simplify working with Kafka Streams.

### Simple topic access

TODO

### Error handling

TODO

### Serde auto configuration

TODO

---
