# What is streams-bootstrap?

`streams-bootstrap` is a Java library that standardizes the development and operation of Kafka-based applications (Kafka
Streams and plain Kafka clients).

The framework supports Apache Kafka 4.1 and Java 17. Its modules are published to Maven Central for straightforward
integration into existing projects.

## Why use it?

Kafka Streams and the core Kafka clients provide strong primitives for stream processing and messaging, but they do not
prescribe:

- How to structure a full application around those primitives
- How to configure applications consistently
- How to deploy and operate these services on Kubernetes
- How to perform repeatable reprocessing and cleanup
- How to handle errors and large messages uniformly

`streams-bootstrap` addresses these aspects by supplying:

1. **Standardized base classes** for Kafka Streams and client applications.
2. **A common CLI/configuration contract** for all Kafka applications.
3. **Helm-based deployment templates** and conventions for Kubernetes.
4. **Built-in reset/clean workflows** for reprocessing and state management.
5. **Consistent error-handling** and dead-letter integration.
6. **Testing infrastructure** for local development and CI environments.
7. **Optional blob-storage-backed serialization** for large messages.

## Framework Architecture

The framework uses a modular architecture with a clear separation of concerns.

### Core Modules

- `streams-bootstrap-core`: Base classes such as `KafkaApplication`, `Runner`, and `CleanUpRunner`
- `streams-bootstrap-cli`: CLI framework based on `picocli`
- `streams-bootstrap-test`: Testing utilities (`TestApplicationRunner`, `KafkaTestClient`)
- `streams-bootstrap-large-messages`: Support for handling large Kafka messages
- `streams-bootstrap-cli-test`: Test support for CLI-based applications

### External Dependencies

- Apache Kafka: `kafka-streams`, `kafka-clients`
- Confluent Platform: Schema Registry and Avro SerDes
- Picocli: Command-line parsing and CLI framework
