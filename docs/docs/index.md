# streams-bootstrap Overview

The `streams-bootstrap` repository is a comprehensive framework for building, testing, and deploying Kafka Streams and Producer applications with standardized patterns and Kubernetes deployment support.

The streams-bootstrap framework simplifies the development lifecycle of Kafka applications by providing base classes, CLI utilities, testing infrastructure, and Helm charts for deployment. For detailed information about the core framework architecture, see Core Framework Architecture. For deployment-specific configuration, see Helm Charts Overview.

## Purpose and Scope

`streams-bootstrap` is a Java framework designed to standardize the development and deployment of Apache Kafka applications.

### Features

- **Application Framework**: Base classes and utilities for Kafka Streams and Producer applications
- **CLI Integration**: Unified command-line interface for application configuration
- **Testing Infrastructure**: Rich test tools for development and CI
- **Kubernetes Deployment**: Helm charts for production deployment with auto-scaling and monitoring
- **Lifecycle Management**: Cleanup, reset, and topic management support

The framework supports Apache Kafka 4.0 and Java 17, with modules published to Maven Central for easy integration into existing projects.

---

## Framework Architecture

The framework uses a modular architecture with clear separation of concerns.

### Core Modules

- `streams-bootstrap-core`: Base classes like `KafkaApplication`, `Runner`, and `CleanUpRunner`
- `streams-bootstrap-cli`: CLI framework using `picocli`
- `streams-bootstrap-test`: Testing utilities (`TestApplicationRunner`, `KafkaTestClient`)
- `streams-bootstrap-large-messages`: Support for handling large Kafka messages
- `streams-bootstrap-cli-test`: CLI test support

<h3> Application Types </h3>

- `KafkaStreamsApplication`: Kafka Streams-based apps
- `KafkaProducerApplication`: Kafka Producer-based apps

<h3> External Dependencies </h3>

- Apache Kafka: `kafka-streams`, `kafka-clients`
- Confluent Platform: `schema-registry`, `avro-serde`
- Picocli: CLI framework

---

## Application Types and CLI Framework

The framework supports two core application types, extending `KafkaApplication`.

### CLI Commands

- `run`: Run the application
- `clean`: Delete topics and consumer groups
- `reset`: Reset internal state and offsets

### Common CLI Configuration Options

- `--bootstrap-servers`: Kafka bootstrap servers (required)
- `--schema-registry-url`: URL for Avro serialization
- `--kafka-config`: Key-value Kafka configuration
- `--output-topic`: Main output topic
- `--labeled-output-topics`: Named output topics
- `--input-topics`: Input topics (for Streams apps)
- `--input-pattern`: Input topic pattern (for Streams apps)
- `--application-id`: Unique app ID (Streams apps)

---

## Deployment and Operations

The framework includes full support for Kubernetes deployments using Helm charts.

### Helm Charts

- `streams-app`: Kafka Streams deployment
- `producer-app`: Kafka Producer deployment
- `streams-app-cleanup-job`: Cleanup job for Streams apps
- `producer-app-cleanup-job`: Cleanup job for Producer apps

### Features

- **Deployment Modes**: Job, CronJob, or Deployment based on application type
- **Auto-scaling**: KEDA integration for Kafka lag-based scaling
- **Monitoring**: JMX metrics export and Prometheus integration
- **State Storage**: Persistent volumes for Kafka Streams state stores
- **Security**: Secret management and service account configuration

---

## Testing Features

- **Test Application Runner**: Simplified test execution with automatic configuration
- **Kafka Test Client**: Utilities for topic and consumer group management in tests
- **Consumer Group Verification**: Validation of consumer group state and offsets
- **Schema Registry Mock**: In-memory schema registry for testing serialization
- 
---

## Summary

streams-bootstrap provides a complete framework for Kafka application development with standardized patterns for configuration, deployment, and testing. The modular architecture allows developers to use only the components they need while maintaining consistency across different application types and deployment environments.

The framework's key strengths include:

### Highlights

-  **Standardized CLI**: Consistent and predictable configuration
-  **Production-ready Deployment**: Helm charts with monitoring and scaling
-  **Robust Testing Support**: Tools for unit and integration tests
-  **Lifecycle Management**: Easy cleanup and reset operations

Modular by design, it enables developers to use only what they need—while maintaining operational consistency across deployments.
