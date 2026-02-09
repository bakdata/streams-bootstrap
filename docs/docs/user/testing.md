# Testing

The `streams-bootstrap` testing tools provide utilities for testing Kafka Streams, Consumer, Producer and Consumer-Producer
applications, covering both unit-level and integration-style scenarios.

They abstract common test concerns such as Kafka infrastructure setup, Schema Registry integration, application
lifecycle handling, and consumer group verification, and are designed to work with real Kafka clusters as well as
schema-aware test environments.

## TestApplicationRunner

`TestApplicationRunner` is a test utility for running, configuring, and verifying Kafka applications in integration and system tests.

It abstracts away repetitive setup such as:
- bootstrap servers
- Schema Registry
- Kafka client configuration
- CLI argument wiring
- lifecycle commands (`run`, `clean`, `reset`)

Typical use cases:
- end-to-end tests
- containerized test environments
- embedded Kafka setups
- CI pipelines

## Typical Usage

```java
TestApplicationRunner runner =
        TestApplicationRunner.create("localhost:9092")
                .withSchemaRegistry()
                .withStateDir(tempDir)
                .withNoStateStoreCaching();
```

All applications executed via this runner automatically inherit this configuration.

Bootstrap Servers
- Passed via `--bootstrap-servers`
- Also set directly on the application instance

Kafka Configuration
- All provided Kafka properties are injected
- Passed via `--kafka-config key=value`
- Also merged into `app.setKafkaConfig(...)`

Schema Registry (optional)
- Passed via `--schema-registry-url`
- Only configured when explicitly enabled

---

### Configuring Kafka for Tests

```java
runner = runner.withKafkaConfig(Map.of("auto.offset.reset", "earliest"));
```

Behavior:
- merged with existing configuration
- immutable after creation
- overrides application defaults

---

#### Kafka Streams–Specific Helpers

##### Configure State Directory

```java
runner = runner.withStateDir(tempDir);
```

Sets:
```
state.dir = <tempDir>
```

Use this to:
- isolate test runs
- avoid state leakage between tests

---

##### Disable State Store Caching

```java
runner = runner.withNoStateStoreCaching();
```

Sets:
```
statestore.cache.max.bytes = 0
```

Useful when:
- asserting exact record counts
- debugging processor behavior
- avoiding cache-related timing issues

---

#### Consumer-Specific Helpers

##### Configure Session Timeout

```java
runner = runner.withSessionTimeout(Duration.ofSeconds(5));
```

Sets:
```
session.timeout.ms = 5000
```

Useful for:
- fast consumer group rebalancing
- deterministic failure testing

---

### Schema Registry Support

#### Enable a Test Schema Registry

```java
runner = runner.withSchemaRegistry();
```

Creates:
- isolated in-memory Schema Registry
- random scope to avoid collisions
- transparent integration for the application

---

#### Use a Custom TestSchemaRegistry

```java
TestSchemaRegistry registry = new TestSchemaRegistry();
runner = runner.withSchemaRegistry(registry);
```

Use this when:
- sharing schemas across applications
- inspecting registered schemas during tests

---

### Running Applications

#### CLI

```java
CompletableFuture<Integer> exitCode =
        runner.run(app, "--some-flag");
```

- invokes `startApplicationWithoutExit`
- returns application exit code

---

#### Runnable

```java
CompletableFuture<Void> execution = runner.run(app);
```

- calls `onApplicationStart()`
- runs application directly
- suitable for long-running tests

---

### Cleaning and Resetting Applications

#### Clean

```java
runner.clean(app);
```

or

```java
runner.clean(app, "--custom-arg");
```

Used to:
- delete Kafka topics
- clean local state
- execute cleanup hooks

---

#### Reset

Supported for:
- Streams applications
- Consumer applications
- Consumer–Producer applications

```java
runner.reset(streamsApp);
```

---

### Consumer Group Verification

```java
ConsumerGroupVerifier verifier = runner.verify(streamsApp);
```

Allows you to:
- assert consumer group existence
- check stability
- inspect committed offsets

---

### Creating Test Clients

```java
KafkaTestClient client = runner.newTestClient();
```

Provides:
- AdminClient access
- Producer/Consumer helpers
- runtime-aware configuration

---

## TestApplicationTopologyFactory

`TestApplicationTopologyFactory` is a test helper for Kafka Streams applications that integrates with Fluent Kafka Streams Tests.

It allows you to:
- derive a `TestTopology` from a real application
- reuse production topology and configuration
- inject test-specific runtime settings

---

### Typical Usage

```java
TestApplicationTopologyFactory factory =
        TestApplicationTopologyFactory.withSchemaRegistry();
```

or without Schema Registry:

```java
TestApplicationTopologyFactory factory = new TestApplicationTopologyFactory();
```

---

### Schema Registry Support

#### Automatic Schema Registry

```java
TestApplicationTopologyFactory factory =
        TestApplicationTopologyFactory.withSchemaRegistry();
```

- random isolated scope
- no cross-test collisions
- safe for parallel execution

---

#### Custom Schema Registry

```java
TestSchemaRegistry registry = new TestSchemaRegistry();
TestApplicationTopologyFactory factory =
        TestApplicationTopologyFactory.withSchemaRegistry(registry);
```

---

### Modifying Kafka Configuration

```java
factory = factory.with(Map.of("commit.interval.ms", 100));
```

- merged into runtime configuration
- applies only to tests
- does not mutate application

---

### Creating a TestTopology

```java
TestTopology<String, MyValue> topology = factory.createTopology(app);
```

Execution flow:
1. application prepared
2. runtime configuration injected
3. topology extracted
4. `TestTopology` created

---

### JUnit 5 Integration

```java
TestTopologyExtension<String, MyValue> extension = factory.createTopologyExtension(app);
```

---

### Accessing Kafka Properties

```java
Map<String, Object> props = factory.getKafkaProperties(app);
```
---
