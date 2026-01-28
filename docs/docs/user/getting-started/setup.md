# Setup

This page describes dependency setup, configuration options, commands, and Helm-based deployment for
`streams-bootstrap`.

## Dependencies

### Gradle

```gradle
implementation group: 'com.bakdata.kafka', name: 'streams-bootstrap-cli', version: '6.1.0'
```

With Kotlin DSL:

```gradle
implementation(group = "com.bakdata.kafka", name = "streams-bootstrap-cli", version = "6.1.0")
```

### Maven

```xml

<dependency>
    <groupId>com.bakdata.kafka</groupId>
    <artifactId>streams-bootstrap-cli</artifactId>
    <version>6.1.0</version>
</dependency>
```

For other build tools or versions, refer to the
[latest version in MvnRepository](https://mvnrepository.com/artifact/com.bakdata.kafka/streams-bootstrap/latest).
