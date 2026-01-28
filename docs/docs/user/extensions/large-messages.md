# Large messages

## Overview

The **Large Messages Extension** adds support for handling messages that exceed Kafka's size limitations by using
external storage mechanisms with automatic cleanup.  
It integrates with the *streams-bootstrap* framework to transparently manage:

- large message serialization
- large message deserialization
- external storage cleanup

For both **producer** and **streams** applications.

---

## Core Components

### LargeMessageAppUtils

`LargeMessageAppUtils` is the central utility for managing large message cleanup operations.  
It provides factory methods for creating **topic hooks** that automatically clean up externally stored large message
files when topics are deleted.

**Key methods:**

- `createTopicHook(Map<String, Object> kafkaProperties)`  
  Creates a cleanup hook from Kafka properties.
- `createTopicHook(AppConfiguration<?> configuration)`  
  Creates a cleanup hook from application configuration.
- `registerTopicHook()`  
  Registers the cleanup hook with a cleanup configuration.

---

### LargeMessageProducerApp

`LargeMessageProducerApp` extends `ProducerApp` to automatically handle cleanup of large message files for producer
applications.

When a topic is deleted, it removes all associated large message files stored externally via the
`LargeMessageSerializer`.

The interface automatically registers the cleanup hook inside `setupCleanUp()`:

---

### LargeMessageStreamsApp

`LargeMessageStreamsApp` extends `StreamsApp` to provide automatic cleanup for Kafka Streams applications using large
messages.

When streams topics are cleaned up, the extension ensures corresponding external large message files are also removed.

It also registers the cleanup hook during `setupCleanUp()`:

---

## Implementation Details

### Topic Hook Mechanism

Cleanup is implemented through the `LargeMessageTopicHook` class, which implements the `TopicHook` interface.

When a topic is deleted, the hook's `deleted()` method is triggered, which calls `deleteAllFiles()` on the configured
`LargeMessageStoringClient`.

---

### Configuration Requirements

The large message extension requires Kafka properties needed to build an `AbstractLargeMessageConfig`.

This configuration is used to instantiate the appropriate `LargeMessageStoringClient` for the storage backend.

---

## Usage Examples

### Producer Application

```java
public class MyLargeMessageProducer extends KafkaProducerApplication<LargeMessageProducerApp> {
    @Override
    public LargeMessageProducerApp createApp() {
        return new LargeMessageProducerApp() {

            @Override
            public ProducerRunnable buildRunnable(final ProducerBuilder builder) {
                return () -> {
                    try (final Producer<Object, Object> producer = builder.createProducer()) {
                        // Producer logic with large message support  
                    }
                };
            }

            @Override
            public SerializerConfig defaultSerializationConfig() {
                // Configure LargeMessageSerializer  
                return new SerializerConfig(LargeMessageSerializer.class, LargeMessageSerializer.class);
            }
        };
    }
}

```
