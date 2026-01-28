# Large messages

## Overview

The **Large Messages Extension** adds support for handling messages that exceed Kafka's size limitations by using
external storage mechanisms with automatic cleanup.
It integrates with the *streams-bootstrap* framework to transparently manage:

- large message serialization
- large message deserialization
- blob storage files cleanup

For more details, see the large messages
module: [streams-bootstrap-large-messages GitHub repository](https://github.com/bakdata/streams-bootstrap/tree/master/streams-bootstrap-large-messages)

There are two supported ways to enable cleanup for large messages:

- Implement `LargeMessageStreamsApp`
- Register a topic cleanup hook manually

---

### Option 1: Implement `LargeMessageStreamsApp`

Use this option for Kafka Streams applications where large message cleanup should always run together with topic
cleanup.

```java
public final class MyStreamsApp implements LargeMessageStreamsApp {

    @Override
    public void buildTopology(final StreamsBuilderX builder) {
        // build topology here
    }
}
```

### Option 2: Register a cleanup hook manually

If cleanup should only happen conditionally or requires custom behavior, a topic hook can be registered explicitly.

```java
private final boolean largeMessageCleanupEnabled;

@Override
public StreamsCleanUpConfiguration setupCleanUp(
        final AppConfiguration<StreamsTopicConfig> configuration) {

    final StreamsCleanUpConfiguration cleanUp =
            StreamsApp.super.setupCleanUp(configuration);

    if (this.largeMessageCleanupEnabled) {
        LargeMessageAppUtils.registerTopicHook(cleanUp, configuration);
    }

    return cleanUp;
}
```
