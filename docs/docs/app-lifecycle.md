This section documents the lifecycle management aspects of applications built with streams-bootstrap, focusing on how
applications are started, stopped, cleaned up, and reset. The framework provides standardized ways to manage the
complete lifecycle of both Kafka Streams and Producer applications.

For information about creating Kafka Streams applications, see Kafka Streams Applications. For information about
creating Kafka Producer applications, see Kafka Producer Applications.

## Application Lifecycle States

Applications built with streams-bootstrap follow a defined lifecycle with specific states and transitions:

- Deployed
- Running
- Processing
- Cleanup

## Running Applications

Applications built with streams-bootstrap can be started in two primary ways:

- **Via Command Line Interface**: When deployed as a container, the application's `run` command is the default
  entrypoint.
- **Programmatically**: In code, the `run()` method or appropriate runner class can be used.

### Running Streams Applications

Streams applications start Kafka Streams processing through the `StreamsRunner` class, which handles the lifecycle of a
Kafka Streams instance.

When an application is running, it processes records from the input topics and writes results to the output topic. The
application remains in the running state until explicitly stopped or encounters an unrecoverable error.

### Running Producer Applications

Producer applications use the `ProducerRunner` class, which executes a runnable defined by the application:

Unlike Streams applications, Producer applications typically run to completion and then terminate, unless configured as
a continuous service.

## Cleaning Up Applications

The framework provides a built-in mechanism to clean up all resources associated with an application.

### What Gets Cleaned

When the cleanup operation is triggered, the following resources are removed:

| Resource Type       | Description                                               | Streams Apps | Producer Apps |
|---------------------|-----------------------------------------------------------|--------------|---------------|
| Output Topics       | The main output topic of the application                  | ✓            | ✓             |
| Intermediate Topics | Topics for stream operations like `through()`             | ✓            | N/A           |
| Internal Topics     | Topics for state stores or repartitioning (Kafka Streams) | ✓            | N/A           |
| Consumer Groups     | Consumer group metadata                                   | ✓            | N/A           |
| Schema Registry     | All registered schemas                                    | ✓            | ✓             |

### Triggering Cleanup

Cleanup can be triggered:

- **Via Command Line**: Helm cleanup jobs
- **Programmatically**:

```java
// For streams applications
try(StreamsCleanUpRunner cleanUpRunner=streamsApp.createCleanUpRunner()){
        cleanUpRunner.clean();
        }

// For producer applications
        try(CleanUpRunner cleanUpRunner=producerApp.createCleanUpRunner()){
        cleanUpRunner.clean();
        }
```

The framework ensures that cleanup operations are idempotent, meaning they can be safely retried without causing
additional issues.

## Resetting Stream Applications

For Kafka Streams applications, the framework provides a special "reset" operation that preserves the application but
resets its processing state. This is useful for reprocessing data without having to recreate the application's
resources.

### What Gets Reset

When reset is triggered, the following resources are affected:

| Resource           | Action                                    |
|--------------------|-------------------------------------------|
| State Stores       | Cleared locally, changelog topics deleted |
| Internal Topics    | Deleted (e.g. repartition topics)         |
| Consumer Offsets   | Reset to earliest for input topics        |
| Output Topic       | Preserved                                 |
| Application Config | Preserved                                 |

### Triggering Reset

Via Command Line (when deployed with Helm charts) or programmatically:

```java
try(StreamsCleanUpRunner cleanUpRunner=streamsApp.createCleanUpRunner()){
        cleanUpRunner.reset();
        }
```

After reset, the application can be started again and will reprocess all data from the beginning, effectively providing
a "replay" capability while maintaining the original application structure.

## Error Handling

The framework provides default handling for common errors:

- **Missing Input Topics**: Handled gracefully at startup
- **Consumer Group Errors**: Safe during cleanup
- **Schema Registry Errors**: Handled during registry operations

Kafka Streams-specific errors are managed via `StateListener` and `StreamsUncaughtExceptionHandler`.

## Custom Cleanup Hooks

Applications can register custom cleanup logic.

Useful for removing external metadata or triggering system-side notifications.
