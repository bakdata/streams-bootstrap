# Local deployment

Applications can be run locally for development and testing purposes. This can be done programmatically within your
code.

### Programmatic Local Execution

Here is an example of how to run a producer application programmatically. This is useful for simple applications or for
testing.

```java
try(final KafkaProducerApplication<?> app = new SimpleKafkaProducerApplication<>(() ->
        new ProducerApp() {
            @Override
            public ProducerRunnable buildRunnable(final ProducerBuilder builder) {
                return () -> {
                    try (final Producer<Object, Object> producer = builder.createProducer()) {
                        // Producer logic
                    }
                };
            }

            @Override
            public SerializerConfig defaultSerializationConfig() {
                return new SerializerConfig(StringSerializer.class, StringSerializer.class);
            }
        }
)){
        app.

setBootstrapServers("localhost:9092");
    app.

setOutputTopic("output-topic");
    app.

run();
}
```

### Command Line Execution

You can also run the application from the command line by packaging it as a JAR file.

```bash
java -jar my-producer-app.jar --bootstrap-servers localhost:9092 --output-topic my-topic run
```
