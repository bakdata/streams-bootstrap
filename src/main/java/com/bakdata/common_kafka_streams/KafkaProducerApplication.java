package com.bakdata.common_kafka_streams;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import java.util.Properties;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.log4j.Level;
import picocli.CommandLine;


/**
 * <p>The base class of the entry point of the streaming application.</p>
 * This class provides common configuration options e.g. {@link #brokers} for streaming application. Hereby it
 * automatically populates the passed in command line arguments with matching environment arguments {@link
 * EnvironmentArgumentsParser}. To implement your streaming application inherit from this class and add your custom
 * options. Call {@link #startApplication(KafkaProducerApplication, String[])} with a fresh instance of your class from
 * your main.
 */
@Data
@Slf4j
public abstract class KafkaProducerApplication extends KafkaApplication {
    /**
     * This variable is usually set on application start. When the application is running in debug mode it is used to
     * reconfigure the child app package logger. On default it points to the package of this class allowing to execute
     * the run method independently.
     */
    private static String appPackageName = KafkaProducerApplication.class.getPackageName();

    /**
     * <p>This methods needs to be called in the executable custom application class inheriting from
     * {@code KafkaProducerApplication}.</p>
     *
     * @param app An instance of the custom application class.
     * @param args Arguments passed in by the custom application class.
     */
    protected static void startApplication(final KafkaProducerApplication app, final String[] args) {
        appPackageName = app.getClass().getPackageName();
        final String[] populatedArgs = addEnvironmentVariablesArguments(args);
        final int exitCode = new CommandLine(app).execute(populatedArgs);
        System.exit(exitCode);
    }

    public void run() {
        log.info("Starting application");
        if (this.debug) {
            org.apache.log4j.Logger.getLogger("com.bakdata").setLevel(Level.DEBUG);
            org.apache.log4j.Logger.getLogger(appPackageName).setLevel(Level.DEBUG);
        }
        log.debug(this.toString());
        if (this.cleanUp) {
            this.runCleanUp();
        } else {
            this.runApplication();
        }
    }

    protected abstract void runApplication();

    /**
     * <p>This method should give a default configuration to run your streaming application with.</p>
     * To add a custom configuration please add a similar method to your custom application class:
     * <pre>{@code
     *   protected Properties createKafkaProperties() {
     *       # Try to always use the kafka properties from the super class as base Map
     *       Properties kafkaConfig = super.createKafkaProperties();
     *       kafkaConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
     *       kafkaConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
     *       return kafkaConfig;
     *   }
     * }</pre>
     *
     * @return Returns a default Kafka Streams configuration {@link Properties}
     */
    protected Properties createKafkaProperties() {
        final Properties kafkaConfig = new Properties();

        kafkaConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class);
        kafkaConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class);
        // exactly once and order
        kafkaConfig.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        kafkaConfig.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        // compression
        kafkaConfig.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        kafkaConfig.setProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.getSchemaRegistryUrl());
        kafkaConfig.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokers);
        return kafkaConfig;
    }

    protected <K, V> KafkaProducer<K, V> createProducer() {
        final Properties properties = new Properties();
        properties.putAll(this.getKafkaProperties());
        return new KafkaProducer<>(properties);
    }

    /**
     * This methods deletes all output topics.
     */
    protected void runCleanUp() {
        final ProducerCleanUpRunner cleanUpRunner = ProducerCleanUpRunner.builder()
                .kafkaProperties(this.getKafkaProperties())
                .schemaRegistryUrl(this.getSchemaRegistryUrl())
                .brokers(this.getBrokers())
                .build();

        this.cleanUpRun(cleanUpRunner);
    }

    protected void cleanUpRun(final ProducerCleanUpRunner cleanUpRunner) {
        cleanUpRunner.run(ImmutableList.<String>builder()
                .add(this.getOutputTopic())
                .addAll(this.extraOutputTopics.values())
                .build());
    }
}
