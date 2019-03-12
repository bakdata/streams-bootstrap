package com.bakdata.common_kafka_streams;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.Data;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.log4j.Level;
import picocli.CommandLine;

import java.util.Optional;
import java.util.Properties;


/**
 * <p>The base class of the entry point of the streaming application.</p>
 * This class provides common configuration options e.g. {@link #brokers}, {@link #productive} for streaming application.
 * Hereby it automatically populates the passed in command line arguments with matching environment arguments {@link EnvironmentArgumentsParser}.
 * To implement your streaming application inherit from this class and add your custom options.
 */

@Data
public abstract class KafkaStreamsApplication implements Runnable {
    @CommandLine.Option(names = "--brokers", required = true)
    private String brokers = "";

    @CommandLine.Option(names = "--schema-registry-url", required = true)
    private String schemaRegistryUrl = "";

    @CommandLine.Option(names = "--productive", required = false)
    private boolean productive = true;

    @CommandLine.Option(names = "--debug", required = false)
    private boolean debug = false;

    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "print this help and exit")
    private boolean helpRequested = false;

    private KafkaStreams streams;

    private static final String ENV_PREFIX = Optional.ofNullable(System.getenv("ENV_PREFIX")).orElse("APP_");

    private static String[] addEnvironmentVariablesArguments(final String[] args) {
        final String[] environmentArguments = new EnvironmentArgumentsParser(ENV_PREFIX).parseVariables(System.getenv());
        return ArrayUtils.addAll(args, environmentArguments);
    }

    /**
     * <p>This methods needs to be called in the executable custom application class inheriting from {@link KafkaStreamsApplication}.</p>
     *
     * @param app An instance of the custom application class.
     * @param args Arguments passed in by the custom application class.
     */
    protected static void startApplication(KafkaStreamsApplication app, String[] args) {
        final String[] populatedArgs = addEnvironmentVariablesArguments(args);
        CommandLine.run(app, System.out, populatedArgs);
    }

    @Override
    public void run() {
        if(debug) {
            org.apache.log4j.Logger.getLogger("com.bakdata").setLevel(Level.DEBUG);
        }

        var kafkaProperties = getKafkaProperties();
        streams = new KafkaStreams(createTopology(kafkaProperties), kafkaProperties);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> streams.close()));
    }

    public abstract void buildTopology(StreamsBuilder builder, Properties kafkaProperties);

    public Topology createTopology(Properties kafkaProperties) {
        StreamsBuilder builder = new StreamsBuilder();
        buildTopology(builder, kafkaProperties);
        return builder.build();
    }

    /**
     * <p>This method should give a default topic configuration to run your streaming application with.</p>
     * To add a custom configuration please add a similar method to your custom application class:
     * <pre>{@code
     *   public Properties getKafkaProperties() {
     *       # Try to always use the kafka properties from the super class as base Map
     *       Properties kafkaConfig = super.getKafkaProperties();
     *       kafkaConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
     *       kafkaConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
     *       return kafkaConfig;
     *   }
     * }</pre>
     *
     * @return Returns a default kafka configuration {@link Properties}
     */
    public Properties getKafkaProperties() {
        Properties kafkaConfig = new Properties();

        // exactly once and order
        kafkaConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        kafkaConfig.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION), 1);

        // resilience
        if(productive) {
            kafkaConfig.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        }
        kafkaConfig.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");

        // compression
        kafkaConfig.put(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "gzip");

        // topology
        kafkaConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, getClass().getSimpleName());
        kafkaConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        kafkaConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        kafkaConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, getSchemaRegistryUrl());
        kafkaConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokers());

        return kafkaConfig;
    }
}
