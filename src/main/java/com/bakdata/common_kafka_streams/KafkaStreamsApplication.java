/*
 * MIT License
 *
 * Copyright (c) 2019 bakdata GmbH
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.bakdata.common_kafka_streams;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import kafka.tools.StreamsResetter;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.log4j.Level;
import picocli.CommandLine;


/**
 * <p>The base class of the entry point of the streaming application.</p>
 * This class provides common configuration options e.g. {@link #brokers}, {@link #productive} for streaming
 * application. Hereby it automatically populates the passed in command line arguments with matching environment
 * arguments {@link EnvironmentArgumentsParser}. To implement your streaming application inherit from this class and add
 * your custom options. Call {@link #startApplication(KafkaStreamsApplication, String[])} with a fresh instance of your
 * class from your main.
 */
@Data
@Slf4j
public abstract class KafkaStreamsApplication implements Runnable, AutoCloseable {
    private static final String ENV_PREFIX = Optional.ofNullable(
            System.getenv("ENV_PREFIX")).orElse("APP_");
    public static final int RESET_SLEEP_MS = 5000;

    @CommandLine.Option(names = "--brokers", required = true)
    private String brokers = "";

    @CommandLine.Option(names = "--schema-registry-url", required = true)
    private String schemaRegistryUrl = "";

    @CommandLine.Option(names = "--productive", arity = "1")
    private boolean productive = true;

    @CommandLine.Option(names = "--debug", arity = "1")
    private boolean debug = false;

    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "print this help and exit")
    private boolean helpRequested = false;

    @CommandLine.Option(names = "--reprocess", arity = "1",
            description = "Reprocess all data by clearing the state store and the global Kafka offsets for the "
                    + "consumer group. Be careful with running in production and with enabling this flag - it "
                    + "might cause inconsistent processing with multiple replicas.")
    private boolean forceReprocessing = false;

    @CommandLine.Option(names = "--streams-config", split = " ")
    private Map<String, String> streamsConfig = new HashMap<>();

    @CommandLine.Option(names = "--input-topic", description = "Input topic")
    protected String inputTopic = "";

    @CommandLine.Option(names = "--output-topic", description = "Output topic")
    protected String outputTopic = "";

    private KafkaStreams streams;

    private static String[] addEnvironmentVariablesArguments(final String[] args) {
        final List<String> environmentArguments = new EnvironmentArgumentsParser(ENV_PREFIX)
                .parseVariables(System.getenv());
        final ArrayList<String> allArgs = new ArrayList<>(environmentArguments);
        allArgs.addAll(Arrays.asList(args));
        return allArgs.toArray(String[]::new);
    }

    /**
     * This variable is usually set on application start. When the application is running in debug mode it is used to
     * reconfigure the child app package logger. On default it points to the package of this class allowing to execute
     * the run method independently.
     */
    private static String appPackageName = KafkaStreamsApplication.class.getPackageName();

    /**
     * <p>This methods needs to be called in the executable custom application class inheriting from
     * {@link KafkaStreamsApplication}.</p>
     *
     * @param app An instance of the custom application class.
     * @param args Arguments passed in by the custom application class.
     */
    protected static void startApplication(final KafkaStreamsApplication app, final String[] args) {
        appPackageName = app.getClass().getPackageName();
        final String[] populatedArgs = addEnvironmentVariablesArguments(args);
        CommandLine.run(app, System.out, populatedArgs);
    }

    @Override
    public void run() {
        log.info("Starting application");
        if (this.debug) {
            org.apache.log4j.Logger.getLogger("com.bakdata").setLevel(Level.DEBUG);
            org.apache.log4j.Logger.getLogger(appPackageName).setLevel(Level.DEBUG);
        }
        log.debug(this.toString());
        final var kafkaProperties = this.getKafkaProperties();
        this.streams = new KafkaStreams(this.createTopology(), kafkaProperties);

        if (this.forceReprocessing) {
            this.cleanUp();
        }

        this.streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    }


    @Override
    public void close() {
        log.info("Stopping application");
        if (this.streams == null) {
            return;
        }
        this.streams.close();
    }

    public abstract void buildTopology(StreamsBuilder builder);

    /**
     * This must be set to a unique value for every application interacting with your kafka cluster to ensure internal
     * state encapsulation. Could be set to: className-inputTopic-outputTopic
     */
    public abstract String getUniqueAppId();

    public Topology createTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        this.buildTopology(builder);
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
        final Properties kafkaConfig = new Properties();

        // exactly once and order
        kafkaConfig.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        kafkaConfig.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION), 1);

        // resilience
        if (this.productive) {
            kafkaConfig.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        }

        kafkaConfig.setProperty(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");

        // compression
        kafkaConfig.setProperty(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "gzip");

        // topology
        kafkaConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, this.getUniqueAppId());
        kafkaConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        kafkaConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        kafkaConfig.setProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.getSchemaRegistryUrl());
        kafkaConfig.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.getBrokers());

        this.streamsConfig.forEach(kafkaConfig::setProperty);

        return kafkaConfig;
    }

    protected static void runResetter(final String inputTopics, final String brokers, final String appId) {
        final String[] args = {
                "--application-id", appId,
                "--bootstrap-servers", brokers,
                "--input-topics", inputTopics
        };
        final StreamsResetter resetter = new StreamsResetter();
        resetter.run(args);
    }

    protected void cleanUp() {
        if (!this.inputTopic.isBlank()) {
            runResetter(this.inputTopic, this.brokers, this.getUniqueAppId());
        }
        this.streams.cleanUp();
        try {
            Thread.sleep(RESET_SLEEP_MS);
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
