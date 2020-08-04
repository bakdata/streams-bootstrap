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

import com.google.common.base.Preconditions;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreams.StateListener;
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
    public static final int RESET_SLEEP_MS = 5000;
    private static final String ENV_PREFIX = Optional.ofNullable(
            System.getenv("ENV_PREFIX")).orElse("APP_");
    /**
     * This variable is usually set on application start. When the application is running in debug mode it is used to
     * reconfigure the child app package logger. On default it points to the package of this class allowing to execute
     * the run method independently.
     */
    private static String appPackageName = KafkaStreamsApplication.class.getPackageName();
    @CommandLine.Option(names = "--input-topics", description = "Input topics", split = ",")
    protected List<String> inputTopics = new ArrayList<>();
    @CommandLine.Option(names = "--output-topic", description = "Output topic")
    protected String outputTopic = "";
    @CommandLine.Option(names = "--error-topic", description = "Error topic (default: ${DEFAULT-VALUE}")
    protected String errorTopic = "error_topic";
    @CommandLine.Option(names = "--extra-output-topics", split = ",", description = "Additional output topics")
    protected Map<String, String> extraOutputTopics = new HashMap<>();
    @CommandLine.Option(names = "--brokers", required = true)
    private String brokers = "";
    @CommandLine.Option(names = "--schema-registry-url", required = true)
    private String schemaRegistryUrl = "";
    @CommandLine.Option(names = "--productive", arity = "1")
    private boolean productive = true;
    @CommandLine.Option(names = "--debug", arity = "0..1")
    private boolean debug = false;
    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "print this help and exit")
    private boolean helpRequested = false;
    @CommandLine.Option(names = "--clean-up", arity = "0..1",
            description = "Clear the state store and the global Kafka offsets for the "
                    + "consumer group. Be careful with running in production and with enabling this flag - it "
                    + "might cause inconsistent processing with multiple replicas.")
    private boolean cleanUp = false;
    @CommandLine.Option(names = "--delete-output", arity = "0..1",
            description = "Delete the output topic during the clean up.")
    private boolean deleteOutputTopic = false;
    @CommandLine.Option(names = "--streams-config", split = ",", description = "Additional Kafka Streams properties")
    private Map<String, String> streamsConfig = new HashMap<>();
    private KafkaStreams streams;

    private static String[] addEnvironmentVariablesArguments(final String[] args) {
        Preconditions.checkArgument(!ENV_PREFIX.equals(EnvironmentStreamsConfigParser.PREFIX),
                "Prefix '" + EnvironmentStreamsConfigParser.PREFIX + "' is reserved for Streams config");
        final List<String> environmentArguments = new EnvironmentArgumentsParser(ENV_PREFIX)
                .parseVariables(System.getenv());
        final ArrayList<String> allArgs = new ArrayList<>(environmentArguments);
        allArgs.addAll(Arrays.asList(args));
        return allArgs.toArray(String[]::new);
    }

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
        // deprecated command call is the only one that properly exits the application in both clean up and streams
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

        try {
            final var kafkaProperties = this.getKafkaProperties();
            this.streams = new KafkaStreams(this.createTopology(), kafkaProperties);
            Optional.ofNullable(this.getUncaughtExceptionHandler())
                    .ifPresent(this.streams::setUncaughtExceptionHandler);
            Optional.ofNullable(this.getStateListener())
                    .ifPresent(this.streams::setStateListener);

            if (this.cleanUp) {
                this.runCleanUp();
            } else {
                this.runStreamsApplication();
            }
        } catch (final Exception e) {
            this.closeResources();
            throw e;
        }
    }

    @Override
    public void close() {
        log.info("Stopping application");
        this.closeResources();
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
     * <p>This method specifies the configuration to run your streaming application with.</p>
     * To add a custom configuration please override {@link #createKafkaProperties()}. Configuration properties
     * specified via environment (starting with STREAMS_) or via cli option {@code --streams-config} are always applied
     * with highest priority (the latter overrides the former).
     *
     * @return Returns Kafka Streams configuration {@link Properties}
     */
    public final Properties getKafkaProperties() {
        final Properties kafkaConfig = this.createKafkaProperties();

        EnvironmentStreamsConfigParser.parseVariables(System.getenv())
                .forEach(kafkaConfig::setProperty);
        this.streamsConfig.forEach(kafkaConfig::setProperty);

        return kafkaConfig;
    }

    public String getInputTopic() {
        if (this.getInputTopics().isEmpty() || this.getInputTopics().get(0).isBlank()) {
            throw new IllegalArgumentException("One input topic required");
        }
        return this.getInputTopics().get(0);
    }

    protected Optional<String> getOutputTopic(final String role) {
        return Optional.ofNullable(this.extraOutputTopics.get(role));
    }

    /**
     * Create an {@link UncaughtExceptionHandler} to use for Kafka Streams. Will not be configured if {@code null} is
     * returned.
     *
     * @return {@code null} by default.
     * @see KafkaStreams#setUncaughtExceptionHandler(UncaughtExceptionHandler)
     */
    protected UncaughtExceptionHandler getUncaughtExceptionHandler() {
        return null;
    }

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
        return kafkaConfig;
    }

    protected void runStreamsApplication() {
        this.streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    }

    /**
     * Method to close resources outside of {@link KafkaStreams}. Will be called by default on {@link #close()} and on
     * transitioning to {@link State#ERROR}.
     */
    protected void closeResources() {
        //do nothing by default
    }

    /**
     * Create a {@link StateListener} to use for Kafka Streams. Will not be configured if {@code null} is returned.
     *
     * @return {@link StateListener} that calls {@link #closeResources()} on transition to {@link State#ERROR}.
     * @see KafkaStreams#setStateListener(StateListener)
     */
    protected StateListener getStateListener() {
        return (newState, oldState) -> {
            if (newState == State.ERROR) {
                log.info("Kafka Streams transitioned from {} to {}", oldState, State.ERROR);
                KafkaStreamsApplication.this.closeResources();
            }
        };
    }

    /**
     * This methods resets the offset for all input topics and deletes internal topics, application state, and
     * optionally the output and error topic.
     */
    protected void runCleanUp() {
        final CleanUpRunner cleanUpRunner = CleanUpRunner.builder()
                .topology(this.createTopology())
                .appId(this.getUniqueAppId())
                .kafkaProperties(this.getKafkaProperties())
                .schemaRegistryUrl(this.schemaRegistryUrl)
                .brokers(this.brokers)
                .streams(this.streams)
                .build();

        this.cleanUpRun(cleanUpRunner);
        this.close();
    }

    protected void cleanUpRun(final CleanUpRunner cleanUpRunner) {
        cleanUpRunner.run(this.deleteOutputTopic);
    }
}
