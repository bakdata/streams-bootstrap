/*
 * MIT License
 *
 * Copyright (c) 2023 bakdata
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

package com.bakdata.kafka;

import static java.util.Objects.nonNull;

import com.bakdata.kafka.util.ImprovedAdminClient;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.CloseOptions;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import picocli.CommandLine;
import picocli.CommandLine.UseDefaultConverter;


/**
 * <p>The base class of the entry point of the streaming application.</p>
 * This class provides common configuration options e.g. {@link #brokers}, {@link #productive} for streaming
 * application. Hereby it automatically populates the passed in command line arguments with matching environment
 * arguments {@link EnvironmentArgumentsParser}. To implement your streaming application inherit from this class and add
 * your custom options. Call {@link #startApplication(KafkaApplication, String[])} with a fresh instance of your class
 * from your main.
 */
@ToString(callSuper = true)
@Getter
@Setter
@RequiredArgsConstructor
@Slf4j
public abstract class KafkaStreamsApplication extends KafkaApplication implements AutoCloseable {
    private static final int DEFAULT_PRODUCTIVE_REPLICATION_FACTOR = 3;
    private final CountDownLatch streamsShutdown = new CountDownLatch(1);
    @CommandLine.Option(names = "--input-topics", description = "Input topics", split = ",")
    protected List<String> inputTopics = new ArrayList<>();
    @CommandLine.Option(names = "--input-pattern", description = "Input pattern")
    protected Pattern inputPattern;
    @CommandLine.Option(names = "--error-topic", description = "Error topic")
    protected String errorTopic;
    @CommandLine.Option(names = "--extra-input-topics", split = ",", description = "Additional named input topics",
        converter = {UseDefaultConverter.class, StringListConverter.class})
    protected Map<String, List<String>> extraInputTopics = new HashMap<>();
    @CommandLine.Option(names = "--extra-input-patterns", split = ",", description = "Additional named input patterns")
    protected Map<String, Pattern> extraInputPatterns = new HashMap<>();
    @CommandLine.Option(names = "--productive", arity = "1",
        description = "Whether to use Kafka Streams configuration values, such as replication.factor=3, that are "
            + "more suitable for production environments")
    private boolean productive = true;
    @CommandLine.Option(names = "--delete-output", arity = "0..1",
        description = "Delete the output topic during the clean up.")
    private boolean deleteOutputTopic;
    @CommandLine.Option(names = "--volatile-group-instance-id", arity = "0..1",
        description = "Whether the group instance id is volatile, i.e., it will change on a Streams shutdown.")
    private boolean volatileGroupInstanceId;
    private KafkaStreams streams;
    private Throwable lastException;

    private static boolean isError(final State newState) {
        return newState == State.ERROR;
    }

    /**
     * Run the application. If Kafka Streams is run, this method blocks until Kafka Streams has completed shutdown,
     * either because it caught an error or the application has received a shutdown event.
     */
    @Override
    public void run() {
        super.run();

        try {
            final Properties kafkaProperties = this.getKafkaProperties();
            this.streams = new KafkaStreams(this.createTopology(), kafkaProperties);
            final StreamsUncaughtExceptionHandler uncaughtExceptionHandler = this.getUncaughtExceptionHandler();
            this.streams.setUncaughtExceptionHandler(
                new CapturingStreamsUncaughtExceptionHandler(uncaughtExceptionHandler));
            final StateListener stateListener = this.getStateListener();
            this.streams.setStateListener(new ClosingResourcesStateListener(stateListener));

            if (this.cleanUp) {
                this.runCleanUp();
            } else {
                this.runAndAwaitStreamsApplications();
            }
        } catch (final Throwable e) {
            this.closeResources();
            throw e;
        }
        if (isError(this.streams.state())) {
            // let PicoCLI exit with an error code
            if (this.lastException instanceof RuntimeException) {
                throw (RuntimeException) this.lastException;
            }
            throw new StreamsApplicationException("Kafka Streams has transitioned to error", this.lastException);
        }
    }

    @Override
    public void close() {
        log.info("Stopping application");
        if (this.streams != null) {
            final boolean staticMembershipDisabled = this.isStaticMembershipDisabled();
            final boolean leaveGroup = staticMembershipDisabled || this.volatileGroupInstanceId;
            this.closeStreams(leaveGroup);
        }
        // close resources after streams because messages currently processed might depend on resources
        this.closeResources();
    }

    /**
     * Build the Kafka Streams topology to be run by the app
     *
     * @param builder builder to use for building the topology
     */
    public abstract void buildTopology(StreamsBuilder builder);

    /**
     * This must be set to a unique value for every application interacting with your kafka cluster to ensure internal
     * state encapsulation. Could be set to: className-inputTopic-outputTopic
     */
    public abstract String getUniqueAppId();

    /**
     * Create the topology of the Kafka Streams app
     *
     * @return topology of the Kafka Streams app
     */
    public Topology createTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        this.buildTopology(builder);
        return builder.build();
    }

    /**
     * Get first input topic.
     *
     * @return topic name
     * @deprecated Use {@link #getInputTopics()}
     */
    @Deprecated(since = "2.1.0")
    public String getInputTopic() {
        if (this.getInputTopics().isEmpty() || this.getInputTopics().get(0).isBlank()) {
            throw new IllegalArgumentException("One input topic required");
        }
        return this.getInputTopics().get(0);
    }

    /**
     * Get extra input topic for a specified role
     *
     * @param role role of input topic specified in CLI argument
     * @return topic name
     * @deprecated Use {@link #getInputTopics(String)}
     */
    @Deprecated(since = "2.4.0")
    public String getInputTopic(final String role) {
        final List<String> topic = this.extraInputTopics.get(role);
        Preconditions.checkNotNull(topic, "No input topic for role '%s' available", role);
        Preconditions.checkArgument(!topic.isEmpty(), "No input topic for role '%s' available", role);
        return topic.get(0);
    }

    /**
     * Get extra input topics for a specified role
     *
     * @param role role of input topics specified in CLI argument
     * @return topic names
     */
    public List<String> getInputTopics(final String role) {
        final List<String> topics = this.extraInputTopics.get(role);
        Preconditions.checkNotNull(topics, "No input topics for role '%s' available", role);
        return topics;
    }

    /**
     * Get extra input pattern for a specified role
     *
     * @param role role of input pattern specified in CLI argument
     * @return topic pattern
     */
    public Pattern getInputPattern(final String role) {
        final Pattern pattern = this.extraInputPatterns.get(role);
        Preconditions.checkNotNull(pattern, "No input pattern for role '%s' available", role);
        return pattern;
    }

    /**
     * Create a {@link StreamsUncaughtExceptionHandler} to use for Kafka Streams.
     *
     * @return {@code StreamsUncaughtExceptionHandler}.
     * @see KafkaStreams#setUncaughtExceptionHandler(StreamsUncaughtExceptionHandler)
     */
    protected StreamsUncaughtExceptionHandler getUncaughtExceptionHandler() {
        return new DefaultStreamsUncaughtExceptionHandler();
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
    @Override
    protected Properties createKafkaProperties() {
        final Properties kafkaConfig = new Properties();

        // exactly once and order
        kafkaConfig.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        kafkaConfig.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION), 1);

        // resilience
        if (this.productive) {
            kafkaConfig.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, DEFAULT_PRODUCTIVE_REPLICATION_FACTOR);
        }

        kafkaConfig.setProperty(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");

        // compression
        kafkaConfig.setProperty(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "gzip");

        // topology
        kafkaConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, this.getUniqueAppId());

        this.configureDefaultSerde(kafkaConfig);
        kafkaConfig.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.getBrokers());
        return kafkaConfig;
    }

    private void configureDefaultSerde(final Properties kafkaConfig) {
        if (nonNull(this.schemaRegistryUrl)) {
            kafkaConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
            kafkaConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
            kafkaConfig.setProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.schemaRegistryUrl);
        } else {
            kafkaConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
            kafkaConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde.class);
        }
    }

    /**
     * Run the Streams application. This method blocks until Kafka Streams has completed shutdown, either because it
     * caught an error or the application has received a shutdown event.
     */
    protected void runAndAwaitStreamsApplications() {
        this.runStreamsApplication();
        this.awaitStreamsShutdown();
    }

    /**
     * Start Kafka Streams and register a ShutdownHook for closing Kafka Streams.
     */
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
     * Create a {@link StateListener} to use for Kafka Streams.
     *
     * @return {@code StateListener}.
     * @see KafkaStreams#setStateListener(StateListener)
     */
    protected StateListener getStateListener() {
        return new NoOpStateListener();
    }

    /**
     * This method resets the offset for all input topics and deletes internal topics, application state, and optionally
     * the output and error topic.
     */
    @Override
    protected void runCleanUp() {
        try (final ImprovedAdminClient adminClient = this.createAdminClient()) {
            final CleanUpRunner cleanUpRunner = CleanUpRunner.builder()
                .topology(this.createTopology())
                .appId(this.getUniqueAppId())
                .adminClient(adminClient)
                .streams(this.streams)
                .build();

            this.cleanUpRun(cleanUpRunner);
        }
        this.close();
    }

    protected void cleanUpRun(final CleanUpRunner cleanUpRunner) {
        cleanUpRunner.run(this.deleteOutputTopic);
    }

    /**
     * Wait for Kafka Streams to shut down. Shutdown is detected by a {@link StateListener}.
     *
     * @see State#hasCompletedShutdown()
     */
    protected void awaitStreamsShutdown() {
        try {
            this.streamsShutdown.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StreamsApplicationException("Error awaiting Streams shutdown", e);
        }
    }

    @VisibleForTesting
    void closeStreams(final boolean leaveGroup) {
        final CloseOptions options = new CloseOptions().leaveGroup(leaveGroup);
        log.debug("Closing Kafka Streams with leaveGroup={}", leaveGroup);
        this.streams.close(options);
    }

    private boolean isStaticMembershipDisabled() {
        final Properties kafkaProperties = this.getKafkaProperties();
        return kafkaProperties.getProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG) == null;
    }

    @RequiredArgsConstructor
    private class CapturingStreamsUncaughtExceptionHandler implements StreamsUncaughtExceptionHandler {

        private @NonNull StreamsUncaughtExceptionHandler wrapped;

        @Override
        public StreamThreadExceptionResponse handle(final Throwable exception) {
            final StreamThreadExceptionResponse response = this.wrapped.handle(exception);
            KafkaStreamsApplication.this.lastException = exception;
            return response;
        }
    }

    @RequiredArgsConstructor
    private class ClosingResourcesStateListener implements StateListener {

        private @NonNull StateListener wrapped;

        @Override
        public void onChange(final State newState, final State oldState) {
            this.wrapped.onChange(newState, oldState);
            if (isError(newState)) {
                log.debug("Closing resources because of state transition from {} to {}", oldState, newState);
                KafkaStreamsApplication.this.closeResources();
            }
            if (newState.hasCompletedShutdown()) {
                KafkaStreamsApplication.this.streamsShutdown.countDown();
            }
        }
    }
}
