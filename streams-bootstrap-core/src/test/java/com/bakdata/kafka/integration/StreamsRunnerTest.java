/*
 * MIT License
 *
 * Copyright (c) 2024 bakdata
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

package com.bakdata.kafka.integration;

import static net.mguenther.kafka.junit.Wait.delay;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bakdata.kafka.AppConfiguration;
import com.bakdata.kafka.ConfiguredStreamsApp;
import com.bakdata.kafka.SerdeConfig;
import com.bakdata.kafka.StreamsApp;
import com.bakdata.kafka.StreamsExecutionOptions;
import com.bakdata.kafka.StreamsRunner;
import com.bakdata.kafka.StreamsTopicConfig;
import com.bakdata.kafka.TopologyBuilder;
import com.bakdata.kafka.test_applications.Mirror;
import com.bakdata.kafka.test_applications.NamedInputTopics;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ReadKeyValues;
import net.mguenther.kafka.junit.SendKeyValuesTransactional;
import net.mguenther.kafka.junit.TopicConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.MissingSourceTopicException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.kstream.KStream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(SoftAssertionsExtension.class)
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class StreamsRunnerTest extends KafkaTest {
    private static final int TIMEOUT_SECONDS = 10;
    @Mock
    private StreamsUncaughtExceptionHandler uncaughtExceptionHandler;
    @Mock
    private StateListener stateListener;
    @InjectSoftAssertions
    private SoftAssertions softly;

    static Thread run(final StreamsRunner runner) {
        // run in Thread because the application blocks indefinitely
        final Thread thread = new Thread(runner);
        final UncaughtExceptionHandler handler = new CapturingUncaughtExceptionHandler();
        thread.setUncaughtExceptionHandler(handler);
        thread.start();
        return thread;
    }

    static ConfiguredStreamsApp<StreamsApp> configureApp(final StreamsApp app, final StreamsTopicConfig topics) {
        final AppConfiguration<StreamsTopicConfig> configuration = new AppConfiguration<>(topics, Map.of(
                StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0",
                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000"
        ));
        return new ConfiguredStreamsApp<>(app, configuration);
    }

    private static ConfiguredStreamsApp<StreamsApp> createMirrorApplication() {
        return configureApp(new Mirror(), StreamsTopicConfig.builder()
                .inputTopics(List.of("input"))
                .outputTopic("output")
                .build());
    }

    private static ConfiguredStreamsApp<StreamsApp> createNamedInputTopicsApplication() {
        return configureApp(new NamedInputTopics(), StreamsTopicConfig.builder()
                .namedInputTopics(Map.of("role", List.of("input1", "input2")))
                .outputTopic("output")
                .build());
    }

    private static ConfiguredStreamsApp<StreamsApp> createErrorApplication() {
        return configureApp(new ErrorApplication(), StreamsTopicConfig.builder()
                .inputTopics(List.of("input"))
                .outputTopic("output")
                .build());
    }

    @Test
    void shouldRunApp() throws InterruptedException {
        try (final ConfiguredStreamsApp<StreamsApp> app = createMirrorApplication();
                final StreamsRunner runner = app.withEndpoint(this.createEndpointWithoutSchemaRegistry())
                        .createRunner()) {
            final String inputTopic = app.getTopics().getInputTopics().get(0);
            this.kafkaCluster.createTopic(TopicConfig.withName(inputTopic).useDefaults());
            final String outputTopic = app.getTopics().getOutputTopic();
            this.kafkaCluster.createTopic(TopicConfig.withName(outputTopic).useDefaults());
            run(runner);
            final SendKeyValuesTransactional<String, String> kvSendKeyValuesTransactionalBuilder =
                    SendKeyValuesTransactional.inTransaction(inputTopic, List.of(new KeyValue<>("foo", "bar")))
                            .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                            .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                            .build();
            this.kafkaCluster.send(kvSendKeyValuesTransactionalBuilder);
            delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            this.softly.assertThat(this.kafkaCluster.read(ReadKeyValues.from(outputTopic, String.class, String.class)
                            .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                            .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                            .build()))
                    .hasSize(1);
        }
    }

    @Test
    void shouldUseMultipleNamedInputTopics() throws InterruptedException {
        try (final ConfiguredStreamsApp<StreamsApp> app = createNamedInputTopicsApplication();
                final StreamsRunner runner = app.withEndpoint(this.createEndpointWithoutSchemaRegistry())
                        .createRunner()) {
            final List<String> inputTopics = app.getTopics().getNamedInputTopics().get("role");
            final String inputTopic1 = inputTopics.get(0);
            final String inputTopic2 = inputTopics.get(1);
            final String outputTopic = app.getTopics().getOutputTopic();
            this.kafkaCluster.createTopic(TopicConfig.withName(inputTopic1).useDefaults());
            this.kafkaCluster.createTopic(TopicConfig.withName(inputTopic2).useDefaults());
            this.kafkaCluster.createTopic(TopicConfig.withName(outputTopic).useDefaults());
            run(runner);
            this.kafkaCluster.send(
                    SendKeyValuesTransactional.inTransaction(inputTopic1, List.of(new KeyValue<>("foo", "bar")))
                            .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                            .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                            .build());
            this.kafkaCluster.send(
                    SendKeyValuesTransactional.inTransaction(inputTopic2, List.of(new KeyValue<>("foo", "baz")))
                            .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                            .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                            .build());
            delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            this.softly.assertThat(this.kafkaCluster.read(ReadKeyValues.from(outputTopic, String.class, String.class)
                            .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                            .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                            .build()))
                    .hasSize(2);
        }
    }

    @Test
    void shouldThrowOnMissingInputTopic() throws InterruptedException {
        when(this.uncaughtExceptionHandler.handle(any())).thenReturn(StreamThreadExceptionResponse.SHUTDOWN_CLIENT);
        try (final ConfiguredStreamsApp<StreamsApp> app = createMirrorApplication();
                final StreamsRunner runner = app.withEndpoint(this.createEndpointWithoutSchemaRegistry())
                        .createRunner(StreamsExecutionOptions.builder()
                                .stateListener(() -> this.stateListener)
                                .uncaughtExceptionHandler(() -> this.uncaughtExceptionHandler)
                                .build())) {
            final Thread thread = run(runner);
            final CapturingUncaughtExceptionHandler handler =
                    (CapturingUncaughtExceptionHandler) thread.getUncaughtExceptionHandler();
            delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            this.softly.assertThat(thread.isAlive()).isFalse();
            this.softly.assertThat(handler.getLastException()).isInstanceOf(MissingSourceTopicException.class);
            verify(this.uncaughtExceptionHandler).handle(any());
            verify(this.stateListener).onChange(State.ERROR, State.PENDING_ERROR);
        }
    }

    @Test
    void shouldCloseOnMapError() throws InterruptedException {
        when(this.uncaughtExceptionHandler.handle(any())).thenReturn(StreamThreadExceptionResponse.SHUTDOWN_CLIENT);
        try (final ConfiguredStreamsApp<StreamsApp> app = createErrorApplication();
                final StreamsRunner runner = app.withEndpoint(this.createEndpointWithoutSchemaRegistry())
                        .createRunner(StreamsExecutionOptions.builder()
                                .stateListener(() -> this.stateListener)
                                .uncaughtExceptionHandler(() -> this.uncaughtExceptionHandler)
                                .build())) {
            final String inputTopic = app.getTopics().getInputTopics().get(0);
            this.kafkaCluster.createTopic(TopicConfig.withName(inputTopic).useDefaults());
            final String outputTopic = app.getTopics().getOutputTopic();
            this.kafkaCluster.createTopic(TopicConfig.withName(outputTopic).useDefaults());
            final Thread thread = run(runner);
            final CapturingUncaughtExceptionHandler handler =
                    (CapturingUncaughtExceptionHandler) thread.getUncaughtExceptionHandler();
            final SendKeyValuesTransactional<String, String> kvSendKeyValuesTransactionalBuilder =
                    SendKeyValuesTransactional.inTransaction(inputTopic, List.of(new KeyValue<>("foo", "bar")))
                            .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                            .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                            .build();
            this.kafkaCluster.send(kvSendKeyValuesTransactionalBuilder);
            delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            this.softly.assertThat(thread.isAlive()).isFalse();
            this.softly.assertThat(handler.getLastException()).isInstanceOf(StreamsException.class)
                    .satisfies(e -> this.softly.assertThat(e.getCause()).hasMessage("Error in map"));
            verify(this.uncaughtExceptionHandler).handle(any());
            verify(this.stateListener).onChange(State.ERROR, State.PENDING_ERROR);
        }
    }

    @Getter
    private static class CapturingUncaughtExceptionHandler implements UncaughtExceptionHandler {
        private Throwable lastException;

        @Override
        public void uncaughtException(final Thread t, final Throwable e) {
            this.lastException = e;
        }
    }

    private static class ErrorApplication implements StreamsApp {

        @Override
        public void buildTopology(final TopologyBuilder builder) {
            final KStream<String, String> input = builder.streamInput();
            input.map((k, v) -> {throw new RuntimeException("Error in map");})
                    .to(builder.getTopics().getOutputTopic());
        }

        @Override
        public String getUniqueAppId(final StreamsTopicConfig topics) {
            return this.getClass().getSimpleName() + "-" + topics.getOutputTopic();
        }

        @Override
        public SerdeConfig defaultSerializationConfig() {
            return new SerdeConfig(StringSerde.class, StringSerde.class);
        }
    }
}
