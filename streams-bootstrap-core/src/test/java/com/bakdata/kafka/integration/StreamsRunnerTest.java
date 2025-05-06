/*
 * MIT License
 *
 * Copyright (c) 2025 bakdata
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

import static com.bakdata.kafka.AsyncRunnable.runAsync;
import static com.bakdata.kafka.TestTopologyFactory.createStreamsTestConfig;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bakdata.kafka.AppConfiguration;
import com.bakdata.kafka.AsyncRunnable;
import com.bakdata.kafka.ConfiguredStreamsApp;
import com.bakdata.kafka.KStreamX;
import com.bakdata.kafka.KafkaTest;
import com.bakdata.kafka.KafkaTestClient;
import com.bakdata.kafka.SenderBuilder.SimpleProducerRecord;
import com.bakdata.kafka.SerdeConfig;
import com.bakdata.kafka.StreamsApp;
import com.bakdata.kafka.StreamsBuilderX;
import com.bakdata.kafka.StreamsExecutionOptions;
import com.bakdata.kafka.StreamsRunner;
import com.bakdata.kafka.StreamsTopicConfig;
import com.bakdata.kafka.TestHelper.CapturingUncaughtExceptionHandler;
import com.bakdata.kafka.test_applications.LabeledInputTopics;
import com.bakdata.kafka.test_applications.Mirror;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.errors.MissingSourceTopicException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(SoftAssertionsExtension.class)
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class StreamsRunnerTest extends KafkaTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    @Mock
    private StreamsUncaughtExceptionHandler uncaughtExceptionHandler;
    @Mock
    private StateListener stateListener;
    @InjectSoftAssertions
    private SoftAssertions softly;
    @TempDir
    private Path stateDir;

    static ConfiguredStreamsApp<StreamsApp> configureApp(final StreamsApp app, final StreamsTopicConfig topics,
            final Path stateDir) {
        final AppConfiguration<StreamsTopicConfig> configuration =
                new AppConfiguration<>(topics, createStreamsTestConfig(stateDir));
        return new ConfiguredStreamsApp<>(app, configuration);
    }

    ConfiguredStreamsApp<StreamsApp> configureApp(final StreamsApp app, final StreamsTopicConfig topics) {
        return configureApp(app, topics, this.stateDir);
    }

    @Test
    void shouldRunApp() {
        try (final ConfiguredStreamsApp<StreamsApp> app = this.createMirrorApplication();
                final StreamsRunner runner = app.withEndpoint(this.createEndpointWithoutSchemaRegistry())
                        .createRunner()) {
            final String inputTopic = app.getTopics().getInputTopics().get(0);
            final String outputTopic = app.getTopics().getOutputTopic();
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(outputTopic);
            runAsync(runner);
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .to(inputTopic, List.of(new SimpleProducerRecord<>("foo", "bar")));
            this.softly.assertThat(testClient.read()
                            .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                            .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                            .from(outputTopic, POLL_TIMEOUT))
                    .hasSize(1);
        }
    }

    @Test
    void shouldUseMultipleLabeledInputTopics() {
        try (final ConfiguredStreamsApp<StreamsApp> app = this.createLabeledInputTopicsApplication();
                final StreamsRunner runner = app.withEndpoint(this.createEndpointWithoutSchemaRegistry())
                        .createRunner()) {
            final List<String> inputTopics = app.getTopics().getLabeledInputTopics().get("label");
            final String inputTopic1 = inputTopics.get(0);
            final String inputTopic2 = inputTopics.get(1);
            final String outputTopic = app.getTopics().getOutputTopic();
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(inputTopic1);
            testClient.createTopic(inputTopic2);
            testClient.createTopic(outputTopic);
            runAsync(runner);
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .to(inputTopic1, List.of(new SimpleProducerRecord<>("foo", "bar")));
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .to(inputTopic2, List.of(new SimpleProducerRecord<>("foo", "baz")));
            this.softly.assertThat(testClient.read()
                            .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                            .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                            .from(outputTopic, POLL_TIMEOUT))
                    .hasSize(2);
        }
    }

    @Test
    void shouldThrowOnMissingInputTopic() {
        when(this.uncaughtExceptionHandler.handle(any())).thenReturn(StreamThreadExceptionResponse.SHUTDOWN_CLIENT);
        try (final ConfiguredStreamsApp<StreamsApp> app = this.createMirrorApplication();
                final StreamsRunner runner = app.withEndpoint(this.createEndpointWithoutSchemaRegistry())
                        .createRunner(StreamsExecutionOptions.builder()
                                .stateListener(() -> this.stateListener)
                                .uncaughtExceptionHandler(() -> this.uncaughtExceptionHandler)
                                .build())) {
            final AsyncRunnable runnable = runAsync(runner);
            this.softly.assertThatThrownBy(() -> runnable.await(TIMEOUT))
                    .isInstanceOf(MissingSourceTopicException.class);
            verify(this.uncaughtExceptionHandler).handle(any());
            verify(this.stateListener).onChange(State.ERROR, State.PENDING_ERROR);
        }
    }

    @Test
    void shouldCloseOnMapError() {
        when(this.uncaughtExceptionHandler.handle(any())).thenReturn(StreamThreadExceptionResponse.SHUTDOWN_CLIENT);
        try (final ConfiguredStreamsApp<StreamsApp> app = this.createErrorApplication();
                final StreamsRunner runner = app.withEndpoint(this.createEndpointWithoutSchemaRegistry())
                        .createRunner(StreamsExecutionOptions.builder()
                                .stateListener(() -> this.stateListener)
                                .uncaughtExceptionHandler(() -> this.uncaughtExceptionHandler)
                                .build())) {
            final String inputTopic = app.getTopics().getInputTopics().get(0);
            final String outputTopic = app.getTopics().getOutputTopic();
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(outputTopic);
            final AsyncRunnable runnable = runAsync(runner);
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .to(inputTopic, List.of(new SimpleProducerRecord<>("foo", "bar")));
            this.softly.assertThatThrownBy(() -> runnable.await(TIMEOUT)).isInstanceOf(StreamsException.class)
                    .satisfies(e -> this.softly.assertThat(e.getCause()).hasMessage("Error in map"));
            verify(this.uncaughtExceptionHandler).handle(any());
            verify(this.stateListener).onChange(State.ERROR, State.PENDING_ERROR);
        }
    }

    private ConfiguredStreamsApp<StreamsApp> createMirrorApplication() {
        return this.configureApp(new Mirror(), StreamsTopicConfig.builder()
                .inputTopics(List.of("input"))
                .outputTopic("output")
                .build());
    }

    private ConfiguredStreamsApp<StreamsApp> createLabeledInputTopicsApplication() {
        return this.configureApp(new LabeledInputTopics(), StreamsTopicConfig.builder()
                .labeledInputTopics(Map.of("label", List.of("input1", "input2")))
                .outputTopic("output")
                .build());
    }

    private ConfiguredStreamsApp<StreamsApp> createErrorApplication() {
        return this.configureApp(new ErrorApplication(), StreamsTopicConfig.builder()
                .inputTopics(List.of("input"))
                .outputTopic("output")
                .build());
    }

    private static class ErrorApplication implements StreamsApp {

        @Override
        public void buildTopology(final StreamsBuilderX builder) {
            final KStreamX<String, String> input = builder.streamInput();
            input.map((k, v) -> {throw new RuntimeException("Error in map");})
                    .toOutputTopic();
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
