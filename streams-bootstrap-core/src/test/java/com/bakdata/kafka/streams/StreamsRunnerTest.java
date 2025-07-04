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

package com.bakdata.kafka.streams;

import static java.util.concurrent.CompletableFuture.runAsync;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bakdata.kafka.KafkaTest;
import com.bakdata.kafka.KafkaTestClient;
import com.bakdata.kafka.RuntimeConfiguration;
import com.bakdata.kafka.SenderBuilder.SimpleProducerRecord;
import com.bakdata.kafka.streams.kstream.KStreamX;
import com.bakdata.kafka.streams.kstream.StreamsBuilderX;
import com.bakdata.kafka.streams.test.LabeledInputTopics;
import com.bakdata.kafka.streams.test.Mirror;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
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

    static ExecutableStreamsApp<StreamsApp> createExecutableApp(final ConfiguredStreamsApp<StreamsApp> app,
            final RuntimeConfiguration runtimeConfiguration, final Path stateDir) {
        return app.withRuntimeConfiguration(runtimeConfiguration.withStateDir(stateDir)
                .withNoStateStoreCaching()
                .withSessionTimeout(SESSION_TIMEOUT));
    }

    private static ConfiguredStreamsApp<StreamsApp> createMirrorApplication() {
        return new ConfiguredStreamsApp<>(new Mirror(), StreamsTopicConfig.builder()
                .inputTopics(List.of("input"))
                .outputTopic("output")
                .build());
    }

    private static ConfiguredStreamsApp<StreamsApp> createLabeledInputTopicsApplication() {
        return new ConfiguredStreamsApp<>(new LabeledInputTopics(), StreamsTopicConfig.builder()
                .labeledInputTopics(Map.of("label", List.of("input1", "input2")))
                .outputTopic("output")
                .build());
    }

    private static ConfiguredStreamsApp<StreamsApp> createErrorApplication() {
        return new ConfiguredStreamsApp<>(new ErrorApplication(), StreamsTopicConfig.builder()
                .inputTopics(List.of("input"))
                .outputTopic("output")
                .build());
    }

    @Test
    void shouldRunApp() {
        try (final ConfiguredStreamsApp<StreamsApp> app = createMirrorApplication();
                final StreamsRunner runner = this.createExecutableApp(app)
                        .createRunner()) {
            final String inputTopic = app.getTopics().getInputTopics().get(0);
            final String outputTopic = app.getTopics().getOutputTopic();
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(outputTopic);
            runAsync(runner);
            testClient.send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new StringSerializer())
                    .to(inputTopic, List.of(new SimpleProducerRecord<>("foo", "bar")));
            this.softly.assertThat(testClient.read()
                            .withKeyDeserializer(new StringDeserializer())
                            .withValueDeserializer(new StringDeserializer())
                            .from(outputTopic, POLL_TIMEOUT))
                    .hasSize(1);
        }
    }

    @Test
    void shouldUseMultipleLabeledInputTopics() {
        try (final ConfiguredStreamsApp<StreamsApp> app = createLabeledInputTopicsApplication();
                final StreamsRunner runner = this.createExecutableApp(app)
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
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new StringSerializer())
                    .to(inputTopic1, List.of(new SimpleProducerRecord<>("foo", "bar")));
            testClient.send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new StringSerializer())
                    .to(inputTopic2, List.of(new SimpleProducerRecord<>("foo", "baz")));
            this.softly.assertThat(testClient.read()
                            .withKeyDeserializer(new StringDeserializer())
                            .withValueDeserializer(new StringDeserializer())
                            .from(outputTopic, POLL_TIMEOUT))
                    .hasSize(2);
        }
    }

    @Test
    void shouldThrowOnMissingInputTopic() {
        when(this.uncaughtExceptionHandler.handle(any())).thenReturn(StreamThreadExceptionResponse.SHUTDOWN_CLIENT);
        try (final ConfiguredStreamsApp<StreamsApp> app = createMirrorApplication();
                final StreamsRunner runner = this.createExecutableApp(app)
                        .createRunner(StreamsExecutionOptions.builder()
                                .stateListener(() -> this.stateListener)
                                .uncaughtExceptionHandler(() -> this.uncaughtExceptionHandler)
                                .build())) {
            final CompletableFuture<Void> future = runAsync(runner);
            this.softly.assertThatThrownBy(() -> future.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                    .cause()
                    .isInstanceOf(MissingSourceTopicException.class);
            verify(this.uncaughtExceptionHandler).handle(any());
            verify(this.stateListener).onChange(State.ERROR, State.PENDING_ERROR);
        }
    }

    @Test
    void shouldCloseOnMapError() {
        when(this.uncaughtExceptionHandler.handle(any())).thenReturn(StreamThreadExceptionResponse.SHUTDOWN_CLIENT);
        try (final ConfiguredStreamsApp<StreamsApp> app = createErrorApplication();
                final StreamsRunner runner = this.createExecutableApp(app)
                        .createRunner(StreamsExecutionOptions.builder()
                                .stateListener(() -> this.stateListener)
                                .uncaughtExceptionHandler(() -> this.uncaughtExceptionHandler)
                                .build())) {
            final String inputTopic = app.getTopics().getInputTopics().get(0);
            final String outputTopic = app.getTopics().getOutputTopic();
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(outputTopic);
            final CompletableFuture<Void> future = runAsync(runner);
            testClient.send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new StringSerializer())
                    .to(inputTopic, List.of(new SimpleProducerRecord<>("foo", "bar")));
            this.softly.assertThatThrownBy(() -> future.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                    .cause()
                    .isInstanceOf(StreamsException.class)
                    .satisfies(e -> this.softly.assertThat(e.getCause()).hasMessage("Error in map"));
            verify(this.uncaughtExceptionHandler).handle(any());
            verify(this.stateListener).onChange(State.ERROR, State.PENDING_ERROR);
        }
    }

    private ExecutableStreamsApp<StreamsApp> createExecutableApp(final ConfiguredStreamsApp<StreamsApp> app) {
        return createExecutableApp(app, this.createConfigWithoutSchemaRegistry(), this.stateDir);
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
