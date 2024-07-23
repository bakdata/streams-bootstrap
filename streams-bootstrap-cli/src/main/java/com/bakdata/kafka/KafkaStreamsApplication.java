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

package com.bakdata.kafka;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.UseDefaultConverter;


/**
 * <p>The base class for creating Kafka Streams applications.</p>
 * This class provides the following configuration options in addition to those provided by {@link KafkaApplication}:
 * <ul>
 *     <li>{@link #inputTopics}</li>
 *     <li>{@link #inputPattern}</li>
 *     <li>{@link #errorTopic}</li>
 *     <li>{@link #extraInputTopics}</li>
 *     <li>{@link #extraInputPatterns}</li>
 *     <li>{@link #volatileGroupInstanceId}</li>
 * </ul>
 * To implement your Kafka Streams application inherit from this class and add your custom options. Run it by calling
 * {@link #startApplication(KafkaApplication, String[])} with a instance of your class from your main.
 */
@ToString(callSuper = true)
@Getter
@Setter
@RequiredArgsConstructor
@Slf4j
@Command(description = "Run a Kafka Streams application.")
public abstract class KafkaStreamsApplication extends
        KafkaApplication<StreamsRunner, StreamsCleanUpRunner, StreamsExecutionOptions,
                ExecutableStreamsApp<StreamsApp>, ConfiguredStreamsApp<StreamsApp>, StreamsTopicConfig, StreamsApp> {
    @CommandLine.Option(names = "--input-topics", description = "Input topics", split = ",")
    private List<String> inputTopics = emptyList();
    @CommandLine.Option(names = "--input-pattern", description = "Input pattern")
    private Pattern inputPattern;
    @CommandLine.Option(names = "--error-topic", description = "Error topic")
    private String errorTopic;
    @CommandLine.Option(names = "--extra-input-topics", split = ",", description = "Additional named input topics",
            converter = {UseDefaultConverter.class, StringListConverter.class})
    private Map<String, List<String>> extraInputTopics = emptyMap();
    @CommandLine.Option(names = "--extra-input-patterns", split = ",", description = "Additional named input patterns")
    private Map<String, Pattern> extraInputPatterns = emptyMap();
    @CommandLine.Option(names = "--volatile-group-instance-id", arity = "0..1",
            description = "Whether the group instance id is volatile, i.e., it will change on a Streams shutdown.")
    private boolean volatileGroupInstanceId;
    @CommandLine.Option(names = "--application-id",
            description = "Unique application ID to use for Kafka Streams. Can also be provided by implementing "
                          + "StreamsApp#getUniqueAppId()")
    private String uniqueAppId;

    /**
     * Reset the Kafka Streams application. Additionally, delete the consumer group and all output and intermediate
     * topics associated with the Kafka Streams application.
     */
    @Command(description = "Reset the Kafka Streams application. Additionally, delete the consumer group and all "
                           + "output and intermediate topics associated with the Kafka Streams application.")
    @Override
    public void clean() {
        super.clean();
    }

    /**
     * Clear all state stores, consumer group offsets, and internal topics associated with the Kafka Streams
     * application.
     */
    @Command(description = "Clear all state stores, consumer group offsets, and internal topics associated with the "
                           + "Kafka Streams application.")
    public void reset() {
        try (final CleanableApp<StreamsCleanUpRunner> app = this.createCleanableApp()) {
            final StreamsCleanUpRunner runner = app.getCleanUpRunner();
            runner.reset();
        }
    }

    @Override
    public final Optional<StreamsExecutionOptions> createExecutionOptions() {
        final StreamsExecutionOptions options = StreamsExecutionOptions.builder()
                .volatileGroupInstanceId(this.volatileGroupInstanceId)
                .uncaughtExceptionHandler(this::createUncaughtExceptionHandler)
                .stateListener(this::createStateListener)
                .onStart(this::onStreamsStart)
                .build();
        return Optional.of(options);
    }

    @Override
    public final StreamsTopicConfig createTopicConfig() {
        return StreamsTopicConfig.builder()
                .inputTopics(this.inputTopics)
                .extraInputTopics(this.extraInputTopics)
                .inputPattern(this.inputPattern)
                .extraInputPatterns(this.extraInputPatterns)
                .outputTopic(this.getOutputTopic())
                .extraOutputTopics(this.getExtraOutputTopics())
                .errorTopic(this.errorTopic)
                .build();
    }

    @Override
    public final ConfiguredStreamsApp<StreamsApp> createConfiguredApp(final StreamsApp app,
            final AppConfiguration<StreamsTopicConfig> configuration) {
        final ConfiguredStreamsApp<StreamsApp> configuredApp = new ConfiguredStreamsApp<>(app, configuration);
        if (this.uniqueAppId != null && !configuredApp.getUniqueAppId().equals(this.uniqueAppId)) {
            throw new IllegalArgumentException(
                    "Application ID provided via --application-id does not match StreamsApp#getUniqueAppId()");
        }
        return configuredApp;
    }

    /**
     * Create a {@link StateListener} to use for Kafka Streams.
     *
     * @return {@code StateListener}. {@link NoOpStateListener} by default
     * @see KafkaStreams#setStateListener(StateListener)
     */
    protected StateListener createStateListener() {
        return new NoOpStateListener();
    }

    /**
     * Create a {@link StreamsUncaughtExceptionHandler} to use for Kafka Streams.
     *
     * @return {@code StreamsUncaughtExceptionHandler}. {@link DefaultStreamsUncaughtExceptionHandler} by default
     * @see KafkaStreams#setUncaughtExceptionHandler(StreamsUncaughtExceptionHandler)
     */
    protected StreamsUncaughtExceptionHandler createUncaughtExceptionHandler() {
        return new DefaultStreamsUncaughtExceptionHandler();
    }

    /**
     * Called after starting Kafka Streams
     * @param runningStreams running {@link KafkaStreams} instance along with its {@link StreamsConfig} and
     * {@link org.apache.kafka.streams.Topology}
     */
    protected void onStreamsStart(final RunningStreams runningStreams) {
        // do nothing by default
    }
}
