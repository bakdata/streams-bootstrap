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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.StateListener;
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
public abstract class KafkaStreamsApplication
        extends KafkaApplication<StreamsRunner, StreamsCleanUpRunner, StreamsExecutionOptions> {
    @CommandLine.Option(names = "--input-topics", description = "Input topics", split = ",")
    private List<String> inputTopics = new ArrayList<>();
    @CommandLine.Option(names = "--input-pattern", description = "Input pattern")
    private Pattern inputPattern;
    @CommandLine.Option(names = "--error-topic", description = "Error topic")
    private String errorTopic;
    @CommandLine.Option(names = "--extra-input-topics", split = ",", description = "Additional named input topics",
            converter = {UseDefaultConverter.class, StringListConverter.class})
    private Map<String, List<String>> extraInputTopics = new HashMap<>();
    @CommandLine.Option(names = "--extra-input-patterns", split = ",", description = "Additional named input patterns")
    private Map<String, Pattern> extraInputPatterns = new HashMap<>();
    @CommandLine.Option(names = "--volatile-group-instance-id", arity = "0..1",
            description = "Whether the group instance id is volatile, i.e., it will change on a Streams shutdown.")
    private boolean volatileGroupInstanceId;

    /**
     * @see StreamsRunner#run()
     */
    @Override
    public void run() {
        super.run();
    }

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
        try (final ExecutableApp<?, StreamsCleanUpRunner, ?> app = this.createExecutableApp(true)) {
            final StreamsCleanUpRunner runner = app.createCleanUpRunner();
            runner.reset();
        }
    }

    /**
     * Create a new {@code StreamsApp} that will be configured and executed according to this application.
     * @param cleanUp whether {@code StreamsApp} is created for clean up purposes. In that case, the user might want
     * to skip initialization of expensive resources.
     * @return {@code StreamsApp}
     */
    protected abstract StreamsApp createApp(boolean cleanUp);

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
     * @return {@code StreamsUncaughtExceptionHandler}. {@link DefaultUncaughtExceptionHandler} by default
     * @see KafkaStreams#setUncaughtExceptionHandler(StreamsUncaughtExceptionHandler)
     */
    protected StreamsUncaughtExceptionHandler createUncaughtExceptionHandler() {
        return new DefaultUncaughtExceptionHandler();
    }

    /**
     * Called after starting Kafka Streams
     * @param streams running {@code KafkaStreams} instance
     */
    protected void onStreamsStart(final KafkaStreams streams) {
        // do nothing by default
    }

    @Override
    final StreamsExecutionOptions createExecutionOptions() {
        return StreamsExecutionOptions.builder()
                .volatileGroupInstanceId(this.volatileGroupInstanceId)
                .uncaughtExceptionHandler(this::createUncaughtExceptionHandler)
                .stateListener(this::createStateListener)
                .onStart(this::onStreamsStart)
                .build();
    }

    @Override
    final ConfiguredStreamsApp<StreamsApp> createConfiguredApp(final boolean cleanUp) {
        final StreamsApp streamsApp = this.createApp(cleanUp);
        final StreamsAppConfiguration configuration = this.createConfiguration();
        return new ConfiguredStreamsApp<>(streamsApp, configuration);
    }

    private StreamsAppConfiguration createConfiguration() {
        final StreamsTopicConfig topics = this.createTopicConfig();
        final Map<String, String> kafkaConfig = this.getKafkaConfig();
        return StreamsAppConfiguration.builder()
                .topics(topics)
                .kafkaConfig(kafkaConfig)
                .build();
    }

    private StreamsTopicConfig createTopicConfig() {
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
}
