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

import com.bakdata.kafka.KafkaApplication;
import com.bakdata.kafka.mixin.ConsumerOptions;
import com.bakdata.kafka.mixin.ErrorOptions;
import com.bakdata.kafka.mixin.InputOptions;
import com.bakdata.kafka.mixin.OutputOptions;
import java.util.Optional;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

/**
 * <p>The base class for creating Kafka Streams applications.</p>
 * This class provides the following configuration options in addition to those provided by {@link KafkaApplication}:
 * <ul>
 *     <li>{@link #getInputTopics()}</li>
 *     <li>{@link #getInputPattern()}</li>
 *     <li>{@link #getLabeledInputTopics()}</li>
 *     <li>{@link #getLabeledInputPatterns()}</li>
 *     <li>{@link #getOutputTopic()}</li>
 *     <li>{@link #getLabeledOutputTopics()}</li>
 *     <li>{@link #getErrorTopic()}</li>
 *     <li>{@link #isVolatileGroupInstanceId()}</li>
 *     <li>{@link #getApplicationId()}</li>
 * </ul>
 * To implement your Kafka Streams application inherit from this class and add your custom options.  Run it by
 * creating an instance of your class and calling {@link #startApplication(String[])} from your main.
 *
 * @param <T> type of {@link StreamsApp} created by this application
 */
@ToString(callSuper = true)
@Getter
@Setter
@RequiredArgsConstructor
@Slf4j
@Command(description = "Run a Kafka Streams application.")
public abstract class KafkaStreamsApplication<T extends StreamsApp> extends
        KafkaApplication<StreamsRunner, StreamsCleanUpRunner, StreamsExecutionOptions,
                ExecutableStreamsApp<T>, ConfiguredStreamsApp<T>, StreamsTopicConfig, T, StreamsAppConfiguration> {
    @Mixin
    @Delegate
    private InputOptions inputOptions = new InputOptions();
    @Mixin
    @Delegate
    private OutputOptions outputOptions = new OutputOptions();
    @Mixin
    @Delegate
    private ErrorOptions errorOptions = new ErrorOptions();
    @Mixin
    @Delegate
    private ConsumerOptions consumerOptions = new ConsumerOptions();

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
        this.prepareClean();
        try (final CleanableApp<StreamsCleanUpRunner> app = this.createCleanableApp()) {
            final StreamsCleanUpRunner runner = app.getCleanUpRunner();
            runner.reset();
        }
    }

    @Override
    public final Optional<StreamsExecutionOptions> createExecutionOptions() {
        final StreamsExecutionOptions options = StreamsExecutionOptions.builder()
                .volatileGroupInstanceId(this.isVolatileGroupInstanceId())
                .uncaughtExceptionHandler(this::createUncaughtExceptionHandler)
                .stateListener(this::createStateListener)
                .onStart(this::onStreamsStart)
                .build();
        return Optional.of(options);
    }

    @Override
    public final StreamsTopicConfig createTopicConfig() {
        return StreamsTopicConfig.builder()
                .inputTopics(this.getInputTopics())
                .labeledInputTopics(this.getLabeledInputTopics())
                .inputPattern(this.getInputPattern())
                .labeledInputPatterns(this.getLabeledInputPatterns())
                .outputTopic(this.getOutputTopic())
                .labeledOutputTopics(this.getLabeledOutputTopics())
                .errorTopic(this.getErrorTopic())
                .build();
    }

    @Override
    public final ConfiguredStreamsApp<T> createConfiguredApp(final T app, final StreamsAppConfiguration configuration) {
        return new ConfiguredStreamsApp<>(app, configuration);
    }

    @Override
    public StreamsAppConfiguration createConfiguration(final StreamsTopicConfig topics) {
        return new StreamsAppConfiguration(topics, this.getApplicationId());
    }

    /**
     * Called before cleaning the application, i.e., invoking {@link #clean()} or {@link #reset()}
     */
    @Override
    public void prepareClean() {
        super.prepareClean();
    }

    /**
     * Create a {@link StateListener} to use for Kafka Streams.
     *
     * @return {@link StateListener}. {@link NoOpStateListener} by default
     * @see KafkaStreams#setStateListener(StateListener)
     */
    protected StateListener createStateListener() {
        return new NoOpStateListener();
    }

    /**
     * Create a {@link StreamsUncaughtExceptionHandler} to use for Kafka Streams.
     *
     * @return {@link StreamsUncaughtExceptionHandler}. {@link DefaultStreamsUncaughtExceptionHandler} by default
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
