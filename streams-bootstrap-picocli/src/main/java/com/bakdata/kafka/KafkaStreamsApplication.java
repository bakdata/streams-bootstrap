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
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.NonNull;
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
@Command(description = "Run a Kafka Streams application.")
public abstract class KafkaStreamsApplication extends KafkaApplication {
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
    @CommandLine.Option(names = "--productive", arity = "0..1",
            description = "Whether to use Kafka Streams configuration values, such as replication.factor=3, that are "
                          + "more suitable for production environments")
    private boolean productive;
    @CommandLine.Option(names = "--volatile-group-instance-id", arity = "0..1",
            description = "Whether the group instance id is volatile, i.e., it will change on a Streams shutdown.")
    private boolean volatileGroupInstanceId;
    @ToString.Exclude
    // ConcurrentLinkedDeque required because calling #close() causes asynchronous #run() calls to finish and thus
    // concurrently iterating on #runners and removing from #runners
    private ConcurrentLinkedDeque<RunningApp> runningApps = new ConcurrentLinkedDeque<>();

    /**
     * Run the application. If Kafka Streams is run, this method blocks until Kafka Streams has completed shutdown,
     * either because it caught an error or the application has received a shutdown event.
     */
    @Override
    public void run() {
        try (final RunningApp runningApp = this.createRunningApp()) {
            this.runningApps.add(runningApp);
            runningApp.run();
            this.runningApps.remove(runningApp);
        }
    }

    public void stop() {
        this.runningApps.forEach(RunningApp::close);
    }

    @Command(
            description = "Reset the Kafka Streams application. Additionally, delete the consumer group and all "
                          + "output and intermediate topics associated with the Kafka Streams application.")
    @Override
    public void clean() {
        try (final ExecutableStreamsApp<StreamsApp> app = this.createExecutableApp(true)) {
            final StreamsCleanUpRunner runner = app.createCleanUpRunner();
            runner.clean();
        }
    }

    @Override
    public void close() {
        super.close();
        this.stop();
    }

    @Command(
            description = "Clear all state stores, consumer group offsets, and internal topics associated with the "
                          + "Kafka Streams application.")
    public void reset() {
        try (final ExecutableStreamsApp<StreamsApp> app = this.createExecutableApp(true)) {
            final StreamsCleanUpRunner runner = app.createCleanUpRunner();
            runner.reset();
        }
    }

    public StreamsRunner createRunner(final ExecutableStreamsApp<StreamsApp> app) {
        final StreamsExecutionOptions executionOptions = this.createExecutionOptions();
        final StreamsHooks hooks = this.createHooks();
        return app.createRunner(executionOptions, hooks);
    }

    private RunningApp createRunningApp() {
        final ExecutableStreamsApp<StreamsApp> app = this.createExecutableApp(false);
        final StreamsRunner runner = this.createRunner(app);
        return new RunningApp(app, runner);
    }

    public abstract StreamsApp createApp(boolean cleanUp);

    public StreamsExecutionOptions createExecutionOptions() {
        return StreamsExecutionOptions.builder()
                .volatileGroupInstanceId(this.volatileGroupInstanceId)
                .build();
    }

    @RequiredArgsConstructor
    private static class RunningApp implements AutoCloseable {
        private final @NonNull ExecutableStreamsApp<StreamsApp> app;
        private final @NonNull StreamsRunner runner;

        @Override
        public void close() {
            this.runner.close();
            // close app after streams because messages currently processed might depend on resources
            this.app.close();
        }

        private void run() {
            this.runner.run();
        }
    }

    public ConfiguredStreamsApp<StreamsApp> createConfiguredApp(final boolean cleanUp) {
        final StreamsApp streamsApp = this.createApp(cleanUp);
        final StreamsAppConfiguration streamsAppConfiguration = this.createConfiguration();
        return new ConfiguredStreamsApp<>(streamsApp, streamsAppConfiguration);
    }

    public StreamsAppConfiguration createConfiguration() {
        final StreamsTopicConfig topics = this.createTopicConfig();
        final Map<String, String> kafkaConfig = this.getKafkaConfig();
        final StreamsOptions streamsOptions = this.createStreamsOptions();
        return StreamsAppConfiguration.builder()
                .topics(topics)
                .kafkaConfig(kafkaConfig)
                .options(streamsOptions)
                .build();
    }

    public StreamsTopicConfig createTopicConfig() {
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

    public ExecutableStreamsApp<StreamsApp> createExecutableApp(final boolean cleanUp) {
        final ConfiguredStreamsApp<StreamsApp> configuredStreamsApp = this.createConfiguredApp(cleanUp);
        final KafkaEndpointConfig endpointConfig = this.getEndpointConfig();
        return configuredStreamsApp.withEndpoint(endpointConfig);
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
     * Create a {@link StreamsUncaughtExceptionHandler} to use for Kafka Streams.
     *
     * @return {@code StreamsUncaughtExceptionHandler}.
     * @see KafkaStreams#setUncaughtExceptionHandler(StreamsUncaughtExceptionHandler)
     */
    protected StreamsUncaughtExceptionHandler getUncaughtExceptionHandler() {
        return new DefaultStreamsUncaughtExceptionHandler();
    }

    protected void onStreamsStart(final KafkaStreams streams) {
        // do nothing by default
    }

    private StreamsHooks createHooks() {
        return StreamsHooks.builder()
                .uncaughtExceptionHandler(this.getUncaughtExceptionHandler())
                .stateListener(this.getStateListener())
                .onStart(this::onStreamsStart)
                .build();
    }

    private StreamsOptions createStreamsOptions() {
        return StreamsOptions.builder()
                .productive(this.productive)
                .build();
    }
}
