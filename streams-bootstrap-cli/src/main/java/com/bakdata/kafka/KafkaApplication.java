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

package com.bakdata.kafka;

import static java.util.Collections.emptyMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Consumer;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParseResult;

/**
 * <p>The base class for creating Kafka applications.</p>
 * This class provides the following configuration options:
 * <ul>
 *     <li>{@link #bootstrapServers}</li>
 *     <li>{@link #outputTopic}</li>
 *     <li>{@link #labeledOutputTopics}</li>
 *     <li>{@link #schemaRegistryUrl}</li>
 *     <li>{@link #kafkaConfig}</li>
 * </ul>
 * To implement your Kafka application inherit from this class and add your custom options. Run it by creating an
 * instance of your class and calling {@link #startApplication(String[])} from your main.
 *
 * @param <R> type of {@link Runner} used by this app
 * @param <CR> type of {@link CleanUpRunner} used by this app
 * @param <O> type of execution options to create runner
 * @param <E> type of {@link ExecutableApp} used by this app
 * @param <CA> type of {@link ConfiguredApp} used by this app
 * @param <T> type of topic config used by this app
 * @param <A> type of app
 * @param <AC> type of configuration used by this app
 */
@ToString
@Getter
@Setter
@RequiredArgsConstructor
@Slf4j
@Command(mixinStandardHelpOptions = true)
public abstract class KafkaApplication<R extends Runner, CR extends CleanUpRunner, O, E extends ExecutableApp<R, CR,
        O>, CA extends ConfiguredApp<E>, T, A, AC>
        implements Runnable, AutoCloseable {
    private static final String ENV_PREFIX = Optional.ofNullable(System.getenv("ENV_PREFIX")).orElse("APP_");
    @ToString.Exclude
    @Getter(AccessLevel.NONE)
    // ConcurrentLinkedDeque required because calling #stop() causes asynchronous #run() calls to finish and thus
    // concurrently iterating and removing from #runners
    private final ConcurrentLinkedDeque<Stoppable> activeApps = new ConcurrentLinkedDeque<>();
    @CommandLine.Option(names = "--output-topic", description = "Output topic")
    private String outputTopic;
    @CommandLine.Option(names = "--labeled-output-topics", split = ",",
            description = "Additional labeled output topics")
    private Map<String, String> labeledOutputTopics = emptyMap();
    @CommandLine.Option(names = {"--bootstrap-servers", "--bootstrap-server"}, required = true,
            description = "Kafka bootstrap servers to connect to")
    private String bootstrapServers;
    @CommandLine.Option(names = "--schema-registry-url", description = "URL of Schema Registry")
    private String schemaRegistryUrl;
    @CommandLine.Option(names = "--kafka-config", split = ",", description = "Additional Kafka properties")
    private Map<String, String> kafkaConfig = emptyMap();

    /**
     * <p>This method should be called in the main method of your application</p>
     * <p>This method calls System exit</p>
     *
     * @param args Arguments passed in by the custom application class.
     * @see #startApplicationWithoutExit(String[])
     */
    public void startApplication(final String[] args) {
        final int exitCode = this.startApplicationWithoutExit(args);
        System.exit(exitCode);
    }

    /**
     * <p>This method should be called in the main method of your application</p>
     *
     * @param args Arguments passed in by the custom application class.
     * @return Exit code of application
     */
    public int startApplicationWithoutExit(final String[] args) {
        final String[] populatedArgs = addEnvironmentVariablesArguments(args);
        final CommandLine commandLine = new CommandLine(this)
                .setExecutionStrategy(this::execute);
        return commandLine.execute(populatedArgs);
    }

    private static String[] addEnvironmentVariablesArguments(final String[] args) {
        if (ENV_PREFIX.equals(EnvironmentKafkaConfigParser.PREFIX)) {
            throw new IllegalArgumentException(
                    String.format("Prefix '%s' is reserved for Kafka config", EnvironmentKafkaConfigParser.PREFIX));
        }
        final List<String> environmentArguments = new EnvironmentArgumentsParser(ENV_PREFIX)
                .parseVariables(System.getenv());
        final Collection<String> allArgs = new ArrayList<>(environmentArguments);
        allArgs.addAll(Arrays.asList(args));
        return allArgs.toArray(String[]::new);
    }

    /**
     * Create options for running the app
     * @return run options if available
     * @see ExecutableApp#createRunner(Object)
     */
    public abstract Optional<O> createExecutionOptions();

    /**
     * Topics used by app
     * @return topic configuration
     */
    public abstract T createTopicConfig();

    /**
     * Create a new app that will be configured and executed according to this application.
     *
     * @return app
     */
    public abstract A createApp();

    /**
     * Clean all resources associated with this application
     */
    public void clean() {
        this.prepareClean();
        try (final CleanableApp<CR> cleanableApp = this.createCleanableApp()) {
            final CR cleanUpRunner = cleanableApp.getCleanUpRunner();
            cleanUpRunner.clean();
        }
    }

    /**
     * @see #stop()
     */
    @Override
    public void close() {
        this.stop();
    }

    /**
     * Stop all applications that have been started asynchronously, e.g., by using {@link #run()} or {@link #clean()}.
     */
    public final void stop() {
        this.activeApps.forEach(Stoppable::stop);
    }

    /**
     * Run the application.
     */
    @Override
    public void run() {
        this.prepareRun();
        try (final RunnableApp<R> runnableApp = this.createRunnableApp()) {
            final R runner = runnableApp.getRunner();
            runner.run();
        }
    }

    public RuntimeConfiguration getRuntimeConfiguration() {
        final RuntimeConfiguration configuration = RuntimeConfiguration.create(this.bootstrapServers)
                .with(this.kafkaConfig);
        return this.schemaRegistryUrl == null ? configuration
                : configuration.withSchemaRegistryUrl(this.schemaRegistryUrl);
    }

    /**
     * Create a new {@link ExecutableApp} that will be executed according to the requested command.
     *
     * @return {@link ExecutableApp}
     */
    public final E createExecutableApp() {
        final ConfiguredApp<E> configuredStreamsApp = this.createConfiguredApp();
        final RuntimeConfiguration runtimeConfiguration = this.getRuntimeConfiguration();
        return configuredStreamsApp.withRuntimeConfiguration(runtimeConfiguration);
    }

    /**
     * Create a new {@link ConfiguredApp} that will be executed according to this application.
     *
     * @return {@link ConfiguredApp}
     */
    public final CA createConfiguredApp() {
        final T topics = this.createTopicConfig();
        final A app = this.createApp();
        final AC appConfiguration = this.createConfiguration(topics);
        return this.createConfiguredApp(app, appConfiguration);
    }

    /**
     * Create configuration to configure app
     *
     * @param topics topic configuration
     * @return configuration
     */
    public abstract AC createConfiguration(T topics);

    /**
     * Create a new {@link RunnableApp}
     * @return {@link RunnableApp}
     */
    public final RunnableApp<R> createRunnableApp() {
        final ExecutableApp<R, ?, O> app = this.createExecutableApp();
        final Optional<O> executionOptions = this.createExecutionOptions();
        final R runner = executionOptions.map(app::createRunner).orElseGet(app::createRunner);
        final RunnableApp<R> runnableApp = new RunnableApp<>(app, runner, this.activeApps::remove);
        this.activeApps.add(runnableApp);
        return runnableApp;
    }

    /**
     * Create a new {@link CleanableApp}
     * @return {@link CleanableApp}
     */
    public final CleanableApp<CR> createCleanableApp() {
        final ExecutableApp<R, CR, O> executableApp = this.createExecutableApp();
        final CR cleanUpRunner = executableApp.createCleanUpRunner();
        final CleanableApp<CR> cleanableApp = new CleanableApp<>(executableApp, cleanUpRunner, this.activeApps::remove);
        this.activeApps.add(cleanableApp);
        return cleanableApp;
    }

    /**
     * Called before starting the application, e.g., invoking {@link #run()}
     */
    public void onApplicationStart() {
        // do nothing by default
    }

    /**
     * Called before running the application, i.e., invoking {@link #run()}
     */
    public void prepareRun() {
        // do nothing by default
    }

    /**
     * Called before cleaning the application, i.e., invoking {@link #clean()}
     */
    public void prepareClean() {
        // do nothing by default
    }

    /**
     * Create a new {@link ConfiguredApp} that will be executed according to the given config.
     *
     * @param app app to configure.
     * @param configuration configuration for app
     * @return {@link ConfiguredApp}
     */
    protected abstract CA createConfiguredApp(final A app, AC configuration);

    private void startApplication() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
        this.onApplicationStart();
        log.info("Starting application");
        log.debug("Starting application: {}", this);
    }

    private int execute(final ParseResult parseResult) {
        this.startApplication();
        return new CommandLine.RunLast().execute(parseResult);
    }

    @FunctionalInterface
    private interface Stoppable {
        void stop();
    }

    /**
     * Provides access to a {@link CleanUpRunner} and closes the associated {@link ExecutableApp}
     *
     * @param <CR> type of {@link CleanUpRunner} used by this app
     */
    @RequiredArgsConstructor(access = AccessLevel.PROTECTED)
    public static class CleanableApp<CR extends CleanUpRunner> implements AutoCloseable, Stoppable {
        private final @NonNull ExecutableApp<?, ?, ?> app;
        @Getter
        private final @NonNull CR cleanUpRunner;
        private final @NonNull Consumer<Stoppable> onClose;

        @Override
        public void close() {
            this.stop();
            this.onClose.accept(this);
        }

        /**
         * Close the app
         */
        @Override
        public void stop() {
            this.cleanUpRunner.close();
            this.app.close();
        }
    }

    /**
     * Provides access to a {@link Runner} and closes the associated {@link ExecutableApp}
     *
     * @param <R> type of {@link Runner} used by this app
     */
    @RequiredArgsConstructor(access = AccessLevel.PROTECTED)
    public static final class RunnableApp<R extends Runner> implements AutoCloseable, Stoppable {
        private final @NonNull ExecutableApp<?, ?, ?> app;
        @Getter
        private final @NonNull R runner;
        private final @NonNull Consumer<Stoppable> onClose;

        @Override
        public void close() {
            this.stop();
            this.onClose.accept(this);
        }

        /**
         * Close the runner and app
         */
        @Override
        public void stop() {
            this.runner.close();
            // close app after runner because messages currently processed might depend on resources
            this.app.close();
        }
    }
}
