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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParseResult;

/**
 * <p>The base class for creating Kafka applications.</p>
 * This class provides the following configuration options:
 * <ul>
 *     <li>{@link #brokers}</li>
 *     <li>{@link #outputTopic}</li>
 *     <li>{@link #extraOutputTopics}</li>
 *     <li>{@link #brokers}</li>
 *     <li>{@link #debug}</li>
 *     <li>{@link #schemaRegistryUrl}</li>
 *     <li>{@link #kafkaConfig}</li>
 * </ul>
 * To implement your Kafka application inherit from this class and add your custom options. Run it by calling
 * {@link #startApplication(KafkaApplication, String[])} with a instance of your class from your main.
 */
@ToString
@Getter
@Setter
@RequiredArgsConstructor
@Slf4j
@Command(mixinStandardHelpOptions = true)
public abstract class KafkaApplication<R extends Runner, C extends CleanUpRunner, O>
        implements Runnable, AutoCloseable {
    private static final String ENV_PREFIX = Optional.ofNullable(System.getenv("ENV_PREFIX")).orElse("APP_");
    @CommandLine.Option(names = "--output-topic", description = "Output topic")
    private String outputTopic;
    @CommandLine.Option(names = "--extra-output-topics", split = ",", description = "Additional named output topics")
    private Map<String, String> extraOutputTopics = new HashMap<>();
    @CommandLine.Option(names = "--brokers", required = true, description = "Broker addresses to connect to")
    private String brokers;
    @CommandLine.Option(names = "--debug", arity = "0..1", description = "Configure logging to debug")
    private boolean debug;
    @CommandLine.Option(names = "--schema-registry-url", description = "URL of Schema Registry")
    private String schemaRegistryUrl;
    @CommandLine.Option(names = "--kafka-config", split = ",", description = "Additional Kafka properties")
    private Map<String, String> kafkaConfig = new HashMap<>();
    @ToString.Exclude
    // ConcurrentLinkedDeque required because calling #close() causes asynchronous #run() calls to finish and thus
    // concurrently iterating on #runners and removing from #runners
    private ConcurrentLinkedDeque<RunningApp<Runner>> runningApps = new ConcurrentLinkedDeque<>();

    /**
     * <p>This methods needs to be called in the executable custom application class inheriting from
     * {@code KafkaApplication}.</p>
     * <p>This method calls System exit</p>
     *
     * @param app An instance of the custom application class.
     * @param args Arguments passed in by the custom application class.
     * @see #startApplicationWithoutExit(KafkaApplication, String[])
     */
    public static void startApplication(final KafkaApplication<?, ?, ?> app, final String[] args) {
        final int exitCode = startApplicationWithoutExit(app, args);
        System.exit(exitCode);
    }

    /**
     * <p>This methods needs to be called in the executable custom application class inheriting from
     * {@code KafkaApplication}.</p>
     *
     * @param app An instance of the custom application class.
     * @param args Arguments passed in by the custom application class.
     * @return Exit code of application
     */
    public static int startApplicationWithoutExit(final KafkaApplication<?, ?, ?> app, final String[] args) {
        final String[] populatedArgs = addEnvironmentVariablesArguments(args);
        final CommandLine commandLine = new CommandLine(app)
                .setExecutionStrategy(app::execute);
        return commandLine.execute(populatedArgs);
    }

    private static String[] addEnvironmentVariablesArguments(final String[] args) {
        Preconditions.checkArgument(!ENV_PREFIX.equals(EnvironmentKafkaConfigParser.PREFIX),
                "Prefix '" + EnvironmentKafkaConfigParser.PREFIX + "' is reserved for Streams config");
        final List<String> environmentArguments = new EnvironmentArgumentsParser(ENV_PREFIX)
                .parseVariables(System.getenv());
        final Collection<String> allArgs = new ArrayList<>(environmentArguments);
        allArgs.addAll(Arrays.asList(args));
        return allArgs.toArray(String[]::new);
    }

    /**
     * Clean all resources associated with this application
     */
    public void clean() {
        try (final ExecutableApp<?, ? extends CleanUpRunner, ?> app = this.createExecutableApp(true)) {
            final CleanUpRunner cleanUpRunner = app.createCleanUpRunner();
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
     * Stop all applications that have been started by {@link #run()}.
     */
    public final void stop() {
        this.runningApps.forEach(RunningApp::close);
    }

    /**
     * Run the application.
     */
    @Override
    public void run() {
        try (final RunningApp<Runner> runningApp = this.createRunningApp()) {
            this.runningApps.add(runningApp);
            runningApp.run();
            this.runningApps.remove(runningApp);
        }
    }

    /**
     * Configure application when running in debug mode. By default, Log4j2 log level is configured to debug for
     * {@code com.bakdata} and the applications package.
     */
    protected void configureDebug() {
        Configurator.setLevel("com.bakdata", Level.DEBUG);
        Configurator.setLevel(this.getClass().getPackageName(), Level.DEBUG);
    }

    /**
     * Create a new {@code ExecutableApp} that will be executed according to the requested command.
     * @param cleanUp whether app is created for clean up purposes. In that case, the user might want to skip
     * initialization of expensive resources.
     * @return {@code ExecutableApp}
     */
    final ExecutableApp<R, C, O> createExecutableApp(final boolean cleanUp) {
        final ConfiguredApp<? extends ExecutableApp<R, C, O>> configuredStreamsApp =
                this.createConfiguredApp(cleanUp);
        final KafkaEndpointConfig endpointConfig = this.getEndpointConfig();
        return configuredStreamsApp.withEndpoint(endpointConfig);
    }

    /**
     * Create a new {@code ConfiguredApp} that will be executed according to this application.
     * @param cleanUp whether {@code ConfiguredApp} is created for clean up purposes. In that case, the user might want
     * to skip initialization of expensive resources.
     * @return {@code ConfiguredApp}
     */
    abstract ConfiguredApp<? extends ExecutableApp<R, C, O>> createConfiguredApp(boolean cleanUp);

    /**
     * Create options for running the app
     * @return run options
     * @see ExecutableApp#createRunner(Object)
     */
    abstract O createExecutionOptions();

    private KafkaEndpointConfig getEndpointConfig() {
        return KafkaEndpointConfig.builder()
                .brokers(this.brokers)
                .schemaRegistryUrl(this.schemaRegistryUrl)
                .build();
    }

    private void startApplication() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
        log.info("Starting application");
        if (this.debug) {
            this.configureDebug();
        }
        log.debug("Starting application: {}", this);
    }

    private int execute(final ParseResult parseResult) {
        this.startApplication();
        final int exitCode = new CommandLine.RunLast().execute(parseResult);
        this.close();
        return exitCode;
    }

    private RunningApp<Runner> createRunningApp() {
        final ExecutableApp<R, ?, O> app = this.createExecutableApp(false);
        final O executionOptions = this.createExecutionOptions();
        final Runner runner = app.createRunner(executionOptions);
        return new RunningApp<>(app, runner);
    }

    @RequiredArgsConstructor
    private static class RunningApp<T extends Runner> implements AutoCloseable {
        private final @NonNull ExecutableApp<?, ?, ?> app;
        private final @NonNull T runner;

        @Override
        public void close() {
            this.runner.close();
            // close app after runner because messages currently processed might depend on resources
            this.app.close();
        }

        private void run() {
            this.runner.run();
        }
    }
}
