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
import lombok.Getter;
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
 * <p>The base class of the entry point of the Kafka application.</p>
 * This class provides common configuration options, e.g., {@link #brokers}, for Kafka applications. Hereby it
 * automatically populates the passed in command line arguments with matching environment arguments
 * {@link EnvironmentArgumentsParser}. To implement your Kafka application inherit from this class and add your custom
 * options.
 */
@ToString
@Getter
@Setter
@RequiredArgsConstructor
@Slf4j
@Command(mixinStandardHelpOptions = true)
public abstract class KafkaApplication implements Runnable, AutoCloseable {
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

    /**
     * <p>This methods needs to be called in the executable custom application class inheriting from
     * {@code KafkaApplication}.</p>
     * <p>This method calls System exit</p>
     *
     * @param app An instance of the custom application class.
     * @param args Arguments passed in by the custom application class.
     * @see #startApplicationWithoutExit(KafkaApplication, String[])
     */
    public static void startApplication(final KafkaApplication app, final String[] args) {
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
    public static int startApplicationWithoutExit(final KafkaApplication app, final String[] args) {
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

    public abstract void clean();

    @Override
    public void close() {
        // do nothing by default
    }

    protected void configureDebug() {
        Configurator.setLevel("com.bakdata", Level.DEBUG);
        Configurator.setLevel(this.getClass().getPackageName(), Level.DEBUG);
    }

    protected KafkaEndpointConfig getEndpointConfig() {
        return KafkaEndpointConfig.builder()
                .brokers(this.brokers)
                .schemaRegistryUrl(this.schemaRegistryUrl)
                .build();
    }

    private void startApplication() {
        log.info("Starting application");
        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
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
}
