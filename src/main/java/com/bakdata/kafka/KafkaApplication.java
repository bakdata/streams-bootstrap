/*
 * MIT License
 *
 * Copyright (c) 2022 bakdata
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

import com.bakdata.kafka.util.ImprovedAdminClient;
import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import picocli.CommandLine;

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
public abstract class KafkaApplication implements Runnable {
    public static final int RESET_SLEEP_MS = 5000;
    public static final Duration ADMIN_TIMEOUT = Duration.ofSeconds(10L);
    private static final String ENV_PREFIX = Optional.ofNullable(
            System.getenv("ENV_PREFIX")).orElse("APP_");
    /**
     * This variable is usually set on application start. When the application is running in debug mode it is used to
     * reconfigure the child app package logger. By default, it points to the package of this class allowing to execute
     * the run method independently.
     */
    protected static String appPackageName = KafkaApplication.class.getPackageName();
    @CommandLine.Option(names = "--output-topic", description = "Output topic")
    protected String outputTopic;
    @CommandLine.Option(names = "--extra-output-topics", split = ",", description = "Additional named output topics")
    protected Map<String, String> extraOutputTopics = new HashMap<>();
    @CommandLine.Option(names = "--brokers", required = true, description = "Broker addresses to connect to")
    protected String brokers = "";
    @CommandLine.Option(names = "--debug", arity = "0..1", description = "Configure logging to debug")
    protected boolean debug;
    @CommandLine.Option(names = "--clean-up", arity = "0..1",
            description = "Clear the state store and the global Kafka offsets for the "
                    + "consumer group. Be careful with running in production and with enabling this flag - it "
                    + "might cause inconsistent processing with multiple replicas.")
    protected boolean cleanUp;
    @CommandLine.Option(names = "--schema-registry-url", required = true, description = "URL of schema registry")
    private String schemaRegistryUrl = "";
    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "print this help and exit")
    private boolean helpRequested;
    //TODO change to more generic parameter name in the future. Retain old name for backwards compatibility
    @CommandLine.Option(names = "--streams-config", split = ",", description = "Additional Kafka properties")
    private Map<String, String> streamsConfig = new HashMap<>();

    /**
     * <p>This methods needs to be called in the executable custom application class inheriting from
     * {@code KafkaApplication}.</p>
     * <p>This method calls System exit</p>
     *
     * @param app An instance of the custom application class.
     * @param args Arguments passed in by the custom application class.
     * @see #startApplicationWithoutExit(KafkaApplication, String[])
     */
    protected static void startApplication(final KafkaApplication app, final String[] args) {
        final int exitCode = startApplicationWithoutExit(app, args);
        System.exit(exitCode);
    }

    @Override
    public void run() {
        log.info("Starting application");
        if (this.debug) {
            Configurator.setLevel("com.bakdata", Level.DEBUG);
            Configurator.setLevel(appPackageName, Level.DEBUG);
        }
        log.debug(this.toString());
    }

    /**
     * <p>This methods needs to be called in the executable custom application class inheriting from
     * {@code KafkaApplication}.</p>
     *
     * @param app An instance of the custom application class.
     * @param args Arguments passed in by the custom application class.
     * @return Exit code of application
     */
    protected static int startApplicationWithoutExit(final KafkaApplication app, final String[] args) {
        appPackageName = app.getClass().getPackageName();
        final String[] populatedArgs = addEnvironmentVariablesArguments(args);
        final CommandLine commandLine = new CommandLine(app);
        return commandLine.execute(populatedArgs);
    }

    static String[] addEnvironmentVariablesArguments(final String[] args) {
        Preconditions.checkArgument(!ENV_PREFIX.equals(EnvironmentStreamsConfigParser.PREFIX),
                "Prefix '" + EnvironmentStreamsConfigParser.PREFIX + "' is reserved for Streams config");
        final List<String> environmentArguments = new EnvironmentArgumentsParser(ENV_PREFIX)
                .parseVariables(System.getenv());
        final Collection<String> allArgs = new ArrayList<>(environmentArguments);
        allArgs.addAll(Arrays.asList(args));
        return allArgs.toArray(String[]::new);
    }

    /**
     * <p>This method specifies the configuration to run your Kafka application with.</p>
     * To add a custom configuration please override {@link #createKafkaProperties()}. Configuration properties
     * specified via environment (starting with STREAMS_) or via cli option {@code --streams-config} are always applied
     * with highest priority (the latter overrides the former).
     *
     * @return Returns Kafka configuration {@link Properties}
     */
    public final Properties getKafkaProperties() {
        final Properties kafkaConfig = this.createKafkaProperties();

        EnvironmentStreamsConfigParser.parseVariables(System.getenv())
                .forEach(kafkaConfig::setProperty);
        this.streamsConfig.forEach(kafkaConfig::setProperty);

        return kafkaConfig;
    }

    /**
     * Get extra output topic for a specified role
     *
     * @param role role of output topic specified in CLI argument
     * @return topic name
     */
    public String getOutputTopic(final String role) {
        final String topic = this.extraOutputTopics.get(role);
        Preconditions.checkNotNull(topic, "No output topic for role '%s' available", role);
        return topic;
    }

    /**
     * Create an admin client for the configured Kafka cluster
     *
     * @return admin client
     */
    public ImprovedAdminClient createAdminClient() {
        return ImprovedAdminClient.builder()
                .properties(this.getKafkaProperties())
                .schemaRegistryUrl(this.getSchemaRegistryUrl())
                .timeout(ADMIN_TIMEOUT)
                .build();
    }

    protected abstract Properties createKafkaProperties();

    protected abstract void runCleanUp();
}
