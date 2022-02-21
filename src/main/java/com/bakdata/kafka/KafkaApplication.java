/*
 * MIT License
 *
 * Copyright (c) 2021 bakdata
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
import picocli.CommandLine;

/**
 * <p>The base class of the entry point of the Kafka application.</p>
 * This class provides common configuration options, e.g., {@link #brokers}, for Kafka applications. Hereby it
 * automatically populates the passed in command line arguments with matching environment arguments {@link
 * EnvironmentArgumentsParser}. To implement your Kafka application inherit from this class and add your custom
 * options.
 */
@ToString
@Getter
@Setter
@RequiredArgsConstructor
public abstract class KafkaApplication implements Runnable {
    public static final int RESET_SLEEP_MS = 5000;
    private static final String ENV_PREFIX = Optional.ofNullable(
            System.getenv("ENV_PREFIX")).orElse("APP_");
    public static final Duration ADMIN_TIMEOUT = Duration.ofSeconds(10L);
    @CommandLine.Option(names = "--output-topic", description = "Output topic")
    protected String outputTopic;
    @CommandLine.Option(names = "--extra-output-topics", split = ",", description = "Additional output topics")
    protected Map<String, String> extraOutputTopics = new HashMap<>();
    @CommandLine.Option(names = "--brokers", required = true)
    protected String brokers = "";
    @CommandLine.Option(names = "--debug", arity = "0..1")
    protected boolean debug = false;
    @CommandLine.Option(names = "--clean-up", arity = "0..1",
            description = "Clear the state store and the global Kafka offsets for the "
                    + "consumer group. Be careful with running in production and with enabling this flag - it "
                    + "might cause inconsistent processing with multiple replicas.")
    protected boolean cleanUp = false;
    @CommandLine.Option(names = "--schema-registry-url", required = true)
    private String schemaRegistryUrl = "";
    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "print this help and exit")
    private boolean helpRequested = false;
    //TODO change to more generic parameter name in the future. Retain old name for backwards compatibility
    @CommandLine.Option(names = "--streams-config", split = ",", description = "Additional Kafka properties")
    private Map<String, String> streamsConfig = new HashMap<>();

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

    public ImprovedAdminClient createAdminClient() {
        return ImprovedAdminClient.builder()
                .properties(this.createKafkaProperties())
                .schemaRegistryUrl(this.getSchemaRegistryUrl())
                .timeout(ADMIN_TIMEOUT)
                .build();
    }

    protected abstract Properties createKafkaProperties();

    protected abstract void runCleanUp();
}
