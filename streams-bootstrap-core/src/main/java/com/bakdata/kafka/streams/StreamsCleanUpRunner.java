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

import com.bakdata.kafka.CleanUpException;
import com.bakdata.kafka.CleanUpRunner;
import com.bakdata.kafka.SchemaRegistryAppUtils;
import com.bakdata.kafka.admin.AdminClientX;
import com.bakdata.kafka.admin.ConsumerGroupClient;
import com.bakdata.kafka.util.TopologyInformation;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.tools.StreamsResetter;


/**
 * Clean up the state and artifacts of your Kafka Streams app
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class StreamsCleanUpRunner implements CleanUpRunner {
    private static final int EXIT_CODE_SUCCESS = 0;
    private final TopologyInformation topologyInformation;
    private final Topology topology;
    private final @NonNull StreamsConfigX config;
    private final @NonNull StreamsCleanUpConfiguration cleanHooks;

    /**
     * Create a new {@code StreamsCleanUpRunner} with default {@link StreamsCleanUpConfiguration}
     *
     * @param topology topology defining the Kafka Streams app
     * @param streamsConfig configuration to run topology and connect to Kafka admin tools
     * @return {@code StreamsCleanUpRunner}
     */
    public static StreamsCleanUpRunner create(final @NonNull Topology topology,
            final @NonNull StreamsConfig streamsConfig) {
        return create(topology, streamsConfig, new StreamsCleanUpConfiguration());
    }

    /**
     * Create a new {@code StreamsCleanUpRunner}
     *
     * @param topology topology defining the Kafka Streams app
     * @param streamsConfig configuration to run topology and connect to Kafka admin tools
     * @param configuration configuration for hooks that are called when running {@link #clean()} and {@link #reset()}
     * @return {@code StreamsCleanUpRunner}
     */
    public static StreamsCleanUpRunner create(final @NonNull Topology topology,
            final @NonNull StreamsConfig streamsConfig, final @NonNull StreamsCleanUpConfiguration configuration) {
        final StreamsConfigX config = new StreamsConfigX(streamsConfig);
        final TopologyInformation topologyInformation = new TopologyInformation(topology, config.getAppId());
        SchemaRegistryAppUtils.createTopicHook(config.getKafkaProperties())
                .ifPresent(configuration::registerTopicHook);
        return new StreamsCleanUpRunner(topologyInformation, topology, config, configuration);
    }

    /**
     * Run the <a href="https://docs.confluent.io/platform/current/streams/developer-guide/app-reset-tool.html">Kafka
     * Streams Reset Tool</a>
     *
     * @param inputTopics list of input topics of the streams app
     * @param allTopics list of all topics that exists in the Kafka cluster
     * @param streamsAppConfig configuration properties of the streams app
     */
    public static void runResetter(final Collection<String> inputTopics,
            final Collection<String> allTopics, final StreamsConfigX streamsAppConfig) {
        // StreamsResetter's internal AdminClient can only be configured with a properties file
        final String appId = streamsAppConfig.getAppId();
        final File tempFile = createTemporaryPropertiesFile(appId, streamsAppConfig.getKafkaProperties());
        final Collection<String> argList = new ArrayList<>(List.of(
                "--application-id", appId,
                "--bootstrap-server", String.join(",", streamsAppConfig.getBoostrapServers()),
                "--config-file", tempFile.toString()
        ));
        final Collection<String> existingInputTopics = filterExistingTopics(inputTopics, allTopics);
        if (!existingInputTopics.isEmpty()) {
            argList.addAll(List.of("--input-topics", String.join(",", existingInputTopics)));
        }
        final String[] args = argList.toArray(String[]::new);
        final StreamsResetter resetter = new StreamsResetter();
        final int returnCode = resetter.execute(args);
        try {
            Files.delete(tempFile.toPath());
        } catch (final IOException e) {
            log.warn("Error deleting temporary property file", e);
        }
        if (returnCode != EXIT_CODE_SUCCESS) {
            throw new CleanUpException("Error running streams resetter. Exit code " + returnCode);
        }
    }

    static File createTemporaryPropertiesFile(final String appId, final Map<String, Object> config) {
        // Writing properties requires Map<String, String>
        final Properties parsedProperties = toStringBasedProperties(config);
        try {
            final File tempFile = File.createTempFile(appId + "-reset", "temp");
            try (final FileOutputStream out = new FileOutputStream(tempFile)) {
                parsedProperties.store(out, "");
            }
            return tempFile;
        } catch (final IOException e) {
            throw new CleanUpException("Could not run StreamsResetter", e);
        }
    }

    static Properties toStringBasedProperties(final Map<String, Object> config) {
        final Properties parsedProperties = new Properties();
        config.forEach((key, value) -> parsedProperties.setProperty(key, value.toString()));
        return parsedProperties;
    }

    private static Collection<String> filterExistingTopics(final Collection<String> topics,
            final Collection<String> allTopics) {
        return topics.stream()
                .filter(topicName -> {
                    final boolean exists = allTopics.contains(topicName);
                    if (!exists) {
                        log.warn("Not resetting missing topic {}", topicName);
                    }
                    return exists;
                })
                .toList();
    }

    @Override
    public void close() {
        this.cleanHooks.close();
    }

    /**
     * Clean up your Streams app by resetting the app and deleting the output topics
     * and consumer group.
     * @see #reset()
     */
    @Override
    public void clean() {
        try (final AdminClientX adminClient = this.createAdminClient()) {
            final Task task = new Task(adminClient);
            task.cleanAndReset();
        }
    }

    /**
     * Clean up your Streams app by resetting all state stores, consumer group offsets, and internal topics, deleting
     * local state.
     */
    public void reset() {
        try (final AdminClientX adminClient = this.createAdminClient()) {
            final Task task = new Task(adminClient);
            task.reset();
        }
    }

    private Map<String, Object> getKafkaProperties() {
        return this.config.getKafkaProperties();
    }

    private AdminClientX createAdminClient() {
        return AdminClientX.create(this.getKafkaProperties());
    }

    @RequiredArgsConstructor
    private class Task {

        private final @NonNull AdminClientX adminClient;

        private void reset() {
            final Collection<String> allTopics = this.adminClient.getTopicClient().listTopics();
            this.reset(allTopics);
        }

        private void reset(final Collection<String> allTopics) {
            final List<String> inputTopics =
                    StreamsCleanUpRunner.this.topologyInformation.getInputTopics(allTopics);
            runResetter(inputTopics, allTopics, StreamsCleanUpRunner.this.config);
            // the StreamsResetter is responsible for deleting internal topics
            StreamsCleanUpRunner.this.topologyInformation.getInternalTopics()
                    .forEach(this::resetInternalTopic);
            this.deleteIntermediateTopics(allTopics);
            try (final KafkaStreams kafkaStreams = this.createStreams()) {
                kafkaStreams.cleanUp();
            }
            StreamsCleanUpRunner.this.cleanHooks.runResetHooks();
        }

        private KafkaStreams createStreams() {
            return new KafkaStreams(StreamsCleanUpRunner.this.topology,
                    new StreamsConfig(StreamsCleanUpRunner.this.getKafkaProperties()));
        }

        private void cleanAndReset() {
            final Collection<String> allTopics = this.adminClient.getTopicClient().listTopics();
            this.reset(allTopics);
            this.clean(allTopics);
        }

        private void clean(final Collection<String> allTopics) {
            this.deleteOutputTopics(allTopics);
            this.deleteConsumerGroup();
            StreamsCleanUpRunner.this.cleanHooks.runCleanHooks();
        }

        private void deleteIntermediateTopics(final Collection<String> allTopics) {
            final List<String> intermediateTopics =
                    StreamsCleanUpRunner.this.topologyInformation.getIntermediateTopics(allTopics);
            intermediateTopics.forEach(this::deleteTopic);
        }

        private void deleteOutputTopics(final Collection<String> allTopics) {
            final List<String> outputTopics = StreamsCleanUpRunner.this.topologyInformation.getOutputTopics(allTopics);
            outputTopics.forEach(this::deleteTopic);
        }

        private void resetInternalTopic(final String topic) {
            StreamsCleanUpRunner.this.cleanHooks.runTopicDeletionHooks(topic);
        }

        private void deleteTopic(final String topic) {
            this.adminClient.getTopicClient()
                    .deleteTopicIfExists(topic);
            StreamsCleanUpRunner.this.cleanHooks.runTopicDeletionHooks(topic);
        }

        private void deleteConsumerGroup() {
            final ConsumerGroupClient consumerGroupClient = this.adminClient.getConsumerGroupClient();
            consumerGroupClient.deleteGroupIfExists(StreamsCleanUpRunner.this.config.getAppId());
        }
    }

}
