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

import static com.bakdata.kafka.ProducerCleanUpRunner.waitForCleanUp;

import com.bakdata.kafka.StreamsCleanUpConfigurer.StreamsCleanUpHooks;
import com.bakdata.kafka.util.ConsumerGroupClient;
import com.bakdata.kafka.util.ImprovedAdminClient;
import com.bakdata.kafka.util.TopologyInformation;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
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
public final class StreamsCleanUpRunner {
    private static final int EXIT_CODE_SUCCESS = 0;
    private final TopologyInformation topologyInformation;
    private final Topology topology;
    private final @NonNull StreamsAppConfig appConfig;
    private final @NonNull StreamsCleanUpHooks cleanHooks;

    public static StreamsCleanUpRunner create(final @NonNull Topology topology,
            final @NonNull StreamsConfig kafkaProperties) {
        return create(topology, kafkaProperties, new StreamsCleanUpConfigurer());
    }

    public static StreamsCleanUpRunner create(final @NonNull Topology topology,
            final @NonNull StreamsConfig kafkaProperties, final @NonNull StreamsCleanUpConfigurer cleanHooks) {
        final StreamsAppConfig streamsAppConfig = new StreamsAppConfig(kafkaProperties);
        final TopologyInformation topologyInformation = new TopologyInformation(topology, streamsAppConfig.getAppId());
        return new StreamsCleanUpRunner(topologyInformation, topology, streamsAppConfig,
                cleanHooks.create(streamsAppConfig.getKafkaProperties()));
    }

    /**
     * Run the <a href="https://docs.confluent.io/platform/current/streams/developer-guide/app-reset-tool.html">Kafka
     * Streams Reset Tool</a>
     *
     * @param inputTopics list of input topics of the streams app
     * @param intermediateTopics list of intermediate topics of the streams app
     * @param allTopics list of all topics that exists in the Kafka cluster
     * @param streamsAppConfig configuration properties of the streams app
     */
    public static void runResetter(final Collection<String> inputTopics, final Collection<String> intermediateTopics,
            final Collection<String> allTopics, final StreamsAppConfig streamsAppConfig) {
        // StreamsResetter's internal AdminClient can only be configured with a properties file
        final String appId = streamsAppConfig.getAppId();
        final File tempFile = createTemporaryPropertiesFile(appId, streamsAppConfig.getKafkaProperties());
        final ImmutableList.Builder<String> argList = ImmutableList.<String>builder()
                .add("--application-id", appId)
                .add("--bootstrap-server", String.join(",", streamsAppConfig.getBoostrapServers()))
                .add("--config-file", tempFile.toString());
        final Collection<String> existingInputTopics = filterExistingTopics(inputTopics, allTopics);
        if (!existingInputTopics.isEmpty()) {
            argList.add("--input-topics", String.join(",", existingInputTopics));
        }
        final Collection<String> existingIntermediateTopics = filterExistingTopics(intermediateTopics, allTopics);
        if (!existingIntermediateTopics.isEmpty()) {
            argList.add("--intermediate-topics", String.join(",", existingIntermediateTopics));
        }
        final String[] args = argList.build().toArray(String[]::new);
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
                .collect(Collectors.toList());
    }

    public Map<String, Object> getKafkaProperties() {
        return this.appConfig.getKafkaProperties();
    }

    /**
     * Clean up your Streams app by resetting the app, deleting local state and deleting the output topics
     * and consumer group.
     */
    public void clean() {
        try (final ImprovedAdminClient adminClient = this.createAdminClient()) {
            final Task task = new Task(adminClient);
            task.cleanAndReset();
            this.cleanHooks.runFinishHooks();
            waitForCleanUp();
        }
    }

    /**
     * Clean up your Streams app by resetting the app, deleting local state.
     */
    public void reset() {
        try (final ImprovedAdminClient adminClient = this.createAdminClient()) {
            final Task task = new Task(adminClient);
            task.reset();
            this.cleanHooks.runFinishHooks();
            waitForCleanUp();
        }
    }

    private ImprovedAdminClient createAdminClient() {
        return ImprovedAdminClient.create(this.getKafkaProperties());
    }

    @RequiredArgsConstructor
    private class Task {

        private final @NonNull ImprovedAdminClient adminClient;

        private void reset() {
            final Collection<String> allTopics = this.adminClient.getTopicClient().listTopics();
            final List<String> inputTopics =
                    StreamsCleanUpRunner.this.topologyInformation.getExternalSourceTopics(allTopics);
            final List<String> intermediateTopics =
                    StreamsCleanUpRunner.this.topologyInformation.getIntermediateTopics(allTopics);
            runResetter(inputTopics, intermediateTopics, allTopics, StreamsCleanUpRunner.this.appConfig);
            // the StreamsResetter is responsible for deleting internal topics
            StreamsCleanUpRunner.this.topologyInformation.getInternalTopics()
                    .forEach(this::resetInternalTopic);
            try (final KafkaStreams kafkaStreams = this.createStreams()) {
                kafkaStreams.cleanUp();
            }
            StreamsCleanUpRunner.this.cleanHooks.runResetHooks(this.adminClient);
        }

        private KafkaStreams createStreams() {
            return new KafkaStreams(StreamsCleanUpRunner.this.topology,
                    new StreamsConfig(StreamsCleanUpRunner.this.getKafkaProperties()));
        }

        private void cleanAndReset() {
            this.reset();
            this.clean();
        }

        private void clean() {
            this.deleteTopics();
            this.deleteConsumerGroup();
            StreamsCleanUpRunner.this.cleanHooks.runCleanHooks(this.adminClient);
        }

        /**
         * Delete output topics
         */
        private void deleteTopics() {
            final List<String> externalTopics = StreamsCleanUpRunner.this.topologyInformation.getExternalSinkTopics();
            externalTopics.forEach(this::deleteTopic);
        }

        private void resetInternalTopic(final String topic) {
            this.adminClient.getSchemaTopicClient()
                    .resetSchemaRegistry(topic);
            StreamsCleanUpRunner.this.cleanHooks.runTopicDeletionHooks(topic);
        }

        private void deleteTopic(final String topic) {
            this.adminClient.getSchemaTopicClient()
                    .deleteTopicAndResetSchemaRegistry(topic);
            StreamsCleanUpRunner.this.cleanHooks.runTopicDeletionHooks(topic);
        }

        private void deleteConsumerGroup() {
            final ConsumerGroupClient consumerGroupClient = this.adminClient.getConsumerGroupClient();
            consumerGroupClient.deleteGroupIfExists(StreamsCleanUpRunner.this.appConfig.getAppId());
        }
    }

}
