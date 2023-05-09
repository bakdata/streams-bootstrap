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

import static com.bakdata.kafka.KafkaApplication.RESET_SLEEP_MS;

import com.bakdata.kafka.util.ConsumerGroupClient;
import com.bakdata.kafka.util.ImprovedAdminClient;
import com.bakdata.kafka.util.TopologyInformation;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import kafka.tools.StreamsResetter;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;


/**
 * Clean up the state and artifacts of your Kafka Streams app
 */
@Slf4j
public final class CleanUpRunner {
    private static final int EXIT_CODE_SUCCESS = 0;
    private final String appId;
    private final KafkaStreams streams;
    private final TopologyInformation topologyInformation;
    @Getter
    private final @NonNull ImprovedAdminClient adminClient;
    private final @NonNull Collection<Consumer<String>> topicCleanUpHooks = new ArrayList<>();


    @Builder
    private CleanUpRunner(final @NonNull Topology topology, final @NonNull String appId,
            final @NonNull ImprovedAdminClient adminClient, final @NonNull KafkaStreams streams) {
        this.appId = appId;
        this.adminClient = adminClient;
        this.streams = streams;
        this.topologyInformation = new TopologyInformation(topology, appId);
    }

    /**
     * Run the <a href="https://docs.confluent.io/platform/current/streams/developer-guide/app-reset-tool.html">Kafka
     * Streams Reset Tool</a>
     *
     * @param inputTopics list of input topics of the streams app
     * @param intermediateTopics list of intermediate topics of the streams app
     * @param allTopics list of all topics that exists in the Kafka cluster
     * @param adminClient admin client to use for resetting the streams app
     * @param appId unique app id of the streams app
     */
    public static void runResetter(final Collection<String> inputTopics, final Collection<String> intermediateTopics,
            final Collection<String> allTopics, final ImprovedAdminClient adminClient, final String appId) {
        // StreamsResetter's internal AdminClient can only be configured with a properties file
        final File tempFile = createTemporaryPropertiesFile(appId, adminClient.getProperties());
        final ImmutableList.Builder<String> argList = ImmutableList.<String>builder()
                .add("--application-id", appId)
                .add("--bootstrap-servers", adminClient.getBootstrapServers())
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
        final int returnCode = resetter.run(args);
        try {
            Files.delete(tempFile.toPath());
        } catch (final IOException e) {
            log.warn("Error deleting temporary property file", e);
        }
        if (returnCode != EXIT_CODE_SUCCESS) {
            throw new CleanUpException("Error running streams resetter. Exit code " + returnCode);
        }
    }

    static File createTemporaryPropertiesFile(final String appId, final Map<Object, Object> config) {
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

    static Properties toStringBasedProperties(final Map<Object, Object> config) {
        final Properties parsedProperties = new Properties();
        config.forEach((key, value) -> parsedProperties.setProperty(key.toString(), value.toString()));
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

    /**
     * Register a hook that is executed whenever a topic has been deleted by the cleanup runner.
     *
     * @param cleanUpAction Action to run when a topic requires clean up. Topic is passed as parameter
     * @return this for chaining
     */
    public CleanUpRunner registerTopicCleanUpHook(final Consumer<String> cleanUpAction) {
        this.topicCleanUpHooks.add(cleanUpAction);
        return this;
    }

    /**
     * Clean up your Streams app by resetting the app, deleting local state and optionally deleting the output topics
     * and consumer group
     *
     * @param deleteOutputTopic whether to delete output topics and consumer group
     */
    public void run(final boolean deleteOutputTopic) {
        final Collection<String> allTopics = this.adminClient.getTopicClient().listTopics();
        final List<String> inputTopics = this.topologyInformation.getExternalSourceTopics(allTopics);
        final List<String> intermediateTopics = this.topologyInformation.getIntermediateTopics(allTopics);
        runResetter(inputTopics, intermediateTopics, allTopics, this.adminClient, this.appId);
        // the StreamsResetter is responsible for deleting internal topics
        this.topologyInformation.getInternalTopics()
                .forEach(this::resetInternalTopic);
        if (deleteOutputTopic) {
            this.deleteTopics();
            this.deleteConsumerGroup();
        }
        this.streams.cleanUp();
        try {
            Thread.sleep(RESET_SLEEP_MS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CleanUpException("Error waiting for clean up", e);
        }
    }

    /**
     * Delete output topics
     */
    public void deleteTopics() {
        final List<String> externalTopics = this.topologyInformation.getExternalSinkTopics();
        externalTopics.forEach(this::deleteTopic);
    }

    private void resetInternalTopic(final String topic) {
        this.adminClient.getSchemaTopicClient().resetSchemaRegistry(topic);
        this.runTopicCleanUp(topic);
    }

    private void runTopicCleanUp(final String topic) {
        this.topicCleanUpHooks.forEach(hook -> hook.accept(topic));
    }

    private void deleteTopic(final String topic) {
        this.adminClient.getSchemaTopicClient().deleteTopicAndResetSchemaRegistry(topic);
        this.runTopicCleanUp(topic);
    }

    private void deleteConsumerGroup() {
        final ConsumerGroupClient consumerGroupClient = this.adminClient.getConsumerGroupClient();
        consumerGroupClient.deleteGroupIfExists(this.appId);
    }

}
