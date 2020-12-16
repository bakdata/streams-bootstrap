/*
 * MIT License
 *
 * Copyright (c) 2019 bakdata GmbH
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

package com.bakdata.common_kafka_streams;

import static com.bakdata.common_kafka_streams.KafkaApplication.RESET_SLEEP_MS;

import com.bakdata.common_kafka_streams.util.TopicClient;
import com.bakdata.common_kafka_streams.util.TopologyInformation;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import kafka.tools.StreamsResetter;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;


@Slf4j
public class CleanUpRunner {
    private static final int EXIT_CODE_SUCCESS = 0;
    private final String appId;
    private final KafkaStreams streams;
    private final String brokers;
    private final TopologyInformation topologyInformation;
    private final @NonNull TopicCleaner topicCleaner;

    @Builder
    public CleanUpRunner(final @NonNull Topology topology, final @NonNull String appId,
            final @NonNull TopicCleaner topicCleaner, final @NonNull String brokers,
            final @NonNull KafkaStreams streams) {
        this.topicCleaner = topicCleaner;
        this.appId = appId;
        this.brokers = brokers;
        this.streams = streams;
        this.topologyInformation = new TopologyInformation(topology, appId);
    }

    public static void runResetter(final Collection<String> inputTopics, final Collection<String> intermediateTopics,
            final String brokers, final String appId, final Properties config) {
        // StreamsResetter's internal AdminClient can only be configured with a properties file
        final File tempFile = createTemporaryPropertiesFile(appId, config);
        final ImmutableList.Builder<String> argList = ImmutableList.<String>builder()
                .add("--application-id", appId)
                .add("--bootstrap-servers", brokers)
                .add("--config-file", tempFile.toString());
        final Collection<String> allTopics = listTopics(config);
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
        if (returnCode != EXIT_CODE_SUCCESS) {
            throw new RuntimeException("Error running streams resetter. Exit code " + returnCode);
        }
    }

    private static Collection<String> filterExistingTopics(final Collection<String> inputTopics,
            final Collection<String> allTopics) {
        return inputTopics.stream()
                .filter(topicName -> {
                    final boolean exists = allTopics.contains(topicName);
                    if (!exists) {
                        log.warn("Not resetting missing topic {}", topicName);
                    }
                    return exists;
                })
                .collect(Collectors.toList());
    }

    private static Collection<String> listTopics(final Properties config) {
        try (final TopicClient topicClient = TopicClient.create(config, Duration.ofSeconds(10L))) {
            return topicClient.listTopics();
        }
    }

    protected static File createTemporaryPropertiesFile(final String appId, final Properties config) {
        // Writing properties requires Map<String, String>
        final Properties parsedProperties = toStringBasedProperties(config);
        try {
            final File tempFile = File.createTempFile(appId + "-reset", "temp");
            tempFile.deleteOnExit();
            try (final FileOutputStream out = new FileOutputStream(tempFile)) {
                parsedProperties.store(out, "");
            }
            return tempFile;
        } catch (final IOException e) {
            throw new RuntimeException("Could not run StreamsResetter", e);
        }
    }

    protected static Properties toStringBasedProperties(final Properties config) {
        final Properties parsedProperties = new Properties();
        config.forEach((key, value) -> parsedProperties.setProperty(key.toString(), value.toString()));
        return parsedProperties;
    }

    private static boolean doesConsumerGroupExist(final Admin adminClient, final String groupId)
            throws InterruptedException, ExecutionException {
        final Collection<ConsumerGroupListing> consumerGroups = adminClient.listConsumerGroups().all().get();
        return consumerGroups.stream()
                .anyMatch(c -> c.groupId().equals(groupId));
    }

    public void run(final boolean deleteOutputTopic) {
        final List<String> inputTopics = this.topologyInformation.getExternalSourceTopics();
        final List<String> intermediateTopics = this.topologyInformation.getIntermediateTopics();
        runResetter(inputTopics, intermediateTopics, this.brokers, this.appId,
                this.topicCleaner.getKafkaProperties());
        if (deleteOutputTopic) {
            this.deleteTopics();
            this.deleteConsumerGroup();
        }
        this.streams.cleanUp();
        try {
            Thread.sleep(RESET_SLEEP_MS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Error waiting for clean up", e);
        }
    }

    public void deleteTopics() {
        // the StreamsResetter is responsible for deleting internal topics
        this.topologyInformation.getInternalTopics().forEach(this.topicCleaner::resetSchemaRegistry);
        final List<String> externalTopics = this.topologyInformation.getExternalSinkTopics();
        externalTopics.forEach(this.topicCleaner::deleteTopicAndResetSchemaRegistry);
    }

    private void deleteConsumerGroup() {
        try (final AdminClient adminClient = this.topicCleaner.createAdminClient()) {
            if (doesConsumerGroupExist(adminClient, this.appId)) {
                adminClient.deleteConsumerGroups(List.of(this.appId)).all().get();
                log.info("Deleted consumer group");
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Error waiting for clean up", e);
        } catch (final ExecutionException e) {
            throw new RuntimeException("Error deleting consumer group", e);
        }
    }

}