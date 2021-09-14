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

import static com.bakdata.kafka.KafkaApplication.RESET_SLEEP_MS;

import com.bakdata.kafka.util.ConsumerGroupClient;
import com.bakdata.kafka.util.ImprovedAdminClient;
import com.bakdata.kafka.util.SchemaTopicClient;
import com.bakdata.kafka.util.TopologyInformation;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import kafka.tools.StreamsResetter;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;


@Slf4j
public final class CleanUpRunner {
    private static final int EXIT_CODE_SUCCESS = 0;
    private final String appId;
    private final KafkaStreams streams;
    private final TopologyInformation topologyInformation;
    @Getter
    private final @NonNull ImprovedAdminClient adminClient;

    @Builder
    private CleanUpRunner(final @NonNull Topology topology, final @NonNull String appId,
            final @NonNull ImprovedAdminClient adminClient, final @NonNull KafkaStreams streams) {
        this.appId = appId;
        this.adminClient = adminClient;
        this.streams = streams;
        this.topologyInformation = new TopologyInformation(topology, appId);
    }

    public static void runResetter(final Collection<String> inputTopics, final Collection<String> intermediateTopics,
            final ImprovedAdminClient adminClient, final String appId) {
        // StreamsResetter's internal AdminClient can only be configured with a properties file
        final File tempFile = createTemporaryPropertiesFile(appId, adminClient.getProperties());
        final ImmutableList.Builder<String> argList = ImmutableList.<String>builder()
                .add("--application-id", appId)
                .add("--bootstrap-servers", adminClient.getBootstrapServers())
                .add("--config-file", tempFile.toString());
        final Collection<String> allTopics = adminClient.getTopicClient().listTopics();
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

    static File createTemporaryPropertiesFile(final String appId, final Map<Object, Object> config) {
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

    static Properties toStringBasedProperties(final Map<Object, Object> config) {
        final Properties parsedProperties = new Properties();
        config.forEach((key, value) -> parsedProperties.setProperty(key.toString(), value.toString()));
        return parsedProperties;
    }

    public void run(final boolean deleteOutputTopic) {
        final List<String> inputTopics = this.topologyInformation.getExternalSourceTopics();
        final List<String> intermediateTopics = this.topologyInformation.getIntermediateTopics();
        runResetter(inputTopics, intermediateTopics, this.adminClient, this.appId);
        // the StreamsResetter is responsible for deleting internal topics
        this.topologyInformation.getInternalTopics().forEach(this.adminClient.getSchemaTopicClient()::resetSchemaRegistry);
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
        final SchemaTopicClient schemaTopicClient = this.adminClient.getSchemaTopicClient();
        final List<String> externalTopics = this.topologyInformation.getExternalSinkTopics();
        externalTopics.forEach(schemaTopicClient::deleteTopicAndResetSchemaRegistry);
    }

    private void deleteConsumerGroup() {
        final ConsumerGroupClient consumerGroupClient = this.adminClient.getConsumerGroupClient();
        consumerGroupClient.deleteGroupIfExists(this.appId);
    }

}