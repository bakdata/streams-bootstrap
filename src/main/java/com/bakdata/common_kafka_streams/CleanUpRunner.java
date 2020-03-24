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

import static com.bakdata.common_kafka_streams.KafkaStreamsApplication.RESET_SLEEP_MS;

import com.bakdata.common_kafka_streams.util.TopologyInformation;
import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import kafka.tools.StreamsResetter;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;


@Slf4j
public class CleanUpRunner {
    private static final int EXIT_CODE_SUCCESS = 0;
    private final String appId;
    private final Properties kafkaProperties;
    private final SchemaRegistryClient client;
    private final String brokers;
    private final KafkaStreams streams;
    private final TopologyInformation topologyInformation;

    @Builder
    public CleanUpRunner(final @NonNull Topology topology, final @NonNull String appId,
            final @NonNull Properties kafkaProperties, final @NonNull String schemaRegistryUrl,
            final @NonNull String brokers, final @NonNull KafkaStreams streams) {
        this.appId = appId;
        this.kafkaProperties = kafkaProperties;
        this.client = createSchemaRegistryClient(kafkaProperties, schemaRegistryUrl);
        this.brokers = brokers;
        this.streams = streams;
        this.topologyInformation = new TopologyInformation(topology, appId);
    }

    public static void runResetter(final List<String> inputTopics, final List<String> intermediateTopics,
            final String brokers, final String appId, final Properties config) {
        // StreamsResetter's internal AdminClient can only be configured with a properties file
        final File tempFile = createTemporaryPropertiesFile(appId, config);
        final ImmutableList.Builder<String> argList = ImmutableList.<String>builder()
                .add("--application-id", appId)
                .add("--bootstrap-servers", brokers)
                .add("--config-file", tempFile.toString());
        if (!inputTopics.isEmpty()) {
            argList.add("--input-topics", String.join(",", inputTopics));
        }
        if (!intermediateTopics.isEmpty()) {
            argList.add("--intermediate-topics", String.join(",", intermediateTopics));
        }
        final String[] args = argList.build().toArray(String[]::new);
        final StreamsResetter resetter = new StreamsResetter();
        final int returnCode = resetter.run(args);
        if (returnCode != EXIT_CODE_SUCCESS) {
            throw new RuntimeException("Error running streams resetter. Exit code " + returnCode);
        }
    }

    private static CachedSchemaRegistryClient createSchemaRegistryClient(@NonNull final Properties kafkaProperties,
            @NonNull final String schemaRegistryUrl) {
        final StreamsConfig streamsConfig = new StreamsConfig(kafkaProperties);
        return new CachedSchemaRegistryClient(schemaRegistryUrl, 100, streamsConfig.originals());
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

    public void run(final boolean deleteOutputTopic) {
        final List<String> inputTopics = this.topologyInformation.getExternalSourceTopics();
        final List<String> intermediateTopics = this.topologyInformation.getIntermediateTopics();
        runResetter(inputTopics, intermediateTopics, this.brokers, this.appId, this.kafkaProperties);
        if (deleteOutputTopic) {
            this.deleteTopics();
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
        this.topologyInformation.getInternalTopics().forEach(this::resetSchemaRegistry);
        final List<String> externalTopics = this.topologyInformation.getExternalSinkTopics();
        externalTopics.forEach(this::deleteTopicAndResetSchemaRegistry);
    }

    public void deleteTopicAndResetSchemaRegistry(final String topic) {
        this.deleteTopic(topic);
        this.resetSchemaRegistry(topic);
    }

    public void deleteTopic(final String topic) {
        log.info("Delete topic: {}", topic);
        try (final AdminClient adminClient = AdminClient.create(this.kafkaProperties)) {
            adminClient.deleteTopics(List.of(topic));
        }
    }

    public void resetSchemaRegistry(final String topic) {
        log.info("Reset topic: {}", topic);
        try {
            final Collection<String> allSubjects = this.client.getAllSubjects();
            final String keySubject = topic + "-key";
            if (allSubjects.contains(keySubject)) {
                this.client.deleteSubject(keySubject);
                log.info("Cleaned key schema of topic {}", topic);
            } else {
                log.info("No key schema for topic {} available", topic);
            }
            final String valueSubject = topic + "-value";
            if (allSubjects.contains(valueSubject)) {
                this.client.deleteSubject(valueSubject);
                log.info("Cleaned value schema of topic {}", topic);
            } else {
                log.info("No value schema for topic {} available", topic);
            }
        } catch (final IOException | RestClientException e) {
            throw new RuntimeException("Could not reset schema registry for topic " + topic, e);
        }
    }

}