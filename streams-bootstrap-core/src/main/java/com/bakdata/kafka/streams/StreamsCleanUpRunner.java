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

import static com.bakdata.kafka.streams.StreamsResetterClient.runResetter;

import com.bakdata.kafka.CleanUpRunner;
import com.bakdata.kafka.SchemaRegistryAppUtils;
import com.bakdata.kafka.admin.AdminClientX;
import com.bakdata.kafka.admin.ConsumerGroupsClient;
import com.bakdata.kafka.util.TopologyInformation;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;


/**
 * Clean up the state and artifacts of your Kafka Streams app
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class StreamsCleanUpRunner implements CleanUpRunner {
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

    @Override
    public void close() {
        this.cleanHooks.close();
    }

    /**
     * Clean up your Streams app by resetting the app and deleting the output topics and consumer group.
     *
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
            final Collection<String> allTopics = this.adminClient.topics().list();
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
            final Collection<String> allTopics = this.adminClient.topics().list();
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
            this.adminClient.topics()
                    .topic(topic).deleteIfExists();
            StreamsCleanUpRunner.this.cleanHooks.runTopicDeletionHooks(topic);
        }

        private void deleteConsumerGroup() {
            final ConsumerGroupsClient groups = this.adminClient.consumerGroups();
            groups.group(StreamsCleanUpRunner.this.config.getAppId()).deleteIfExists();
        }
    }

}
