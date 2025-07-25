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

package com.bakdata.kafka.admin;

import static java.util.Collections.emptyMap;

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.jooq.lambda.Seq;

/**
 * This class offers helpers to interact with Kafka topics.
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public final class TopicsClient {

    private static final RetryConfig RETRY_CONFIG = RetryConfig.<Boolean>custom()
            .retryOnResult(result -> result)
            .failAfterMaxAttempts(false)
            .maxAttempts(3)
            .waitDuration(Duration.ofSeconds(5L))
            .build();
    private final @NonNull Admin adminClient;
    private final @NonNull Timeout timeout;

    /**
     * List offsets for a set of partitions.
     *
     * @param topicPartitions partitions to list offsets for
     * @return partition offsets
     */
    public Map<TopicPartition, ListOffsetsResultInfo> listOffsets(final Iterable<TopicPartition> topicPartitions) {
        final Map<TopicPartition, OffsetSpec> offsetRequest = Seq.seq(topicPartitions)
                .toMap(Function.identity(), o -> OffsetSpec.latest());
        final ListOffsetsResult result = this.adminClient.listOffsets(offsetRequest);
        return this.timeout.get(result.all(), () -> "Failed to list offsets");
    }

    /**
     * List Kafka topics.
     *
     * @return name of all existing Kafka topics
     */
    public Collection<String> list() {
        final ListTopicsResult result = this.adminClient.listTopics();
        return this.timeout.get(result.names(), () -> "Failed to list topics");
    }

    /**
     * Create a client for a specific topic.
     *
     * @param topicName topic name
     * @return a topic client for the specified topic
     */
    public TopicClient topic(final String topicName) {
        return new TopicClient(topicName);
    }

    /**
     * A client for a specific topic.
     */
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public final class TopicClient {
        private final @NonNull String topicName;

        private static TopicSettings toSettings(final TopicDescription description) {
            final List<TopicPartitionInfo> partitions = description.partitions();
            final int replicationFactor = partitions.stream()
                    .findFirst()
                    .map(TopicPartitionInfo::replicas)
                    .map(List::size)
                    .orElseThrow(() -> new IllegalStateException("Topic " + description.name() + " has no partitions"));
            return TopicSettings.builder()
                    .replicationFactor((short) replicationFactor)
                    .partitions(partitions.size())
                    .build();
        }

        /**
         * Creates a new Kafka topic with the specified number of partitions if it does not yet exist. If the topic
         * exists, its configuration is not updated.
         *
         * @param settings settings for number of partitions and replicationFactor
         * @param config topic configuration
         * @see #create(TopicSettings, Map)
         * @see #exists()
         */
        public void createIfNotExists(final TopicSettings settings, final Map<String, String> config) {
            if (this.exists()) {
                log.info("Topic {} already exists, no need to create.", this.topicName);
            } else {
                this.create(settings, config);
            }
        }

        /**
         * Creates a new Kafka topic with the specified number of partitions if it does not yet exist.
         *
         * @param settings settings for number of partitions and replicationFactor
         * @see #create(TopicSettings, Map)
         * @see #exists()
         */
        public void createIfNotExists(final TopicSettings settings) {
            this.createIfNotExists(settings, emptyMap());
        }

        /**
         * Delete a Kafka topic.
         */
        public void delete() {
            log.info("Deleting topic '{}'", this.topicName);
            final DeleteTopicsResult result = TopicsClient.this.adminClient.deleteTopics(List.of(this.topicName));
            TopicsClient.this.timeout.get(result.all(), () -> "Failed to delete topic " + this.topicName);
            final Retry retry = Retry.of("topic-deleted", RETRY_CONFIG);
            final boolean exists = Retry.decorateSupplier(retry, this::exists).get();
            if (exists) {
                throw new IllegalStateException(String.format("Deletion of topic %s failed", this.topicName));
            }
        }

        /**
         * Get the current settings of a Kafka topic.
         *
         * @return settings of topic including number of partitions and replicationFactor
         */
        public Optional<TopicSettings> getSettings() {
            final Optional<TopicDescription> description = this.describe();
            return description.map(TopicClient::toSettings);
        }

        /**
         * Checks whether a Kafka topic exists.
         *
         * @return whether a Kafka topic with the specified name exists or not
         */
        public boolean exists() {
            final Collection<String> topics = TopicsClient.this.list();
            return topics.stream()
                    .anyMatch(t -> t.equals(this.topicName));
        }

        /**
         * Describe a Kafka topic.
         *
         * @return topic description
         */
        public Optional<TopicDescription> describe() {
            try {
                final DescribeTopicsResult result =
                        TopicsClient.this.adminClient.describeTopics(List.of(this.topicName));
                final Map<String, KafkaFuture<TopicDescription>> kafkaTopicMap = result.topicNameValues();
                final KafkaFuture<TopicDescription> future = kafkaTopicMap.get(this.topicName);
                final TopicDescription description =
                        TopicsClient.this.timeout.get(future,
                                () -> "Failed to retrieve description of topic " + this.topicName);
                return Optional.of(description);
            } catch (final UnknownTopicOrPartitionException e) {
                // topic does not exist
                return Optional.empty();
            }
        }

        /**
         * Creates a new Kafka topic with the specified number of partitions.
         *
         * @param settings settings for number of partitions and replicationFactor
         * @param config topic configuration
         */
        public void create(final TopicSettings settings, final Map<String, String> config) {
            final NewTopic newTopic =
                    new NewTopic(this.topicName, settings.getPartitions(), settings.getReplicationFactor())
                            .configs(config);
            final CreateTopicsResult result = TopicsClient.this.adminClient.createTopics(List.of(newTopic));
            TopicsClient.this.timeout.get(result.all(), () -> "Failed to create topic " + this.topicName);
            final Retry retry = Retry.of("topic-exists", RETRY_CONFIG);
            final boolean doesNotExist = Retry.decorateSupplier(retry, () -> !this.exists()).get();
            if (doesNotExist) {
                throw new IllegalStateException(String.format("Creation of topic %s failed", this.topicName));
            }
        }

        /**
         * Creates a new Kafka topic with the specified number of partitions.
         *
         * @param settings settings for number of partitions and replicationFactor
         */
        public void create(final TopicSettings settings) {
            this.create(settings, emptyMap());
        }

        /**
         * Delete a Kafka topic only if it exists.
         */
        public void deleteIfExists() {
            if (this.exists()) {
                this.delete();
            }
        }

        /**
         * Create a client for the configuration of this topic.
         *
         * @return config client
         */
        public ConfigClient config() {
            return new ConfigClient(TopicsClient.this.adminClient, TopicsClient.this.timeout,
                    new ConfigResource(Type.TOPIC, this.topicName));
        }

    }
}
