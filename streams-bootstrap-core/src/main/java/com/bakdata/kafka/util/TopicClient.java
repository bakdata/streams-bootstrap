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

package com.bakdata.kafka.util;

import static java.util.Collections.emptyMap;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.jooq.lambda.Seq;

/**
 * This class offers helpers to interact with Kafka topics.
 */
@RequiredArgsConstructor
@Slf4j
public final class TopicClient implements AutoCloseable {

    private final @NonNull Admin adminClient;
    private final @NonNull Duration timeout;

    /**
     * Creates a new {@code TopicClient} using the specified configuration.
     *
     * @param configs properties passed to {@link AdminClient#create(Map)}
     * @param timeout timeout for waiting for Kafka admin calls
     * @return {@code TopicClient}
     */
    public static TopicClient create(final Map<String, Object> configs, final Duration timeout) {
        return new TopicClient(AdminClient.create(configs), timeout);
    }

    private static KafkaAdminException failedToDeleteTopic(final String topicName, final Throwable ex) {
        return new KafkaAdminException("Failed to delete topic " + topicName, ex);
    }

    private static KafkaAdminException failedToRetrieveTopicDescription(final String topicName, final Throwable e) {
        return new KafkaAdminException("Failed to retrieve description of topic " + topicName, e);
    }

    private static KafkaAdminException failedToListTopics(final Throwable ex) {
        return new KafkaAdminException("Failed to list topics", ex);
    }

    private static KafkaAdminException failedToCreateTopic(final String topicName, final Throwable ex) {
        return new KafkaAdminException("Failed to create topic " + topicName, ex);
    }

    private static KafkaAdminException failedToListOffsets(final Throwable ex) {
        return new KafkaAdminException("Failed to list offsets", ex);
    }

    /**
     * Creates a new Kafka topic with the specified number of partitions if it does not yet exist. If the topic exists,
     * its configuration is not updated.
     *
     * @param topicName the topic name
     * @param settings settings for number of partitions and replicationFactor
     * @param config topic configuration
     * @see #createTopic(String, TopicSettings, Map)
     * @see #exists(String)
     */
    public void createIfNotExists(final String topicName, final TopicSettings settings,
            final Map<String, String> config) {
        if (this.exists(topicName)) {
            log.info("Topic {} already exists, no need to create.", topicName);
        } else {
            this.createTopic(topicName, settings, config);
        }
    }

    /**
     * Creates a new Kafka topic with the specified number of partitions if it does not yet exist.
     *
     * @param topicName the topic name
     * @param settings settings for number of partitions and replicationFactor
     * @see #createTopic(String, TopicSettings, Map)
     * @see #exists(String)
     */
    public void createIfNotExists(final String topicName, final TopicSettings settings) {
        this.createIfNotExists(topicName, settings, emptyMap());
    }

    /**
     * Delete a Kafka topic.
     *
     * @param topicName the topic name
     */
    public void deleteTopic(final String topicName) {
        log.info("Deleting topic '{}'", topicName);
        try {
            this.adminClient.deleteTopics(List.of(topicName))
                    .all()
                    .get(this.timeout.toSeconds(), TimeUnit.SECONDS);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw failedToDeleteTopic(topicName, ex);
        } catch (final ExecutionException ex) {
            if (ex.getCause() instanceof RuntimeException) {
                throw (RuntimeException) ex.getCause();
            }
            throw failedToDeleteTopic(topicName, ex);
        } catch (final TimeoutException ex) {
            throw failedToDeleteTopic(topicName, ex);
        }
        if (this.exists(topicName)) {
            throw new IllegalStateException(String.format("Deletion of topic %s failed", topicName));
        }
    }

    /**
     * Describes the current configuration of a Kafka topic.
     *
     * @param topicName the topic name
     * @return settings of topic including number of partitions and replicationFactor
     */
    public TopicSettings describe(final String topicName) {
        final TopicDescription description = this.getDescription(topicName);
        final List<TopicPartitionInfo> partitions = description.partitions();
        final int replicationFactor = partitions.stream()
                .findFirst()
                .map(TopicPartitionInfo::replicas)
                .map(List::size)
                .orElseThrow(() -> new IllegalStateException("Topic " + topicName + " has no partitions"));
        return TopicSettings.builder()
                .replicationFactor((short) replicationFactor)
                .partitions(partitions.size())
                .build();
    }

    @Override
    public void close() {
        this.adminClient.close();
    }

    /**
     * Checks whether a Kafka topic exists.
     *
     * @param topicName the topic name
     * @return whether a Kafka topic with the specified name exists or not
     */
    public boolean exists(final String topicName) {
        try {
            this.getDescription(topicName);
            return true;
        } catch (final UnknownTopicOrPartitionException e) {
            return false;
        }
    }

    /**
     * Describe a Kafka topic.
     *
     * @param topicName the topic name
     * @return topic description
     */
    public TopicDescription getDescription(final String topicName) {
        try {
            final Map<String, KafkaFuture<TopicDescription>> kafkaTopicMap =
                    this.adminClient.describeTopics(List.of(topicName)).topicNameValues();
            return kafkaTopicMap.get(topicName).get(this.timeout.toSeconds(), TimeUnit.SECONDS);
        } catch (final ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            throw failedToRetrieveTopicDescription(topicName, e);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw failedToRetrieveTopicDescription(topicName, e);
        } catch (final TimeoutException e) {
            throw failedToRetrieveTopicDescription(topicName, e);
        }
    }

    /**
     * List offsets for a set of partitions.
     *
     * @param topicPartitions partitions to list offsets for
     * @return partition offsets
     */
    public Map<TopicPartition, ListOffsetsResultInfo> listOffsets(final Iterable<TopicPartition> topicPartitions) {
        try {
            final Map<TopicPartition, OffsetSpec> offsetRequest = Seq.seq(topicPartitions)
                    .toMap(Function.identity(), o -> OffsetSpec.latest());
            return this.adminClient.listOffsets(offsetRequest).all()
                    .get(this.timeout.toSeconds(), TimeUnit.SECONDS);
        } catch (final ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            throw failedToListOffsets(e);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw failedToListOffsets(e);
        } catch (final TimeoutException e) {
            throw failedToListOffsets(e);
        }
    }

    /**
     * Creates a new Kafka topic with the specified number of partitions.
     *
     * @param topicName the topic name
     * @param settings settings for number of partitions and replicationFactor
     * @param config topic configuration
     */
    public void createTopic(final String topicName, final TopicSettings settings, final Map<String, String> config) {
        try {
            final NewTopic newTopic =
                    new NewTopic(topicName, settings.getPartitions(), settings.getReplicationFactor());
            this.adminClient
                    .createTopics(List.of(newTopic.configs(config)))
                    .all()
                    .get(this.timeout.toSeconds(), TimeUnit.SECONDS);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw failedToCreateTopic(topicName, ex);
        } catch (final ExecutionException ex) {
            if (ex.getCause() instanceof RuntimeException) {
                throw (RuntimeException) ex.getCause();
            }
            throw failedToCreateTopic(topicName, ex);
        } catch (final TimeoutException ex) {
            throw failedToCreateTopic(topicName, ex);
        }
    }

    /**
     * Creates a new Kafka topic with the specified number of partitions.
     *
     * @param topicName the topic name
     * @param settings settings for number of partitions and replicationFactor
     */
    public void createTopic(final String topicName, final TopicSettings settings) {
        this.createTopic(topicName, settings, emptyMap());
    }

    /**
     * List Kafka topics.
     *
     * @return name of all existing Kafka topics
     */
    public Collection<String> listTopics() {
        try {
            return this.adminClient
                    .listTopics()
                    .names()
                    .get(this.timeout.toSeconds(), TimeUnit.SECONDS);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw failedToListTopics(ex);
        } catch (final ExecutionException ex) {
            if (ex.getCause() instanceof RuntimeException) {
                throw (RuntimeException) ex.getCause();
            }
            throw failedToListTopics(ex);
        } catch (final TimeoutException ex) {
            throw failedToListTopics(ex);
        }
    }

    /**
     * Delete a Kafka topic only if it exists.
     *
     * @param topic the topic name
     */
    public void deleteTopicIfExists(final String topic) {
        if (this.exists(topic)) {
            this.deleteTopic(topic);
        }
    }
}
