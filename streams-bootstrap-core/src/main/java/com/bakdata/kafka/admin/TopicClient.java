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

import com.bakdata.kafka.admin.ConfigClient.ForResource;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import lombok.AccessLevel;
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
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.jooq.lambda.Seq;

/**
 * This class offers helpers to interact with Kafka topics.
 */
@RequiredArgsConstructor
@Slf4j
public final class TopicClient implements AutoCloseable {

    private static final RetryConfig RETRY_CONFIG = RetryConfig.<Boolean>custom()
            .retryOnResult(result -> result)
            .failAfterMaxAttempts(false)
            .maxAttempts(3)
            .waitDuration(Duration.ofSeconds(5L))
            .build();
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

    private static KafkaAdminException failedToListTopics(final Throwable ex) {
        return new KafkaAdminException("Failed to list topics", ex);
    }

    private static KafkaAdminException failedToListOffsets(final Throwable ex) {
        return new KafkaAdminException("Failed to list offsets", ex);
    }

    @Override
    public void close() {
        this.adminClient.close();
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
            if (e.getCause() instanceof final RuntimeException cause) {
                throw cause;
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
            if (ex.getCause() instanceof final RuntimeException cause) {
                throw cause;
            }
            throw failedToListTopics(ex);
        } catch (final TimeoutException ex) {
            throw failedToListTopics(ex);
        }
    }

    public ForTopic forTopic(final String topicName) {
        return new ForTopic(topicName);
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public final class ForTopic {
        private final @NonNull String topicName;

        private static TopicSettings describe(final TopicDescription description) {
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
         * @see #createTopic(TopicSettings, Map)
         * @see #exists()
         */
        public void createIfNotExists(final TopicSettings settings,
                final Map<String, String> config) {
            if (this.exists()) {
                log.info("Topic {} already exists, no need to create.", this.topicName);
            } else {
                this.createTopic(settings, config);
            }
        }

        /**
         * Creates a new Kafka topic with the specified number of partitions if it does not yet exist.
         *
         * @param settings settings for number of partitions and replicationFactor
         * @see #createTopic(TopicSettings, Map)
         * @see #exists()
         */
        public void createIfNotExists(final TopicSettings settings) {
            this.createIfNotExists(settings, emptyMap());
        }

        /**
         * Delete a Kafka topic.
         */
        public void deleteTopic() {
            log.info("Deleting topic '{}'", this.topicName);
            try {
                TopicClient.this.adminClient.deleteTopics(List.of(this.topicName))
                        .all()
                        .get(TopicClient.this.timeout.toSeconds(), TimeUnit.SECONDS);
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw this.failedToDeleteTopic(ex);
            } catch (final ExecutionException ex) {
                if (ex.getCause() instanceof final RuntimeException cause) {
                    throw cause;
                }
                throw this.failedToDeleteTopic(ex);
            } catch (final TimeoutException ex) {
                throw this.failedToDeleteTopic(ex);
            }
            final Retry retry = Retry.of("topic-deleted", RETRY_CONFIG);
            final boolean exists = Retry.decorateSupplier(retry, this::exists).get();
            if (exists) {
                throw new IllegalStateException(String.format("Deletion of topic %s failed", this.topicName));
            }
        }

        /**
         * Describes the current configuration of a Kafka topic.
         *
         * @return settings of topic including number of partitions and replicationFactor
         */
        public Optional<TopicSettings> describe() {
            final Optional<TopicDescription> description = this.getDescription();
            return description.map(ForTopic::describe);
        }

        /**
         * Describes the current configuration of a Kafka topic.
         *
         * @return config of topic
         */
        public Map<String, String> getConfig() {
            return this.getConfigClient().getConfigs();
        }

        /**
         * Checks whether a Kafka topic exists.
         *
         * @return whether a Kafka topic with the specified name exists or not
         */
        public boolean exists() {
            final Collection<String> topics = TopicClient.this.listTopics();
            return topics.stream()
                    .anyMatch(t -> t.equals(this.topicName));
        }

        /**
         * Describe a Kafka topic.
         *
         * @return topic description
         */
        public Optional<TopicDescription> getDescription() {
            try {
                final Map<String, KafkaFuture<TopicDescription>> kafkaTopicMap =
                        TopicClient.this.adminClient.describeTopics(List.of(this.topicName)).topicNameValues();
                return Optional.of(
                        kafkaTopicMap.get(this.topicName).get(TopicClient.this.timeout.toSeconds(), TimeUnit.SECONDS));
            } catch (final ExecutionException e) {
                if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                    return Optional.empty();
                }
                if (e.getCause() instanceof final RuntimeException cause) {
                    throw cause;
                }
                throw this.failedToRetrieveTopicDescription(e);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw this.failedToRetrieveTopicDescription(e);
            } catch (final TimeoutException e) {
                throw this.failedToRetrieveTopicDescription(e);
            }
        }

        /**
         * Creates a new Kafka topic with the specified number of partitions.
         *
         * @param settings settings for number of partitions and replicationFactor
         * @param config topic configuration
         */
        public void createTopic(final TopicSettings settings, final Map<String, String> config) {
            try {
                final NewTopic newTopic =
                        new NewTopic(this.topicName, settings.getPartitions(), settings.getReplicationFactor());
                TopicClient.this.adminClient
                        .createTopics(List.of(newTopic.configs(config)))
                        .all()
                        .get(TopicClient.this.timeout.toSeconds(), TimeUnit.SECONDS);
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw this.failedToCreateTopic(ex);
            } catch (final ExecutionException ex) {
                if (ex.getCause() instanceof final RuntimeException cause) {
                    throw cause;
                }
                throw this.failedToCreateTopic(ex);
            } catch (final TimeoutException ex) {
                throw this.failedToCreateTopic(ex);
            }
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
        public void createTopic(final TopicSettings settings) {
            this.createTopic(settings, emptyMap());
        }

        /**
         * Delete a Kafka topic only if it exists.
         */
        public void deleteTopicIfExists() {
            if (this.exists()) {
                this.deleteTopic();
            }
        }

        private ForResource getConfigClient() {
            return new ConfigClient(TopicClient.this.adminClient, TopicClient.this.timeout)
                    .forResource(new ConfigResource(Type.TOPIC, this.topicName));
        }

        private KafkaAdminException failedToDeleteTopic(final Throwable ex) {
            return new KafkaAdminException("Failed to delete topic " + this.topicName, ex);
        }

        private KafkaAdminException failedToRetrieveTopicDescription(final Throwable e) {
            return new KafkaAdminException("Failed to retrieve description of topic " + this.topicName, e);
        }

        private KafkaAdminException failedToCreateTopic(final Throwable ex) {
            return new KafkaAdminException("Failed to create topic " + this.topicName, ex);
        }
    }
}
