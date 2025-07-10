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

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.errors.GroupIdNotFoundException;

/**
 * This class offers helpers to interact with Kafka consumer groups.
 */
@RequiredArgsConstructor
@Slf4j
public final class ConsumerGroupClient implements AutoCloseable {

    private final @NonNull Admin adminClient;
    private final @NonNull Duration timeout;

    /**
     * Creates a new {@code ConsumerGroupClient} using the specified configuration.
     *
     * @param configs properties passed to {@link AdminClient#create(Map)}
     * @param timeout timeout for waiting for Kafka admin calls
     * @return {@code ConsumerGroupClient}
     */
    public static ConsumerGroupClient create(final Map<String, Object> configs, final Duration timeout) {
        return new ConsumerGroupClient(AdminClient.create(configs), timeout);
    }

    private static KafkaAdminException failedToListGroups(final Throwable ex) {
        return new KafkaAdminException("Failed to list consumer groups", ex);
    }

    @Override
    public void close() {
        this.adminClient.close();
    }

    /**
     * List consumer groups.
     *
     * @return consumer groups
     */
    public Collection<ConsumerGroupListing> listGroups() {
        try {
            return this.adminClient
                    .listConsumerGroups()
                    .all()
                    .get(this.timeout.toSeconds(), TimeUnit.SECONDS);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw failedToListGroups(ex);
        } catch (final ExecutionException ex) {
            if (ex.getCause() instanceof final RuntimeException cause) {
                throw cause;
            }
            throw failedToListGroups(ex);
        } catch (final TimeoutException ex) {
            throw failedToListGroups(ex);
        }
    }

    public ForGroup forGroup(final String groupName) {
        return new ForGroup(groupName);
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public final class ForGroup {
        private final @NonNull String groupName;

        /**
         * Delete a consumer group.
         */
        public void deleteConsumerGroup() {
            log.info("Deleting consumer group '{}'", this.groupName);
            try {
                ConsumerGroupClient.this.adminClient.deleteConsumerGroups(List.of(this.groupName))
                        .all()
                        .get(ConsumerGroupClient.this.timeout.toSeconds(), TimeUnit.SECONDS);
                log.info("Deleted consumer group '{}'", this.groupName);
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw this.failedToDeleteGroup(ex);
            } catch (final ExecutionException ex) {
                if (ex.getCause() instanceof final RuntimeException cause) {
                    throw cause;
                }
                throw this.failedToDeleteGroup(ex);
            } catch (final TimeoutException ex) {
                throw this.failedToDeleteGroup(ex);
            }
        }

        /**
         * Describe a consumer group.
         *
         * @return consumer group description
         */
        public Optional<ConsumerGroupDescription> describe() {
            log.info("Describing consumer group '{}'", this.groupName);
            try {
                final ConsumerGroupDescription description =
                        ConsumerGroupClient.this.adminClient.describeConsumerGroups(List.of(this.groupName))
                                .all()
                                .get(ConsumerGroupClient.this.timeout.toSeconds(), TimeUnit.SECONDS)
                                .get(this.groupName);
                log.info("Described consumer group '{}'", this.groupName);
                return Optional.of(description);
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw this.failedToDescribeGroup(ex);
            } catch (final ExecutionException ex) {
                if (ex.getCause() instanceof GroupIdNotFoundException) {
                    return Optional.empty();
                }
                if (ex.getCause() instanceof final RuntimeException cause) {
                    throw cause;
                }
                throw this.failedToDescribeGroup(ex);
            } catch (final TimeoutException ex) {
                throw this.failedToDescribeGroup(ex);
            }
        }

        /**
         * List offsets for a consumer group.
         *
         * @return consumer group offsets
         */
        public Map<TopicPartition, OffsetAndMetadata> listOffsets() {
            log.info("Listing offsets for consumer group '{}'", this.groupName);
            try {
                final Map<TopicPartition, OffsetAndMetadata> offsets =
                        ConsumerGroupClient.this.adminClient.listConsumerGroupOffsets(this.groupName)
                                .partitionsToOffsetAndMetadata(this.groupName)
                                .get(ConsumerGroupClient.this.timeout.toSeconds(), TimeUnit.SECONDS);
                log.info("Listed offsets for consumer group '{}'", this.groupName);
                return offsets;
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw this.failedToListOffsets(ex);
            } catch (final ExecutionException ex) {
                if (ex.getCause() instanceof final RuntimeException cause) {
                    throw cause;
                }
                throw this.failedToListOffsets(ex);
            } catch (final TimeoutException ex) {
                throw this.failedToListOffsets(ex);
            }
        }

        /**
         * Checks whether a Kafka consumer group exists.
         *
         * @return whether a Kafka consumer group with the specified name exists or not
         */
        public boolean exists() {
            final Collection<ConsumerGroupListing> consumerGroups = ConsumerGroupClient.this.listGroups();
            return consumerGroups.stream()
                    .anyMatch(c -> c.groupId().equals(this.groupName));
        }

        /**
         * Delete a consumer group only if it exists.
         */
        public void deleteGroupIfExists() {
            if (this.exists()) {
                try {
                    this.deleteConsumerGroup();
                } catch (final GroupIdNotFoundException e) {
                    // do nothing
                }
            }
        }

        /**
         * Add a config for a consumer group.
         *
         * @param configEntry the configuration entry to add
         */
        public void addConfig(final ConfigEntry configEntry) {
            new ConfigClient(ConsumerGroupClient.this.adminClient, ConsumerGroupClient.this.timeout).addConfig(
                    new ConfigResource(Type.GROUP, this.groupName), configEntry);
        }

        /**
         * Describes the current configuration of a consumer group.
         *
         * @return config of consumer group
         */
        public Map<String, String> getConfig() {
            return new ConfigClient(ConsumerGroupClient.this.adminClient, ConsumerGroupClient.this.timeout).getConfigs(
                    new ConfigResource(Type.GROUP, this.groupName));
        }

        private KafkaAdminException failedToDeleteGroup(final Throwable ex) {
            return new KafkaAdminException("Failed to delete consumer group " + this.groupName, ex);
        }

        private KafkaAdminException failedToListOffsets(final Throwable ex) {
            return new KafkaAdminException("Failed to list offsets for consumer group " + this.groupName, ex);
        }

        private KafkaAdminException failedToDescribeGroup(final Throwable ex) {
            return new KafkaAdminException("Failed to describe consumer group " + this.groupName, ex);
        }
    }
}
