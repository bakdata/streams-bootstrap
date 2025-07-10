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

import com.bakdata.kafka.admin.ConfigClient.ForResource;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
@Slf4j
public final class ConsumerGroupClient implements AutoCloseable {

    private final @NonNull Admin adminClient;
    private final @NonNull Timeout timeout;

    public ConsumerGroupClient(final Admin adminClient, final Duration timeout) {
        this.adminClient = adminClient;
        this.timeout = new Timeout(timeout);
    }

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
        return this.timeout.get(this.adminClient
                .listConsumerGroups()
                .all(), ConsumerGroupClient::failedToListGroups);
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
            ConsumerGroupClient.this.timeout.get(
                    ConsumerGroupClient.this.adminClient.deleteConsumerGroups(List.of(this.groupName))
                            .all(), this::failedToDeleteGroup);
            log.info("Deleted consumer group '{}'", this.groupName);
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
                        ConsumerGroupClient.this.timeout.get(
                                        ConsumerGroupClient.this.adminClient.describeConsumerGroups(List.of(this.groupName))
                                                .all(), this::failedToDescribeGroup)
                                .get(this.groupName);
                log.info("Described consumer group '{}'", this.groupName);
                return Optional.of(description);
            } catch (final GroupIdNotFoundException ex) {
                return Optional.empty();
            }
        }

        /**
         * List offsets for a consumer group.
         *
         * @return consumer group offsets
         */
        public Map<TopicPartition, OffsetAndMetadata> listOffsets() {
            log.info("Listing offsets for consumer group '{}'", this.groupName);
            final Map<TopicPartition, OffsetAndMetadata> offsets =
                    ConsumerGroupClient.this.timeout.get(
                            ConsumerGroupClient.this.adminClient.listConsumerGroupOffsets(this.groupName)
                                    .partitionsToOffsetAndMetadata(this.groupName), this::failedToListOffsets);
            log.info("Listed offsets for consumer group '{}'", this.groupName);
            return offsets;
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
            this.getConfigClient().addConfig(configEntry);
        }

        /**
         * Describes the current configuration of a consumer group.
         *
         * @return config of consumer group
         */
        public Map<String, String> getConfig() {
            return this.getConfigClient().getConfigs();
        }

        private ForResource getConfigClient() {
            return new ConfigClient(ConsumerGroupClient.this.adminClient, ConsumerGroupClient.this.timeout)
                    .forResource(new ConfigResource(Type.GROUP, this.groupName));
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
