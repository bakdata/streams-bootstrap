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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.errors.GroupIdNotFoundException;

/**
 * This class offers helpers to interact with Kafka consumer groups.
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public final class ConsumerGroupsClient {

    private final @NonNull Admin adminClient;
    private final @NonNull Timeout timeout;

    /**
     * List consumer groups.
     *
     * @return consumer groups
     */
    public Collection<ConsumerGroupListing> list() {
        final ListConsumerGroupsResult result = this.adminClient.listConsumerGroups();
        return this.timeout.get(result.all(), () -> "Failed to list consumer groups");
    }

    /**
     * Create a client for a specific consumer group.
     *
     * @param groupName consumer group name
     * @return a consumer group client for the specified group
     */
    public ConsumerGroupClient group(final String groupName) {
        return new ConsumerGroupClient(groupName);
    }

    /**
     * A client for a specific consumer group.
     */
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public final class ConsumerGroupClient {
        private final @NonNull String groupName;

        /**
         * Delete a consumer group.
         */
        public void delete() {
            log.info("Deleting consumer group '{}'", this.groupName);
            final DeleteConsumerGroupsResult result =
                    ConsumerGroupsClient.this.adminClient.deleteConsumerGroups(List.of(this.groupName));
            ConsumerGroupsClient.this.timeout.get(result.all(),
                    () -> "Failed to delete consumer group " + this.groupName);
            log.info("Deleted consumer group '{}'", this.groupName);
        }

        /**
         * Describe a consumer group.
         *
         * @return consumer group description
         */
        public Optional<ConsumerGroupDescription> describe() {
            log.debug("Describing consumer group '{}'", this.groupName);
            try {
                final DescribeConsumerGroupsResult result =
                        ConsumerGroupsClient.this.adminClient.describeConsumerGroups(List.of(this.groupName));
                final Map<String, KafkaFuture<ConsumerGroupDescription>> groups = result.describedGroups();
                final KafkaFuture<ConsumerGroupDescription> future = groups.get(this.groupName);
                final ConsumerGroupDescription description =
                        ConsumerGroupsClient.this.timeout.get(future,
                                () -> "Failed to describe consumer group " + this.groupName);
                log.debug("Described consumer group '{}'", this.groupName);
                return Optional.of(description);
            } catch (final GroupIdNotFoundException ex) {
                // group does not exist
                return Optional.empty();
            }
        }

        /**
         * List offsets for a consumer group.
         *
         * @return consumer group offsets
         */
        public Map<TopicPartition, OffsetAndMetadata> listOffsets() {
            log.debug("Listing offsets for consumer group '{}'", this.groupName);
            final ListConsumerGroupOffsetsResult result =
                    ConsumerGroupsClient.this.adminClient.listConsumerGroupOffsets(this.groupName);
            final KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> future =
                    result.partitionsToOffsetAndMetadata(this.groupName);
            final Map<TopicPartition, OffsetAndMetadata> offsets =
                    ConsumerGroupsClient.this.timeout.get(future,
                            () -> "Failed to list offsets for consumer group " + this.groupName);
            log.debug("Listed offsets for consumer group '{}'", this.groupName);
            return offsets;
        }

        /**
         * Checks whether a Kafka consumer group exists.
         *
         * @return whether a Kafka consumer group with the specified name exists or not
         */
        public boolean exists() {
            final Collection<ConsumerGroupListing> consumerGroups = ConsumerGroupsClient.this.list();
            return consumerGroups.stream()
                    .anyMatch(this::isThisGroup);
        }

        /**
         * Delete a consumer group only if it exists.
         */
        public void deleteIfExists() {
            if (this.exists()) {
                try {
                    this.delete();
                } catch (final GroupIdNotFoundException e) {
                    // do nothing
                }
            }
        }

        /**
         * Create a client for the configuration of this consumer group.
         *
         * @return config client
         */
        public ConfigClient config() {
            return new ConfigClient(ConsumerGroupsClient.this.adminClient, ConsumerGroupsClient.this.timeout,
                    new ConfigResource(Type.GROUP, this.groupName));
        }

        private boolean isThisGroup(final ConsumerGroupListing listing) {
            return listing.groupId().equals(this.groupName);
        }

    }
}
