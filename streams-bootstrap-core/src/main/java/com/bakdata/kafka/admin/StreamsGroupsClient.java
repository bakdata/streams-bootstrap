/*
 * MIT License
 *
 * Copyright (c) 2026 bakdata
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
import java.util.Set;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteStreamsGroupsResult;
import org.apache.kafka.clients.admin.DescribeStreamsGroupsResult;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListGroupsResult;
import org.apache.kafka.clients.admin.ListStreamsGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListStreamsGroupOffsetsSpec;
import org.apache.kafka.clients.admin.StreamsGroupDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;

/**
 * This class offers helpers to interact with Kafka streams groups.
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public final class StreamsGroupsClient {

    private final @NonNull Admin adminClient;
    private final @NonNull Timeout timeout;

    /**
     * List streams groups.
     *
     * @return streams groups
     */
    public Collection<GroupListing> list() {
        final ListGroupsOptions options = new ListGroupsOptions()
                .withTypes(Set.of(GroupType.STREAMS));
        final ListGroupsResult result = this.adminClient.listGroups(options);
        return this.timeout.get(result.all(), () -> "Failed to list streams groups");
    }

    /**
     * Create a client for a specific streams group.
     *
     * @param groupName streams group name
     * @return a streams group client for the specified group
     */
    public StreamsGroupClient group(final String groupName) {
        return new StreamsGroupClient(groupName);
    }

    /**
     * A client for a specific streams group.
     */
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public final class StreamsGroupClient {
        private final @NonNull String groupName;

        /**
         * Delete a streams group.
         */
        public void delete() {
            log.info("Deleting streams group '{}'", this.groupName);
            final DeleteStreamsGroupsResult result =
                    StreamsGroupsClient.this.adminClient.deleteStreamsGroups(List.of(this.groupName));
            StreamsGroupsClient.this.timeout.get(result.all(),
                    () -> "Failed to delete streams group " + this.groupName);
            log.info("Deleted streams group '{}'", this.groupName);
        }

        /**
         * Describe a streams group.
         *
         * @return streams group description
         */
        public Optional<StreamsGroupDescription> describe() {
            log.debug("Describing streams group '{}'", this.groupName);
            try {
                final DescribeStreamsGroupsResult result =
                        StreamsGroupsClient.this.adminClient.describeStreamsGroups(List.of(this.groupName));
                final Map<String, KafkaFuture<StreamsGroupDescription>> groups = result.describedGroups();
                final KafkaFuture<StreamsGroupDescription> future = groups.get(this.groupName);
                final StreamsGroupDescription description =
                        StreamsGroupsClient.this.timeout.get(future,
                                () -> "Failed to describe streams group " + this.groupName);
                log.debug("Described streams group '{}'", this.groupName);
                return Optional.of(description);
            } catch (final GroupIdNotFoundException ex) {
                // group does not exist
                return Optional.empty();
            }
        }

        /**
         * List offsets for a streams group.
         *
         * @return streams group offsets
         */
        public Map<TopicPartition, OffsetAndMetadata> listOffsets() {
            log.debug("Listing offsets for streams group '{}'", this.groupName);
            final ListStreamsGroupOffsetsResult result =
                    StreamsGroupsClient.this.adminClient.listStreamsGroupOffsets(
                            Map.of(this.groupName, new ListStreamsGroupOffsetsSpec()));
            final KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> future =
                    result.partitionsToOffsetAndMetadata(this.groupName);
            final Map<TopicPartition, OffsetAndMetadata> offsets =
                    StreamsGroupsClient.this.timeout.get(future,
                            () -> "Failed to list offsets for streams group " + this.groupName);
            log.debug("Listed offsets for streams group '{}'", this.groupName);
            return offsets;
        }

        /**
         * Checks whether a Kafka streams group exists.
         *
         * @return whether a Kafka streams group with the specified name exists or not
         */
        public boolean exists() {
            final Collection<GroupListing> groups = StreamsGroupsClient.this.list();
            return groups.stream()
                    .anyMatch(this::isThisGroup);
        }

        /**
         * Delete a streams group only if it exists.
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

        private boolean isThisGroup(final GroupListing listing) {
            return listing.groupId().equals(this.groupName);
        }
    }
}

