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

package com.bakdata.kafka.consumer;

import com.bakdata.kafka.CleanUpException;
import com.bakdata.kafka.CleanUpRunner;
import com.bakdata.kafka.admin.AdminClientX;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

/**
 * Clean up all topics specified by a {@link ConsumerTopicConfig}
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ConsumerCleanUpRunner implements CleanUpRunner {
    private final @NonNull ConsumerTopicConfig topics;
    private final @NonNull Map<String, Object> kafkaProperties;
    private final @NonNull String groupId;
    private final @NonNull ConsumerCleanUpConfiguration cleanHooks;

    /**
     * Create a new {@code ConsumerCleanUpRunner} with default {@link ConsumerCleanUpConfiguration}
     *
     * @param topics topic configuration
     * @param kafkaProperties configuration to connect to Kafka admin tools
     * @return {@code ConsumerCleanUpRunner}
     */
    public static ConsumerCleanUpRunner create(@NonNull final ConsumerTopicConfig topics,
            @NonNull final Map<String, Object> kafkaProperties,
            @NonNull final String groupId) {
        return create(topics, kafkaProperties, groupId, new ConsumerCleanUpConfiguration());
    }

    /**
     * Create a new {@code ConsumerCleanUpRunner}
     *
     * @param topics topic configuration
     * @param kafkaProperties configuration to connect to Kafka admin tools
     * @param configuration configuration for hooks that are called when running {@link #clean()}
     * @return {@code ConsumerCleanUpRunner}
     */
    public static ConsumerCleanUpRunner create(@NonNull final ConsumerTopicConfig topics,
            @NonNull final Map<String, Object> kafkaProperties,
            @NonNull final String groupId,
            @NonNull final ConsumerCleanUpConfiguration configuration) {
        return new ConsumerCleanUpRunner(topics, kafkaProperties, groupId, configuration);
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public void clean() {
        try (final AdminClientX adminClient = this.createAdminClient()) {
            final Task task = new Task(adminClient);
            task.clean();
        }
    }

    /**
     * Reset your Consumer app by resetting consumer group offsets
     */
    public void reset() {
        try (final AdminClientX adminClient = this.createAdminClient()) {
            final ConsumerCleanUpRunner.Task task = new ConsumerCleanUpRunner.Task(adminClient);
            task.reset();
        }
    }

    private AdminClientX createAdminClient() {
        return AdminClientX.create(this.kafkaProperties);
    }

    @RequiredArgsConstructor
    private class Task {

        private final @NonNull AdminClientX adminClient;

        private void reset() {
            final Optional<ConsumerGroupDescription> groupDescription =
                    this.adminClient.consumerGroups().group(ConsumerCleanUpRunner.this.groupId).describe();
            if (groupDescription.isEmpty()) {
                return;
            }
            if (groupDescription.get().groupState() != GroupState.EMPTY) {
                throw new CleanUpException("Error resetting application, consumer group is not empty");
            }

            final Map<TopicPartition, OffsetAndMetadata> groupOffsets = this.adminClient.consumerGroups()
                    .group(ConsumerCleanUpRunner.this.groupId).listOffsets();

            final Map<TopicPartition, OffsetSpec> request = groupOffsets.keySet().stream()
                    .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.earliest()));
            final Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliestOffsets =
                    runAdminFuture(this.adminClient.admin().listOffsets(request).all(),
                            "Error resetting application, beginning consumer group offset could not be found");

            final Map<TopicPartition, OffsetAndMetadata> resetOffsets = earliestOffsets.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey,
                            e -> new OffsetAndMetadata(e.getValue().offset())));
            runAdminFuture(
                    this.adminClient.admin().alterConsumerGroupOffsets(ConsumerCleanUpRunner.this.groupId, resetOffsets)
                            .all(), "Error resetting application, could not alter consumer group offsets");

            ConsumerCleanUpRunner.this.cleanHooks.runResetHooks();
        }

        private static <T> T runAdminFuture(final KafkaFuture<T> action, final String errorMessage) {
            try {
                return action.get();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CleanUpException(errorMessage, e);
            } catch (final ExecutionException e) {
                throw new CleanUpException(errorMessage, e);
            }
        }

        private void clean() {
            this.deleteConsumerGroup();
            ConsumerCleanUpRunner.this.cleanHooks.runCleanHooks();
        }

        private void deleteConsumerGroup() {
            this.adminClient.consumerGroups().group(ConsumerCleanUpRunner.this.groupId).deleteIfExists();
        }
    }

}
