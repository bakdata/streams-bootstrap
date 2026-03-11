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

package com.bakdata.kafka.consumer;

import com.bakdata.kafka.CleanUpRunner;
import com.bakdata.kafka.admin.AdminClientX;
import com.bakdata.kafka.admin.ConsumerGroupsClient.ConsumerGroupClient;
import java.util.Map;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.OffsetSpec;

/**
 * Runner to {@link #clean()} or {@link #reset()} a {@link ConsumerApp}
 *
 * {@link #clean()} deletes the consumer group specified in the constructor. {@link #reset()} resets the consumer group
 * to the earliest offset. Both methods also run hooks registered in a {@link ConsumerCleanUpConfiguration}.
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ConsumerCleanUpRunner implements CleanUpRunner {
    private final @NonNull Map<String, Object> kafkaProperties;
    private final @NonNull String groupId;
    private final @NonNull ConsumerCleanUpConfiguration cleanHooks;

    /**
     * Create a new {@code ConsumerCleanUpRunner} with default {@link ConsumerCleanUpConfiguration}
     *
     * @param kafkaProperties configuration to connect to Kafka admin tools
     * @param groupId consumer group id to clean up
     * @return {@code ConsumerCleanUpRunner}
     */
    public static ConsumerCleanUpRunner create(@NonNull final Map<String, Object> kafkaProperties,
            @NonNull final String groupId) {
        return create(kafkaProperties, groupId, new ConsumerCleanUpConfiguration());
    }

    /**
     * Create a new {@code ConsumerCleanUpRunner}
     *
     * @param kafkaProperties configuration to connect to Kafka admin tools
     * @param groupId consumer group id to clean up
     * @param configuration configuration for hooks that are called when running {@link #clean()}
     * @return {@code ConsumerCleanUpRunner}
     */
    public static ConsumerCleanUpRunner create(@NonNull final Map<String, Object> kafkaProperties,
            @NonNull final String groupId,
            @NonNull final ConsumerCleanUpConfiguration configuration) {
        return new ConsumerCleanUpRunner(kafkaProperties, groupId, configuration);
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
            final ConsumerGroupClient groupClient = this.adminClient.consumerGroups()
                    .group(ConsumerCleanUpRunner.this.groupId);
            groupClient.reset(OffsetSpec.earliest());

            ConsumerCleanUpRunner.this.cleanHooks.runResetHooks();
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
