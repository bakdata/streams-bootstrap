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

package com.bakdata.kafka.consumerproducer;

import com.bakdata.kafka.CleanUpRunner;
import com.bakdata.kafka.SchemaRegistryAppUtils;
import com.bakdata.kafka.admin.AdminClientX;
import com.bakdata.kafka.admin.ConsumerGroupsClient.ConsumerGroupClient;
import java.util.Map;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.jooq.lambda.Seq;

/**
 * Runner to {@link #clean()} or {@link #reset()} a {@link ConsumerProducerApp}
 *
 * {@link #clean()} deletes all output topics specified by a {@link ConsumerProducerTopicConfig} and the consumer group
 * specified in the constructor. {@link #reset()} resets the consumer group to the earliest offset. Both methods also
 * run hooks registered in a {@link ConsumerProducerCleanUpConfiguration}.
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ConsumerProducerCleanUpRunner implements CleanUpRunner {
    private final @NonNull ConsumerProducerTopicConfig topics;
    private final @NonNull Map<String, Object> kafkaProperties;
    private final @NonNull String groupId;
    private final @NonNull ConsumerProducerCleanUpConfiguration cleanUpConfig;

    /**
     * Create a new {@link ConsumerProducerCleanUpRunner} with default {@link ConsumerProducerCleanUpConfiguration}
     *
     * @param topics topic configuration
     * @param kafkaProperties configuration to connect to Kafka admin tools
     * @param groupId consumer group id to clean up
     * @return {@link ConsumerProducerCleanUpRunner}
     */
    public static ConsumerProducerCleanUpRunner create(@NonNull final ConsumerProducerTopicConfig topics,
            @NonNull final Map<String, Object> kafkaProperties,
            @NonNull final String groupId) {
        return create(topics, kafkaProperties, groupId, new ConsumerProducerCleanUpConfiguration());
    }

    /**
     * Create a new {@link ConsumerProducerCleanUpRunner}
     *
     * @param topics topic configuration
     * @param kafkaProperties configuration to connect to Kafka admin tools
     * @param groupId consumer group id to clean up
     * @param configuration configuration for hooks that are called during clean up and reset
     * @return {@link ConsumerProducerCleanUpRunner}
     */
    public static ConsumerProducerCleanUpRunner create(@NonNull final ConsumerProducerTopicConfig topics,
            @NonNull final Map<String, Object> kafkaProperties,
            @NonNull final String groupId,
            @NonNull final ConsumerProducerCleanUpConfiguration configuration) {
        SchemaRegistryAppUtils.createTopicHook(kafkaProperties)
                .ifPresent(configuration::registerTopicHook);
        return new ConsumerProducerCleanUpRunner(topics, kafkaProperties, groupId, configuration);
    }

    @Override
    public void close() {
        this.cleanUpConfig.close();
    }

    @Override
    public void clean() {
        try (final AdminClientX adminClient = this.createAdminClient()) {
            final Task task = new Task(adminClient);
            task.clean();
        }
    }

    /**
     * Reset your ConsumerProducer app by resetting consumer group offsets
     */
    public void reset() {
        try (final AdminClientX adminClient = this.createAdminClient()) {
            final Task task = new Task(adminClient);
            task.reset();
        }
    }

    private AdminClientX createAdminClient() {
        return AdminClientX.create(this.kafkaProperties);
    }

    @RequiredArgsConstructor
    private class Task {
        private final @NonNull AdminClientX adminClient;

        private void clean() {
            this.deleteConsumerGroup();
            this.deleteTopics();
            ConsumerProducerCleanUpRunner.this.cleanUpConfig.runCleanHooks();
        }

        private void reset() {
            final ConsumerGroupClient groupClient = this.adminClient.consumerGroups()
                    .group(ConsumerProducerCleanUpRunner.this.groupId);
            groupClient.reset(OffsetSpec.earliest());

            ConsumerProducerCleanUpRunner.this.cleanUpConfig.runResetHooks();
        }

        private void deleteConsumerGroup() {
            this.adminClient.consumerGroups().group(ConsumerProducerCleanUpRunner.this.groupId).deleteIfExists();
        }

        private void deleteTopics() {
            final Iterable<String> outputTopics = this.getAllOutputTopics();
            outputTopics.forEach(this::deleteTopic);
        }

        private void deleteTopic(final String topic) {
            this.adminClient.topics()
                    .topic(topic).deleteIfExists();
            ConsumerProducerCleanUpRunner.this.cleanUpConfig.runTopicDeletionHooks(topic);
        }

        private Iterable<String> getAllOutputTopics() {
            final String errorTopic = ConsumerProducerCleanUpRunner.this.topics.getErrorTopic();
            return Seq.of(ConsumerProducerCleanUpRunner.this.topics.getOutputTopic())
                    .concat(ConsumerProducerCleanUpRunner.this.topics.getLabeledOutputTopics().values())
                    .append(Optional.ofNullable(errorTopic));
        }
    }
}
