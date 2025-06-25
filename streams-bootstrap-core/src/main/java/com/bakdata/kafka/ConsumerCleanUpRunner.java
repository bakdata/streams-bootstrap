/*
 * MIT License
 *
 * Copyright (c) 2024 bakdata
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

package com.bakdata.kafka;

import com.bakdata.kafka.util.ConsumerGroupClient;
import com.bakdata.kafka.util.ImprovedAdminClient;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


/**
 * Delete all output topics specified by a {@link ProducerTopicConfig}
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
        try (final ImprovedAdminClient adminClient = this.createAdminClient()) {
            final Task task = new Task(adminClient);
            task.clean();
        }
    }

    /**
     * Reset your Consumer app by resetting consumer group offsets
     */
    public void reset() {
        try (final ImprovedAdminClient adminClient = this.createAdminClient()) {
            final ConsumerCleanUpRunner.Task task = new ConsumerCleanUpRunner.Task(adminClient);
            task.reset();
        }
    }

    private ImprovedAdminClient createAdminClient() {
        return ImprovedAdminClient.create(this.kafkaProperties);
    }

    @RequiredArgsConstructor
    private class Task {

        private final @NonNull ImprovedAdminClient adminClient;

        private void reset() {
            final Collection<String> allTopics = this.adminClient.getTopicClient().listTopics();
            // TODO all input topics
            final List<String> inputTopics = ConsumerCleanUpRunner.this.topics.getInputTopics();
            final List<String> bootstrapServers;
            try {
                bootstrapServers = this.adminClient.getAdminClient().describeCluster().nodes().get()
                        .stream()
                        .map(node -> node.host() + ":" + node.port())
                        .collect(Collectors.toList());
            } catch (final InterruptedException | ExecutionException e) {
                // TODO
                throw new CleanUpException("Error getting bootstrap servers", e);
            }

            StreamsResetterWrapper.runResetter(inputTopics,
                    List.of(),
                    allTopics,
                    ConsumerCleanUpRunner.this.groupId,
                    ConsumerCleanUpRunner.this.kafkaProperties,
                    bootstrapServers);

            ConsumerCleanUpRunner.this.cleanHooks.runResetHooks();
        }

        private void clean() {
            this.deleteConsumerGroup();
            ConsumerCleanUpRunner.this.cleanHooks.runCleanHooks();
        }

        private void deleteConsumerGroup() {
            final ConsumerGroupClient consumerGroupClient = this.adminClient.getConsumerGroupClient();
            consumerGroupClient.deleteGroupIfExists(ConsumerCleanUpRunner.this.groupId);
        }
    }

//    @Getter
//    @RequiredArgsConstructor
//    class EarliestOffsetConsumer implements ConsumerApp {
//
//        private final Map<TopicPartition, Long> earliestOffsets = new HashMap<>();
//        private final List<TopicPartition> partitions;
//
//        @Override
//        public DeserializerConfig defaultSerializationConfig() {
//            return new DeserializerConfig(StringDeserializer.class, StringDeserializer.class);
//        }
//
//        @Override
//        public ConsumerRunnable buildRunnable(final ConsumerBuilder builder) {
//            return () -> {
//                try (final Consumer<String, String> consumer = builder.createConsumer()) {
//                    this.fetchEarliestOffsets(consumer);
//                }
//            };
//        }
//
//        private void fetchEarliestOffsets(final Consumer<String, String> consumer) {
//            consumer.assign(this.partitions);
//            consumer.seekToBeginning(this.partitions);
//            this.partitions.forEach(tp -> this.earliestOffsets.put(tp, consumer.position(tp)));
//        }
//
//        @Override
//        public String getUniqueAppId(final ConsumerTopicConfig topics) {
//            return "app-id";
//        }
//    }

}
