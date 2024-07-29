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

import com.bakdata.kafka.util.ImprovedAdminClient;
import java.util.Map;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.lambda.Seq;


/**
 * Delete all output topics specified by a {@link ProducerTopicConfig}
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ProducerCleanUpRunner implements CleanUpRunner {
    private final @NonNull ProducerTopicConfig topics;
    private final @NonNull Map<String, Object> kafkaProperties;
    private final @NonNull ProducerCleanUpConfiguration cleanHooks;

    /**
     * Create a new {@code ProducerCleanUpRunner} with default {@link ProducerCleanUpConfiguration}
     *
     * @param topics topic configuration to infer output topics that require cleaning
     * @param kafkaProperties configuration to connect to Kafka admin tools
     * @return {@code ProducerCleanUpRunner}
     */
    public static ProducerCleanUpRunner create(@NonNull final ProducerTopicConfig topics,
            @NonNull final Map<String, Object> kafkaProperties) {
        return create(topics, kafkaProperties, new ProducerCleanUpConfiguration());
    }

    /**
     * Create a new {@code ProducerCleanUpRunner}
     *
     * @param topics topic configuration to infer output topics that require cleaning
     * @param kafkaProperties configuration to connect to Kafka admin tools
     * @param configuration configuration for hooks that are called when running {@link #clean()}
     * @return {@code ProducerCleanUpRunner}
     */
    public static ProducerCleanUpRunner create(@NonNull final ProducerTopicConfig topics,
            @NonNull final Map<String, Object> kafkaProperties,
            @NonNull final ProducerCleanUpConfiguration configuration) {
        return new ProducerCleanUpRunner(topics, kafkaProperties, configuration);
    }

    @Override
    public void close() {
        this.cleanHooks.close();
    }

    /**
     * Delete all output topics
     */
    @Override
    public void clean() {
        try (final ImprovedAdminClient adminClient = this.createAdminClient()) {
            final Task task = new Task(adminClient);
            task.clean();
        }
    }

    private ImprovedAdminClient createAdminClient() {
        return ImprovedAdminClient.create(this.kafkaProperties);
    }

    @RequiredArgsConstructor
    private class Task {

        private final @NonNull ImprovedAdminClient adminClient;

        private void clean() {
            this.deleteTopics();
            ProducerCleanUpRunner.this.cleanHooks.runCleanHooks();
        }

        private void deleteTopics() {
            final Iterable<String> outputTopics = this.getAllOutputTopics();
            outputTopics.forEach(this::deleteTopic);
        }

        private void deleteTopic(final String topic) {
            this.adminClient.getTopicClient()
                    .deleteTopicIfExists(topic);
            ProducerCleanUpRunner.this.cleanHooks.runTopicDeletionHooks(topic);
        }

        private Iterable<String> getAllOutputTopics() {
            return Seq.of(ProducerCleanUpRunner.this.topics.getOutputTopic())
                    .concat(ProducerCleanUpRunner.this.topics.getLabeledOutputTopics().values());
        }
    }

}
