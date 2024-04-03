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

import com.bakdata.kafka.ProducerCleanUpConfigurer.ProducerCleanUpHooks;
import com.bakdata.kafka.util.ImprovedAdminClient;
import java.util.Map;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.lambda.Seq;


/**
 * Clean up the state and artifacts of your Kafka Streams app
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ProducerCleanUpRunner {
    public static final int RESET_SLEEP_MS = 5000;
    private final @NonNull ProducerTopicConfig topics;
    private final @NonNull Map<String, Object> kafkaProperties;
    private final @NonNull ProducerCleanUpHooks cleanHooks;

    public static ProducerCleanUpRunner create(@NonNull final ProducerTopicConfig topics,
            @NonNull final Map<String, Object> kafkaProperties, @NonNull final ProducerCleanUpConfigurer cleanHooks) {
        return new ProducerCleanUpRunner(topics, kafkaProperties, cleanHooks.create(kafkaProperties));
    }

    static void waitForCleanUp() {
        try {
            Thread.sleep(RESET_SLEEP_MS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CleanUpException("Error waiting for clean up", e);
        }
    }

    /**
     * Clean up your producer app by deleting the output topics.
     */
    public void clean() {
        try (final ImprovedAdminClient adminClient = this.createAdminClient()) {
            final Task task = new Task(adminClient);
            task.clean();
            waitForCleanUp();
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
            ProducerCleanUpRunner.this.cleanHooks.runCleanHooks(this.adminClient);
        }

        private void deleteTopics() {
            final Iterable<String> outputTopics = this.getAllOutputTopics();
            outputTopics.forEach(this::deleteTopic);
        }

        private void deleteTopic(final String topic) {
            this.adminClient.getSchemaTopicClient()
                    .deleteTopicAndResetSchemaRegistry(topic);
            ProducerCleanUpRunner.this.cleanHooks.runTopicDeletionHooks(topic);
        }

        private Iterable<String> getAllOutputTopics() {
            return Seq.of(ProducerCleanUpRunner.this.topics.getOutputTopic())
                    .concat(ProducerCleanUpRunner.this.topics.getExtraOutputTopics().values());
        }
    }

}
