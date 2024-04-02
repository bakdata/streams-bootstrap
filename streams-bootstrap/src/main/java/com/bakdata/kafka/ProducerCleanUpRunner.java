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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.lambda.Seq;


/**
 * Clean up the state and artifacts of your Kafka Streams app
 */
@Slf4j
@RequiredArgsConstructor
public final class ProducerCleanUpRunner {
    public static final int RESET_SLEEP_MS = 5000;
    private final @NonNull ProducerTopicConfig topics;
    private final @NonNull Map<String, Object> kafkaProperties;
    private final @NonNull Collection<Consumer<String>> topicDeletionHooks = new ArrayList<>();
    private final @NonNull Collection<Consumer<ImprovedAdminClient>> cleanHooks = new ArrayList<>();

    static void waitForCleanUp() {
        try {
            Thread.sleep(RESET_SLEEP_MS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CleanUpException("Error waiting for clean up", e);
        }
    }

    public Map<String, Object> getKafkaProperties() {
        return Collections.unmodifiableMap(this.kafkaProperties);
    }

    /**
     * Register a hook that is executed whenever a topic has been deleted by the cleanup runner.
     *
     * @param cleanUpAction Action to run when a topic requires clean up. Topic is passed as parameter
     * @return this for chaining
     */
    public ProducerCleanUpRunner registerTopicDeletionHook(final Consumer<String> cleanUpAction) {
        this.topicDeletionHooks.add(cleanUpAction);
        return this;
    }

    public ProducerCleanUpRunner registerCleanHook(final Consumer<ImprovedAdminClient> action) {
        this.cleanHooks.add(action);
        return this;
    }

    /**
     * Clean up your Streams app by resetting the app, deleting local state and optionally deleting the output topics
     * and consumer group
     *
     * @param deleteOutputTopic whether to delete output topics and consumer group
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
            ProducerCleanUpRunner.this.cleanHooks.forEach(this::run);
        }

        private void deleteTopics() {
            final Iterable<String> outputTopics = this.getAllOutputTopics();
            outputTopics.forEach(this::deleteTopic);
        }

        private void run(final Consumer<? super ImprovedAdminClient> hook) {
            hook.accept(this.adminClient);
        }

        private void runTopicDeletionHooks(final String topic) {
            ProducerCleanUpRunner.this.topicDeletionHooks.forEach(hook -> hook.accept(topic));
        }

        private void deleteTopic(final String topic) {
            this.adminClient.getSchemaTopicClient()
                    .deleteTopicAndResetSchemaRegistry(topic);
            this.runTopicDeletionHooks(topic);
        }

        private Iterable<String> getAllOutputTopics() {
            return Seq.of(ProducerCleanUpRunner.this.topics.getOutputTopic())
                    .concat(ProducerCleanUpRunner.this.topics.getExtraOutputTopics().values());
        }
    }

}
