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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.NonNull;

/**
 * Provides configuration options for {@link ProducerCleanUpRunner}
 */
public class ProducerCleanUpConfiguration
        implements HasTopicHooks<ProducerCleanUpConfiguration>, HasCleanHook<ProducerCleanUpConfiguration> {
    private final @NonNull Collection<HookFactory<TopicHook>> topicDeletionHooks = new ArrayList<>();
    private final @NonNull Collection<HookFactory<Runnable>> cleanHooks = new ArrayList<>();

    /**
     * Register a hook that is executed whenever a topic has been deleted by the cleanup runner.
     *
     * @param hookFactory Action to run. Topic is passed as parameter
     * @return this for chaining
     * @see ProducerCleanUpRunner
     */
    @Override
    public ProducerCleanUpConfiguration registerTopicHook(final HookFactory<TopicHook> hookFactory) {
        this.topicDeletionHooks.add(hookFactory);
        return this;
    }

    /**
     * Register an action that is executed after {@link ProducerCleanUpRunner#clean()} has finished
     * @param action Action to run
     * @return this for chaining
     */
    @Override
    public ProducerCleanUpConfiguration registerCleanHook(final HookFactory<Runnable> action) {
        this.cleanHooks.add(action);
        return this;
    }

    ProducerCleanUpHooks create(final Map<String, Object> kafkaConfig) {
        return ProducerCleanUpHooks.builder()
                .topicHooks(this.topicDeletionHooks.stream()
                        .map(t -> t.create(kafkaConfig))
                        .collect(Collectors.toList()))
                .cleanHooks(this.cleanHooks.stream()
                        .map(c -> c.create(kafkaConfig))
                        .collect(Collectors.toList()))
                .build();
    }

    @Builder(access = AccessLevel.PRIVATE)
    static class ProducerCleanUpHooks {
        private final @NonNull Collection<TopicHook> topicHooks;
        private final @NonNull Collection<Runnable> cleanHooks;

        void runCleanHooks() {
            this.cleanHooks.forEach(Runnable::run);
        }

        void runTopicDeletionHooks(final String topic) {
            this.topicHooks.forEach(hook -> hook.deleted(topic));
        }
    }
}
