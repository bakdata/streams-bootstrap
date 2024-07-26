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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import lombok.NonNull;

/**
 * Provides configuration options for {@link StreamsCleanUpRunner}
 */
public class StreamsCleanUpConfiguration
        implements HasTopicHooks<StreamsCleanUpConfiguration>, HasCleanHook<StreamsCleanUpConfiguration>, Closeable {
    private final @NonNull Collection<TopicHook> topicHooks = new ArrayList<>();
    private final @NonNull Collection<Runnable> cleanHooks = new ArrayList<>();
    private final @NonNull Collection<Runnable> resetHooks = new ArrayList<>();

    /**
     * Register a hook that is executed whenever a topic has been deleted by the cleanup runner.
     */
    @Override
    public StreamsCleanUpConfiguration registerTopicHook(final TopicHook hook) {
        this.topicHooks.add(hook);
        return this;
    }

    /**
     * Register a hook that is executed after {@link StreamsCleanUpRunner#clean()} has finished
     */
    @Override
    public StreamsCleanUpConfiguration registerCleanHook(final Runnable hook) {
        this.cleanHooks.add(hook);
        return this;
    }

    /**
     * Register a hook that is executed after {@link StreamsCleanUpRunner#reset()} has finished
     * @param hook factory to create hook from
     * @return self for chaining
     */
    public StreamsCleanUpConfiguration registerResetHook(final Runnable hook) {
        this.resetHooks.add(hook);
        return this;
    }

    @Override
    public void close() {
        this.topicHooks.forEach(TopicHook::close);
    }

    void runCleanHooks() {
        this.cleanHooks.forEach(Runnable::run);
    }

    void runResetHooks() {
        this.resetHooks.forEach(Runnable::run);
    }

    void runTopicDeletionHooks(final String topic) {
        this.topicHooks.forEach(hook -> hook.deleted(topic));
    }
}
