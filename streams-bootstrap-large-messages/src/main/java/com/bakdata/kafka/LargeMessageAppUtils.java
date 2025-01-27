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

package com.bakdata.kafka;

import com.bakdata.kafka.HasTopicHooks.TopicHook;
import java.util.Map;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.experimental.UtilityClass;

/**
 * Utility class that provides helpers for cleaning {@code LargeMessageSerde} artifacts
 */
@UtilityClass
public class LargeMessageAppUtils {
    /**
     * Create a hook that cleans up LargeMessage files associated with a topic. It is expected that all necessary
     * properties to create a {@link AbstractLargeMessageConfig} are part of {@code kafkaProperties}.
     *
     * @param kafkaProperties Kafka properties to create hook from
     * @return hook that cleans up LargeMessage files associated with a topic
     * @see HasTopicHooks#registerTopicHook(TopicHook)
     */
    public static TopicHook createTopicHook(final Map<String, Object> kafkaProperties) {
        final AbstractLargeMessageConfig largeMessageConfig = new AbstractLargeMessageConfig(kafkaProperties);
        final LargeMessageStoringClient storer = largeMessageConfig.getStorer();
        return new LargeMessageTopicHook(storer);
    }

    /**
     * Create a hook that cleans up LargeMessage files associated with a topic. It is expected that all necessary
     * properties to create a {@link AbstractLargeMessageConfig} are part of
     * {@link EffectiveAppConfiguration#getKafkaProperties()}.
     *
     * @param configuration Configuration to create hook from
     * @return hook that cleans up LargeMessage files associated with a topic
     * @see #createTopicHook(Map)
     */
    public static TopicHook createTopicHook(final EffectiveAppConfiguration<?> configuration) {
        return createTopicHook(configuration.getKafkaProperties());
    }

    /**
     * Register a hook that cleans up LargeMessage files associated with a topic
     * @param cleanUpConfiguration Configuration to register hook on
     * @param configuration Configuration to create hook from
     * @param <T> type of configuration
     * @return Configuration with registered topic hook
     * @see LargeMessageAppUtils#createTopicHook(EffectiveAppConfiguration)
     */
    public static <T> T registerTopicHook(
            final HasTopicHooks<T> cleanUpConfiguration, final EffectiveAppConfiguration<?> configuration) {
        return cleanUpConfiguration.registerTopicHook(createTopicHook(configuration));
    }

    @RequiredArgsConstructor
    private static class LargeMessageTopicHook implements TopicHook {
        private final @NonNull LargeMessageStoringClient storer;

        @Override
        public void deleted(final String topic) {
            this.storer.deleteAllFiles(topic);
        }

        @Override
        public void close() {
            this.storer.close();
        }
    }
}
