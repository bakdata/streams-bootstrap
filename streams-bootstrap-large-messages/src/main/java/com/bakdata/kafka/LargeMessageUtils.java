/*
 * MIT License
 *
 * Copyright (c) 2023 bakdata
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

import java.util.function.Consumer;
import lombok.experimental.UtilityClass;

/**
 * Utility class that provides helpers for using {@code LargeMessageSerde} with {@link KafkaStreamsApplication}
 */
@UtilityClass
public class LargeMessageUtils {
    /**
     * Create a hook that cleans up LargeMessage files associated with a topic. It is expected that all necessary
     * properties to create a {@link AbstractLargeMessageConfig} are part of
     * {@link KafkaStreamsApplication#getKafkaProperties()}.
     *
     * @param app {@code KafkaStreamsApplication} to create hook from
     * @return hook that cleans up LargeMessage files associated with a topic
     * @see CleanUpRunner#registerTopicCleanUpHook(Consumer)
     */
    public static Consumer<String> createLargeMessageCleanUpHook(final KafkaStreamsApplication app) {
        final AbstractLargeMessageConfig largeMessageConfig = new AbstractLargeMessageConfig(app.getKafkaProperties());
        final LargeMessageStoringClient storer = largeMessageConfig.getStorer();
        return storer::deleteAllFiles;
    }

    /**
     * Register a hook that cleans up LargeMessage files associated with a topic.
     *
     * @param app {@code KafkaStreamsApplication} to create hook from
     * @param cleanUpRunner {@code CleanUpRunner} to register hook on
     * @see #createLargeMessageCleanUpHook(KafkaStreamsApplication)
     */
    public static void registerLargeMessageCleanUpHook(final KafkaStreamsApplication app, final CleanUpRunner cleanUpRunner) {
        final Consumer<String> deleteAllFiles = createLargeMessageCleanUpHook(app);
        cleanUpRunner.registerTopicCleanUpHook(deleteAllFiles);
    }
}
