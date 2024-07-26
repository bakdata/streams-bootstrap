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

/**
 * {@link StreamsApp} that automatically removes schemas when deleting topics
 */
public interface SchemaRegistryStreamsApp extends StreamsApp {

    /**
     * Register a hook that cleans up schemas associated with a topic
     * @param cleanUpConfiguration Configuration to register hook on
     * @param configuration Configuration to create hook from
     * @return {@code StreamsCleanUpConfiguration} with registered topic hook
     * @see SchemaRegistryKafkaApplicationUtils#createSchemaRegistryCleanUpHook(EffectiveAppConfiguration)
     */
    static StreamsCleanUpConfiguration registerSchemaRegistryCleanUpHook(
            final StreamsCleanUpConfiguration cleanUpConfiguration, final EffectiveAppConfiguration<?> configuration) {
        return cleanUpConfiguration.registerTopicHook(
                SchemaRegistryKafkaApplicationUtils.createSchemaRegistryCleanUpHook(configuration));
    }

    @Override
    default StreamsCleanUpConfiguration setupCleanUp(
            final EffectiveAppConfiguration<StreamsTopicConfig> configuration) {
        final StreamsCleanUpConfiguration cleanUpConfiguration = StreamsApp.super.setupCleanUp(configuration);
        return registerSchemaRegistryCleanUpHook(cleanUpConfiguration, configuration);
    }

}
