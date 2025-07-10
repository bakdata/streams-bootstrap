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

import static java.util.Collections.emptyMap;

import java.util.Map;

/**
 * Kafka application that defines necessary configurations
 * @param <T> type of topic config
 * @param <C> type of clean up config
 */
public interface App<T, C> extends AutoCloseable {

    /**
     * Configure clean up behavior
     * @param configuration provides all runtime application configurations
     * @return clean up configuration
     */
    C setupCleanUp(final EffectiveAppConfiguration<T> configuration);

    @Override
    default void close() {
        // do nothing by default
    }

    /**
     * This method should give a default configuration to run your application with.
     * @return Returns a default Kafka configuration. Empty by default
     */
    default Map<String, Object> createKafkaProperties() {
        return emptyMap();
    }

    /**
     * Setup Kafka resources, such as topics, before running this app
     * @param configuration provides all runtime application configurations
     */
    default void setup(final EffectiveAppConfiguration<T> configuration) {
        // do nothing by default
    }

    /**
     * Configure default serialization behavior
     * @return {@code SerializationConfig}
     */
    SerializationConfig defaultSerializationConfig();
}
