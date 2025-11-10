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

package com.bakdata.kafka.consumerproducer;

import com.bakdata.kafka.App;
import com.bakdata.kafka.AppConfiguration;
import com.bakdata.kafka.streams.StreamsCleanUpConfiguration;
import com.bakdata.kafka.streams.StreamsCleanUpRunner;

/**
 * Application that defines how to produce or consume messages to and from Kafka and necessary configurations
 */
public interface ConsumerProducerApp extends App<ConsumerProducerTopicConfig, StreamsCleanUpConfiguration> {

    /**
     * Create a runnable that consumes and produces Kafka messages
     *
     * @param builder provides all runtime application configurations
     * @return {@code ConsumerProducerRunnable}
     */
    ConsumerProducerRunnable buildRunnable(ConsumerProducerBuilder builder);

    /**
     * This must be set to a unique value for every application interacting with your Kafka cluster to ensure internal
     * state encapsulation. Could be set to: className-outputTopic.
     * <p>
     * User may provide a unique application identifier via {@link ConsumerProducerAppConfiguration#getUniqueAppId()}.
     * If that is the case, the returned application ID should match the provided one.
     *
     * @param configuration provides runtime configuration
     * @return unique application identifier
     */
    default String getUniqueAppId(final ConsumerProducerAppConfiguration configuration) {
        return configuration.getUniqueAppId()
                .orElseThrow(() -> new IllegalArgumentException("Please provide an application ID"));
    }

    /**
     * @return {@code StreamsCleanUpConfiguration}
     * @see StreamsCleanUpRunner
     */
    @Override
    default StreamsCleanUpConfiguration setupCleanUp(
            final AppConfiguration<ConsumerProducerTopicConfig> configuration) {
        return new StreamsCleanUpConfiguration();
    }

    @Override
    SerializerDeserializerConfig defaultSerializationConfig();
}
