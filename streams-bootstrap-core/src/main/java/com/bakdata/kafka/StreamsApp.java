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

import org.apache.kafka.common.serialization.Serdes.StringSerde;

/**
 * Application that defines a Kafka Streams {@link org.apache.kafka.streams.Topology} and necessary configurations
 */
public interface StreamsApp extends App<StreamsTopicConfig, StreamsCleanUpConfiguration> {

    /**
     * Build the Kafka Streams {@link org.apache.kafka.streams.Topology} to be run by the app.
     *
     * @param builder provides all runtime application configurations and supports building the
     * {@link org.apache.kafka.streams.Topology}
     */
    void buildTopology(TopologyBuilder builder);

    /**
     * This must be set to a unique value for every application interacting with your Kafka cluster to ensure internal
     * state encapsulation. Could be set to: className-outputTopic
     *
     * @param topics provides runtime topic configuration
     * @return unique application identifier
     */
    String getUniqueAppId(StreamsTopicConfig topics);

    /**
     * @return {@code StreamsCleanUpConfiguration}
     * @see StreamsCleanUpRunner
     */
    @Override
    default StreamsCleanUpConfiguration setupCleanUp(
            final EffectiveAppConfiguration<StreamsTopicConfig> configuration) {
        return new StreamsCleanUpConfiguration();
    }

    default SerdeConfig defaultSerdeConfig() {
        return new SerdeConfig(StringSerde.class, StringSerde.class);
    }
}
