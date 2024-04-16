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

import java.util.Map;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Provides all runtime configurations and supports building a {@link Topology} of a {@link StreamsApp}
 *
 * @see StreamsApp#buildTopology(TopologyBuilder)
 */
@Builder(access = AccessLevel.PACKAGE)
@Value
public class TopologyBuilder {

    StreamsBuilder streamsBuilder = new StreamsBuilder();
    @NonNull
    StreamsTopicConfig topics;
    @NonNull
    Map<String, Object> kafkaProperties;

    /**
     * Create a {@code KStream} from all {@link StreamsTopicConfig#getInputTopics()}
     * @param consumed define optional parameters for streaming topics
     * @return a {@code KStream} for all {@link StreamsTopicConfig#getInputTopics()}
     * @param <K> type of keys
     * @param <V> type of values
     */
    public <K, V> KStream<K, V> streamInput(final Consumed<K, V> consumed) {
        return this.streamsBuilder.stream(this.topics.getInputTopics(), consumed);
    }

    /**
     * Create a {@code KStream} from all {@link StreamsTopicConfig#getInputTopics()}
     * @return a {@code KStream} for all {@link StreamsTopicConfig#getInputTopics()}
     * @param <K> type of keys
     * @param <V> type of values
     */
    public <K, V> KStream<K, V> streamInput() {
        return this.streamsBuilder.stream(this.topics.getInputTopics());
    }

    /**
     * Create a {@code KStream} from all {@link StreamsTopicConfig#getInputTopics(String)}
     * @param role role of extra input topics
     * @param consumed define optional parameters for streaming topics
     * @return a {@code KStream} for all {@link StreamsTopicConfig#getInputTopics(String)}
     * @param <K> type of keys
     * @param <V> type of values
     */
    public <K, V> KStream<K, V> streamInput(final String role, final Consumed<K, V> consumed) {
        return this.streamsBuilder.stream(this.topics.getInputTopics(role), consumed);
    }

    /**
     * Create a {@code KStream} from all {@link StreamsTopicConfig#getInputTopics(String)}
     * @param role role of extra input topics
     * @return a {@code KStream} for all {@link StreamsTopicConfig#getInputTopics(String)}
     * @param <K> type of keys
     * @param <V> type of values
     */
    public <K, V> KStream<K, V> streamInput(final String role) {
        return this.streamsBuilder.stream(this.topics.getInputTopics(role));
    }

    /**
     * Create a {@code KStream} from all topics matching {@link StreamsTopicConfig#getInputPattern()}
     * @param consumed define optional parameters for streaming topics
     * @return a {@code KStream} for all topics matching {@link StreamsTopicConfig#getInputPattern()}
     * @param <K> type of keys
     * @param <V> type of values
     */
    public <K, V> KStream<K, V> streamInputPattern(final Consumed<K, V> consumed) {
        return this.streamsBuilder.stream(this.topics.getInputPattern(), consumed);
    }

    /**
     * Create a {@code KStream} from all topics matching {@link StreamsTopicConfig#getInputPattern()}
     * @return a {@code KStream} for all topics matching {@link StreamsTopicConfig#getInputPattern()}
     * @param <K> type of keys
     * @param <V> type of values
     */
    public <K, V> KStream<K, V> streamInputPattern() {
        return this.streamsBuilder.stream(this.topics.getInputPattern());
    }

    /**
     * Create a {@code KStream} from all topics matching {@link StreamsTopicConfig#getInputPattern(String)}
     * @param role role of extra input pattern
     * @param consumed define optional parameters for streaming topics
     * @return a {@code KStream} for all topics matching {@link StreamsTopicConfig#getInputPattern(String)}
     * @param <K> type of keys
     * @param <V> type of values
     */
    public <K, V> KStream<K, V> streamInputPattern(final String role, final Consumed<K, V> consumed) {
        return this.streamsBuilder.stream(this.topics.getInputPattern(role), consumed);
    }

    /**
     * Create a {@code KStream} from all topics matching {@link StreamsTopicConfig#getInputPattern(String)}
     * @param role role of extra input pattern
     * @return a {@code KStream} for all topics matching {@link StreamsTopicConfig#getInputPattern(String)}
     * @param <K> type of keys
     * @param <V> type of values
     */
    public <K, V> KStream<K, V> streamInputPattern(final String role) {
        return this.streamsBuilder.stream(this.topics.getInputPattern(role));
    }

    /**
     * Create {@code Configurator} to configure {@link org.apache.kafka.common.serialization.Serde} and
     * {@link org.apache.kafka.common.serialization.Serializer} using {@link #kafkaProperties}.
     * @return {@code Configurator}
     */
    public Configurator createConfigurator() {
        return new Configurator(this.kafkaProperties);
    }

    /**
     * Create {@code EffectiveAppConfiguration} used by this app
     * @return {@code EffectiveAppConfiguration}
     */
    public EffectiveAppConfiguration<StreamsTopicConfig> createEffectiveConfiguration() {
        return new EffectiveAppConfiguration<>(this.topics, this.kafkaProperties);
    }

    Topology build() {
        return this.streamsBuilder.build();
    }
}
