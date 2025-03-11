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

import java.util.Collection;
import java.util.Map;
import java.util.regex.Pattern;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

/**
 * Provides all runtime configurations and supports building a {@link Topology} of a {@link StreamsApp}
 *
 * @see StreamsApp#buildTopology(StreamsBuilderX)
 */
@RequiredArgsConstructor
@Value
public class StreamsBuilderX {

    StreamsBuilder streamsBuilder = new StreamsBuilder();
    @NonNull
    StreamsTopicConfig topics;
    @NonNull
    Map<String, Object> kafkaProperties;

    /**
     * @see StreamsBuilder#stream(String)
     */
    public <K, V> KStreamX<K, V> stream(final String topic) {
        return this.getContext().wrap(this.streamsBuilder.stream(topic));
    }

    /**
     * @see StreamsBuilder#stream(String, Consumed)
     */
    public <K, V> KStreamX<K, V> stream(final String topic, final Consumed<K, V> consumed) {
        return this.getContext().wrap(this.streamsBuilder.stream(topic, consumed));
    }

    /**
     * @see StreamsBuilder#stream(String, Consumed)
     */
    public <K, V> KStreamX<K, V> stream(final String topic, final ConsumedX<K, V> consumed) {
        return this.stream(topic, consumed.configure(this.createConfigurator()));
    }

    /**
     * @see StreamsBuilder#stream(Collection)
     */
    public <K, V> KStreamX<K, V> stream(final Collection<String> topics) {
        return this.getContext().wrap(this.streamsBuilder.stream(topics));
    }

    /**
     * @see StreamsBuilder#stream(Collection, Consumed)
     */
    public <K, V> KStreamX<K, V> stream(final Collection<String> topics, final Consumed<K, V> consumed) {
        return this.getContext().wrap(this.streamsBuilder.stream(topics, consumed));
    }

    /**
     * @see StreamsBuilder#stream(Collection, Consumed)
     */
    public <K, V> KStreamX<K, V> stream(final Collection<String> topics,
            final ConsumedX<K, V> consumed) {
        return this.stream(topics, consumed.configure(this.createConfigurator()));
    }

    /**
     * @see StreamsBuilder#stream(Pattern)
     */
    public <K, V> KStreamX<K, V> stream(final Pattern topicPattern) {
        return this.getContext().wrap(this.streamsBuilder.stream(topicPattern));
    }

    /**
     * @see StreamsBuilder#stream(Pattern, Consumed)
     */
    public <K, V> KStreamX<K, V> stream(final Pattern topicPattern, final Consumed<K, V> consumed) {
        return this.getContext().wrap(this.streamsBuilder.stream(topicPattern, consumed));
    }

    /**
     * @see StreamsBuilder#stream(Pattern, Consumed)
     */
    public <K, V> KStreamX<K, V> stream(final Pattern topicPattern, final ConsumedX<K, V> consumed) {
        return this.stream(topicPattern, consumed.configure(this.createConfigurator()));
    }

    /**
     * Create a {@link KStreamX} from all {@link StreamsTopicConfig#getInputTopics()}
     * @param consumed define optional parameters for streaming topics
     * @return a {@link KStreamX} for all {@link StreamsTopicConfig#getInputTopics()}
     * @param <K> type of keys
     * @param <V> type of values
     * @see StreamsBuilder#stream(Collection, Consumed)
     */
    public <K, V> KStreamX<K, V> streamInput(final Consumed<K, V> consumed) {
        return this.stream(this.topics.getInputTopics(), consumed);
    }

    /**
     * Create a {@link KStreamX} from all {@link StreamsTopicConfig#getInputTopics()}
     * @param consumed define optional parameters for streaming topics
     * @return a {@link KStreamX} for all {@link StreamsTopicConfig#getInputTopics()}
     * @param <K> type of keys
     * @param <V> type of values
     * @see StreamsBuilder#stream(Collection, Consumed)
     */
    public <K, V> KStreamX<K, V> streamInput(final ConsumedX<K, V> consumed) {
        return this.streamInput(consumed.configure(this.createConfigurator()));
    }

    /**
     * Create a {@link KStreamX} from all {@link StreamsTopicConfig#getInputTopics()}
     * @return a {@link KStreamX} for all {@link StreamsTopicConfig#getInputTopics()}
     * @param <K> type of keys
     * @param <V> type of values
     * @see StreamsBuilder#stream(Collection)
     */
    public <K, V> KStreamX<K, V> streamInput() {
        return this.stream(this.topics.getInputTopics());
    }

    /**
     * Create a {@link KStreamX} from all {@link StreamsTopicConfig#getInputTopics(String)}
     * @param label label of input topics
     * @param consumed define optional parameters for streaming topics
     * @return a {@link KStreamX} for all {@link StreamsTopicConfig#getInputTopics(String)}
     * @param <K> type of keys
     * @param <V> type of values
     * @see StreamsBuilder#stream(Collection, Consumed)
     */
    public <K, V> KStreamX<K, V> streamInput(final String label, final Consumed<K, V> consumed) {
        return this.stream(this.topics.getInputTopics(label), consumed);
    }

    /**
     * Create a {@link KStreamX} from all {@link StreamsTopicConfig#getInputTopics(String)}
     * @param label label of input topics
     * @param consumed define optional parameters for streaming topics
     * @return a {@link KStreamX} for all {@link StreamsTopicConfig#getInputTopics(String)}
     * @param <K> type of keys
     * @param <V> type of values
     * @see StreamsBuilder#stream(Collection, Consumed)
     */
    public <K, V> KStreamX<K, V> streamInput(final String label, final ConsumedX<K, V> consumed) {
        return this.streamInput(label, consumed.configure(this.createConfigurator()));
    }

    /**
     * Create a {@link KStreamX} from all {@link StreamsTopicConfig#getInputTopics(String)}
     * @param label label of input topics
     * @return a {@link KStreamX} for all {@link StreamsTopicConfig#getInputTopics(String)}
     * @param <K> type of keys
     * @param <V> type of values
     * @see StreamsBuilder#stream(Collection)
     */
    public <K, V> KStreamX<K, V> streamInput(final String label) {
        return this.stream(this.topics.getInputTopics(label));
    }

    /**
     * Create a {@link KStreamX} from all topics matching {@link StreamsTopicConfig#getInputPattern()}
     * @param consumed define optional parameters for streaming topics
     * @return a {@link KStreamX} for all topics matching {@link StreamsTopicConfig#getInputPattern()}
     * @param <K> type of keys
     * @param <V> type of values
     * @see StreamsBuilder#stream(Pattern, Consumed)
     */
    public <K, V> KStreamX<K, V> streamInputPattern(final Consumed<K, V> consumed) {
        return this.stream(this.topics.getInputPattern(), consumed);
    }

    /**
     * Create a {@link KStreamX} from all topics matching {@link StreamsTopicConfig#getInputPattern()}
     * @param consumed define optional parameters for streaming topics
     * @return a {@link KStreamX} for all topics matching {@link StreamsTopicConfig#getInputPattern()}
     * @param <K> type of keys
     * @param <V> type of values
     * @see StreamsBuilder#stream(Pattern, Consumed)
     */
    public <K, V> KStreamX<K, V> streamInputPattern(final ConsumedX<K, V> consumed) {
        return this.streamInputPattern(consumed.configure(this.createConfigurator()));
    }

    /**
     * Create a {@link KStreamX} from all topics matching {@link StreamsTopicConfig#getInputPattern()}
     * @return a {@link KStreamX} for all topics matching {@link StreamsTopicConfig#getInputPattern()}
     * @param <K> type of keys
     * @param <V> type of values
     * @see StreamsBuilder#stream(Pattern)
     */
    public <K, V> KStreamX<K, V> streamInputPattern() {
        return this.stream(this.topics.getInputPattern());
    }

    /**
     * Create a {@link KStreamX} from all topics matching {@link StreamsTopicConfig#getInputPattern(String)}
     * @param label label of input pattern
     * @param consumed define optional parameters for streaming topics
     * @return a {@link KStreamX} for all topics matching {@link StreamsTopicConfig#getInputPattern(String)}
     * @param <K> type of keys
     * @param <V> type of values
     * @see StreamsBuilder#stream(Pattern, Consumed)
     */
    public <K, V> KStreamX<K, V> streamInputPattern(final String label, final Consumed<K, V> consumed) {
        return this.stream(this.topics.getInputPattern(label), consumed);
    }

    /**
     * Create a {@link KStreamX} from all topics matching {@link StreamsTopicConfig#getInputPattern(String)}
     * @param label label of input pattern
     * @param consumed define optional parameters for streaming topics
     * @return a {@link KStreamX} for all topics matching {@link StreamsTopicConfig#getInputPattern(String)}
     * @param <K> type of keys
     * @param <V> type of values
     * @see StreamsBuilder#stream(Pattern, Consumed)
     */
    public <K, V> KStreamX<K, V> streamInputPattern(final String label,
            final ConsumedX<K, V> consumed) {
        return this.streamInputPattern(label, consumed.configure(this.createConfigurator()));
    }

    /**
     * Create a {@link KStreamX} from all topics matching {@link StreamsTopicConfig#getInputPattern(String)}
     * @param label label of input pattern
     * @return a {@link KStreamX} for all topics matching {@link StreamsTopicConfig#getInputPattern(String)}
     * @param <K> type of keys
     * @param <V> type of values
     * @see StreamsBuilder#stream(Pattern)
     */
    public <K, V> KStreamX<K, V> streamInputPattern(final String label) {
        return this.stream(this.topics.getInputPattern(label));
    }

    /**
     * @see StreamsBuilder#table(String)
     */
    public <K, V> KTableX<K, V> table(final String topic) {
        return this.getContext().wrap(this.streamsBuilder.table(topic));
    }

    /**
     * @see StreamsBuilder#table(String, Consumed)
     */
    public <K, V> KTableX<K, V> table(final String topic, final Consumed<K, V> consumed) {
        return this.getContext().wrap(this.streamsBuilder.table(topic, consumed));
    }

    /**
     * @see StreamsBuilder#table(String, Consumed)
     */
    public <K, V> KTableX<K, V> table(final String topic, final ConsumedX<K, V> consumed) {
        return this.table(topic, consumed.configure(this.createConfigurator()));
    }

    /**
     * @see StreamsBuilder#table(String, Materialized)
     */
    public <K, V> KTableX<K, V> table(final String topic,
            final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.getContext().wrap(this.streamsBuilder.table(topic, materialized));
    }

    /**
     * @see StreamsBuilder#table(String, Materialized)
     */
    public <K, V> KTableX<K, V> table(final String topic,
            final MaterializedX<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.table(topic, materialized.configure(this.createConfigurator()));
    }

    /**
     * @see StreamsBuilder#table(String, Consumed, Materialized)
     */
    public <K, V> KTableX<K, V> table(final String topic, final Consumed<K, V> consumed,
            final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.getContext().wrap(this.streamsBuilder.table(topic, consumed, materialized));
    }

    /**
     * @see StreamsBuilder#table(String, Consumed, Materialized)
     */
    public <K, V> KTableX<K, V> table(final String topic, final ConsumedX<K, V> consumed,
            final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        final Configurator configurator = this.createConfigurator();
        return this.table(topic, consumed.configure(configurator), materialized);
    }

    /**
     * @see StreamsBuilder#globalTable(String)
     */
    public <K, V> GlobalKTable<K, V> globalTable(final String topic) {
        return this.streamsBuilder.globalTable(topic);
    }

    /**
     * @see StreamsBuilder#globalTable(String, Consumed)
     */
    public <K, V> GlobalKTable<K, V> globalTable(final String topic, final Consumed<K, V> consumed) {
        return this.streamsBuilder.globalTable(topic, consumed);
    }

    /**
     * @see StreamsBuilder#globalTable(String, Consumed)
     */
    public <K, V> GlobalKTable<K, V> globalTable(final String topic, final ConsumedX<K, V> consumed) {
        return this.globalTable(topic, consumed.configure(this.createConfigurator()));
    }

    /**
     * @see StreamsBuilder#globalTable(String, Materialized)
     */
    public <K, V> GlobalKTable<K, V> globalTable(final String topic,
            final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.streamsBuilder.globalTable(topic, materialized);
    }

    /**
     * @see StreamsBuilder#globalTable(String, Materialized)
     */
    public <K, V> GlobalKTable<K, V> globalTable(final String topic,
            final MaterializedX<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.globalTable(topic, materialized.configure(this.createConfigurator()));
    }

    /**
     * @see StreamsBuilder#globalTable(String, Consumed, Materialized)
     */
    public <K, V> GlobalKTable<K, V> globalTable(final String topic, final Consumed<K, V> consumed,
            final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.streamsBuilder.globalTable(topic, consumed, materialized);
    }

    /**
     * @see StreamsBuilder#globalTable(String, Consumed, Materialized)
     */
    public <K, V> GlobalKTable<K, V> globalTable(final String topic, final ConsumedX<K, V> consumed,
            final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        final Configurator configurator = this.createConfigurator();
        return this.globalTable(topic, consumed.configure(configurator), materialized);
    }

    /**
     * @see StreamsBuilder#addStateStore(StoreBuilder)
     */
    public StreamsBuilderX addStateStore(final StoreBuilder<?> builder) {
        this.streamsBuilder.addStateStore(builder);
        return this;
    }

    /**
     * @see StreamsBuilder#addGlobalStore(StoreBuilder, String, Consumed, ProcessorSupplier)
     */
    public <KIn, VIn> StreamsBuilderX addGlobalStore(final StoreBuilder<?> storeBuilder, final String topic,
            final Consumed<KIn, VIn> consumed, final ProcessorSupplier<KIn, VIn, Void, Void> stateUpdateSupplier) {
        this.streamsBuilder.addGlobalStore(storeBuilder, topic, consumed, stateUpdateSupplier);
        return this;
    }

    /**
     * @see StreamsBuilder#addGlobalStore(StoreBuilder, String, Consumed, ProcessorSupplier)
     */
    public <KIn, VIn> StreamsBuilderX addGlobalStore(final StoreBuilder<?> storeBuilder, final String topic,
            final ConsumedX<KIn, VIn> consumed, final ProcessorSupplier<KIn, VIn, Void, Void> stateUpdateSupplier) {
        return this.addGlobalStore(storeBuilder, topic, consumed.configure(this.createConfigurator()),
                stateUpdateSupplier);
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

    /**
     * Create a {@link StreamsContext} to wrap Kafka Streams interfaces
     * @return {@link StreamsContext}
     */
    public StreamsContext getContext() {
        return new StreamsContext(this.topics, this.createConfigurator());
    }

    /**
     * Create stores using application context to lazily configures Serdes
     * @return {@link StoresX}
     */
    public StoresX stores() {
        return new StoresX(this.createConfigurator());
    }

    Topology build() {
        return this.streamsBuilder.build();
    }
}
