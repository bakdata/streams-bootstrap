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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Extends the {@code KGroupedStream} interface by adding methods to simplify Serde configuration, error handling,
 * and topic access
 */
public interface ImprovedKGroupedStream<K, V> extends KGroupedStream<K, V> {

    @Override
    ImprovedKTable<K, Long> count();

    @Override
    ImprovedKTable<K, Long> count(Named named);

    @Override
    ImprovedKTable<K, Long> count(Materialized<K, Long, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * Count the number of records in this stream by the grouped key
     * @param materialized an instance of {@code ConfiguredMaterialized} used to materialize a state store
     * @return a {@code KTable} that contains "update" records with unmodified keys and {@code Long} values that
     * represent the latest (rolling) count (i.e., number of records) for each key
     * @see #count(Materialized)
     */
    ImprovedKTable<K, Long> count(ConfiguredMaterialized<K, Long, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    ImprovedKTable<K, Long> count(Named named, Materialized<K, Long, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * Count the number of records in this stream by the grouped key
     * @param named a {@code Named} config used to name the processor in the topology
     * @param materialized an instance of {@code ConfiguredMaterialized} used to materialize a state store
     * @return a {@code KTable} that contains "update" records with unmodified keys and {@code Long} values that
     * represent the latest (rolling) count (i.e., number of records) for each key
     * @see #count(Named, Materialized)
     */
    ImprovedKTable<K, Long> count(Named named,
            ConfiguredMaterialized<K, Long, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    ImprovedKTable<K, V> reduce(Reducer<V> reducer);

    @Override
    ImprovedKTable<K, V> reduce(Reducer<V> reducer, Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * Combine the value of records in this stream by the grouped key
     * @param reducer a {@code Reducer} that computes a new aggregate result
     * @param materialized an instance of {@code ConfiguredMaterialized} used to materialize a state store
     * @return a {@code KTable} that contains "update" records with unmodified keys, and values that represent the
     * latest (rolling) aggregate for each key
     * @see #reduce(Reducer, Materialized)
     */
    ImprovedKTable<K, V> reduce(Reducer<V> reducer,
            ConfiguredMaterialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    ImprovedKTable<K, V> reduce(Reducer<V> reducer, Named named,
            Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * Combine the value of records in this stream by the grouped key
     * @param reducer a {@code Reducer} that computes a new aggregate result
     * @param named a {@code Named} config used to name the processor in the topology
     * @param materialized an instance of {@code ConfiguredMaterialized} used to materialize a state store
     * @return a {@code KTable} that contains "update" records with unmodified keys, and values that represent the
     * latest (rolling) aggregate for each key
     * @see #reduce(Reducer, Named, Materialized)
     */
    ImprovedKTable<K, V> reduce(Reducer<V> reducer, Named named,
            ConfiguredMaterialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    <VR> ImprovedKTable<K, VR> aggregate(Initializer<VR> initializer, Aggregator<? super K, ? super V, VR> aggregator);

    @Override
    <VR> ImprovedKTable<K, VR> aggregate(Initializer<VR> initializer, Aggregator<? super K, ? super V, VR> aggregator,
            Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * Aggregate the values of records in this stream by the grouped key
     * @param initializer an {@code Initializer} that computes an initial intermediate aggregation result
     * @param aggregator an {@code Aggregator} that computes a new aggregate result
     * @param materialized an instance of {@code ConfiguredMaterialized} used to materialize a state store
     * @return a {@code KTable} that contains "update" records with unmodified keys, and values that represent the
     * latest (rolling) aggregate for each key
     * @param <VR> the value type of the resulting {@code KTable}
     * @see #aggregate(Initializer, Aggregator, Materialized)
     */
    <VR> ImprovedKTable<K, VR> aggregate(Initializer<VR> initializer, Aggregator<? super K, ? super V, VR> aggregator,
            ConfiguredMaterialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    <VR> ImprovedKTable<K, VR> aggregate(Initializer<VR> initializer, Aggregator<? super K, ? super V, VR> aggregator,
            Named named, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * Aggregate the values of records in this stream by the grouped key
     * @param initializer an {@code Initializer} that computes an initial intermediate aggregation result
     * @param aggregator an {@code Aggregator} that computes a new aggregate result
     * @param named a {@code Named} config used to name the processor in the topology
     * @param materialized an instance of {@code ConfiguredMaterialized} used to materialize a state store
     * @return a {@code KTable} that contains "update" records with unmodified keys, and values that represent the
     * latest (rolling) aggregate for each key
     * @param <VR> the value type of the resulting {@code KTable}
     * @see #aggregate(Initializer, Aggregator, Named, Materialized)
     */
    <VR> ImprovedKTable<K, VR> aggregate(Initializer<VR> initializer, Aggregator<? super K, ? super V, VR> aggregator,
            Named named, ConfiguredMaterialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    <W extends Window> ImprovedTimeWindowedKStream<K, V> windowedBy(Windows<W> windows);

    @Override
    ImprovedTimeWindowedKStream<K, V> windowedBy(SlidingWindows windows);

    @Override
    ImprovedSessionWindowedKStream<K, V> windowedBy(SessionWindows windows);

    @Override
    <VOut> ImprovedCogroupedKStream<K, VOut> cogroup(Aggregator<? super K, ? super V, VOut> aggregator);
}
