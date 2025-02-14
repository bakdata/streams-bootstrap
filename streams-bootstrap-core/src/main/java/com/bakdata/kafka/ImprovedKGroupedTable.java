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
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Extends the {@code KGroupedTable} interface by adding methods to simplify Serde configuration, error handling, and
 * topic access
 * @param <K> type of keys
 * @param <V> type of values
 */
public interface ImprovedKGroupedTable<K, V> extends KGroupedTable<K, V> {

    @Override
    ImprovedKTable<K, Long> count(Materialized<K, Long, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * Count number of records of the original {@code KTable} that got mapped to the same key into a new instance of
     * {@code KTable}
     * @param materialized the instance of {@code ConfiguredMaterialized} used to materialize the state store
     * @return a {@code KTable} that contains "update" records with unmodified keys and Long values that represent the
     * latest (rolling) count (i.e., number of records) for each key
     * @see #count(Materialized)
     */
    ImprovedKTable<K, Long> count(ConfiguredMaterialized<K, Long, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    ImprovedKTable<K, Long> count(Named named, Materialized<K, Long, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * Count number of records of the original {@code KTable} that got mapped to the same key into a new instance of
     * {@code KTable}
     * @param named the {@code Named} config used to name the processor in the topology
     * @param materialized the instance of {@code ConfiguredMaterialized} used to materialize the state store
     * @return a {@code KTable} that contains "update" records with unmodified keys and Long values that represent the
     * latest (rolling) count (i.e., number of records) for each key
     * @see #count(Named, Materialized)
     */
    ImprovedKTable<K, Long> count(Named named,
            ConfiguredMaterialized<K, Long, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    ImprovedKTable<K, Long> count();

    @Override
    ImprovedKTable<K, Long> count(Named named);

    @Override
    ImprovedKTable<K, V> reduce(Reducer<V> adder, Reducer<V> subtractor,
            Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * Combine the value of records of the original {@code KTable} that got mapped to the same key into a new
     * instance of {@code KTable}
     * @param adder a {@code Reducer} that adds a new value to the aggregate result
     * @param subtractor a {@code Reducer} that removed an old value from the aggregate result
     * @param materialized the instance of {@code ConfiguredMaterialized} used to materialize the state store
     * @return a {@code KTable} that contains "update" records with unmodified keys, and values that represent the
     * latest (rolling) aggregate for each key
     * @see #reduce(Reducer, Reducer, Materialized)
     */
    ImprovedKTable<K, V> reduce(Reducer<V> adder, Reducer<V> subtractor,
            ConfiguredMaterialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    ImprovedKTable<K, V> reduce(Reducer<V> adder, Reducer<V> subtractor, Named named,
            Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * Combine the value of records of the original {@code KTable} that got mapped to the same key into a new
     * instance of {@code KTable}
     * @param adder a {@code Reducer} that adds a new value to the aggregate result
     * @param subtractor a {@code Reducer} that removed an old value from the aggregate result
     * @param named a {@code Named} config used to name the processor in the topology
     * @param materialized the instance of {@code ConfiguredMaterialized} used to materialize the state store
     * @return a {@code KTable} that contains "update" records with unmodified keys, and values that represent the
     * latest (rolling) aggregate for each key
     * @see #reduce(Reducer, Reducer, Named, Materialized)
     */
    ImprovedKTable<K, V> reduce(Reducer<V> adder, Reducer<V> subtractor, Named named,
            ConfiguredMaterialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    ImprovedKTable<K, V> reduce(Reducer<V> adder, Reducer<V> subtractor);

    @Override
    <VR> ImprovedKTable<K, VR> aggregate(Initializer<VR> initializer, Aggregator<? super K, ? super V, VR> adder,
            Aggregator<? super K, ? super V, VR> subtractor,
            Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * Aggregate the value of records of the original {@code KTable} that got mapped to the same key into a new
     * instance of {@code KTable}
     * @param initializer an {@code Initializer} that provides an initial aggregate result value
     * @param adder an {@code Aggregator} that adds a new record to the aggregate result
     * @param subtractor an {@code Aggregator} that removed an old record from the aggregate result
     * @param materialized the instance of {@code ConfiguredMaterialized} used to materialize the state store
     * @return a {@code KTable} that contains "update" records with unmodified keys, and values that represent the
     * latest (rolling) aggregate for each key
     * @param <VR> the value type of the aggregated {@code KTable}
     * @see #aggregate(Initializer, Aggregator, Aggregator, Materialized)
     */
    <VR> ImprovedKTable<K, VR> aggregate(Initializer<VR> initializer, Aggregator<? super K, ? super V, VR> adder,
            Aggregator<? super K, ? super V, VR> subtractor,
            ConfiguredMaterialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    <VR> ImprovedKTable<K, VR> aggregate(Initializer<VR> initializer, Aggregator<? super K, ? super V, VR> adder,
            Aggregator<? super K, ? super V, VR> subtractor, Named named,
            Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * Aggregate the value of records of the original {@code KTable} that got mapped to the same key into a new
     * instance of {@code KTable}
     * @param initializer an {@code Initializer} that provides an initial aggregate result value
     * @param adder an {@code Aggregator} that adds a new record to the aggregate result
     * @param subtractor an {@code Aggregator} that removed an old record from the aggregate result
     * @param named a {@code Named} config used to name the processor in the topology
     * @param materialized the instance of {@code ConfiguredMaterialized} used to materialize the state store
     * @return a {@code KTable} that contains "update" records with unmodified keys, and values that represent the
     * latest (rolling) aggregate for each key
     * @param <VR> the value type of the aggregated {@code KTable}
     * @see #aggregate(Initializer, Aggregator, Aggregator, Named, Materialized)
     */
    <VR> ImprovedKTable<K, VR> aggregate(Initializer<VR> initializer, Aggregator<? super K, ? super V, VR> adder,
            Aggregator<? super K, ? super V, VR> subtractor, Named named,
            ConfiguredMaterialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    <VR> ImprovedKTable<K, VR> aggregate(Initializer<VR> initializer, Aggregator<? super K, ? super V, VR> adder,
            Aggregator<? super K, ? super V, VR> subtractor);

    @Override
    <VR> ImprovedKTable<K, VR> aggregate(Initializer<VR> initializer, Aggregator<? super K, ? super V, VR> adder,
            Aggregator<? super K, ? super V, VR> subtractor, Named named);
}
