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

package com.bakdata.kafka.streams.kstream;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Extends the {@link KGroupedTable} interface by adding methods to simplify Serde configuration, error handling, and
 * topic access
 * @param <K> type of keys
 * @param <V> type of values
 */
public interface KGroupedTableX<K, V> extends KGroupedTable<K, V> {

    @Override
    KTableX<K, Long> count(Materialized<K, Long, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #count(Materialized)
     */
    KTableX<K, Long> count(MaterializedX<K, Long, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    KTableX<K, Long> count(Named named, Materialized<K, Long, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #count(Named, Materialized)
     */
    KTableX<K, Long> count(Named named,
            MaterializedX<K, Long, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    KTableX<K, Long> count();

    @Override
    KTableX<K, Long> count(Named named);

    @Override
    KTableX<K, V> reduce(Reducer<V> adder, Reducer<V> subtractor,
            Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #reduce(Reducer, Reducer, Materialized)
     */
    KTableX<K, V> reduce(Reducer<V> adder, Reducer<V> subtractor,
            MaterializedX<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    KTableX<K, V> reduce(Reducer<V> adder, Reducer<V> subtractor, Named named,
            Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #reduce(Reducer, Reducer, Named, Materialized)
     */
    KTableX<K, V> reduce(Reducer<V> adder, Reducer<V> subtractor, Named named,
            MaterializedX<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    KTableX<K, V> reduce(Reducer<V> adder, Reducer<V> subtractor);

    @Override
    <VR> KTableX<K, VR> aggregate(Initializer<VR> initializer, Aggregator<? super K, ? super V, VR> adder,
            Aggregator<? super K, ? super V, VR> subtractor,
            Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #aggregate(Initializer, Aggregator, Aggregator, Materialized)
     */
    <VR> KTableX<K, VR> aggregate(Initializer<VR> initializer, Aggregator<? super K, ? super V, VR> adder,
            Aggregator<? super K, ? super V, VR> subtractor,
            MaterializedX<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    <VR> KTableX<K, VR> aggregate(Initializer<VR> initializer, Aggregator<? super K, ? super V, VR> adder,
            Aggregator<? super K, ? super V, VR> subtractor, Named named,
            Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #aggregate(Initializer, Aggregator, Aggregator, Named, Materialized)
     */
    <VR> KTableX<K, VR> aggregate(Initializer<VR> initializer, Aggregator<? super K, ? super V, VR> adder,
            Aggregator<? super K, ? super V, VR> subtractor, Named named,
            MaterializedX<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    <VR> KTableX<K, VR> aggregate(Initializer<VR> initializer, Aggregator<? super K, ? super V, VR> adder,
            Aggregator<? super K, ? super V, VR> subtractor);

    @Override
    <VR> KTableX<K, VR> aggregate(Initializer<VR> initializer, Aggregator<? super K, ? super V, VR> adder,
            Aggregator<? super K, ? super V, VR> subtractor, Named named);
}
