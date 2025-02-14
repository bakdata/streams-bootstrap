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
 * @param <K> type of keys
 * @param <V> type of values
 */
public interface KGroupedStreamX<K, V> extends KGroupedStream<K, V> {

    @Override
    KTableX<K, Long> count();

    @Override
    KTableX<K, Long> count(Named named);

    @Override
    KTableX<K, Long> count(Materialized<K, Long, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #count(Materialized)
     */
    KTableX<K, Long> count(AutoMaterialized<K, Long, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    KTableX<K, Long> count(Named named, Materialized<K, Long, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #count(Named, Materialized)
     */
    KTableX<K, Long> count(Named named,
            AutoMaterialized<K, Long, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    KTableX<K, V> reduce(Reducer<V> reducer);

    @Override
    KTableX<K, V> reduce(Reducer<V> reducer, Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #reduce(Reducer, Materialized)
     */
    KTableX<K, V> reduce(Reducer<V> reducer,
            AutoMaterialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    KTableX<K, V> reduce(Reducer<V> reducer, Named named,
            Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #reduce(Reducer, Named, Materialized)
     */
    KTableX<K, V> reduce(Reducer<V> reducer, Named named,
            AutoMaterialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    <VR> KTableX<K, VR> aggregate(Initializer<VR> initializer, Aggregator<? super K, ? super V, VR> aggregator);

    @Override
    <VR> KTableX<K, VR> aggregate(Initializer<VR> initializer, Aggregator<? super K, ? super V, VR> aggregator,
            Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #aggregate(Initializer, Aggregator, Materialized)
     */
    <VR> KTableX<K, VR> aggregate(Initializer<VR> initializer, Aggregator<? super K, ? super V, VR> aggregator,
            AutoMaterialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    <VR> KTableX<K, VR> aggregate(Initializer<VR> initializer, Aggregator<? super K, ? super V, VR> aggregator,
            Named named, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #aggregate(Initializer, Aggregator, Named, Materialized)
     */
    <VR> KTableX<K, VR> aggregate(Initializer<VR> initializer, Aggregator<? super K, ? super V, VR> aggregator,
            Named named, AutoMaterialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    <W extends Window> TimeWindowedKStreamX<K, V> windowedBy(Windows<W> windows);

    @Override
    TimeWindowedKStreamX<K, V> windowedBy(SlidingWindows windows);

    @Override
    SessionWindowedKStreamX<K, V> windowedBy(SessionWindows windows);

    @Override
    <VOut> CogroupedKStreamX<K, VOut> cogroup(Aggregator<? super K, ? super V, VOut> aggregator);
}
