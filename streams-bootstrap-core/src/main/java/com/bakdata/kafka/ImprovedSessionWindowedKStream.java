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
import org.apache.kafka.streams.kstream.EmitStrategy;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.SessionWindowedKStream;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.SessionStore;

/**
 * Extends the {@code SessionWindowedKStream} interface by adding methods to simplify Serde configuration, error
 * handling, and topic access
 */
public interface ImprovedSessionWindowedKStream<K, V> extends SessionWindowedKStream<K, V> {

    @Override
    ImprovedKTable<Windowed<K>, Long> count();

    @Override
    ImprovedKTable<Windowed<K>, Long> count(Named named);

    @Override
    ImprovedKTable<Windowed<K>, Long> count(Materialized<K, Long, SessionStore<Bytes, byte[]>> materialized);

    /**
     * Count the number of records in this stream by the grouped key and defined sessions
     * @param materialized an instance of {@code ConfiguredMaterialized} used to materialize a state store
     * @return a windowed {@code KTable} that contains "update" records with unmodified keys and {@code Long} values
     * that represent
     * the latest (rolling) count (i.e., number of records) for each key per session
     * @see #count(Materialized)
     */
    ImprovedKTable<Windowed<K>, Long> count(ConfiguredMaterialized<K, Long, SessionStore<Bytes, byte[]>> materialized);

    @Override
    ImprovedKTable<Windowed<K>, Long> count(Named named,
            Materialized<K, Long, SessionStore<Bytes, byte[]>> materialized);

    /**
     * Count the number of records in this stream by the grouped key and defined sessions
     * @param named a {@code Named} config used to name the processor in the topology
     * @param materialized an instance of {@code ConfiguredMaterialized} used to materialize a state store
     * @return a windowed {@code KTable} that contains "update" records with unmodified keys and {@code Long} values
     * that represent
     * the latest (rolling) count (i.e., number of records) for each key per session
     * @see #count(Named, Materialized)
     */
    ImprovedKTable<Windowed<K>, Long> count(Named named,
            ConfiguredMaterialized<K, Long, SessionStore<Bytes, byte[]>> materialized);

    @Override
    <VR> ImprovedKTable<Windowed<K>, VR> aggregate(Initializer<VR> initializer,
            Aggregator<? super K, ? super V, VR> aggregator,
            Merger<? super K, VR> sessionMerger);

    @Override
    <VR> ImprovedKTable<Windowed<K>, VR> aggregate(Initializer<VR> initializer,
            Aggregator<? super K, ? super V, VR> aggregator,
            Merger<? super K, VR> sessionMerger, Named named);

    @Override
    <VR> ImprovedKTable<Windowed<K>, VR> aggregate(Initializer<VR> initializer,
            Aggregator<? super K, ? super V, VR> aggregator,
            Merger<? super K, VR> sessionMerger, Materialized<K, VR, SessionStore<Bytes, byte[]>> materialized);

    /**
     * Aggregate the values of records in this stream by the grouped key and defined sessions
     * @param initializer an {@code Initializer} that computes an initial intermediate aggregation result
     * @param aggregator an {@code Aggregator} that computes a new aggregate result
     * @param sessionMerger a {@code Merger} that combines two aggregation results
     * @param materialized a {@code ConfiguredMaterialized} config used to materialize a state store
     * @return a windowed {@code KTable} that contains "update" records with unmodified keys, and values that
     * represent the latest (rolling) aggregate for each key per session
     * @param <VR> the value type of the resulting {@code KTable}
     * @see #aggregate(Initializer, Aggregator, Merger, Materialized)
     */
    <VR> ImprovedKTable<Windowed<K>, VR> aggregate(Initializer<VR> initializer,
            Aggregator<? super K, ? super V, VR> aggregator,
            Merger<? super K, VR> sessionMerger,
            ConfiguredMaterialized<K, VR, SessionStore<Bytes, byte[]>> materialized);

    @Override
    <VR> ImprovedKTable<Windowed<K>, VR> aggregate(Initializer<VR> initializer,
            Aggregator<? super K, ? super V, VR> aggregator,
            Merger<? super K, VR> sessionMerger, Named named,
            Materialized<K, VR, SessionStore<Bytes, byte[]>> materialized);

    /**
     * Aggregate the values of records in this stream by the grouped key and defined sessions
     * @param initializer an {@code Initializer} that computes an initial intermediate aggregation result
     * @param aggregator an {@code Aggregator} that computes a new aggregate result
     * @param sessionMerger a {@code Merger} that combines two aggregation results
     * @param named a {@code Named} config used to name the processor in the topology
     * @param materialized a {@code ConfiguredMaterialized} config used to materialize a state store
     * @return a windowed {@code KTable} that contains "update" records with unmodified keys, and values that
     * represent the latest (rolling) aggregate for each key per session
     * @param <VR> the value type of the resulting {@code KTable}
     * @see #aggregate(Initializer, Aggregator, Merger, Named, Materialized)
     */
    <VR> ImprovedKTable<Windowed<K>, VR> aggregate(Initializer<VR> initializer,
            Aggregator<? super K, ? super V, VR> aggregator,
            Merger<? super K, VR> sessionMerger, Named named,
            ConfiguredMaterialized<K, VR, SessionStore<Bytes, byte[]>> materialized);

    @Override
    ImprovedKTable<Windowed<K>, V> reduce(Reducer<V> reducer);

    @Override
    ImprovedKTable<Windowed<K>, V> reduce(Reducer<V> reducer, Named named);

    @Override
    ImprovedKTable<Windowed<K>, V> reduce(Reducer<V> reducer,
            Materialized<K, V, SessionStore<Bytes, byte[]>> materialized);

    /**
     * Combine the values of records in this stream by the grouped key and defined sessions
     * @param reducer a {@code Reducer} that computes a new aggregate result
     * @param materialized a {@code ConfiguredMaterialized} config used to materialize a state store
     * @return a windowed {@code KTable} that contains "update" records with unmodified keys, and values that
     * represent the
     * latest (rolling) aggregate for each key per session
     * @see #reduce(Reducer, Materialized)
     */
    ImprovedKTable<Windowed<K>, V> reduce(Reducer<V> reducer,
            ConfiguredMaterialized<K, V, SessionStore<Bytes, byte[]>> materialized);

    @Override
    ImprovedKTable<Windowed<K>, V> reduce(Reducer<V> reducer, Named named,
            Materialized<K, V, SessionStore<Bytes, byte[]>> materialized);

    /**
     * Combine the values of records in this stream by the grouped key and defined sessions
     * @param reducer a {@code Reducer} that computes a new aggregate result
     * @param named a {@code Named} config used to name the processor in the topology
     * @param materialized a {@code ConfiguredMaterialized} config used to materialize a state store
     * @return a windowed {@code KTable} that contains "update" records with unmodified keys, and values that
     * represent the
     * latest (rolling) aggregate for each key per session
     * @see #reduce(Reducer, Named, Materialized)
     */
    ImprovedKTable<Windowed<K>, V> reduce(Reducer<V> reducer, Named named,
            ConfiguredMaterialized<K, V, SessionStore<Bytes, byte[]>> materialized);

    @Override
    ImprovedSessionWindowedKStream<K, V> emitStrategy(EmitStrategy emitStrategy);
}
