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
 * Extends the {@link SessionWindowedKStream} interface by adding methods to simplify Serde configuration, error
 * handling, and topic access
 * @param <K> type of keys
 * @param <V> type of values
 */
public interface SessionWindowedKStreamX<K, V> extends SessionWindowedKStream<K, V> {

    @Override
    KTableX<Windowed<K>, Long> count();

    @Override
    KTableX<Windowed<K>, Long> count(Named named);

    @Override
    KTableX<Windowed<K>, Long> count(Materialized<K, Long, SessionStore<Bytes, byte[]>> materialized);

    /**
     * @see #count(Materialized)
     */
    KTableX<Windowed<K>, Long> count(MaterializedX<K, Long, SessionStore<Bytes, byte[]>> materialized);

    @Override
    KTableX<Windowed<K>, Long> count(Named named,
            Materialized<K, Long, SessionStore<Bytes, byte[]>> materialized);

    /**
     * @see #count(Named, Materialized)
     */
    KTableX<Windowed<K>, Long> count(Named named,
            MaterializedX<K, Long, SessionStore<Bytes, byte[]>> materialized);

    @Override
    <VR> KTableX<Windowed<K>, VR> aggregate(Initializer<VR> initializer,
            Aggregator<? super K, ? super V, VR> aggregator,
            Merger<? super K, VR> sessionMerger);

    @Override
    <VR> KTableX<Windowed<K>, VR> aggregate(Initializer<VR> initializer,
            Aggregator<? super K, ? super V, VR> aggregator,
            Merger<? super K, VR> sessionMerger, Named named);

    @Override
    <VR> KTableX<Windowed<K>, VR> aggregate(Initializer<VR> initializer,
            Aggregator<? super K, ? super V, VR> aggregator,
            Merger<? super K, VR> sessionMerger, Materialized<K, VR, SessionStore<Bytes, byte[]>> materialized);

    /**
     * @see #aggregate(Initializer, Aggregator, Merger, Materialized)
     */
    <VR> KTableX<Windowed<K>, VR> aggregate(Initializer<VR> initializer,
            Aggregator<? super K, ? super V, VR> aggregator,
            Merger<? super K, VR> sessionMerger,
            MaterializedX<K, VR, SessionStore<Bytes, byte[]>> materialized);

    @Override
    <VR> KTableX<Windowed<K>, VR> aggregate(Initializer<VR> initializer,
            Aggregator<? super K, ? super V, VR> aggregator,
            Merger<? super K, VR> sessionMerger, Named named,
            Materialized<K, VR, SessionStore<Bytes, byte[]>> materialized);

    /**
     * @see #aggregate(Initializer, Aggregator, Merger, Named, Materialized)
     */
    <VR> KTableX<Windowed<K>, VR> aggregate(Initializer<VR> initializer,
            Aggregator<? super K, ? super V, VR> aggregator,
            Merger<? super K, VR> sessionMerger, Named named,
            MaterializedX<K, VR, SessionStore<Bytes, byte[]>> materialized);

    @Override
    KTableX<Windowed<K>, V> reduce(Reducer<V> reducer);

    @Override
    KTableX<Windowed<K>, V> reduce(Reducer<V> reducer, Named named);

    @Override
    KTableX<Windowed<K>, V> reduce(Reducer<V> reducer,
            Materialized<K, V, SessionStore<Bytes, byte[]>> materialized);

    /**
     * @see #reduce(Reducer, Materialized)
     */
    KTableX<Windowed<K>, V> reduce(Reducer<V> reducer,
            MaterializedX<K, V, SessionStore<Bytes, byte[]>> materialized);

    @Override
    KTableX<Windowed<K>, V> reduce(Reducer<V> reducer, Named named,
            Materialized<K, V, SessionStore<Bytes, byte[]>> materialized);

    /**
     * @see #reduce(Reducer, Named, Materialized)
     */
    KTableX<Windowed<K>, V> reduce(Reducer<V> reducer, Named named,
            MaterializedX<K, V, SessionStore<Bytes, byte[]>> materialized);

    @Override
    SessionWindowedKStreamX<K, V> emitStrategy(EmitStrategy emitStrategy);
}
