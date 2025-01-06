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
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;

public interface ImprovedTimeWindowedKStream<K, V> extends TimeWindowedKStream<K, V> {

    @Override
    ImprovedKTable<Windowed<K>, Long> count();

    @Override
    ImprovedKTable<Windowed<K>, Long> count(Named named);

    @Override
    ImprovedKTable<Windowed<K>, Long> count(Materialized<K, Long, WindowStore<Bytes, byte[]>> materialized);

    @Override
    ImprovedKTable<Windowed<K>, Long> count(Named named,
            Materialized<K, Long, WindowStore<Bytes, byte[]>> materialized);

    @Override
    <VR> ImprovedKTable<Windowed<K>, VR> aggregate(Initializer<VR> initializer,
            Aggregator<? super K, ? super V, VR> aggregator);

    @Override
    <VR> ImprovedKTable<Windowed<K>, VR> aggregate(Initializer<VR> initializer,
            Aggregator<? super K, ? super V, VR> aggregator,
            Named named);

    @Override
    <VR> ImprovedKTable<Windowed<K>, VR> aggregate(Initializer<VR> initializer,
            Aggregator<? super K, ? super V, VR> aggregator,
            Materialized<K, VR, WindowStore<Bytes, byte[]>> materialized);

    @Override
    <VR> ImprovedKTable<Windowed<K>, VR> aggregate(Initializer<VR> initializer,
            Aggregator<? super K, ? super V, VR> aggregator,
            Named named, Materialized<K, VR, WindowStore<Bytes, byte[]>> materialized);

    @Override
    ImprovedKTable<Windowed<K>, V> reduce(Reducer<V> reducer);

    @Override
    ImprovedKTable<Windowed<K>, V> reduce(Reducer<V> reducer, Named named);

    @Override
    ImprovedKTable<Windowed<K>, V> reduce(Reducer<V> reducer,
            Materialized<K, V, WindowStore<Bytes, byte[]>> materialized);

    @Override
    ImprovedKTable<Windowed<K>, V> reduce(Reducer<V> reducer, Named named,
            Materialized<K, V, WindowStore<Bytes, byte[]>> materialized);

    @Override
    ImprovedTimeWindowedKStream<K, V> emitStrategy(EmitStrategy emitStrategy);
}
