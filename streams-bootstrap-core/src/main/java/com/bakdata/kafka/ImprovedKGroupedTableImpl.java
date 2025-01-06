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

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.state.KeyValueStore;

@RequiredArgsConstructor
class ImprovedKGroupedTableImpl<K, V> implements ImprovedKGroupedTable<K, V> {

    private final @NonNull KGroupedTable<K, V> wrapped;
    private final @NonNull StreamsContext context;

    @Override
    public ImprovedKTable<K, Long> count(final Materialized<K, Long, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.context.newTable(this.wrapped.count(materialized));
    }

    @Override
    public ImprovedKTable<K, Long> count(final Named named,
            final Materialized<K, Long, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.context.newTable(this.wrapped.count(named, materialized));
    }

    @Override
    public ImprovedKTable<K, Long> count() {
        return this.context.newTable(this.wrapped.count());
    }

    @Override
    public ImprovedKTable<K, Long> count(final Named named) {
        return this.context.newTable(this.wrapped.count(named));
    }

    @Override
    public ImprovedKTable<K, V> reduce(final Reducer<V> adder, final Reducer<V> subtractor,
            final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.context.newTable(this.wrapped.reduce(adder, subtractor, materialized));
    }

    @Override
    public ImprovedKTable<K, V> reduce(final Reducer<V> adder, final Reducer<V> subtractor, final Named named,
            final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.context.newTable(this.wrapped.reduce(adder, subtractor, materialized));
    }

    @Override
    public ImprovedKTable<K, V> reduce(final Reducer<V> adder, final Reducer<V> subtractor) {
        return this.context.newTable(this.wrapped.reduce(adder, subtractor));
    }

    @Override
    public <VR> ImprovedKTable<K, VR> aggregate(final Initializer<VR> initializer,
            final Aggregator<? super K, ? super V, VR> adder,
            final Aggregator<? super K, ? super V, VR> subtractor,
            final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.context.newTable(this.wrapped.aggregate(initializer, adder, subtractor, materialized));
    }

    @Override
    public <VR> ImprovedKTable<K, VR> aggregate(final Initializer<VR> initializer,
            final Aggregator<? super K, ? super V, VR> adder,
            final Aggregator<? super K, ? super V, VR> subtractor, final Named named,
            final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.context.newTable(this.wrapped.aggregate(initializer, adder, subtractor, materialized));
    }

    @Override
    public <VR> ImprovedKTable<K, VR> aggregate(final Initializer<VR> initializer,
            final Aggregator<? super K, ? super V, VR> adder,
            final Aggregator<? super K, ? super V, VR> subtractor) {
        return this.context.newTable(this.wrapped.aggregate(initializer, adder, subtractor));
    }

    @Override
    public <VR> ImprovedKTable<K, VR> aggregate(final Initializer<VR> initializer,
            final Aggregator<? super K, ? super V, VR> adder,
            final Aggregator<? super K, ? super V, VR> subtractor, final Named named) {
        return this.context.newTable(this.wrapped.aggregate(initializer, adder, subtractor, named));
    }
}
