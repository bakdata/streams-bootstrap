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

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
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

@RequiredArgsConstructor
class ImprovedKGroupedStreamImpl<K, V> implements ImprovedKGroupedStream<K, V> {

    @Getter(AccessLevel.PACKAGE)
    private final @NonNull KGroupedStream<K, V> wrapped;
    private final @NonNull StreamsContext context;

    @Override
    public ImprovedKTable<K, Long> count() {
        return this.context.wrap(this.wrapped.count());
    }

    @Override
    public ImprovedKTable<K, Long> count(final Named named) {
        return this.context.wrap(this.wrapped.count(named));
    }

    @Override
    public ImprovedKTable<K, Long> count(final Materialized<K, Long, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.context.wrap(this.wrapped.count(materialized));
    }

    @Override
    public ImprovedKTable<K, Long> count(
            final AutoMaterialized<K, Long, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.count(materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public ImprovedKTable<K, Long> count(final Named named,
            final Materialized<K, Long, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.context.wrap(this.wrapped.count(named, materialized));
    }

    @Override
    public ImprovedKTable<K, Long> count(final Named named,
            final AutoMaterialized<K, Long, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.count(named, materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public ImprovedKTable<K, V> reduce(final Reducer<V> reducer) {
        return this.context.wrap(this.wrapped.reduce(reducer));
    }

    @Override
    public ImprovedKTable<K, V> reduce(final Reducer<V> reducer,
            final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.context.wrap(this.wrapped.reduce(reducer, materialized));
    }

    @Override
    public ImprovedKTable<K, V> reduce(final Reducer<V> reducer,
            final AutoMaterialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.reduce(reducer, materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public ImprovedKTable<K, V> reduce(final Reducer<V> reducer, final Named named,
            final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.context.wrap(this.wrapped.reduce(reducer, named, materialized));
    }

    @Override
    public ImprovedKTable<K, V> reduce(final Reducer<V> reducer, final Named named,
            final AutoMaterialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.reduce(reducer, named, materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public <VR> ImprovedKTable<K, VR> aggregate(final Initializer<VR> initializer,
            final Aggregator<? super K, ? super V, VR> aggregator) {
        return this.context.wrap(this.wrapped.aggregate(initializer, aggregator));
    }

    @Override
    public <VR> ImprovedKTable<K, VR> aggregate(final Initializer<VR> initializer,
            final Aggregator<? super K, ? super V, VR> aggregator,
            final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.context.wrap(this.wrapped.aggregate(initializer, aggregator, materialized));
    }

    @Override
    public <VR> ImprovedKTable<K, VR> aggregate(final Initializer<VR> initializer,
            final Aggregator<? super K, ? super V, VR> aggregator,
            final AutoMaterialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.aggregate(initializer, aggregator, materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public <VR> ImprovedKTable<K, VR> aggregate(final Initializer<VR> initializer,
            final Aggregator<? super K, ? super V, VR> aggregator, final Named named,
            final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.context.wrap(this.wrapped.aggregate(initializer, aggregator, named, materialized));
    }

    @Override
    public <VR> ImprovedKTable<K, VR> aggregate(final Initializer<VR> initializer,
            final Aggregator<? super K, ? super V, VR> aggregator, final Named named,
            final AutoMaterialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.aggregate(initializer, aggregator, named, materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public <W extends Window> ImprovedTimeWindowedKStream<K, V> windowedBy(final Windows<W> windows) {
        return this.context.wrap(this.wrapped.windowedBy(windows));
    }

    @Override
    public ImprovedTimeWindowedKStream<K, V> windowedBy(final SlidingWindows windows) {
        return this.context.wrap(this.wrapped.windowedBy(windows));
    }

    @Override
    public ImprovedSessionWindowedKStream<K, V> windowedBy(final SessionWindows windows) {
        return this.context.wrap(this.wrapped.windowedBy(windows));
    }

    @Override
    public <VOut> CogroupedKStreamX<K, VOut> cogroup(final Aggregator<? super K, ? super V, VOut> aggregator) {
        return this.context.wrap(this.wrapped.cogroup(aggregator));
    }
}
