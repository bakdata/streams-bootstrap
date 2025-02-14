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
import org.apache.kafka.streams.kstream.EmitStrategy;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.SessionWindowedKStream;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.SessionStore;

@RequiredArgsConstructor
class ImprovedSessionWindowedStreamImpl<K, V> implements ImprovedSessionWindowedKStream<K, V> {

    private final @NonNull SessionWindowedKStream<K, V> wrapped;
    private final @NonNull StreamsContext context;

    @Override
    public ImprovedKTable<Windowed<K>, Long> count() {
        return this.context.wrap(this.wrapped.count());
    }

    @Override
    public ImprovedKTable<Windowed<K>, Long> count(final Named named) {
        return this.context.wrap(this.wrapped.count(named));
    }

    @Override
    public ImprovedKTable<Windowed<K>, Long> count(
            final Materialized<K, Long, SessionStore<Bytes, byte[]>> materialized) {
        return this.context.wrap(this.wrapped.count(materialized));
    }

    @Override
    public ImprovedKTable<Windowed<K>, Long> count(
            final AutoMaterialized<K, Long, SessionStore<Bytes, byte[]>> materialized) {
        return this.count(materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public ImprovedKTable<Windowed<K>, Long> count(final Named named,
            final Materialized<K, Long, SessionStore<Bytes, byte[]>> materialized) {
        return this.context.wrap(this.wrapped.count(named, materialized));
    }

    @Override
    public ImprovedKTable<Windowed<K>, Long> count(final Named named,
            final AutoMaterialized<K, Long, SessionStore<Bytes, byte[]>> materialized) {
        return this.count(named, materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public <VR> ImprovedKTable<Windowed<K>, VR> aggregate(final Initializer<VR> initializer,
            final Aggregator<? super K, ? super V, VR> aggregator, final Merger<? super K, VR> sessionMerger) {
        return this.context.wrap(this.wrapped.aggregate(initializer, aggregator, sessionMerger));
    }

    @Override
    public <VR> ImprovedKTable<Windowed<K>, VR> aggregate(final Initializer<VR> initializer,
            final Aggregator<? super K, ? super V, VR> aggregator, final Merger<? super K, VR> sessionMerger,
            final Named named) {
        return this.context.wrap(this.wrapped.aggregate(initializer, aggregator, sessionMerger, named));
    }

    @Override
    public <VR> ImprovedKTable<Windowed<K>, VR> aggregate(final Initializer<VR> initializer,
            final Aggregator<? super K, ? super V, VR> aggregator, final Merger<? super K, VR> sessionMerger,
            final Materialized<K, VR, SessionStore<Bytes, byte[]>> materialized) {
        return this.context.wrap(this.wrapped.aggregate(initializer, aggregator, sessionMerger, materialized));
    }

    @Override
    public <VR> ImprovedKTable<Windowed<K>, VR> aggregate(final Initializer<VR> initializer,
            final Aggregator<? super K, ? super V, VR> aggregator, final Merger<? super K, VR> sessionMerger,
            final AutoMaterialized<K, VR, SessionStore<Bytes, byte[]>> materialized) {
        return this.aggregate(initializer, aggregator, sessionMerger,
                materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public <VR> ImprovedKTable<Windowed<K>, VR> aggregate(final Initializer<VR> initializer,
            final Aggregator<? super K, ? super V, VR> aggregator, final Merger<? super K, VR> sessionMerger,
            final Named named,
            final Materialized<K, VR, SessionStore<Bytes, byte[]>> materialized) {
        return this.context.wrap(this.wrapped.aggregate(initializer, aggregator, sessionMerger, materialized));
    }

    @Override
    public <VR> ImprovedKTable<Windowed<K>, VR> aggregate(final Initializer<VR> initializer,
            final Aggregator<? super K, ? super V, VR> aggregator, final Merger<? super K, VR> sessionMerger,
            final Named named,
            final AutoMaterialized<K, VR, SessionStore<Bytes, byte[]>> materialized) {
        return this.aggregate(initializer, aggregator, sessionMerger, named,
                materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public ImprovedKTable<Windowed<K>, V> reduce(final Reducer<V> reducer) {
        return this.context.wrap(this.wrapped.reduce(reducer));
    }

    @Override
    public ImprovedKTable<Windowed<K>, V> reduce(final Reducer<V> reducer, final Named named) {
        return this.context.wrap(this.wrapped.reduce(reducer, named));
    }

    @Override
    public ImprovedKTable<Windowed<K>, V> reduce(final Reducer<V> reducer,
            final Materialized<K, V, SessionStore<Bytes, byte[]>> materialized) {
        return this.context.wrap(this.wrapped.reduce(reducer, materialized));
    }

    @Override
    public ImprovedKTable<Windowed<K>, V> reduce(final Reducer<V> reducer,
            final AutoMaterialized<K, V, SessionStore<Bytes, byte[]>> materialized) {
        return this.reduce(reducer, materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public ImprovedKTable<Windowed<K>, V> reduce(final Reducer<V> reducer, final Named named,
            final Materialized<K, V, SessionStore<Bytes, byte[]>> materialized) {
        return this.context.wrap(this.wrapped.reduce(reducer, named, materialized));
    }

    @Override
    public ImprovedKTable<Windowed<K>, V> reduce(final Reducer<V> reducer, final Named named,
            final AutoMaterialized<K, V, SessionStore<Bytes, byte[]>> materialized) {
        return this.reduce(reducer, named, materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public ImprovedSessionWindowedKStream<K, V> emitStrategy(final EmitStrategy emitStrategy) {
        return this.context.wrap(this.wrapped.emitStrategy(emitStrategy));
    }
}
