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

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.CogroupedKStream;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.state.KeyValueStore;

@RequiredArgsConstructor
class CogroupedStreamXImpl<K, V> implements CogroupedKStreamX<K, V> {

    private final @NonNull CogroupedKStream<K, V> wrapped;
    private final @NonNull StreamsContext context;

    @Override
    public <VIn> CogroupedKStreamX<K, V> cogroup(final KGroupedStream<K, VIn> groupedStream,
            final Aggregator<? super K, ? super VIn, V> aggregator) {
        final KGroupedStream<K, VIn> other = StreamsContext.maybeUnwrap(groupedStream);
        return this.context.wrap(this.wrapped.cogroup(other, aggregator));
    }

    @Override
    public KTableX<K, V> aggregate(final Initializer<V> initializer) {
        return this.context.wrap(this.wrapped.aggregate(initializer));
    }

    @Override
    public KTableX<K, V> aggregate(final Initializer<V> initializer, final Named named) {
        return this.context.wrap(this.wrapped.aggregate(initializer, named));
    }

    @Override
    public KTableX<K, V> aggregate(final Initializer<V> initializer,
            final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.context.wrap(this.wrapped.aggregate(initializer, materialized));
    }

    @Override
    public KTableX<K, V> aggregate(final Initializer<V> initializer,
            final MaterializedX<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.aggregate(initializer, materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public KTableX<K, V> aggregate(final Initializer<V> initializer, final Named named,
            final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.context.wrap(this.wrapped.aggregate(initializer, named, materialized));
    }

    @Override
    public KTableX<K, V> aggregate(final Initializer<V> initializer, final Named named,
            final MaterializedX<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.aggregate(initializer, named, materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public <W extends Window> TimeWindowedCogroupedKStreamX<K, V> windowedBy(final Windows<W> windows) {
        return this.context.wrap(this.wrapped.windowedBy(windows));
    }

    @Override
    public TimeWindowedCogroupedKStreamX<K, V> windowedBy(final SlidingWindows windows) {
        return this.context.wrap(this.wrapped.windowedBy(windows));
    }

    @Override
    public SessionWindowedCogroupedKStreamX<K, V> windowedBy(final SessionWindows windows) {
        return this.context.wrap(this.wrapped.windowedBy(windows));
    }
}
