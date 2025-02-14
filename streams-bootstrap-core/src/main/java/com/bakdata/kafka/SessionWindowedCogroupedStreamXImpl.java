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
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.SessionWindowedCogroupedKStream;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.SessionStore;

@RequiredArgsConstructor
class SessionWindowedCogroupedStreamXImpl<K, V> implements SessionWindowedCogroupedKStreamX<K, V> {

    private final @NonNull SessionWindowedCogroupedKStream<K, V> wrapped;
    private final @NonNull StreamsContext context;

    @Override
    public KTableX<Windowed<K>, V> aggregate(final Initializer<V> initializer,
            final Merger<? super K, V> sessionMerger) {
        return this.context.wrap(this.wrapped.aggregate(initializer, sessionMerger));
    }

    @Override
    public KTableX<Windowed<K>, V> aggregate(final Initializer<V> initializer,
            final Merger<? super K, V> sessionMerger,
            final Named named) {
        return this.context.wrap(this.wrapped.aggregate(initializer, sessionMerger, named));
    }

    @Override
    public KTableX<Windowed<K>, V> aggregate(final Initializer<V> initializer,
            final Merger<? super K, V> sessionMerger,
            final Materialized<K, V, SessionStore<Bytes, byte[]>> materialized) {
        return this.context.wrap(this.wrapped.aggregate(initializer, sessionMerger, materialized));
    }

    @Override
    public KTableX<Windowed<K>, V> aggregate(final Initializer<V> initializer,
            final Merger<? super K, V> sessionMerger,
            final AutoMaterialized<K, V, SessionStore<Bytes, byte[]>> materialized) {
        return this.aggregate(initializer, sessionMerger, materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public KTableX<Windowed<K>, V> aggregate(final Initializer<V> initializer,
            final Merger<? super K, V> sessionMerger,
            final Named named, final Materialized<K, V, SessionStore<Bytes, byte[]>> materialized) {
        return this.context.wrap(this.wrapped.aggregate(initializer, sessionMerger, materialized));
    }

    @Override
    public KTableX<Windowed<K>, V> aggregate(final Initializer<V> initializer,
            final Merger<? super K, V> sessionMerger,
            final Named named, final AutoMaterialized<K, V, SessionStore<Bytes, byte[]>> materialized) {
        return this.aggregate(initializer, sessionMerger, named,
                materialized.configure(this.context.getConfigurator()));
    }
}
