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
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.TimeWindowedCogroupedKStream;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;

@RequiredArgsConstructor
class TimeWindowedCogroupedStreamXImpl<K, V> implements TimeWindowedCogroupedKStreamX<K, V> {

    private final @NonNull TimeWindowedCogroupedKStream<K, V> wrapped;
    private final @NonNull StreamsContext context;

    @Override
    public KTableX<Windowed<K>, V> aggregate(final Initializer<V> initializer) {
        return this.context.wrap(this.wrapped.aggregate(initializer));
    }

    @Override
    public KTableX<Windowed<K>, V> aggregate(final Initializer<V> initializer, final Named named) {
        return this.context.wrap(this.wrapped.aggregate(initializer, named));
    }

    @Override
    public KTableX<Windowed<K>, V> aggregate(final Initializer<V> initializer,
            final Materialized<K, V, WindowStore<Bytes, byte[]>> materialized) {
        return this.context.wrap(this.wrapped.aggregate(initializer, materialized));
    }

    @Override
    public KTableX<Windowed<K>, V> aggregate(final Initializer<V> initializer,
            final MaterializedX<K, V, WindowStore<Bytes, byte[]>> materialized) {
        return this.aggregate(initializer, materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public KTableX<Windowed<K>, V> aggregate(final Initializer<V> initializer, final Named named,
            final Materialized<K, V, WindowStore<Bytes, byte[]>> materialized) {
        return this.context.wrap(this.wrapped.aggregate(initializer, named, materialized));
    }

    @Override
    public KTableX<Windowed<K>, V> aggregate(final Initializer<V> initializer, final Named named,
            final MaterializedX<K, V, WindowStore<Bytes, byte[]>> materialized) {
        return this.aggregate(initializer, named, materialized.configure(this.context.getConfigurator()));
    }
}
