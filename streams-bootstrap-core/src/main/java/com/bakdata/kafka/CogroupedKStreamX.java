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

/**
 * Extends the {@code CogroupedKStream} interface by adding methods to simplify Serde configuration, error handling,
 * and topic access
 * @param <K> type of keys
 * @param <VOut> type of values
 */
public interface CogroupedKStreamX<K, VOut> extends CogroupedKStream<K, VOut> {

    @Override
    <VIn> CogroupedKStreamX<K, VOut> cogroup(KGroupedStream<K, VIn> groupedStream,
            Aggregator<? super K, ? super VIn, VOut> aggregator);

    @Override
    KTableX<K, VOut> aggregate(Initializer<VOut> initializer);

    @Override
    KTableX<K, VOut> aggregate(Initializer<VOut> initializer, Named named);

    @Override
    KTableX<K, VOut> aggregate(Initializer<VOut> initializer,
            Materialized<K, VOut, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #aggregate(Initializer, Materialized)
     */
    KTableX<K, VOut> aggregate(Initializer<VOut> initializer,
            AutoMaterialized<K, VOut, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    KTableX<K, VOut> aggregate(Initializer<VOut> initializer, Named named,
            Materialized<K, VOut, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #aggregate(Initializer, Named, Materialized)
     */
    KTableX<K, VOut> aggregate(Initializer<VOut> initializer, Named named,
            AutoMaterialized<K, VOut, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    <W extends Window> TimeWindowedCogroupedKStreamX<K, VOut> windowedBy(Windows<W> windows);

    @Override
    TimeWindowedCogroupedKStreamX<K, VOut> windowedBy(SlidingWindows windows);

    @Override
    SessionWindowedCogroupedKStreamX<K, VOut> windowedBy(SessionWindows windows);
}
