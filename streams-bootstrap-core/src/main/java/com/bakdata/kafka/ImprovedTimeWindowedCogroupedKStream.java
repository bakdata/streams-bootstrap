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
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.TimeWindowedCogroupedKStream;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;

/**
 * Extends the {@code TimeWindowedCogroupedKStream} interface by adding methods to simplify Serde configuration,
 * error handling, and topic access
 */
public interface ImprovedTimeWindowedCogroupedKStream<K, VOut> extends TimeWindowedCogroupedKStream<K, VOut> {

    @Override
    ImprovedKTable<Windowed<K>, VOut> aggregate(Initializer<VOut> initializer);

    @Override
    ImprovedKTable<Windowed<K>, VOut> aggregate(Initializer<VOut> initializer, Named named);

    @Override
    ImprovedKTable<Windowed<K>, VOut> aggregate(Initializer<VOut> initializer,
            Materialized<K, VOut, WindowStore<Bytes, byte[]>> materialized);

    /**
     * Aggregate the values of records in this stream by the grouped key and defined windows
     * @param initializer an {@code Initializer} that computes an initial intermediate aggregation result
     * @param materialized a {@code ConfiguredMaterialized} config used to materialize a state store
     * @return a windowed {@code KTable} that contains "update" records with unmodified keys, and values that
     * represent the
     * latest (rolling) aggregate for each key within a window
     * @see #aggregate(Initializer, Materialized)
     */
    ImprovedKTable<Windowed<K>, VOut> aggregate(Initializer<VOut> initializer,
            ConfiguredMaterialized<K, VOut, WindowStore<Bytes, byte[]>> materialized);

    @Override
    ImprovedKTable<Windowed<K>, VOut> aggregate(Initializer<VOut> initializer, Named named,
            Materialized<K, VOut, WindowStore<Bytes, byte[]>> materialized);

    /**
     * Aggregate the values of records in this stream by the grouped key and defined windows
     * @param initializer an {@code Initializer} that computes an initial intermediate aggregation result
     * @param named a {@code Named} config used to name the processor in the topology
     * @param materialized a {@code ConfiguredMaterialized} config used to materialize a state store
     * @return a windowed {@code KTable} that contains "update" records with unmodified keys, and values that
     * represent the
     * latest (rolling) aggregate for each key within a window
     * @see #aggregate(Initializer, Named, Materialized)
     */
    ImprovedKTable<Windowed<K>, VOut> aggregate(Initializer<VOut> initializer, Named named,
            ConfiguredMaterialized<K, VOut, WindowStore<Bytes, byte[]>> materialized);
}
