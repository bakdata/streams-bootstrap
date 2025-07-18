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
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.SessionWindowedCogroupedKStream;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.SessionStore;

/**
 * Extends the {@link SessionWindowedCogroupedKStream} interface by adding methods to simplify Serde configuration,
 * error handling, and topic access
 * @param <K> type of keys
 * @param <VOut> type of values
 */
public interface SessionWindowedCogroupedKStreamX<K, VOut> extends SessionWindowedCogroupedKStream<K, VOut> {

    @Override
    KTableX<Windowed<K>, VOut> aggregate(Initializer<VOut> initializer, Merger<? super K, VOut> sessionMerger);

    @Override
    KTableX<Windowed<K>, VOut> aggregate(Initializer<VOut> initializer, Merger<? super K, VOut> sessionMerger,
            Named named);

    @Override
    KTableX<Windowed<K>, VOut> aggregate(Initializer<VOut> initializer, Merger<? super K, VOut> sessionMerger,
            Materialized<K, VOut, SessionStore<Bytes, byte[]>> materialized);

    /**
     * @see #aggregate(Initializer, Merger, Materialized)
     */
    KTableX<Windowed<K>, VOut> aggregate(Initializer<VOut> initializer, Merger<? super K, VOut> sessionMerger,
            MaterializedX<K, VOut, SessionStore<Bytes, byte[]>> materialized);

    @Override
    KTableX<Windowed<K>, VOut> aggregate(Initializer<VOut> initializer, Merger<? super K, VOut> sessionMerger,
            Named named, Materialized<K, VOut, SessionStore<Bytes, byte[]>> materialized);

    /**
     * @see #aggregate(Initializer, Merger, Named, Materialized)
     */
    KTableX<Windowed<K>, VOut> aggregate(Initializer<VOut> initializer, Merger<? super K, VOut> sessionMerger,
            Named named, MaterializedX<K, VOut, SessionStore<Bytes, byte[]>> materialized);
}
